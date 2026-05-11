# Shared Trading Rule Engine — Operator Runbook

The `TradingRuleEngine` is the single source of truth for entry / exit / stop /
sizing decisions across **paper trading** and **research backtesting**. This
runbook covers production smoke verification and day-to-day ops.

## 1. What's wired up

| Surface | Activation | Notes |
|---|---|---|
| Paper trading | `RISK_PROFILE=<name>` env var, or `context.params["risk_profile"]` in pipeline | Falls back to legacy `build_trade_actions` logic when unset |
| Backtest CLI | `python -m ai_trading_system.research.backtesting --risk-profile <name>` | Walks `data/pipeline_runs/*` |
| Backtest API | `GET /api/execution/backtest/profiles`, `POST /api/execution/backtest/run` | Header: `x-api-key` |
| Dashboard | Sidebar → **Backtest** (`/backtest`) | Profile inspector + runner + trades table |

Profiles live as YAML under `config/risk_profiles/`:
- `aggressive_momentum.yaml` — tight ATR stop (×1.5), 11-DMA exit, 12 positions
- `balanced_swing.yaml`     — hybrid stop, 20-DMA exit, 8 positions
- `positional_trend.yaml`   — swing-low stop, 50-DMA exit + 200-DMA emergency, 6 positions

## 2. Production smoke (do this once on first deploy)

### 2a. Backtest CLI

```bash
# 1. Run a one-off backtest against last 90 days of historical pipeline runs.
python -m ai_trading_system.research.backtesting \
    --risk-profile balanced_swing \
    --pipeline-runs-dir data/pipeline_runs \
    --from $(date -v-90d +%F) \
    --equity 1000000 \
    --out data/research/engine_backtests
```

Acceptance:
- Exits with status 0.
- Prints a `summary` JSON with `trade_count >= 1` and a non-empty
  `exit_reason_counts` dict.
- Writes `trades.csv`, `equity_curve.csv`, `summary.json` under
  `data/research/engine_backtests/balanced_swing/<timestamp>/`.
- `trades.csv` has populated `entry_reason`, `exit_reason`, `stop_price`,
  `stop_method`, `rank_at_entry`, `rank_at_exit`, `score_at_entry`,
  `score_at_exit` columns.

### 2b. Paper trading

```bash
# 2. Run a daily pipeline with the engine enabled.
RISK_PROFILE=balanced_swing python -m ai_trading_system.pipeline.daily_runner
```

In the `execute` stage logs:
```
execute stage: using risk_profile=balanced_swing
```

After the run, inspect `data/execution.duckdb`:

```bash
duckdb data/execution.duckdb -c "
  SELECT
    symbol_id, side,
    json_extract_string(metadata_json, '$.reason')      AS reason,
    json_extract_string(metadata_json, '$.intent_kind') AS intent_kind,
    json_extract_string(metadata_json, '$.stop_method') AS stop_method
  FROM execution_fill
  ORDER BY filled_at DESC
  LIMIT 20;
"
```

Acceptance:
- BUYs have `reason = 'entry_confirmed'`, `intent_kind = 'entry'`, and a
  non-null `stop_method` (one of `atr` / `hybrid` / `swing_low` / `percent` /
  `breakout_candle_low`).
- SELLs have `reason` in `{hard_stop, close_below_200dma, close_below_20dma,
  close_below_50dma, close_below_11dma, rank_deterioration_streak,
  score_deterioration_streak, time_stop}` and `intent_kind = 'exit'`.
- The corresponding `execution_position_stop` row's `status = 'INACTIVE'`
  for the closed symbol.

Streak counters live on the active stop record:

```bash
duckdb data/execution.duckdb -c "
  SELECT
    symbol_id,
    json_extract_string(metadata_json, '$.rank_above_threshold_streak') AS rank_streak,
    json_extract_string(metadata_json, '$.score_below_threshold_streak') AS score_streak,
    json_extract_string(metadata_json, '$.bars_held') AS bars_held
  FROM execution_position_stop
  WHERE status = 'ACTIVE';
"
```

### 2c. API + UI

1. Start the API: `uvicorn ai_trading_system.ui.execution_api.app:app`.
2. Confirm profiles endpoint:
   ```bash
   curl -H "x-api-key: $EXECUTION_API_KEY" \
        http://localhost:8090/api/execution/backtest/profiles | jq '.profiles | length'
   ```
   Should print `3` (or more if you've added custom profiles).
3. Start the dashboard: `cd web/execution-console-v2/ai-trading-dashboard-starter && npm run dev`.
4. Navigate to **Backtest** in the sidebar.
5. Pick a profile, set a date window, click **Run backtest**.
6. Confirm the results table shows entry/exit reasons as coloured badges.

## 3. Authoring a new risk profile

1. Create `config/risk_profiles/<name>.yaml` modelled on the three starters.
2. The loader picks it up immediately — restart isn't required for the CLI or
   API (each request reads from disk).
3. Validate via the CLI before exposing it to live paper trading:
   ```bash
   python -m ai_trading_system.research.backtesting --risk-profile <name> --strict-profile
   ```

## 4. Disabling the engine

Unset `RISK_PROFILE` (and remove `context.params["risk_profile"]` from any
pipeline configs). The paper-trade execute stage will fall back to the legacy
`build_trade_actions` path unchanged.

## 5. Where to look when things go wrong

| Symptom | Likely cause | Where to check |
|---|---|---|
| No fills, no errors | Stage 2 + volume gates rejecting everything | `summary["exit_reason_counts"]` in backtest output, or grep `execute` stage logs for engine reasons |
| `stop_unavailable:swing_low_20` rejections | Feature panel hasn't backfilled the new column | Re-run features stage; verify ranked_signals.csv contains `swing_low_20` |
| Engine SELL but stop record stays ACTIVE | Position was immediately re-entered same bar | Expected — check that a fresh BUY fill exists for the symbol on the same date |
| UI shows "No profiles found" | API can't read `config/risk_profiles/` | Confirm `AI_TRADING_PROJECT_ROOT` is set or the API process is running from the repo root |

## 6. Module map

- Pure engine: `src/ai_trading_system/domains/risk/`
- Profiles: `config/risk_profiles/*.yaml`
- Paper integration: `src/ai_trading_system/domains/execution/{policies,autotrader,service,adapters/paper}.py`
- Backtest: `src/ai_trading_system/research/backtesting/{engine_runner,pipeline_loader,cli}.py`
- API: `src/ai_trading_system/ui/execution_api/routes/backtest.py`
- UI: `web/execution-console-v2/ai-trading-dashboard-starter/src/pages/BacktestPage.tsx`
- Tests: `tests/domains/risk/`, `tests/execute/test_engine_persistence.py`,
  `tests/execute/test_paper_uses_risk_engine.py`,
  `tests/research/backtesting/`,
  `tests/integration/test_backtest_paper_parity.py`,
  `tests/test_execution_api_backtest.py`
