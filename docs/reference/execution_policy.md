# Execution Policy

- **Purpose:** Describe how the execute stage turns ranked signals into orders, which risk gates run, and where the paper / live boundary sits.
- **Audience:** Operator, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/domains/execution/`, `src/ai_trading_system/domains/risk/`, `config/risk_profiles/`.

> **Disclaimer — live trading is NOT verified.** Paper trading is the only execution path that has been smoke-tested end-to-end. The live Dhan adapter is disabled at the adapter level: `src/ai_trading_system/domains/execution/adapters/dhan.py:63-65` raises `RuntimeError("Live Dhan execution is intentionally disabled...")` unless the adapter is constructed with `dry_run=True`. Production guardrails for live execution (margin checks, kill-switch, broker error handling, sandbox parity) have not been audited. **Do not enable live execution from these docs.**

---

## 1. Action generation

Entry-point: `AutoTrader.run(...)` in `src/ai_trading_system/domains/execution/autotrader.py:28`.

Per cycle, the autotrader:

1. Loads `positions_before` from `PortfolioManager.open_positions()` (`portfolio.py:80`), which derives current long positions by replaying the fills ledger.
2. Computes the heat gate via `PortfolioManager.check_heat_gate(...)` (`portfolio.py:129`) using either the actual ACTIVE stop or a fallback 10% risk estimate per position.
3. For each open position, calls `service.check_stop_triggered(...)`. If a stop fires, a forced `SELL` `TradeAction` with `reason="stop_triggered:..."` is prepended (`autotrader.py:83-95`).
4. Calls `build_trade_actions(...)` (`policies.py:50`). Behavior splits in two:
   - **With `risk_config`** (operator passed `RISK_PROFILE`): the shared `TradingRuleEngine` produces exits then entries. Engine entry point: `rule_engine.py::TradingRuleEngine.generate_order_intents` (`rule_engine.py:61`).
   - **Without `risk_config`** (legacy default): top-N diff vs. current positions, optionally constrained by the ML-overlay probability `ml_<horizon>d_prob >= ml_confirm_threshold` (`policies.py:165-214`). Supported `strategy_mode` values: `technical`, `ml`, `hybrid_confirm`, `hybrid_overlay` (`policies.py:24-29`).
5. Routes each action to the execution service. `BUY` orders go through `service.execute_signal(...)`; `SELL` orders go through `service.submit_order(OrderIntent(..., side="SELL"))` (`autotrader.py:261-298`).

### Exit priority (engine mode)

`evaluate_exit` in `src/ai_trading_system/domains/risk/exit_policy.py:28` evaluates exits in this priority order (lowest number wins):

| Priority | Reason | Trigger |
|---|---|---|
| 0 | `hard_stop` | Intrabar `market.low` (or close, if low missing) breaches `position.stop_price` |
| 1 | `close_below_200dma` | Close below `sma_200 * (1 - dma_whipsaw_buffer_pct/100)`; enabled by `exit.emergency_exit_below_sma200` |
| 2 | `close_below_<window>dma` | Close below the configured `exit.dma_exit_window` (11 / 20 / 50) |
| 3 | `rank_deterioration_streak` | `rank_above_threshold_streak >= exit.rank_deterioration_bars` |
| 4 | `score_deterioration_streak` | `score_below_threshold_streak >= exit.score_deterioration_bars` |
| 5 | `time_stop` | `bars_held >= exit.time_stop_days` |

Streak counters are stored in the active stop record's `metadata_json` and bumped each bar by `_bump_streaks_in_stop_record` (`autotrader.py:333-382`).

---

## 2. Risk gates

### Engine entry gates

`src/ai_trading_system/domains/risk/entry_policy.py:18` — every gate appends a reason; non-empty reason list → `should_enter=False`. Gates (all configurable via the YAML profile):

- `require_stage_2` (default True)
- `require_price_above_sma200` / `_sma50` / `_ema20`
- `require_sma50_above_sma200_or_rising_20d`
- `require_sector_positive` (candidate's sector strength > 0)
- `min_volume_ratio` (default 1.5)
- `require_delivery_above_sector_median`
- `min_close_to_52w_high`, `min_return_20_pct`, `min_return_50_pct`
- `max_drawdown_from_recent_high_pct`, `max_below_ema20_days_20`
- `portfolio.open_positions_count >= constraints.max_concurrent_positions` → `portfolio_full`
- `portfolio.holds(symbol)` → `already_held`

After all gates pass, the initial stop is computed via `calculate_initial_stop` (`stop_policy.py:13`); if the chosen stop method needs data the snapshot lacks, the entry is rejected with `stop_unavailable:<method>` (`entry_policy.py:103-106`).

### Heat gate (portfolio-level, applies in both engine and legacy modes)

`PortfolioManager.check_heat_gate` (`portfolio.py:129`) sums per-position risk (using each position's ACTIVE stop, else `avg_entry_price * 10%`) and rejects any new `BUY` when `total_risk / capital > heat_gate_threshold` (default `0.15`, autotrader.py:52). Rejected BUYs are recorded with `result.status="REJECTED"`, `reason="heat_gate_exceeded"` (`autotrader.py:222-234`).

### Portfolio constraints (legacy autotrader path, opt-in)

`use_portfolio_constraints=True` triggers `check_portfolio_constraints` (`portfolio.py:28`) with `max_positions`, `max_sector_exposure`, `max_single_stock_weight`. Engine-mode constraints live in `portfolio_constraints.py::check_constraints` and use the YAML `constraints` block.

### Engine post-sizing constraints

`src/ai_trading_system/domains/risk/portfolio_constraints.py:13` re-checks after sizing:

- `max_concurrent_positions`
- per-stock weight ≤ `max_stock_weight_pct`
- per-sector exposure ≤ `max_sector_exposure_pct`

---

## 3. Stops and trailing stops

### Initial stop (engine, `stop_policy.py:13`)

| Method | Formula |
|---|---|
| `atr` | `close - atr_14 * atr_multiple` |
| `percent` | `close * (1 - stop_pct)` |
| `swing_low` | `swing_low_20` |
| `breakout_candle_low` | `breakout_candle_low` |
| `hybrid` | `max(swing_low_20, close - atr_14 * hybrid_atr_multiple)` |

### Trailing stops

**Not implemented as a separate trailing-stop primitive.** The engine relies on the DMA exit (`close_below_<window>dma`) as a coarse trailing mechanism — as the symbol climbs, its DMA climbs with it and the exit threshold ratchets up. There is no per-position "raise stop after X% gain" code path in `src/ai_trading_system/domains/risk/` as of 2026-05-16. The persisted `execution_position_stop` row is set once at entry and updated only for streak metadata; the `stop_price` itself is never moved by the engine.

### Stop persistence

Active stops live in DuckDB table `execution_position_stop` (in `data/control_plane.duckdb`). The autotrader marks them `INACTIVE` after a confirmed engine-driven or `stop_triggered:`-prefixed SELL (`autotrader.py:299-307`).

---

## 4. Position sizing

### Engine sizing (`src/ai_trading_system/domains/risk/sizing_policy.py:13`)

```
equity = portfolio.equity
max_position_value = equity * constraints.max_stock_weight_pct / 100

if method == "equal_weight":
    slot_value = equity / constraints.max_concurrent_positions
    target_value = min(slot_value, max_position_value)
    shares = int(target_value // market.close)

if method == "atr_risk":
    stop_distance = market.close - entry.initial_stop  # must be > 0
    risk_budget = equity * sizing.risk_per_trade_pct / 100
    risk_shares = int(risk_budget // stop_distance)
    cap_shares  = int(max_position_value // market.close)
    shares = min(risk_shares, cap_shares)
```

### Legacy autotrader sizing

`compute_atr_position_size` (`policies.py:32`):

```
qty = int((capital * risk_per_trade) / (atr * atr_multiple))
```

If `use_atr_position_sizing=False`, sizing falls back to `service.risk_manager.compute_position_size(...)` (`autotrader.py:481-487`).

---

## 5. Paper vs. live boundary

| Adapter | Module | Behavior |
|---|---|---|
| Paper (default) | `domains/execution/adapters/paper.py::PaperExecutionAdapter` | Deterministic fill simulator. MARKET orders always fill; LIMIT orders fill on touch. Applies `slippage_bps` (default 5.0 bps, `paper.py:67`) and computes NSE transaction costs (brokerage, GST, STT, exchange/SEBI/stamp fees) in `NSETransactionCost.calculate` (`paper.py:23-59`). |
| Dhan (scaffold) | `domains/execution/adapters/dhan.py::DhanExecutionAdapter` | `dry_run=True` returns an `OrderRecord` with `status="SIMULATED"` and metadata `{"dry_run": True, "note": "Live Dhan execution is intentionally disabled..."}`. `dry_run=False` raises `RuntimeError` (`dhan.py:63-65`). |

Adapter selection is performed inside `ExecutionService`; default is paper. Live Dhan requires the full credential set (`DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, `DHAN_API_KEY`) AND explicit removal of the dry-run guard in the adapter. No env-var override exists to bypass the guard.

---

## 6. Order-type fixity

`OrderIntent` (`src/ai_trading_system/domains/execution/models.py:15-30`) defaults:

```
order_type   = "MARKET"
product_type = "INTRADAY"
validity     = "DAY"
exchange     = "NSE"
```

These defaults are the only values exercised in current call sites — `AutoTrader` constructs `OrderIntent` without overriding `order_type` or `product_type` (`autotrader.py:287-296`). LIMIT/STOP order routing exists in the paper adapter's fill logic (`paper.py:153-170`) but is not reachable from the current action-generation code.

**Practical consequence:** All paper fills are intraday market orders at simulated close ± slippage. Multi-day positions persist in the ledger but the order tag remains `INTRADAY`; this does not match real broker semantics and would need fixing before live use.

---

## 7. Risk profile YAML

Profiles live in `config/risk_profiles/<name>.yaml`. Loader: `RiskPolicyConfig.from_dict` (`src/ai_trading_system/domains/risk/config.py:80-95`), called by `load_profile` (`config.py:117-128`). Selection is via `RISK_PROFILE` env var or pipeline context param `risk_profile`; unknown name falls back to `balanced_swing`.

Shipped profiles: `aggressive_momentum.yaml`, `balanced_swing.yaml`, `permissive_test.yaml`, `positional_trend.yaml`, `stage1_watchlist.yaml`.

### Structure (from `balanced_swing.yaml`, verified)

```yaml
name: balanced_swing

entry:
  require_stage_2: true                          # EntryConfig defaults in config.py:17
  require_price_above_sma200: true
  require_sector_positive: true
  min_volume_ratio: 1.5
  require_delivery_above_sector_median: false
  # Optional fields: require_price_above_sma50, require_price_above_ema20,
  # require_sma50_above_sma200_or_rising_20d, min_close_to_52w_high,
  # min_return_20_pct, min_return_50_pct,
  # max_drawdown_from_recent_high_pct, max_below_ema20_days_20

stop:
  method: hybrid                                 # atr | percent | swing_low | breakout_candle_low | hybrid
  atr_multiple: 2.0
  stop_pct: 0.05
  hybrid_atr_multiple: 2.5

exit:
  emergency_exit_below_sma200: true
  dma_exit_window: 20                            # 11 | 20 | 50 | null
  dma_whipsaw_buffer_pct: 0.5
  exit_on_rank_deterioration: true
  max_hold_rank: 50
  rank_deterioration_bars: 3
  exit_on_score_deterioration: true
  min_hold_score: 60.0
  score_deterioration_bars: 3
  time_stop_days: 60

sizing:
  method: equal_weight                           # equal_weight | atr_risk
  risk_per_trade_pct: 1.0
  max_position_pct: 14.0

constraints:
  max_concurrent_positions: 8
  max_stock_weight_pct: 14.0
  max_sector_exposure_pct: 30.0
```

Unknown keys in each section are silently dropped by `_coerce` (`config.py:82-86`). Whole sections may be omitted; defaults from the corresponding `@dataclass(frozen=True)` apply.

---

## 8. Where execution state is written

| Artifact | Destination |
|---|---|
| `trade_actions.csv`, `executed_orders.csv`, `fills.csv` | `data/pipeline_runs/<run_id>/execute/attempt_<n>/` |
| `execution_order`, `execution_fill`, `execution_position_stop` tables | `data/control_plane.duckdb` (no separate `execution.duckdb` — older runbook references to that file are stale; see `docs/_audit/current_code_truth_map.md:185`) |

---

## 9. Disabling execution

- Operator-side: pass `execution_enabled=False` to `AutoTrader.run` (`autotrader.py:155-162`) — actions are still generated for inspection but no orders are placed; status returns `"disabled"`.
- Preview-only: `preview_only=True` short-circuits each action into a `PREVIEW` payload (`autotrader.py:500-516`).
- Engine off: unset `RISK_PROFILE` and remove `context.params["risk_profile"]` to fall back to the legacy `build_trade_actions` diff path.
