# Stage: execute

- **Purpose:** Convert ranked signals into paper (or live-scaffold) orders, persist fills, and update portfolio state.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-07-14
- **Source of truth:** [`src/ai_trading_system/pipeline/stages/execute.py`](../../src/ai_trading_system/pipeline/stages/execute.py), [`src/ai_trading_system/domains/execution/`](../../src/ai_trading_system/domains/execution/), [`src/ai_trading_system/domains/risk/`](../../src/ai_trading_system/domains/risk/)

---

## Live-trading disclaimer

**Paper trading is the only verified execution path.** A `DhanExecutionAdapter` scaffold exists at [`adapters/dhan.py`](../../src/ai_trading_system/domains/execution/adapters/dhan.py) but its `place_order` raises `RuntimeError` unless `dry_run=True` ([`dhan.py:62-65`](../../src/ai_trading_system/domains/execution/adapters/dhan.py)). The stage wrapper hardcodes `PaperExecutionAdapter` ([`execute.py:187`](../../src/ai_trading_system/pipeline/stages/execute.py)) — there is no live adapter wired in. Production guardrails for live trading (kill-switch, position caps enforced against broker state, broker reconciliation) have **not been verified**. Do not enable live execution without a separate hardening pass.

## Purpose

Read rank/candidate outputs, apply risk + portfolio + regime gates, size positions, place paper orders through the local `PaperExecutionAdapter`, persist orders/fills to DuckDB, and update trailing stops. Emits trade-action and order/fill CSVs plus a portfolio-drawdown snapshot.

## Entrypoints

- Stage wrapper: [`src/ai_trading_system/pipeline/stages/execute.py::ExecuteStage`](../../src/ai_trading_system/pipeline/stages/execute.py)
- Orchestrator order: runs after `events`, before `insight` — `PIPELINE_ORDER` in [`pipeline/orchestrator.py:41`](../../src/ai_trading_system/pipeline/orchestrator.py)
- Invoked by CLI: `ai-trading-pipeline` (full) or `ai-trading-daily` (legacy 5-stage wrapper)

## Input data

- **Required artifact:** `rank.ranked_signals` ([`execute.py:160`](../../src/ai_trading_system/pipeline/stages/execute.py))
- **Other rank outputs consumed via `ExecutionCandidateBuilder`:** breakout/pattern scans, ML overlay (when `--ml-mode` set), sector dashboard ([`candidate_builder.py`](../../src/ai_trading_system/domains/execution/candidate_builder.py))
- **Risk profile:** `context.params["risk_profile"]` or `RISK_PROFILE` env var → loaded by [`risk/config.py::load_profile`](../../src/ai_trading_system/domains/risk/config.py); falls back to legacy `build_trade_actions` when unset ([`execute.py:47-60`](../../src/ai_trading_system/pipeline/stages/execute.py))
- **Market regime:** `RegimeDetector.get_market_regime()` ([`execute.py:178`](../../src/ai_trading_system/pipeline/stages/execute.py))
- **Prior execution state:** orders, fills, position stops, drawdown peak via `ExecutionStore`
- **OHLCV (for MTM portfolio value):** read-only DuckDB query on `_catalog` table in `context.db_path` ([`execute.py:294-310`](../../src/ai_trading_system/pipeline/stages/execute.py))

## Output artifacts

Written under `data/pipeline_runs/<run_id>/execute/attempt_<n>/`:

| Artifact | File | Notes |
|---|---|---|
| `trade_actions` | `trade_actions.csv` | High-level actions (BUY/SELL/HOLD/SKIP) with reason |
| `executed_orders` | `executed_orders.csv` | One row per placed order |
| `executed_fills` | `executed_fills.csv` | One row per fill (paper fills synthesised by `PaperExecutionAdapter`) |
| `positions` | `positions.csv` | Open positions after the cycle |
| `execute_summary` | `execute_summary.json` | Full metadata: counts, regime, trust, drawdown, params |

Persistent state written to **`data/execution.duckdb`** (default in [`store.py`](../../src/ai_trading_system/domains/execution/store.py)) — tables: `execution_submission_intent`, `execution_order`, `execution_fill`, `execution_trade_note`, `execution_position_stop`, plus drawdown snapshots.

## Main modules

- [`domains/execution/service.py::ExecutionService`](../../src/ai_trading_system/domains/execution/service.py) — submit/refresh orders, persist stop on fill, trailing-stop maintenance
- [`domains/execution/autotrader.py::AutoTrader`](../../src/ai_trading_system/domains/execution/autotrader.py) — turn ranked rows into actions; applies entry/exit policies, portfolio constraints, heat gate
- [`domains/execution/candidate_builder.py::ExecutionCandidateBuilder`](../../src/ai_trading_system/domains/execution/candidate_builder.py) — load rank artifacts; build `ExecutionRequest` from `context.params`
- [`domains/execution/portfolio.py::PortfolioManager`](../../src/ai_trading_system/domains/execution/portfolio.py) — open positions, exposure, sector caps
- [`domains/execution/policies.py`](../../src/ai_trading_system/domains/execution/policies.py) — ATR position sizing, exit policies
- [`domains/execution/store.py::ExecutionStore`](../../src/ai_trading_system/domains/execution/store.py) — DuckDB persistence (`data/execution.duckdb`)
- [`domains/execution/adapters/paper.py::PaperExecutionAdapter`](../../src/ai_trading_system/domains/execution/adapters/paper.py) — deterministic paper fills + NSE transaction-cost model
- [`domains/execution/adapters/dhan.py::DhanExecutionAdapter`](../../src/ai_trading_system/domains/execution/adapters/dhan.py) — dry-run scaffold only (see disclaimer above)
- [`domains/risk/`](../../src/ai_trading_system/domains/risk/) — `TradingRuleEngine` shared with backtesting (entry/exit/stop/sizing, single source of truth)

## Process flow

1. Require `rank.ranked_signals` artifact.
2. Build `ExecutionRequest` from `context.params` (capital, top-N, ML horizon, breakout linkage, regime overrides, portfolio-constraint toggles, etc. — full key list in `ExecuteStage.PARAMETER_KEYS`, [`execute.py:104-130`](../../src/ai_trading_system/pipeline/stages/execute.py)).
3. Resolve risk profile (`risk_profile` param or `RISK_PROFILE` env). If set, `TradingRuleEngine` drives entries/exits/sizing/stops; else fall back to legacy `build_trade_actions`.
4. Detect market regime (`RegimeDetector`) — used for sizing multiplier.
5. Construct `ExecutionStore` → `PortfolioManager` → `ExecutionService(PaperExecutionAdapter)` → `AutoTrader`.
6. `AutoTrader.run(...)` holds the execution-ledger batch lock while producing `actions`, `executions`, and `positions_before/after`. Defaults: order_type=MARKET, product_type=INTRADAY, validity=DAY. Pipeline-generated correlation IDs are scoped to `run_id`, so a retry of the same run is stable while a later run can legitimately trade the symbol again. Before adapter dispatch, `ExecutionService` durably reserves the intent, replays a completed identical key, rejects conflicting reuse, and leaves unknown outcomes for explicit reconciliation without redispatch.
7. If `execution_enabled` and not preview: refresh trailing stops via `service.maintain_trailing_stops(...)` using current prices + ATR from ranked df.
8. Compute MTM portfolio value, record intraday drawdown snapshot (and EOD if `is_eod`).
9. Write CSVs + `execute_summary.json`.

## DQ / trust gates

- `data_trust_status` and `trust_confidence` propagated from rank/candidate stage into summary metadata.
- Stage-2 (`stage2_gate`), breakout-linkage tier counts surfaced in summary.
- Heat gate: `execution_heat_gate_threshold` (default `0.08`) — before each
  buy submission, projects existing risk plus the candidate's stop risk and
  risk reserved by earlier accepted buys in the same batch. The buy is rejected
  when projected cumulative heat exceeds the threshold; rejected orders do not
  consume a reservation.
- Competing AutoTrader batches for the same execution store are serialized by a
  store-adjacent inter-process lock, so each batch reloads positions and heat
  after the prior batch finishes.
- Risk-profile-driven gates: position count cap, sector exposure cap, single-stock weight cap (all in `ExecutionRequest`).
- `canary` mode: when `context.params["canary"]` is truthy and `canary_blocked` is set, blocks execution and records `canary_blocked: true` in metadata.

## Failure modes

- **Missing rank artifact:** `context.require_artifact("rank", "ranked_signals")` raises and aborts the stage.
- **Live Dhan request:** if a future caller passes `dry_run=False`, the adapter raises `RuntimeError("Live Dhan execution is intentionally disabled...")`.
- **Risk profile not found:** `load_profile(name)` raises if YAML missing under `config/risk_profiles/`.
- **DuckDB connect failure for MTM query:** caught and swallowed; portfolio value falls back to entry-cost approximation ([`execute.py:311-312`](../../src/ai_trading_system/pipeline/stages/execute.py)).
- **Regime detector failure:** caught; regime defaults to request value or `"TREND"`.

## Retry behavior

Each invocation writes to a fresh `attempt_<n>` directory; orchestrator retry policy is per-stage. Order submission is idempotent when the caller supplies a non-empty correlation ID: the durable intent is reserved before dispatch, identical completed retries return the first persisted order/fills, conflicting payloads are rejected, and an unknown outcome returns `RECONCILIATION_REQUIRED`. `ExecutionService.reconcile_submission()` asks a capable adapter for the original outcome and never submits a replacement. Calls without a correlation ID retain normal create-new-order behavior.

Stops are reconciled from the cumulative fill ledger after submit, refresh,
cancel, and recovered outcomes. Unfilled orders do not change stop state; partial
buys protect filled quantity; partial sells retain protection for remaining
quantity; zero net position deactivates the stop.

## Downstream consumers

- [`insight` stage](insight.md): reads `execute.positions` to derive `portfolio_symbols`.
- [`publish` stage](publish.md): portfolio handler re-pulls live state via `daily_pipeline.run_portfolio_analysis()`; not via execute artifacts directly.
- [`perf_tracker`](perf_tracker.md): does not read execute artifacts (operates on rank cohort only).
- FastAPI execution console: queries `data/execution.duckdb` directly via `ui/execution_api/routes/`.

## Commands

```bash
# Full pipeline (runs all 11 stages including execute)
ai-trading-pipeline

# Legacy 5-stage daily (ingest -> features -> rank -> execute -> publish)
ai-trading-daily

# Engine-driven paper trading with a named profile
RISK_PROFILE=balanced_swing ai-trading-pipeline

# Inspect persisted fills
duckdb data/execution.duckdb -c "SELECT symbol_id, side, quantity, price, filled_at FROM execution_fill ORDER BY filled_at DESC LIMIT 20;"
```

See [`../risk_engine_runbook.md`](../_legacy/archived_2026-05-16/risk_engine_runbook.md) for the full risk-profile operator guide (profile authoring, smoke checks, troubleshooting).
