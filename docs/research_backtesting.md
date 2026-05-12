# Research Backtesting

This document describes the current research/backtest implementation, how to use it, and what remains.

## What Exists Now

There are now two backtest data sources.

### 1. Pipeline Replay

Pipeline replay reads saved ranked outputs:

```text
data/pipeline_runs/*/rank/attempt_*/ranked_signals.csv
```

Use this when you want to answer:

```text
What would the strategy have done using the actual ranked output produced by the daily pipeline?
```

Main module:

```text
src/ai_trading_system/research/backtesting/pipeline_loader.py
```

Limitation: old `ranked_signals.csv` files may not contain newer risk-engine columns such as `sma_200`, `atr_14`, `volume_ratio_20`, or `swing_low_20`.

### 2. Research Dynamic

Research dynamic reads the research DuckDB directly:

```text
data/research/research_ohlcv.duckdb
```

It computes the strategy input data on the fly:

```text
sma_11
sma_20
sma_50
sma_200
atr_14
volume_ratio_20
swing_low_20
composite_score
eligible_rank
```

Use this when you want to answer:

```text
What would this rule/profile do on historical market data if indicators and ranks are calculated dynamically?
```

Main module:

```text
src/ai_trading_system/research/backtesting/research_loader.py
```

## Shared Rule Engine

Both backtest sources feed the same runner:

```text
src/ai_trading_system/research/backtesting/engine_runner.py
```

That runner sends per-day ranked frames into:

```text
src/ai_trading_system/domains/risk/rule_engine.py
```

So paper trading and engine backtesting share the same core entry, exit, stop, and sizing rules.

The runner records:

```text
entry_reason
exit_reason
stop_price
stop_method
rank_at_entry
rank_at_exit
score_at_entry
score_at_exit
bars_held
pnl
pnl_pct
equity_curve
```

## Operational To Research Sync

Operational data is the cleaned production truth:

```text
data/ohlcv.duckdb
```

Research data is the sandbox:

```text
data/research/research_ohlcv.duckdb
```

Sync module:

```text
src/ai_trading_system/research/sync_operational_data.py
```

Dry run:

```bash
.venv/bin/python -m ai_trading_system.research.sync_operational_data
```

Apply:

```bash
.venv/bin/python -m ai_trading_system.research.sync_operational_data --apply
```

The sync is automatic when Backtest Lab runs with:

```text
Data source = Research dynamic
```

Sync behavior:

```text
1. Detect operational DB date range automatically.
2. Delete only research rows inside that operational date range.
3. Insert deduped operational rows for that range.
4. Preserve older research history outside the operational range.
```

This is important because research may have 10-15 years of history while operational may only have the latest 12-15 months.

Deduplication key:

```text
symbol_id + exchange + timestamp
```

If duplicate operational rows exist, the sync keeps the latest by:

```text
ingestion_ts DESC
```

The dynamic loader also dedupes per symbol/date before computing indicators.

## Backtest Lab UI

Backtest Lab is available at:

```text
http://localhost:5173/backtest
```

It supports:

```text
Profile selection
Data source selection
Date range
Starting equity
Custom strategy parameters
Results table
Exit reason summary
```

Data source options:

```text
Pipeline replay
Research dynamic
```

Custom parameters include:

```text
Entry filters:
- Stage 2 required
- Price above SMA200
- Sector positive
- Delivery above sector median
- Min volume ratio

Stops:
- Stop method
- ATR multiple
- Percent stop
- Hybrid ATR multiple

Exits:
- 200DMA emergency exit
- DMA exit window
- Whipsaw buffer
- Rank deterioration exit
- Score deterioration exit
- Time stop

Sizing and constraints:
- Sizing method
- Risk per trade
- Max position size
- Max positions
- Max stock weight
- Max sector exposure
```

Custom UI runs do not write new YAML profiles. They send `custom_config` in the backtest API request.

## API

Profiles:

```http
GET /api/execution/backtest/profiles
```

Run:

```http
POST /api/execution/backtest/run
```

Important request fields:

```json
{
  "profile": "balanced_swing",
  "data_source": "research_dynamic",
  "from_date": "2025-10-01",
  "to_date": "2025-12-29",
  "equity": 1000000,
  "persist": true,
  "custom_config": {}
}
```

For `research_dynamic`, the backend syncs operational data into research data before running.

## CLI

Pipeline replay:

```bash
.venv/bin/python -m ai_trading_system.research.backtesting \
  --risk-profile balanced_swing \
  --data-source pipeline_replay \
  --from 2026-04-01 \
  --to 2026-05-12
```

Research dynamic:

```bash
.venv/bin/python -m ai_trading_system.research.backtesting \
  --risk-profile balanced_swing \
  --data-source research_dynamic \
  --from 2025-10-01 \
  --to 2025-12-29
```

## Trade Selection And Sizing

Each trading day:

```text
1. Build candidate signals.
2. Evaluate exits first.
3. Evaluate new entries by rank.
4. Apply sizing.
5. Apply portfolio constraints.
6. Record trades and equity.
```

Sizing modes:

```text
equal_weight:
  shares = equity * max_position_pct / close

atr_risk:
  shares = equity * risk_per_trade_pct / stop_distance
```

Constraints can block entries:

```text
max_concurrent_positions
max_stock_weight_pct
max_sector_exposure_pct
```

## Recent Fixes

Implemented fixes:

```text
- Paper exits use current market close instead of stale fill price.
- Missing held-symbol market data no longer creates false hard-stop exits.
- Backtest-end trades close at latest known close instead of entry price.
- Same symbol cannot exit and re-enter on the same bar.
- Research dynamic backtest source added.
- Operational-to-research sync added.
- Sync preserves old research history outside the operational source range.
```

## Verification

Current focused verification:

```bash
.venv/bin/python -m pytest \
  tests/domains/risk \
  tests/execute/test_paper_uses_risk_engine.py \
  tests/execute/test_engine_persistence.py \
  tests/research/backtesting \
  tests/research/test_sync_operational_data.py \
  tests/integration/test_backtest_paper_parity.py \
  tests/test_execution_api_backtest.py
```

Expected:

```text
73 passed
```

Frontend:

```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm run build
```

Expected:

```text
Build passes
```

Known warning:

```text
Vite reports a large bundle warning.
FastAPI/Starlette show Python 3.14 deprecation warnings in tests.
```

## Remaining Work

### High Priority

1. Improve dynamic ranking parity.

Current `research_dynamic` uses a simple composite score based on relative returns, trend, and volume. It is good enough for research iteration, but it is not identical to the production rank pipeline.

Remaining:

```text
Reuse canonical ranking components from domains/ranking where practical.
Document any intentional difference between dynamic research ranking and production ranking.
```

2. Add sync status to the UI.

Backtest Lab should show:

```text
Last sync status
Source date range
Rows copied
Research rows in refreshed range
```

Currently this is returned by the backend but not displayed prominently.

3. Add data-quality checks for research dynamic.

Before running a dynamic backtest, validate:

```text
minimum warmup coverage
missing OHLCV count
duplicate count
symbols with insufficient SMA200 history
```

### Medium Priority

4. Persist research backtest metadata.

Persist:

```text
data_source
profile/custom_config
sync summary
indicator/ranking method version
code version or git hash
```

5. Add unique/index validation for research `_catalog`.

DuckDB does not enforce the desired uniqueness in all existing paths. Add a validation query/report for:

```text
symbol_id, exchange, timestamp
```

6. Add benchmark metrics to engine backtests.

Existing older research backtest code has benchmark comparison helpers. Bring that into engine backtests:

```text
CAGR
max drawdown
win rate
average win/loss
NIFTY comparison
alpha/beta
```

### Lower Priority

7. Add strategy profile saving.

Current custom UI settings run only as one-off `custom_config`. Later, save tuned configs as versioned research candidates.

8. Add promotion workflow.

Possible lifecycle:

```text
draft → research_candidate → paper_shadow → active → retired
```

9. Add charts.

Backtest Lab should eventually include:

```text
equity curve
drawdown curve
exit reason breakdown
sector exposure over time
trade distribution
```

## Recommended Usage

For strategy tuning:

```text
Use Research dynamic.
```

For auditing what the daily pipeline actually produced:

```text
Use Pipeline replay.
```

For production/paper trading:

```text
Use named YAML profiles only.
Do not use UI custom_config directly for paper trading until promotion workflow exists.
```
