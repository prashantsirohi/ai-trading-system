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
canonical factor scores
composite_score
composite_score_adjusted
eligible_rank
```

Ranking method version:

```text
research_dynamic_v3_canonical_factor_scoring_stage2_benchmark
```

The dynamic loader computes historical raw factors from OHLCV, then reuses the
canonical ranking factor/scoring helpers from `domains/ranking` for relative
strength, volume intensity, trend persistence, momentum acceleration,
proximity-to-highs, delivery imputation, sector strength scoring, composite
score, and penalties. When `NIFTY50` history is present, it also blends
benchmark-relative RS into relative strength. When `weekly_stage_snapshot`
exists in the research DuckDB, it attaches weekly Stage 2 context and applies
the same Stage 2 freshness/transition bonuses used by production ranking.

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
5. Copy operational masterdata into data/research/masterdata.db.
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

For sector metadata, research dynamic prefers:

```text
data/research/masterdata.db
```

If that file is absent, it falls back to:

```text
data/masterdata.db
```

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
Research sync summary
Research data-quality summary
Run metadata
Winner Capture analysis
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

### Winner Capture

Winner Capture answers:

```text
Did the current research_dynamic ranking capture the year's biggest winners?
```

It uses the research DuckDB directly, syncs operational data into research first, finds the top yearly gainers for a completed calendar year, then scans daily dynamic ranks from Jan 1 to Dec 31.

Default interpretation:

```text
winner set = top 50 NSE gainers
capture rule = symbol appeared in daily Top 50 at least once
year mode = completed calendar year
```

Outputs include:

```text
capture rate
captured / missed count
median days to first capture
median first capture rank
captured vs missed average yearly return
per-symbol first capture date, first rank, best rank, days to capture, and remaining return
```

## API

Profiles:

```http
GET /api/execution/backtest/profiles
```

Run:

```http
POST /api/execution/backtest/run
```

Winner Capture:

```http
POST /api/execution/backtest/winner-capture
```

Request:

```json
{
  "year": 2025,
  "exchange": "NSE",
  "top_gainers": 50,
  "rank_cutoff": 50,
  "persist": true
}
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
The response includes:

```text
sync
data_quality
run_metadata
```

Persisted runs write:

```text
summary.json
metadata.json
trades.csv
equity_curve.csv
```

Persisted Winner Capture runs write:

```text
data/research/winner_capture/<year>/<timestamp>/summary.json
data/research/winner_capture/<year>/<timestamp>/metadata.json
data/research/winner_capture/<year>/<timestamp>/winners.csv
```

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
- Operational-to-research masterdata sync added.
- Sync preserves old research history outside the operational source range.
- Research dynamic data-quality summary added.
- Backtest Lab displays sync, data-quality, and run metadata.
- Persisted backtest runs now write metadata.json.
- Research dynamic ranking now uses canonical factor scoring instead of the
  earlier simple RS/trend/volume composite.
- Research dynamic ranking now blends NIFTY-relative RS when benchmark rows
  are present.
- Research dynamic ranking now uses weekly Stage 2 context/freshness bonuses
  when `weekly_stage_snapshot` exists.
- Winner Capture analysis added for measuring whether current dynamic ranking
  finds the top yearly gainers.
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
Focused backtesting/API/paper parity suite passes.
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

1. Document and monitor any remaining ranking differences.

Current `research_dynamic` now uses canonical factor scoring, benchmark-relative
RS, and weekly Stage 2 context when available. It still does not run the full
production `StockRanker` facade because that facade is wired to latest
operational loaders and sidecar artifacts.

Remaining differences to document/monitor:

```text
Some production sidecar fields may still be absent in pure OHLCV-only research history.
Weekly stage hard-gating remains optional; risk profiles still control trade entry gates.
Production rank routing/market-stage mode selection is not automatically replayed yet.
```

2. Add stricter data-quality gates for research dynamic.

Current implementation reports DQ facts but does not block a run. Add optional hard gates for:

```text
minimum warmup coverage
maximum missing OHLCV rows
maximum duplicate rows
minimum symbol coverage
```

### Medium Priority

3. Add unique/index validation for research `_catalog`.

DuckDB does not enforce the desired uniqueness in all existing paths. Add a validation query/report for:

```text
symbol_id, exchange, timestamp
```

4. Add benchmark metrics to engine backtests.

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

5. Add strategy profile saving.

Current custom UI settings run only as one-off `custom_config`. Later, save tuned configs as versioned research candidates.

6. Add promotion workflow.

Possible lifecycle:

```text
draft → research_candidate → paper_shadow → active → retired
```

7. Add charts.

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
