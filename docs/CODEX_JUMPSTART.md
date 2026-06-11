# Codex Jumpstart — AI Trading System

This file is the fast orientation brief for Codex or any coding agent.

Read this before scanning the repo.

## One-screen summary

This repo is an NSE AI trading system.

Pipeline:

```text
ingest -> features -> rank -> execute -> publish
```

Primary entrypoint:

```bash
python -m run.orchestrator
```

Runtime data is external and must be resolved from `.env`.

Current local operator setting:

```bash
DATA_ROOT=/Volumes/MacData/Trading/data
```

Never assume live data is in repo-local `data/`.

Use `$DATA_ROOT`.

## Fast path rules for Codex

Before coding:

1. Read `AGENTS.md`.
2. Read this file.
3. Identify the affected stage.
4. Read only the relevant layer doc.
5. Inspect only necessary source files.
6. Patch minimally.
7. Run targeted tests or a canary command.
8. Report files changed, tests run, and risks.

Do not scan the whole repo unless the task is explicitly cross-cutting.

## Runtime path contract

All live runtime paths should resolve under `$DATA_ROOT`.

```text
$DATA_ROOT/ohlcv.duckdb
$DATA_ROOT/control_plane.duckdb
$DATA_ROOT/execution.duckdb
$DATA_ROOT/raw/NSE_EQ/
$DATA_ROOT/feature_store/
$DATA_ROOT/pipeline_runs/<run_id>/<stage>/attempt_<n>/
```

Application code should read `DATA_ROOT` through the repo's existing config/path helpers.

Do not hardcode:

```text
data/...
/Volumes/MacData/Trading/data/...
```

Exception: documentation may mention `/Volumes/MacData/Trading/data` as the current local `.env` value.

## DB map

### `$DATA_ROOT/ohlcv.duckdb`

Purpose:

- market OHLCV
- delivery data
- feature registry/snapshots
- trust/quarantine state

Important tables:

```text
_catalog
_delivery
_catalog_provenance
_catalog_quarantine
_feature_registry
_snapshots
_file_registry
_ingestion_status
feat_*
```

### `$DATA_ROOT/control_plane.duckdb`

Purpose:

- pipeline governance
- run/stage metadata
- artifacts
- DQ results
- publish logs
- model registry
- operator task state

Important tables:

```text
pipeline_run
pipeline_stage_run
pipeline_artifact
dq_rule
dq_result
publisher_delivery_log
pipeline_alert
model_registry
model_eval
operator_task
```

### `$DATA_ROOT/execution.duckdb`

Purpose:

- execution orders
- fills
- positions
- paper/live execution state

## Stage map

### Ingest

Purpose:

Refresh trusted OHLCV and delivery data.

Key files:

```text
run/stages/ingest.py
collectors/daily_update_runner.py
collectors/nse_collector.py
collectors/dhan_collector.py
collectors/delivery_collector.py
analytics/data_trust.py
analytics/dq/engine.py
```

Primary artifacts:

```text
$DATA_ROOT/pipeline_runs/<run_id>/ingest/attempt_<n>/ingest_summary.json
```

Source contract:

- NSE bhavcopy is operational primary.
- yfinance is fallback.
- Dhan exists but is not default operational source of record.
- Quarantine unresolved recent gaps instead of silently accepting them.

### Features

Purpose:

Compute indicators and sector-strength context.

Key files:

```text
run/stages/features.py
features/feature_store.py
features/compute_sector_rs.py
features/indicators.py
collectors/daily_update_runner.py
```

Common features:

```text
rsi_14
adx_14
atr_14
ema_9
ema_21
ema_50
ema_200
sma_50
sma_200
macd_line
macd_signal
macd_hist
roc_1d
roc_5d
roc_20d
supertrend
bb_upper
bb_middle
bb_lower
sma50_slope_20d_pct
sma200_slope_20d_pct
near_52w_high_pct
rel_strength_20d
rel_strength_60d
volume_ratio_20
```

Primary artifact:

```text
$DATA_ROOT/pipeline_runs/<run_id>/features/attempt_<n>/feature_snapshot.json
```

### Rank

Purpose:

Create operator-facing decision artifacts.

Key files:

```text
run/stages/rank.py
analytics/ranker.py
channel/breakout_scan.py
channel/stock_scan.py
channel/sector_dashboard.py
analytics/patterns/*
analytics/dq/engine.py
analytics/registry/store.py
```

Current factor model:

```text
relative_strength
volume_intensity
trend_persistence
proximity_to_highs
delivery_pct
sector_strength
```

Primary artifacts:

```text
$DATA_ROOT/pipeline_runs/<run_id>/rank/attempt_<n>/ranked_signals.csv
$DATA_ROOT/pipeline_runs/<run_id>/rank/attempt_<n>/breakout_scan.csv
$DATA_ROOT/pipeline_runs/<run_id>/rank/attempt_<n>/pattern_scan.csv
$DATA_ROOT/pipeline_runs/<run_id>/rank/attempt_<n>/stock_scan.csv
$DATA_ROOT/pipeline_runs/<run_id>/rank/attempt_<n>/sector_dashboard.csv
$DATA_ROOT/pipeline_runs/<run_id>/rank/attempt_<n>/dashboard_payload.json
$DATA_ROOT/pipeline_runs/<run_id>/rank/attempt_<n>/rank_summary.json
$DATA_ROOT/pipeline_runs/<run_id>/rank/attempt_<n>/task_status.json
```

### Execute

Purpose:

Convert ranked candidates into trade actions, orders, fills, and positions.

Key files:

```text
run/stages/execute.py
execution/autotrader.py
execution/policies.py
execution/service.py
execution/store.py
execution/portfolio.py
execution/adapters/paper.py
execution/adapters/dhan.py
```

Default behavior:

- paper execution
- preview-safe unless explicitly enabled
- trust status can block execution

Primary artifacts:

```text
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/trade_actions.csv
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/executed_orders.csv
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/executed_fills.csv
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/positions.csv
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/execute_summary.json
```

### Publish

Purpose:

Deliver already-materialized artifacts without recomputing upstream stages.

Key files:

```text
run/stages/publish.py
run/publisher.py
publishers/dashboard.py
publishers/google_sheets.py
publishers/telegram.py
publishers/quantstats_dashboard.py
```

Primary artifact:

```text
$DATA_ROOT/pipeline_runs/<run_id>/publish/attempt_<n>/publish_summary.json
```

Publish is safe to retry using the same `run_id`.

## Commands

Always load `.env` first when running locally.

```bash
set -a
source .env
set +a
```

### Full operational run

```bash
set -a
source .env
set +a

PYTHONPATH=. ./.venv/bin/python -m run.orchestrator --data-domain operational
```

### Canary verification

```bash
set -a
source .env
set +a

PYTHONPATH=. ./.venv/bin/python -m run.orchestrator --canary --symbol-limit 25 --local-publish
```

### Retry publish only

```bash
set -a
source .env
set +a

PYTHONPATH=. ./.venv/bin/python -m run.orchestrator --run-id <run_id> --stages publish
```

### Ingest only

```bash
set -a
source .env
set +a

PYTHONPATH=. ./.venv/bin/python -m collectors.daily_update_runner --symbols-only --nse-primary
```

### Features only

```bash
set -a
source .env
set +a

PYTHONPATH=. ./.venv/bin/python -m collectors.daily_update_runner --features-only
```

### Research UI

```bash
set -a
source .env
set +a

PYTHONPATH=. ./.venv/bin/streamlit run ui/research/app.py
```

### Execution API

```bash
set -a
source .env
set +a

PYTHONPATH=. ./.venv/bin/python -m ui.execution_api.app --port 8090
```

### React execution console

```bash
cd web/execution-console
npm install
npm run dev
```

## Diagnostics

Use `$DATA_ROOT`.

```bash
duckdb "$DATA_ROOT/control_plane.duckdb" -cmd "SELECT * FROM pipeline_run ORDER BY started_at DESC LIMIT 1"

duckdb "$DATA_ROOT/control_plane.duckdb" -cmd "SELECT rule_id, severity, failed_count FROM dq_result"

duckdb "$DATA_ROOT/ohlcv.duckdb" -cmd "SELECT MIN(date), MAX(date), COUNT(*) FROM _catalog"

duckdb "$DATA_ROOT/ohlcv.duckdb" -cmd "SELECT COUNT(DISTINCT symbol) FROM _catalog"

du -sh "$DATA_ROOT"/*

curl http://localhost:8090/api/execution/health
```

## Safe SQL rule

Never interpolate symbol, exchange, date, or user-provided input into SQL strings.

Unsafe:

```python
conn.execute(f"SELECT * FROM _catalog WHERE symbol = '{symbol}'")
```

Safe:

```python
conn.execute(
    "SELECT * FROM _catalog WHERE symbol = ?",
    [symbol],
)
```

Dynamic table names are only acceptable when they come from internal trusted constants.

## High-priority known review areas

Before implementing these, check current code to see whether they are already fixed.

### Execution

- Stop-loss trigger may use entry/last-fill price instead of current market close.
- Stop should deactivate after SELL fill.
- Portfolio constraints may receive empty `sector_exposure` and `symbol_weights`.
- Heat gate should use capital-at-risk below stop, not total exposure.
- Trailing stop support may need wiring.

### Data/SQL

- f-string `WHERE` clauses in feature store or execute-stage queries may be unsafe.
- Use DuckDB parameter binding.
- Do not hardcode repo-local `data/`.

### Ranking

- Sector demeaning may be absent from composite factor scoring.
- Trend score may over-penalize stocks below SMA by zeroing ADX.
- Delivery percentage missing values may be flat-filled instead of sector-median imputed.
- Rank stability should sort previous frame by score before assigning previous rank.

### Package migration

- Legacy imports may still point to old modules.
- `TrustConfidenceEnvelope` may need a new package-local home.
- Ensure standalone import and `python -m ... --help` still work.

### Registry/DuckDB

- Multiple writers to DuckDB need serialization.
- Read-only queries should avoid unnecessary writer connections.

## Response format after coding

When finished, report:

```text
Files changed:
- ...

Tests run:
- ...

Behavior change:
- ...

Risks/follow-ups:
- ...
```

Do not claim success unless tests or commands actually passed.

## Minimal task prompt to give Codex

```text
Read AGENTS.md and docs/CODEX_JUMPSTART.md first.
Use DATA_ROOT from .env; do not hardcode repo-local data/.
Identify the affected pipeline stage, inspect only relevant files, make a minimal patch, run targeted tests or a canary, and report files changed, tests run, behavior change, and risks.
```
