# AGENTS.md — AI Trading System Repo Guide

## First-read rule

Before scanning the full repo, read these files first:

1. `AGENTS.md`
2. `docs/CODEX_JUMPSTART.md`
3. `high_level_operational_data_flow.md`
4. The relevant layer document only:
   - `ingest_layer_architecture.md`
   - `features_layer_architecture.md`
   - `rank_layer_architecture.md`
   - `execute_layer_architecture.md`
   - `publish_layer_architecture.md`

Do not waste time rediscovering DB paths, artifact paths, run commands, stage order, or data-source contracts.

## Most important path rule

Runtime data is external.

Do not assume live data lives inside the repo.

Canonical runtime data root is defined in `.env`:

```bash
DATA_ROOT=/Volumes/MacData/Trading/data
```

All DBs, raw files, feature stores, and pipeline artifacts must be resolved relative to `$DATA_ROOT`.

Do not hardcode repo-local `data/...`.

Do not create a second repo-local `data/` folder unless explicitly asked.

When touching any DB/path/artifact code, first check how `DATA_ROOT` is loaded and propagated.

## System shape

This is a production-oriented NSE AI trading system with a staged pipeline:

```text
ingest -> features -> rank -> execute -> publish
```

Core design:

- `ingest`: refresh trusted OHLCV and delivery data.
- `features`: compute technical indicators and sector-strength context.
- `rank`: produce ranked signals, breakout evidence, patterns, sector dashboards, and dashboard payloads.
- `execute`: convert selected candidates into paper/live trade actions, orders, fills, and positions.
- `publish`: deliver already-materialized artifacts to Sheets, Telegram, dashboard, QuantStats, or local summary.

## Runtime storage map

Always resolve these paths from `$DATA_ROOT`.

```text
$DATA_ROOT/ohlcv.duckdb
$DATA_ROOT/control_plane.duckdb
$DATA_ROOT/execution.duckdb
$DATA_ROOT/raw/NSE_EQ/
$DATA_ROOT/feature_store/
$DATA_ROOT/pipeline_runs/<run_id>/<stage>/attempt_<n>/
```

### `$DATA_ROOT/ohlcv.duckdb`

Important tables:

- `_catalog`: OHLCV source of record.
- `_delivery`: delivery percentage history.
- `_catalog_provenance`: provider and validation lineage.
- `_catalog_quarantine`: unresolved trust issues with `active`, `observed`, `resolved` states.
- `_feature_registry`: feature compute metadata.
- `_snapshots`: feature snapshot lineage.
- `_file_registry`: parquet feature file registry.
- `_ingestion_status`: freshness markers.
- `feat_*`: indicator tables.

### `$DATA_ROOT/control_plane.duckdb`

Important tables:

- `pipeline_run`
- `pipeline_stage_run`
- `pipeline_artifact`
- `dq_rule`
- `dq_result`
- `publisher_delivery_log`
- `pipeline_alert`
- `model_registry`
- `model_eval`
- `operator_task`

### `$DATA_ROOT/execution.duckdb`

Important data:

- paper/live orders
- fills
- open/closed positions
- stop records, if enabled by current code path

## Operational source-of-record contract

Default operational OHLC source contract:

1. Primary: NSE bhavcopy.
2. Fallback: yfinance only when required bhavcopy data is unavailable.
3. Dhan support exists for diagnostics and broker workflows, but it is not the default operational source of record.

Synthetic smoke data is intentionally disabled.

The system is trust-first. Recent unresolved gaps are quarantined rather than silently accepted.

## Trust and DQ model

Trust-sensitive pipeline stages depend on:

- `_catalog`
- `_catalog_provenance`
- `_catalog_quarantine`

Trust status values:

- `trusted`
- `degraded`
- `blocked`
- `legacy`
- `missing`

Important trust metrics:

- `latest_trade_date`
- `latest_validated_date`
- `fallback_ratio_latest`
- `unknown_ratio_latest`
- `active_quarantined_dates`
- `active_quarantined_symbols`
- `latest_provider_stats`

DQ gates run on ingest, features, and rank.

Critical DQ failures block downstream execution.

Examples:

- duplicate OHLCV key
- required OHLCV fields missing
- OHLC consistency failure
- recent universe-wide jump anomaly
- weak provider coverage
- unresolved active quarantine
- missing feature snapshot
- empty rank artifact

## Main modules by stage

### Ingest

- `run/stages/ingest.py`
- `collectors/daily_update_runner.py`
- `collectors/nse_collector.py`
- `collectors/dhan_collector.py`
- `collectors/delivery_collector.py`
- `analytics/data_trust.py`
- `analytics/dq/engine.py`

### Features

- `run/stages/features.py`
- `features/feature_store.py`
- `features/compute_sector_rs.py`
- `features/indicators.py`
- `collectors/daily_update_runner.py`

### Rank

- `run/stages/rank.py`
- `analytics/ranker.py`
- `channel/breakout_scan.py`
- `channel/stock_scan.py`
- `channel/sector_dashboard.py`
- `analytics/patterns/*`
- `analytics/dq/engine.py`
- `analytics/registry/store.py`

### Execute

- `run/stages/execute.py`
- `execution/autotrader.py`
- `execution/policies.py`
- `execution/service.py`
- `execution/store.py`
- `execution/portfolio.py`
- `execution/adapters/paper.py`
- `execution/adapters/dhan.py`

### Publish

- `run/stages/publish.py`
- `run/publisher.py`
- `publishers/dashboard.py`
- `publishers/google_sheets.py`
- `publishers/telegram.py`
- `publishers/quantstats_dashboard.py`

## Important artifacts

Always resolve paths from `$DATA_ROOT`.

### Ingest

```text
$DATA_ROOT/pipeline_runs/<run_id>/ingest/attempt_<n>/ingest_summary.json
```

### Features

```text
$DATA_ROOT/pipeline_runs/<run_id>/features/attempt_<n>/feature_snapshot.json
```

### Rank

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

```text
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/trade_actions.csv
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/executed_orders.csv
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/executed_fills.csv
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/positions.csv
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_<n>/execute_summary.json
```

### Publish

```text
$DATA_ROOT/pipeline_runs/<run_id>/publish/attempt_<n>/publish_summary.json
```

## Standard command pattern

Load `.env` before running commands that need runtime paths.

```bash
set -a
source .env
set +a
```

Then run commands with `PYTHONPATH=.`.

## Primary commands

### Full operational run

```bash
set -a
source .env
set +a

PYTHONPATH=. ./.venv/bin/python -m run.orchestrator --data-domain operational
```

### Safe canary run

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

### Streamlit research UI

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

## Quick diagnostic commands

Use `$DATA_ROOT`, not hardcoded `data`.

```bash
duckdb "$DATA_ROOT/control_plane.duckdb" -cmd "SELECT * FROM pipeline_run ORDER BY started_at DESC LIMIT 1"

duckdb "$DATA_ROOT/control_plane.duckdb" -cmd "SELECT rule_id, severity, failed_count FROM dq_result"

duckdb "$DATA_ROOT/ohlcv.duckdb" -cmd "SELECT MIN(date), MAX(date), COUNT(*) FROM _catalog"

duckdb "$DATA_ROOT/ohlcv.duckdb" -cmd "SELECT COUNT(DISTINCT symbol) FROM _catalog"

curl http://localhost:8090/api/execution/health
```

## Coding guardrails

### Path handling

- Use existing path/domain helpers if present.
- Respect `.env` and `DATA_ROOT`.
- Avoid hardcoded `data/...`.
- Avoid hardcoded `/Volumes/MacData/Trading/data` inside application code.
- It is acceptable to mention `/Volumes/MacData/Trading/data` in documentation as the local operator setting.
- Tests should use temp directories, not the live `$DATA_ROOT`.

### SQL safety

Use parameterized DuckDB queries for all user-controlled or market-data values.

Do not build `WHERE symbol = '...'`, `WHERE symbol_id = '...'`, or `WHERE exchange = '...'` using f-strings.

Safe pattern:

```python
conn.execute(
    "SELECT * FROM _catalog WHERE symbol = ? AND date = ?",
    [symbol, date],
)
```

For `IN` lists, use a safe parameterized/list-binding pattern supported by the current DuckDB version, or construct placeholders safely.

Do not parameterize table names. Only use dynamic table names when derived from trusted internal constants.

### Credentials and secrets

Never commit:

- `.env`
- Dhan API credentials
- Telegram bot token
- Telegram chat ID
- Google OAuth files
- broker tokens
- service-account secrets

### Execution safety

- Paper execution is the safe default.
- Do not switch to live broker placement unless explicitly requested.
- Preserve trust-blocking behavior for `blocked` data.
- Preview mode should not mutate live broker state.

### Testing and verification

For small changes:

1. Run targeted unit tests.
2. Run canary with `--local-publish`.
3. Report exact commands and results.

For feature/ranking logic changes:

- Mention whether a full feature rebuild is needed.
- Check rank artifacts are non-empty.
- Check DQ results.

For DB migrations/repairs:

- Back up live DuckDB files first.
- Do not mutate live DBs unless the task explicitly asks for it.

## Common implementation workflow

When asked to implement a change:

1. Identify the pipeline stage affected.
2. Read this file and `docs/CODEX_JUMPSTART.md`.
3. Read the relevant layer architecture doc.
4. Inspect only relevant source files.
5. Make the smallest safe patch.
6. Add or update tests.
7. Run targeted tests or canary.
8. Summarize:
   - files changed
   - tests run
   - behavior change
   - risks or follow-ups

## Known high-priority themes from review notes

Check whether these are already fixed before patching:

- Stop-loss current-price bug in execute/autotrader flow.
- SQL injection from f-string `WHERE` clauses in feature/execution DB queries.
- Portfolio constraints receiving empty exposure dictionaries.
- Legacy import unification issues.
- `TrustConfidenceEnvelope` missing during package migration.
- Sector demeaning absent from ranking composite.
- Trend score ADX/SMA coupling issue.
- Delivery percentage flat-fill issue.
- Heat gate using exposure instead of capital-at-risk.
- Rank stability previous-frame sorting issue.
- RegistryStore write concurrency with DuckDB.
