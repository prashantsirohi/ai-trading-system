# Target Architecture

## Intent
The production-hardening refactor splits the daily workflow into four explicit stages and wraps them in a small DuckDB-backed control plane for lineage, retries, and governance.
The repo now keeps production and research in the same project, but on separate data domains.
The active production scoring path is technical-only; no fundamental overlay is currently applied in the rank stage.

## Repo Boundary

### `operational`
- Purpose:
  - daily ingest, features, rank, publish
  - 1-year rolling trading data
- Default storage:
  - legacy-compatible `data/ohlcv.duckdb` and `data/feature_store/`
  - or domain layout under `data/operational/` for new setups

### `research`
- Purpose:
  - static backtesting
  - model training and evaluation
  - reproducible historical studies
- Default storage:
  - `data/research/research_ohlcv.duckdb`
  - `data/research/feature_store/`
  - `models/research/`
  - `reports/research/`

### Shared code
- `analytics/`, `features/`, and `collectors/` contain shared deterministic logic.
- `run/` remains production-only orchestration.
- `research/` contains backtest/train/eval entrypoints that resolve the research domain explicitly.

## Stage Topology

```text
PipelineOrchestrator
  -> ingest
  -> features
  -> rank
  -> publish
```

## Stage Contracts

### `ingest`
- Input: provider config, trading date, batch settings
- Reused implementation: `collectors.daily_update_runner.run(..., symbols_only=True)`
- Canary support:
  - optional `symbol_limit` restricts the live universe for smaller operational runs
- Output artifacts:
  - `ingest_summary`
- Metadata captured:
  - total `_catalog` rows
  - distinct symbol count
  - latest timestamp
- DQ gate:
  - duplicate raw OHLCV key check
  - `_catalog` not empty
  - required OHLCV fields not null
  - OHLC consistency
  - negative volume logged as non-blocking high severity

### `features`
- Input: successful ingest stage
- Reused implementation: `collectors.daily_update_runner.run(..., features_only=True)`
- Canary support:
  - optional `symbol_limit` restricts feature recomputation to the canary universe
- Sector-strength universe:
  - `compute_sector_rs.py` now builds sector leadership from a liquidity-filtered broad universe
  - default rule is top `800` symbols by recent median traded value with at least `180` recent trading days
- Output artifacts:
  - `feature_snapshot`
- Metadata captured:
  - `snapshot_id`
  - feature rows written
  - feature registry entry count
- DQ gate:
  - snapshot reference must exist
  - feature rows must be non-zero
  - stale source catalog data is logged as high severity

### `rank`
- Input: successful features stage
- Reused implementation:
  - `analytics.ranker.StockRanker`
  - `channel.stock_scan` computations
  - `channel.sector_dashboard` computations
- Output artifacts:
  - `ranked_signals`
  - `stock_scan`
  - `sector_dashboard`
  - `rank_summary`
- Degradation handling:
  - optional side outputs that fail to compute are recorded in `rank_summary.degraded_outputs`
  - the stage no longer hides those failures completely; operators can inspect degraded output count in stage metadata
- DQ gate:
  - ranking artifact must not be empty
  - required ranking columns must be present
  - low symbol coverage is logged as high severity
  - duplicate ranked symbols are logged as medium severity
  - duplicate ranked symbols logged as non-blocking medium severity

### `publish`
- Input: successful rank artifacts only
- Contract:
  - reads the `ranked_signals` artifact URI and hash from metadata
  - applies exponential-backoff retries per channel
  - uses dedupe on `run_id + channel + artifact hash`
  - records per-channel delivery attempts and dedupe skips
- Reused implementation:
  - Google Sheets updates from stock scan / sector dashboard helpers
  - portfolio analysis
  - Telegram summary delivery
- Output artifacts:
  - `publish_summary`
- Delivery metadata:
  - persisted in `publisher_delivery_log`
  - includes attempt history and external message/report IDs when available
- Failure behavior:
  - retryable
  - does not invalidate upstream artifacts
  - can be rerun alone with the same `run_id`

## Registry Tables

### `pipeline_run`
- One row per logical pipeline run
- Tracks current stage, overall status, and terminal error metadata

### `pipeline_stage_run`
- One row per stage attempt
- Captures retry-safe attempt numbering

### `pipeline_artifact`
- Immutable artifact catalog for each stage attempt
- Stores URI, hash, row count, and metadata JSON

## Governance Tables

### DQ
- `dq_rule`
- `dq_result`

### Publisher
- `publisher_delivery_log`

### Alerts
- `pipeline_alert`

### Model governance
- `model_registry`
- `model_eval`
- `model_deployment`

## Model Governance Behavior
- `model_registry` stores model artifact URI, feature schema hash, training snapshot reference, and approval status.
- `model_eval` stores one row per metric per model evaluation.
- `model_deployment` stores deployment history, active environment state, and rollback target model references.

## Retry Model
- Upstream stage failures block downstream stages.
- Publish failures mark the run `completed_with_publish_errors`.
- `python -m run.orchestrator --run-id <run_id> --stages publish` retries publish only.
- Previously delivered publish channels are skipped idempotently on rerun.
- Retry requests are appended to `pipeline_run.metadata.events` for auditability.

## Operational Readiness
- `run.preflight.PreflightChecker` validates local runtime prerequisites before live runs.
- Preflight also warns when `.env` uses CRLF line endings because shell-sourced live credentials can be corrupted.
- `--canary` enables a smaller real pipeline pass, defaulting to `ingest,features,rank`.
- `python3 -m run.publish_test` exercises live publish channels outside the main pipeline.
- `--data-domain operational|research` lets shared code resolve the intended storage plane.

## Research Entry Points
- `python -m research.backtest_pipeline`
- `python -m research.train_pipeline`
- `python -m research.eval_pipeline`

Research defaults to a static historical cutoff of Dec 31 of the prior year so studies do not drift with live data refreshes.

## Smoke Run
- Self-contained smoke execution:
  - `python3 -m run.orchestrator --smoke --local-publish`
- This writes run, stage, artifact, and DQ metadata without needing live providers or delivery credentials.
