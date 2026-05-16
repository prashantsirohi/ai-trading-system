# Artifacts

- **Purpose:** Per-stage artifact name, path pattern, producer, consumer, and authority for each materialized output.
- **Audience:** Operator, developer, debugging.
- **Last verified:** 2026-05-16
- **Source of truth:** Stage docs under [`docs/stages/`](../stages/) (each cites its writer module).

---

## Authoritative artifact root

Per-run stage artifacts are written under:
- `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`
- `data/research/pipeline_runs/<run_id>/<stage>/attempt_<n>/` for research-domain runs

These directories are the authoritative materialized outputs for a specific pipeline run and stage attempt.

## Stage artifacts

### `ingest`

Writes:
- `ingest_summary.json`

Authority:
- authoritative for ingest-stage run output and trust summary snapshot for that attempt

Current meanings include:
- updated symbol list
- provider coverage by date
- unresolved dates
- quarantine counts
- reference validation result
- delivery collection result

### `features`

Writes:
- `feature_snapshot.json`

Authority:
- authoritative for the feature-stage snapshot metadata for that attempt

Current meanings include:
- snapshot id
- feature row count
- registry counts
- compute mode

### `rank`

Writes:
- `ranked_signals.csv`
- `breakout_scan.csv`
- `pattern_scan.csv`
- `stock_scan.csv`
- `sector_dashboard.csv`
- `dashboard_payload.json`
- `rank_summary.json`
- optional `ml_overlay.csv`
- `task_status.json`

Authority split:
- `ranked_signals.csv`: authoritative ranked output for the attempt
- `dashboard_payload.json`: authoritative aggregated operator payload for the attempt
- sidecar CSVs: authoritative only for their own sidecar views
- `task_status.json`: authoritative per-task bookkeeping for retry and operator diagnostics
- `rank_summary.json`: convenience summary, not the sole source of truth when the CSVs exist

### `fundamentals` (optional)

Writes (under `data/pipeline_runs/<run_id>/fundamentals/attempt_<n>/`):
- `fundamental_scores.csv`
- `fundamental_summary.csv`

Authority:
- authoritative for the fundamentals enrichment of that attempt
- stage is skipped if Screener credentials are missing — absence is not an error

See [`docs/stages/fundamentals.md`](../stages/fundamentals.md).

### `candidates`

Writes (under `data/pipeline_runs/<run_id>/candidates/attempt_<n>/`):
- `candidates.json` — deterministic candidate list with entry/exit logic

Authority:
- authoritative input to the execute stage for that attempt

See [`docs/stages/candidates.md`](../stages/candidates.md).

### `events`

Writes (under `data/pipeline_runs/<run_id>/events/attempt_<n>/`):
- `event_packet.json`
- `event_enriched_rank.csv`

Authority:
- authoritative event context for the attempt; consumed by insight + publish

See [`docs/stages/events.md`](../stages/events.md).

### `execute`

Writes:
- `trade_actions.csv`
- `executed_orders.csv`
- `executed_fills.csv`
- `positions.csv`
- `execute_summary.json`

Authority split:
- CSV artifacts are authoritative for that run attempt
- persistent execution rows in `data/execution.duckdb` are authoritative across attempts
- `execute_summary.json` is a convenience summary of the attempt

### `insight`

Writes (under `data/pipeline_runs/<run_id>/insight/attempt_<n>/`):
- `market_insight.json` — analyst brief packet for the narrative stage

See [`docs/stages/insight.md`](../stages/insight.md).

### `narrative`

Writes (under `data/pipeline_runs/<run_id>/narrative/attempt_<n>/`):
- `market_report.json` — LLM-generated trading narrative

See [`docs/stages/narrative.md`](../stages/narrative.md).

### `publish`

Writes:
- `publish_summary.json`

Authority:
- authoritative summary for publish-stage assembly and channel outcomes for that attempt
- per-channel delivery lineage is also authoritative in `publisher_delivery_log`

### `perf_tracker`

Writes (under `data/pipeline_runs/<run_id>/perf_tracker/attempt_<n>/`):
- `perf_tracker_summary.json` — status + `dates_processed` + `rows_upserted`. On failure: `status: failed` (pipeline continues — perf_tracker is non-blocking).

DuckDB writes:
- `rank_cohort_performance` table in `data/research.duckdb` (primary key `(run_date, symbol_id, exchange)`)

See [`docs/stages/perf_tracker.md`](../stages/perf_tracker.md).

## Non-stage report outputs

### QuantStats reports

Current location:
- `reports/quantstats/`

Current outputs can include:
- enriched dashboard tear sheet HTML
- return-series CSV
- supporting JSON
- optional raw QuantStats core HTML

Authority:
- publish convenience outputs derived from rank history
- not the system of record for ranking or execution state

### Data repair reports

Current location:
- `reports/data_repairs/`

Current outputs include repair-window evidence and validation results for reset-reingest flows.

Authority:
- authoritative for the repair run report itself
- supporting evidence alongside `data_repair_run` rows in the control plane

### ML shadow overlay reports

Current location:
- `reports/ml_rank_overlay.csv`
- `reports/ml_rank_overlay_<date>.csv`
- `reports/research/...`

Authority:
- convenience files for latest and dated overlay review
- control-plane rows in `prediction_log`, `model_shadow_prediction`, and related tables remain authoritative for registry-tracked ML state

## Feature-store outputs

Current location:
- `data/feature_store/...`
- `data/research/feature_store/...`

Authority:
- authoritative for current feature-consumer inputs
- parquet files are the active feature-serving path
- `_feature_registry`, `_file_registry`, and `_snapshots` provide metadata and lineage, not the full serving payload by themselves

## Convenience vs source-of-record summary

Authoritative source-of-record items:
- pipeline-run stage artifacts under `data[/research]/pipeline_runs/...`
- market and trust tables in `ohlcv.duckdb`
- control-plane tables in `control_plane.duckdb`
- persistent paper execution tables in `execution.duckdb`
- feature parquet files under `data[/research]/feature_store/`

Convenience outputs:
- `rank_summary.json`
- `execute_summary.json`
- QuantStats HTML wrappers
- latest ML overlay CSV copies in `reports/`
