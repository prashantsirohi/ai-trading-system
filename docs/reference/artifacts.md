# Artifacts

- **Purpose:** Per-stage artifact name, path pattern, producer, consumer, and authority for each materialized output.
- **Audience:** Operator, developer, debugging.
- **Last verified:** 2026-07-15
- **Source of truth:** Stage docs under [`docs/stages/`](../stages/) (each cites its writer module).

---

## Authoritative artifact root

Per-run stage artifacts are written under:
- `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`
- `data/research/pipeline_runs/<run_id>/<stage>/attempt_<n>/` for research-domain runs

These directories contain materialized evidence for a specific pipeline run and
stage attempt. A registered file is authoritative for default downstream
resolution only after its registry lifecycle reaches `promoted` and the exact
producing stage attempt is `completed`. Failed or interrupted attempts remain
diagnostic evidence even when their files are intact.

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

### `opportunities` (optional shadow)

Writes under `data/pipeline_runs/<run_id>/opportunities/attempt_<n>/`:

- `opportunity_shadow_summary.json`
- `candidate_admissions.csv`
- `candidate_updates.csv`
- `candidate_transitions.csv`
- `candidate_closures.csv`
- `candidate_reconciliation.csv`
- `adapter_warnings.csv`
- `adapter_rejections.csv`
- `registry_conflicts.csv`
- `current_candidate_state.csv`
- `position_episode_compatibility.csv`
- `position_recovery_proposals.csv`
- `position_recovery_actions.csv`
- `position_monitor_reconciliation.csv`

Authority:

- audit and reconciliation evidence for the attempt
- canonical episode history in `control_plane.duckdb` remains authoritative
- these files are not execution or publish inputs

See [`docs/stages/opportunities.md`](../stages/opportunities.md).

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

## Phase 3B shadow artifacts

`weekly_stage` writes `weekly_stock_stage_universe.csv`, `weekly_sector_stage_universe.csv`, `weekly_stage_exclusions.csv`, `weekly_stage_summary.json`, `light_pattern_scan.csv`, and `stage_promotion_candidates.csv`.

Phase 3C-1 adds membership trust and membership-observation lineage to stock
coverage rows. Sector rows include constituent source hashes, membership IDs,
and aggregate membership trust. These are additive columns; artifact names and
downstream execution/publish payloads are unchanged.

`scan_router` writes `scan_routing.csv`, `stage_discovery_candidates.csv`, `deep_scan_universe.csv`, `position_monitor_universe.csv`, `routing_conflicts.csv`, `scan_coverage_summary.json`, and `scan_routing_comparison.json`. Phase 3C-2 preserves existing routing columns and appends policy-v2 lineage fields including effective tier, winning reason, all selection reasons, selection details, structural new-long block/risk fields, routing input hash, and routing decision ID. Invalid routing rows are reported in `routing_conflicts.csv` and excluded from trusted route artifacts before persistence.

Phase 3C-3 additionally writes `active_position_coverage.csv`,
`active_position_missing_data.csv`, and `position_monitor_reconciliation.csv`.
The coverage summary separates routed, complete-data, complete-evidence, fully
monitored, and missing-coverage counts plus alert emit/dedupe/resolve counts.

When routing is enabled, Investigator additionally writes `routed_investigator_scores.csv`, `routed_pattern_scan.csv`, `position_risk_monitor.csv`, and `routed_routing_conflicts.csv`. Existing Investigator artifacts and publish consumers are unchanged.

## Phase 3C-4 performance artifacts

Instrumented runs and benchmark roots contain exactly these canonical telemetry
files:

- `phase3c4_performance_metrics.csv`: operation timings, counts, throughput,
  memory, cache/replay mode, and threshold status.
- `phase3c4_performance_summary.json`: run/stage rollup with separate functional
  and performance status.
- `phase3c4_artifact_metrics.csv`: artifact rows/columns, bytes, write time, and
  content hash.
- `phase3c4_database_metrics.csv`: measured read/write time, query/transaction
  counters, and row counts.
- `phase3c4_replay_comparison.json`: semantic, artifact-hash, routing-decision,
  and opportunity-identity equivalence.

The files are audit evidence, not trading inputs or a replacement for canonical
DuckDB stores. No Phase 3C-4 schema migration exists.
