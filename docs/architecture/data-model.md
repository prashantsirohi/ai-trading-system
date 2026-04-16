# Data Model

## Storage layout

Operational domain paths:
- `data/ohlcv.duckdb`
- `data/masterdata.db`
- `data/feature_store/`
- `data/pipeline_runs/`
- `data/training_datasets/`
- `reports/`
- `models/`

Research domain paths:
- `data/research/research_ohlcv.duckdb`
- `data/masterdata.db`
- `data/research/feature_store/`
- `data/research/pipeline_runs/`
- `data/research/training_datasets/`
- `reports/research/`
- `models/research/`

Separate non-domain stores:
- `data/control_plane.duckdb`
- `data/execution.duckdb`

## Database files

### `data/ohlcv.duckdb`

Primary operational market store.

Important tables:
- `_catalog`: OHLCV rows with trust columns such as `provider`, `provider_priority`, `validation_status`, `validated_against`, `ingest_run_id`, and `repair_batch_id`
- `_catalog_history`: historical copy table maintained by collector paths
- `_catalog_provenance`: append-only per-row provenance records
- `_catalog_quarantine`: unresolved or resolved symbol-date trust exceptions
- `_delivery`: delivery percentage history
- `_feature_registry`: feature registry metadata
- `_snapshots`: feature snapshot rows
- `_file_registry`: parquet bookkeeping initialized by `FeatureStore`
- `_ingestion_status`: feature write bookkeeping initialized by `FeatureStore`

### `data/research/research_ohlcv.duckdb`

Research OHLCV store. It serves the same role for research workflows that `data/ohlcv.duckdb` serves for operational workflows.

### `data/control_plane.duckdb`

Control-plane source of truth.

Important tables:
- `pipeline_run`
- `pipeline_stage_run`
- `pipeline_artifact`
- `dq_rule`
- `dq_result`
- `pipeline_alert`
- `publisher_delivery_log`
- `data_repair_run`
- `dataset_registry`
- `prediction_log`
- `shadow_eval`
- `model_shadow_prediction`
- `model_shadow_outcome`
- `model_registry`
- `model_eval`
- `model_deployment`
- `promotion_gate_result`
- `drift_metric`
- `operator_task`
- `operator_task_log`

### `data/execution.duckdb`

Paper-execution store.

Important tables:
- `execution_order`
- `execution_fill`
- `execution_trade_note`

### `data/masterdata.db`

SQLite reference store.

Common tables used in current code:
- `stock_details`
- `symbols`
- `sectors`
- `sector_constituents`
- `nse_holidays`

## Artifact directories

### Pipeline-run artifacts

Per-attempt stage outputs are written under:
- `data/pipeline_runs/<run_id>/ingest/attempt_<n>/`
- `data/pipeline_runs/<run_id>/features/attempt_<n>/`
- `data/pipeline_runs/<run_id>/rank/attempt_<n>/`
- `data/pipeline_runs/<run_id>/execute/attempt_<n>/`
- `data/pipeline_runs/<run_id>/publish/attempt_<n>/`

The same structure exists under `data/research/pipeline_runs/` when the research data domain is selected.

These directories are authoritative for per-run materialized stage outputs.

### Feature store

Current feature consumers read parquet outputs under:
- `data/feature_store/<feature>/<exchange>/<symbol>.parquet`
- `data/feature_store/all_symbols/sector_rs.parquet`
- `data/feature_store/all_symbols/stock_vs_sector.parquet`
- `data/feature_store/delivery/<exchange>/<symbol>.parquet`

The research domain mirrors this under `data/research/feature_store/`.

### Raw caches and reports

Current cache and report paths include:
- `data/raw/NSE_EQ/`
- `data/raw/NSE_MTO/`
- `data/raw/NSE_security_delivery/`
- `reports/data_repairs/`
- `reports/quantstats/`
- `reports/research/`
- `reports/ml_rank_overlay.csv` and dated ML overlay files

## Lineage model

Lineage is split across three layers.

Market-write lineage in `ohlcv.duckdb`:
- `_catalog` carries the latest trust columns per row
- `_catalog_provenance` records ingested or repaired rows as append-only evidence
- `_catalog_quarantine` records unresolved, observed, and resolved trust exceptions

Pipeline lineage in `control_plane.duckdb`:
- `pipeline_run` stores top-level run metadata and status
- `pipeline_stage_run` stores per-stage attempts and outcomes
- `pipeline_artifact` stores artifact URI, type, hash, row count, and metadata

Delivery and execution lineage:
- `publisher_delivery_log` records publish attempts, retries, duplicates, and failures
- `execution.duckdb` stores persistent paper orders and fills

## Trust and quarantine model

Trust columns added to market tables include:
- `provider`
- `provider_priority`
- `validation_status`
- `validated_against`
- `ingest_run_id`
- `repair_batch_id`

Trust support tables:
- `_catalog_provenance`: full per-row trust log
- `_catalog_quarantine`: per-symbol, per-date exception rows with reason, status, run reference, and notes

Current quarantine statuses:
- `active`: unresolved and trust-sensitive
- `observed`: retained for visibility but treated as non-blocking historical or contextual state
- `resolved`: previously active but cleared by later ingest or repair

Current trust summary fields include:
- `status`
- `latest_trade_date`
- `latest_validated_date`
- `provider_counts_by_date`
- `active_quarantined_dates`
- `active_quarantined_symbols`
- `latest_quarantined_symbols`
- `latest_quarantined_symbol_ratio`
- `fallback_ratio_latest`
- `primary_ratio_latest`
- `unknown_ratio_latest`
- `latest_provider_stats`
- `latest_repair_batch`

## Execution storage model

The execute stage writes both stage artifacts and persistent execution rows.

Artifacts:
- `trade_actions.csv`
- `executed_orders.csv`
- `executed_fills.csv`
- `positions.csv`
- `execute_summary.json`

Persistent tables:
- `execution_order`: latest order state per `order_id`
- `execution_fill`: fill-level records keyed by `fill_id`
- `execution_trade_note`: journal-style trade notes

This store is paper-trading state for the current pipeline stage. It is not a live broker ledger.
