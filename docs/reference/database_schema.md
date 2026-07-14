# Database Schema

- **Purpose:** Canonical reference for every DuckDB table the system reads or writes — file location, owning stage, columns, indexes.
- **Audience:** Operator, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/pipeline/migrations/*.sql` (17 files), `src/ai_trading_system/research/perf_tracker/schema.py`, `src/ai_trading_system/domains/execution/store.py`, `src/ai_trading_system/platform/db/paths.py`, `src/ai_trading_system/domains/ingest/repository.py`.

---

## DuckDB store layout

| File | Owner stage / domain | Schema source | Notes |
|---|---|---|---|
| `data/ohlcv.duckdb` | Ingest stage (operational domain) | `domains/ingest/repository.py::initialize_ingest_duckdb` (+ `trust.py`, `delivery.py`, `masterdata.py`) | Catalog of OHLCV bars, snapshots, parquet pointers, masterdata, trust state. |
| `data/control_plane.duckdb` | Pipeline orchestrator (writes governance) + several read paths | `src/ai_trading_system/pipeline/migrations/*.sql` (17 files, applied by `pipeline/registry.py` — see `registry.py:300`) | Run lifecycle, DQ, artifacts, model registry, monitoring, optimizer, universe, pattern cache. |
| `data/execution.duckdb` | Execute stage (`domains/execution/service.py`) | `domains/execution/store.py::ExecutionStore._init_db` | Orders, fills, trade journal, stops, drawdown snapshots. **Created by `ExecutionStore`** — default path is `<project_root>/data/execution.duckdb` (`store.py:29`). |
| `data/research.duckdb` | Perf tracker stage + research perf-tracker API endpoints | `research/perf_tracker/schema.py::RANK_COHORT_DDL` | `rank_cohort_performance`. Path resolved via `research_db_path()` -> `paths.root_dir / "research.duckdb"` (`schema.py:55-60`). |
| `data/research_ohlcv.duckdb` | Research-domain OHLCV (selected when `DATA_DOMAIN=research`) | `domains/ingest/repository.py` (same DDL as operational) | Isolation per `platform/db/paths.py:111`: ohlcv file name is `research_ohlcv.duckdb` for the research domain. |

> **Correction vs older docs.** Documentation prior to 2026-05-16 sometimes asserted that execution tables live in `data/control_plane.duckdb`. **This is wrong.** `ExecutionStore` writes to `data/execution.duckdb` by default (`domains/execution/store.py:29`). The control-plane database does *not* contain `execution_order`/`execution_fill` rows.

---

## `data/ohlcv.duckdb` — operational OHLCV catalog

Schema source: `src/ai_trading_system/domains/ingest/repository.py:102-220` (`initialize_ingest_duckdb`) plus extensions in `domains/ingest/trust.py`, `delivery.py`, `masterdata.py`. Sequence objects (`snapshot_id_seq`, `_snap_id_seq`, `_hist_id_seq`, `_pfile_id_seq`) are created alongside the tables.

### Table: `_catalog`
- **DDL source:** `domains/ingest/repository.py:112-130`
- **Database:** `data/ohlcv.duckdb`
- **Owner stage/domain:** ingest
- **Purpose:** Current OHLCV bars per symbol/exchange/timestamp.

| Column | Type | Notes |
|---|---|---|
| symbol_id | TEXT | PK part. |
| security_id | TEXT | |
| exchange | TEXT | PK part. |
| timestamp | TIMESTAMP | PK part. |
| open, high, low, close | DOUBLE | |
| volume | BIGINT | |
| parquet_file | TEXT | Pointer to backing parquet. |
| ingestion_version | BIGINT | `DEFAULT nextval('snapshot_id_seq')`. |
| ingestion_ts | TIMESTAMP | `DEFAULT CURRENT_TIMESTAMP`. |

Indexes: `idx_catalog_symbol(symbol_id, exchange)` (`repository.py:202-205`).

### Table: `_snapshots`
- **DDL source:** `domains/ingest/repository.py:138-150`
- **Database:** `data/ohlcv.duckdb`
- **Owner stage/domain:** ingest

| Column | Type | Notes |
|---|---|---|
| snapshot_id | BIGINT | PK; default `nextval('_snap_id_seq')`. |
| snapshot_ts | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| symbols_processed | INTEGER | |
| rows_written | BIGINT | |
| from_date, to_date | TEXT | |
| status | TEXT | Default `'running'`. |
| note | TEXT | |

Indexes: `idx_snapshots_ts(snapshot_ts)`.

### Table: `_catalog_history`
- **DDL source:** `domains/ingest/repository.py:159-178`
- **Database:** `data/ohlcv.duckdb`
- **Purpose:** Append-only history of `_catalog` rows superseded by re-ingest.

| Column | Type | Notes |
|---|---|---|
| hist_id | BIGINT | PK; default `nextval('_hist_id_seq')`. |
| snapshot_id | BIGINT | |
| symbol_id, security_id, exchange | TEXT | |
| timestamp | TIMESTAMP | |
| open, high, low, close | DOUBLE | |
| volume | BIGINT | |
| parquet_file | TEXT | |
| ingestion_version | BIGINT | |
| ingestion_ts | TIMESTAMP | |
| archived_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |

### Table: `_parquet_files`
- **DDL source:** `domains/ingest/repository.py:187-197`

| Column | Type | Notes |
|---|---|---|
| pfile_id | BIGINT | PK; default `nextval('_pfile_id_seq')`. |
| parquet_file | TEXT | `UNIQUE`. |
| symbol_id, exchange | TEXT | |
| rows_count | BIGINT | |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| active | BOOLEAN | Default `TRUE`. |

### Trust + masterdata tables (same database)

Additional tables co-located in `ohlcv.duckdb` and ensured at startup. Read each cited file for the full column list.

- `_catalog_provenance` (`domains/ingest/trust.py:201`)
- `_catalog_quarantine` (`trust.py:255`)
- `_symbol_state_overrides` (`trust.py:284`)
- `_index_metadata` (`trust.py:516`)
- `_index_catalog` (`trust.py:538`)
- `sector_to_index` (`trust.py:562`)
- `_delivery` (`domains/ingest/delivery.py:124`)
- `symbols`, `sectors`, `sector_constituents`, `sector_mapping` (`domains/ingest/masterdata.py:46/85/94/105`)

---

## `data/control_plane.duckdb` — pipeline governance

Schema source: SQL migrations under `src/ai_trading_system/pipeline/migrations/`, applied by the registry initializer. Migrations are additive (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`).

### Table: `pipeline_run`
- **DDL source:** `pipeline/migrations/001_pipeline_governance.sql`
- **Database:** `data/control_plane.duckdb`
- **Owner:** orchestrator
- **Purpose:** Top-level run lifecycle record.

| Column | Type | Notes |
|---|---|---|
| run_id | VARCHAR | PK. |
| pipeline_name | VARCHAR | NOT NULL. |
| run_date | DATE | NOT NULL. |
| trigger | VARCHAR | |
| status | VARCHAR | NOT NULL. |
| current_stage | VARCHAR | |
| started_at, ended_at | TIMESTAMP | |
| error_class, error_message | VARCHAR | |
| metadata_json | VARCHAR | |

### Table: `pipeline_stage_run`
- **DDL source:** `migrations/001_pipeline_governance.sql`

| Column | Type | Notes |
|---|---|---|
| stage_run_id | VARCHAR | PK. |
| run_id, stage_name | VARCHAR | NOT NULL. |
| attempt_number | INTEGER | NOT NULL. |
| status | VARCHAR | NOT NULL. |
| started_at, ended_at | TIMESTAMP | |
| error_class, error_message | VARCHAR | |
| metadata_json | VARCHAR | |

Index: `idx_pipeline_stage_attempt(run_id, stage_name, attempt_number)` UNIQUE.

### Table: `pipeline_artifact`
- **DDL source:** `migrations/001_pipeline_governance.sql`, lifecycle columns in `031_artifact_lifecycle.sql`

| Column | Type | Notes |
|---|---|---|
| artifact_id | VARCHAR | PK. |
| run_id, stage_name | VARCHAR | NOT NULL. |
| attempt_number | INTEGER | NOT NULL. |
| artifact_type | VARCHAR | NOT NULL. |
| uri | VARCHAR | NOT NULL. |
| content_hash | VARCHAR | |
| row_count | BIGINT | |
| created_at | TIMESTAMP | |
| metadata_json | VARCHAR | |
| lifecycle_status | VARCHAR | `written`, `dq_passed`, or `promoted`. |
| dq_passed_at, promoted_at | TIMESTAMP | Lifecycle transition timestamps. |
| uri | VARCHAR | NOT NULL. |
| content_hash | VARCHAR | |
| row_count | BIGINT | |
| created_at | TIMESTAMP | |
| metadata_json | VARCHAR | |

### Table: `dq_rule`
- **DDL source:** `migrations/001_pipeline_governance.sql` + `002_post_refactor_hardening.sql`

| Column | Type | Notes |
|---|---|---|
| rule_id | VARCHAR | PK. |
| stage_name, dataset_name | VARCHAR | NOT NULL. |
| severity | VARCHAR | NOT NULL. |
| description, owner | VARCHAR | |
| enabled | BOOLEAN | Default TRUE. |
| rollout_date | DATE | |
| rule_sql | VARCHAR | Added by 002. |
| active | BOOLEAN | Added by 002, default TRUE. |

### Table: `dq_result`
- **DDL source:** `migrations/001_pipeline_governance.sql`

| Column | Type | Notes |
|---|---|---|
| result_id | VARCHAR | PK. |
| run_id, stage_name, rule_id, severity, status | VARCHAR | NOT NULL. |
| failed_count | BIGINT | Default 0. |
| message, sample_uri | VARCHAR | |
| created_at | TIMESTAMP | |

### Table: `model_registry`
- **DDL source:** `migrations/001` + `002`

| Column | Type | Notes |
|---|---|---|
| model_id | VARCHAR | PK. |
| model_name, model_version, artifact_uri, feature_schema_hash, training_snapshot_ref | VARCHAR | NOT NULL. |
| status | VARCHAR | Default `'registered'`. |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| metadata_json | VARCHAR | |
| train_snapshot_ref | VARCHAR | Added by 002. |
| approval_status | VARCHAR | Added by 002, default `'pending'`. |

### Table: `model_eval`
- **DDL source:** `migrations/001`

| Column | Type | Notes |
|---|---|---|
| eval_id | VARCHAR | PK. |
| model_id | VARCHAR | NOT NULL. |
| evaluated_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| metric_name | VARCHAR | NOT NULL. |
| metric_value | DOUBLE | NOT NULL. |
| dataset_ref, notes | VARCHAR | |

### Table: `model_deployment`
- **DDL source:** `migrations/001`

| Column | Type | Notes |
|---|---|---|
| deployment_id | VARCHAR | PK. |
| model_id, environment, status | VARCHAR | NOT NULL. |
| approved_by | VARCHAR | |
| approved_at, deployed_at | TIMESTAMP | |
| rollback_model_id, notes | VARCHAR | |

### Table: `publisher_delivery_log`
- **DDL source:** `migrations/002_post_refactor_hardening.sql`

| Column | Type | Notes |
|---|---|---|
| delivery_log_id | VARCHAR | PK. |
| run_id, stage_name, channel, artifact_uri, dedupe_key, status | VARCHAR | NOT NULL. |
| artifact_hash | VARCHAR | |
| attempt_number | INTEGER | NOT NULL. |
| external_message_id, external_report_id, error_message | VARCHAR | |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| metadata_json | VARCHAR | |

Index: `idx_publisher_delivery_dedupe(dedupe_key, status)`.

### Table: `pipeline_alert`
- **DDL source:** `migrations/003_preflight_and_alerts.sql`

| Column | Type | Notes |
|---|---|---|
| alert_id | VARCHAR | PK. |
| run_id, alert_type, severity, message | VARCHAR | NOT NULL (except severity/type). |
| stage_name | VARCHAR | Nullable. |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |

### Table: `model_shadow_prediction`
- **DDL source:** `migrations/004_shadow_monitoring.sql`

| Column | Type | Notes |
|---|---|---|
| prediction_id | VARCHAR | PK. |
| prediction_date | DATE | NOT NULL. |
| symbol_id, exchange | VARCHAR | NOT NULL. |
| close, technical_score | DOUBLE | |
| technical_rank | INTEGER | |
| technical_top_decile | BOOLEAN | |
| ml_5d_prob, ml_20d_prob, blend_5d_score, blend_20d_score | DOUBLE | |
| ml_5d_rank, ml_20d_rank, blend_5d_rank, blend_20d_rank | INTEGER | |
| ml_5d_top_decile, ml_20d_top_decile, blend_5d_top_decile, blend_20d_top_decile | BOOLEAN | |
| artifact_uri | VARCHAR | |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| metadata_json | VARCHAR | |

Index: `idx_shadow_prediction_date_symbol(prediction_date, symbol_id, exchange)` UNIQUE.

### Table: `model_shadow_outcome`
- **DDL source:** `migrations/004_shadow_monitoring.sql`

| Column | Type | Notes |
|---|---|---|
| outcome_id | VARCHAR | PK. |
| prediction_id | VARCHAR | NOT NULL. |
| prediction_date | DATE | NOT NULL. |
| symbol_id, exchange | VARCHAR | NOT NULL. |
| horizon | INTEGER | NOT NULL. |
| future_date | DATE | |
| realized_return | DOUBLE | |
| hit | BOOLEAN | |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |

Index: `idx_shadow_outcome_prediction_horizon(prediction_id, horizon)` UNIQUE.

### Table: `dataset_registry`
- **DDL source:** `migrations/005_ml_datasets.sql`

| Column | Type | Notes |
|---|---|---|
| dataset_id | VARCHAR | PK. |
| dataset_ref, dataset_uri, data_domain | VARCHAR | NOT NULL. |
| engine_name, feature_schema_version, feature_schema_hash, label_version, target_column | VARCHAR | |
| from_date, to_date | DATE | |
| horizon | INTEGER | |
| row_count, symbol_count | BIGINT | |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| metadata_json | VARCHAR | |

Index: `idx_dataset_registry_ref(dataset_ref)` UNIQUE.

### Table: `prediction_log`
- **DDL source:** `migrations/006_prediction_monitoring.sql`

| Column | Type | Notes |
|---|---|---|
| prediction_log_id | VARCHAR | PK. |
| prediction_date | DATE | NOT NULL. |
| model_id, model_name, model_version | VARCHAR | |
| deployment_mode | VARCHAR | NOT NULL. |
| horizon | INTEGER | NOT NULL. |
| symbol_id, exchange | VARCHAR | NOT NULL. |
| score, probability | DOUBLE | |
| prediction, rank | INTEGER | |
| artifact_uri | VARCHAR | |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| metadata_json | VARCHAR | |

Index: `idx_prediction_log_scope(prediction_date, deployment_mode, horizon, symbol_id, exchange)` UNIQUE.

### Table: `shadow_eval`
- **DDL source:** `migrations/006_prediction_monitoring.sql`

| Column | Type | Notes |
|---|---|---|
| shadow_eval_id | VARCHAR | PK. |
| prediction_log_id | VARCHAR | NOT NULL. |
| prediction_date | DATE | NOT NULL. |
| model_id | VARCHAR | |
| deployment_mode | VARCHAR | NOT NULL. |
| horizon | INTEGER | NOT NULL. |
| symbol_id, exchange | VARCHAR | NOT NULL. |
| future_date | DATE | |
| realized_return | DOUBLE | |
| hit | BOOLEAN | |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| metadata_json | VARCHAR | |

Index: `idx_shadow_eval_prediction_horizon(prediction_log_id, horizon)` UNIQUE.

### Table: `drift_metric`
- **DDL source:** `migrations/007_model_guardrails.sql` (extended by `010_factor_monitoring.sql` documentation note — no schema change)

| Column | Type | Notes |
|---|---|---|
| drift_metric_id | VARCHAR | PK. |
| measured_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| prediction_date | DATE | |
| model_id, deployment_mode | VARCHAR | |
| horizon | INTEGER | |
| metric_name | VARCHAR | NOT NULL. |
| metric_value | DOUBLE | NOT NULL. |
| threshold_value | DOUBLE | |
| status | VARCHAR | NOT NULL. |
| metadata_json | VARCHAR | |

Index: `idx_drift_metric_scope(model_id, deployment_mode, horizon, prediction_date, metric_name)`.

### Table: `promotion_gate_result`
- **DDL source:** `migrations/007_model_guardrails.sql`

| Column | Type | Notes |
|---|---|---|
| gate_result_id | VARCHAR | PK. |
| model_id | VARCHAR | NOT NULL. |
| evaluated_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| gate_name, status | VARCHAR | NOT NULL. |
| metric_value, threshold_value | DOUBLE | |
| metadata_json | VARCHAR | |

Index: `idx_promotion_gate_model(model_id, evaluated_at, gate_name)`.

### Table: `data_repair_run`
- **DDL source:** `migrations/008_data_trust.sql` (+ `012_data_repair_run_index_fix.sql` rebuilds the index)

| Column | Type | Notes |
|---|---|---|
| repair_run_id | VARCHAR | PK. |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| from_date, to_date | DATE | |
| exchange | VARCHAR | |
| status | VARCHAR | NOT NULL. |
| repaired_row_count, unresolved_symbol_count, unresolved_date_count | BIGINT | Default 0. |
| report_uri | VARCHAR | |
| metadata_json | VARCHAR | |

Index: `idx_data_repair_run_created(exchange, created_at)`.

### Table: `operator_task`
- **DDL source:** `migrations/009_operator_tasks.sql`

| Column | Type | Notes |
|---|---|---|
| task_id | VARCHAR | PK. |
| task_type, label, status | VARCHAR | NOT NULL. |
| started_at, finished_at | TIMESTAMP | |
| result_json, error, metadata_json | VARCHAR | |
| created_at, updated_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |

Indexes: `idx_operator_task_started_at(started_at)`, `idx_operator_task_status(status)`.

### Table: `operator_task_log`
- **DDL source:** `migrations/009_operator_tasks.sql`

| Column | Type | Notes |
|---|---|---|
| task_id | VARCHAR | PK part. |
| log_order | BIGINT | PK part. |
| message | VARCHAR | NOT NULL. |
| created_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |

### Table: `pattern_cache`
- **DDL source:** `migrations/011_pattern_cache.sql`

| Column | Type | Notes |
|---|---|---|
| symbol_id | VARCHAR | PK part. |
| exchange | VARCHAR | PK part, default `'NSE'`. |
| pattern_family, pattern_state | VARCHAR | PK part. |
| stage2_score | DOUBLE | |
| stage2_label | VARCHAR | |
| signal_date | DATE | PK part. |
| breakout_level, watchlist_trigger_level, invalidation_price, pattern_score, setup_quality | DOUBLE | |
| width_bars | INTEGER | |
| scanned_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |
| scan_run_id, payload_json | VARCHAR | |

Indexes: `idx_pattern_cache_signal_date(signal_date)`, `idx_pattern_cache_stage2(stage2_score, pattern_state)`.

### Table: `events_enrichment_log`
- **DDL source:** `migrations/013_events_enrichment_log.sql`

| Column | Type | Notes |
|---|---|---|
| run_id, symbol, trigger_type | TEXT | Composite PK. |
| as_of_date | DATE | |
| trigger_strength | DOUBLE | |
| trigger_metadata_json, event_hashes_json | TEXT | |
| materiality_label | TEXT | low / medium / high / critical. |
| top_category | TEXT | |
| event_count | INTEGER | Default 0. |
| suppressed | BOOLEAN | Default FALSE. |
| suppress_reason, severity | TEXT | |
| created_at | TIMESTAMP | Default `current_timestamp`. |

Indexes: `idx_events_enrichment_log_symbol`, `..._as_of`, `..._severity`.

### Table: `watchlist_candidate_history`
- **DDL source:** `migrations/014_watchlist_candidate_history.sql`

| Column | Type | Notes |
|---|---|---|
| watchlist_date | DATE | PK part. |
| run_id | TEXT | PK part. |
| attempt_number | INTEGER | NOT NULL. |
| symbol_id | TEXT | PK part. |
| rank, previous_rank, rank_change, days_on_watchlist | INTEGER | `days_on_watchlist` default 1. |
| is_new_entry | BOOLEAN | Default TRUE. |
| sector, sector_status, stage, momentum_tags, setup_label | TEXT | |
| watchlist_score, composite_score | DOUBLE | |
| action, technical_catalyst_summary, catalyst_tags, catalyst_confidence | TEXT | |
| bull_case, risk_flags, watchlist_reason, data_trust_status, artifact_uri, metadata_json | TEXT | |
| created_at | TIMESTAMP | Default `current_timestamp`. |

Indexes: `idx_watchlist_candidate_history_symbol`, `..._date`, `..._score`.

### Table: `strategy_rule_pack`
- **DDL source:** `migrations/015_strategy_optimizer.sql`

| Column | Type | Notes |
|---|---|---|
| rule_pack_id | TEXT | SHA256 of canonical YAML; uniqueness via `uq_strategy_rule_pack_id`. |
| parent_rule_pack_id | TEXT | |
| strategy_id | TEXT | NOT NULL. |
| version | INTEGER | Default 1. |
| rule_yaml, rule_json | TEXT | NOT NULL. |
| lifecycle_status | TEXT | Default `'draft'` (draft -> backtested -> walkforward_passed -> shadow -> paper_approved -> production_candidate -> active). |
| description | TEXT | |
| created_at | TIMESTAMP | Default `current_timestamp`. |

Indexes: `uq_strategy_rule_pack_id(rule_pack_id)` UNIQUE, `idx_strategy_rule_pack_strategy(strategy_id)`, `idx_strategy_rule_pack_status(lifecycle_status)`.

### Table: `strategy_optimization_run`
- **DDL source:** `migrations/015_strategy_optimizer.sql` (+ `018_optimizer_study_storage_uri.sql` adds `study_storage_uri` for resumable studies)

| Column | Type | Notes |
|---|---|---|
| optimization_run_id | TEXT | Uniqueness via `uq_strategy_optimization_run_id`. |
| recipe_name, strategy_id, baseline_rule_pack_id | TEXT | NOT NULL. |
| from_date, to_date | DATE | NOT NULL. |
| seed, max_trials | INTEGER | NOT NULL. |
| status | TEXT | pending / running / completed / failed / cancelled. |
| champion_rule_pack_id | TEXT | |
| recipe_json | TEXT | NOT NULL. |
| error | TEXT | |
| started_at | TIMESTAMP | Default `current_timestamp`. |
| completed_at | TIMESTAMP | |
| study_storage_uri | TEXT | Path to the per-run Optuna `JournalStorage` file (added by 018; e.g. `data/optuna/<run_id>.log`). `NULL` for pre-Wave-5a rows. Consumed by `ai-trading-optimize resume <run_id>`. |

Indexes: `uq_strategy_optimization_run_id` UNIQUE, `idx_strategy_optimization_run_strategy`, `idx_strategy_optimization_run_status`.

### Table: `strategy_iteration_result`
- **DDL source:** `migrations/015_strategy_optimizer.sql` (+ `017_strategy_optimizer_rename.sql` adds benchmark columns and backfills)

| Column | Type | Notes |
|---|---|---|
| optimization_run_id | TEXT | NOT NULL. |
| iteration | INTEGER | Optuna trial number. |
| rule_pack_id | TEXT | NOT NULL. |
| fold_index | INTEGER | -1 for aggregate. |
| fold_role | TEXT | train / val / aggregate. |
| fitness, cagr, sharpe, sortino, max_drawdown_pct, win_rate, profit_factor, trades_per_year, total_return_pct, nifty_return_pct | DOUBLE | `nifty_return_pct` is legacy; superseded by `benchmark_return_pct` from migration 017. |
| trade_count | INTEGER | |
| accepted | BOOLEAN | |
| rejection_reason | TEXT | |
| created_at | TIMESTAMP | Default `current_timestamp`. |
| benchmark_return_pct | DOUBLE | Added by 017. |
| benchmark_symbol | TEXT | Added by 017. |

Indexes: `uq_strategy_iteration_result(optimization_run_id, iteration, fold_index)` UNIQUE, `idx_strategy_iteration_result_pack(rule_pack_id)`.

### Table: `strategy_backtest_trade`
- **DDL source:** `migrations/015_strategy_optimizer.sql`

| Column | Type | Notes |
|---|---|---|
| optimization_run_id, rule_pack_id | TEXT | NOT NULL. |
| iteration, fold_index | INTEGER | NOT NULL. |
| symbol_id, exchange | TEXT | NOT NULL. |
| entry_date | DATE | NOT NULL. |
| entry_price | DOUBLE | NOT NULL. |
| entry_reason | TEXT | |
| exit_date | DATE | |
| exit_price | DOUBLE | |
| exit_reason | TEXT | |
| bars_held | INTEGER | |
| pnl, pnl_pct, score_at_entry | DOUBLE | |
| sector | TEXT | |
| rank_at_entry | INTEGER | |

Index: `idx_strategy_backtest_trade_run(optimization_run_id, iteration)`.

### Table: `_universe_membership`
- **DDL source:** `migrations/016_universe_index.sql`

| Column | Type | Notes |
|---|---|---|
| rebalance_date | DATE | PK part. |
| symbol_id | TEXT | PK part. |
| rank_by_turnover | INTEGER | NOT NULL. |
| median_turnover | DOUBLE | |
| recent_days | INTEGER | |
| sparse_history | BOOLEAN | Default FALSE. |
| created_at | TIMESTAMP | Default `current_timestamp`. |

Index: `idx_universe_membership_rebalance(rebalance_date)`.

### Table: `_universe_index_diagnostics`
- **DDL source:** `migrations/016_universe_index.sql`

| Column | Type | Notes |
|---|---|---|
| index_code | TEXT | PK part. |
| date | DATE | PK part. |
| rebalance_date | DATE | NOT NULL. |
| n_members, n_used, n_missing | INTEGER | NOT NULL. |
| used_ratio, daily_return | DOUBLE | |
| index_level | DOUBLE | NOT NULL. |
| quality_flag | TEXT | Default `'ok'` (ok / low_coverage / sparse_membership / gap). |
| created_at | TIMESTAMP | Default `current_timestamp`. |

Indexes: `idx_universe_index_diagnostics_date(date)`, `idx_universe_index_diagnostics_quality(quality_flag)`.

### Migration 010 — placeholder

`010_factor_monitoring.sql` is a documentation-only no-op (`SELECT 1`). `drift_metric` already stores factor-monitoring metrics via `metadata_json`.

---

## `data/execution.duckdb` — execute stage store

Schema source: `src/ai_trading_system/domains/execution/store.py::ExecutionStore._init_db` (`store.py:36-132`). Tables created on first `ExecutionStore` instantiation.

### Table: `execution_order`
- **DDL source:** `domains/execution/store.py:41-63`

| Column | Type | Notes |
|---|---|---|
| order_id | TEXT | PK. |
| broker, symbol_id, side, exchange, order_type, product_type, validity, status | TEXT | NOT NULL. |
| quantity | INTEGER | NOT NULL. |
| submitted_at, updated_at | TIMESTAMP | NOT NULL. |
| correlation_id, broker_order_id | TEXT | |
| limit_price, stop_price, requested_price, avg_fill_price | DOUBLE | |
| filled_quantity | INTEGER | NOT NULL. |
| metadata_json | TEXT | |

### Table: `execution_submission_intent`
- **DDL source:** `domains/execution/store.py::_init_db`

| Column | Type | Notes |
|---|---|---|
| correlation_id | TEXT | PK; durable idempotency key. |
| payload_hash | TEXT | Hash of broker-relevant normalized order fields. |
| status | TEXT | `reserved`, `completed`, or `reconciliation_required`. |
| order_id | TEXT | Linked only after the original outcome is persisted. |
| created_at, updated_at | TIMESTAMP | NOT NULL. |
| last_error | TEXT | Diagnostic reason for unknown outcomes. |
| payload_json | TEXT | Normalized payload used for conflict detection/reconciliation. |

### Table: `execution_fill`
- **DDL source:** `domains/execution/store.py:67-80`

| Column | Type | Notes |
|---|---|---|
| fill_id | TEXT | PK. |
| order_id, broker, symbol_id, side, exchange | TEXT | NOT NULL. |
| quantity | INTEGER | NOT NULL. |
| price | DOUBLE | NOT NULL. |
| filled_at | TIMESTAMP | NOT NULL. |
| broker_fill_id, metadata_json | TEXT | |

### Table: `execution_trade_note`
- **DDL source:** `domains/execution/store.py:84-97`

| Column | Type | Notes |
|---|---|---|
| trade_ref | TEXT | PK. |
| symbol_id, exchange | TEXT | |
| thesis, setup_note, exit_note, lesson_learned, tags | TEXT | |
| created_at, updated_at | TIMESTAMP | NOT NULL. |
| metadata_json | TEXT | |

### Table: `execution_position_stop`
- **DDL source:** `domains/execution/store.py:101-115` (partial in this snapshot — see file for the remaining columns)

| Column | Type | Notes |
|---|---|---|
| position_key | TEXT | PK. |
| symbol_id, exchange | TEXT | NOT NULL. |
| quantity | INTEGER | NOT NULL. |
| entry_price, stop_price, atr_multiplier | DOUBLE | NOT NULL. |
| status | TEXT | Default `'ACTIVE'`. |
| created_at, updated_at | TIMESTAMP | NOT NULL. |
| metadata_json | TEXT | Stop method and originating signal context. |

### Table: `execution_drawdown`
- **DDL source:** `domains/execution/store.py:118-129`

| Column | Type | Notes |
|---|---|---|
| run_id | TEXT | PK part. |
| timestamp | TIMESTAMP | PK part. |
| snapshot_type | TEXT | PK part, default `'intraday'`. |
| portfolio_value, peak_value, drawdown_pct | DOUBLE | NOT NULL. |
| portfolio_heat | DOUBLE | |
| metadata_json | TEXT | |

---

## `data/research.duckdb` — perf tracker

Schema source: `src/ai_trading_system/research/perf_tracker/schema.py::RANK_COHORT_DDL` (`schema.py:17-47`). Ensured idempotently on every non-read-only connect via `ensure_schema()`.

### Table: `rank_cohort_performance`
- **DDL source:** `research/perf_tracker/schema.py:17-47`
- **Database:** `data/research.duckdb` (path from `research_db_path()` -> `paths.root_dir / "research.duckdb"`, `schema.py:55-60`).
- **Owner:** `perf_tracker` pipeline stage (writes), `routes/perf_tracker.py` (reads).

| Column | Type | Notes |
|---|---|---|
| run_date | DATE | PK part. |
| symbol_id | VARCHAR | PK part. |
| exchange | VARCHAR | PK part. |
| rank_position | INTEGER | |
| composite_score, composite_score_adjusted | DOUBLE | |
| rank_mode | VARCHAR | |
| watchlist_bucket | VARCHAR | Values include `TRIGGERED_TODAY`, `CORE_MOMENTUM`, `EARLY_STAGE2`, `AVOID_WEAK_CONFIRMATION` (see `routes/perf_tracker.py:170-176`). |
| config_id | VARCHAR | Nullable until Phase 1 ships. |
| fwd_5d_return, fwd_10d_return, fwd_20d_return, fwd_60d_return | DOUBLE | |
| fwd_5d_matured_at, fwd_10d_matured_at, fwd_20d_matured_at, fwd_60d_matured_at | DATE | |
| factor_rs, factor_vol, factor_trend, factor_prox, factor_deliv, factor_sector, factor_momentum_accel | DOUBLE | Per-factor scores for IC tracking. |
| sector_name | VARCHAR | |
| inserted_at | TIMESTAMP | Default `CURRENT_TIMESTAMP`. |

Index: `idx_rank_cohort_date(run_date)` (`schema.py:50-52`).

---

## `data/research_ohlcv.duckdb` — research-domain OHLCV

Schema is identical to `data/ohlcv.duckdb` (created by the same `initialize_ingest_duckdb` code). Selection happens at path-resolution time in `platform/db/paths.py:111` when `DATA_DOMAIN=research`. No separate migrations.

---

## Notes on migration application

- Control-plane DDL is applied by the registry initializer at `src/ai_trading_system/pipeline/registry.py:300` (`self.db_path = ... / "data" / "control_plane.duckdb"`); the executor reads the 17 SQL files from `pipeline/migrations/` in lexicographic order.
- DuckDB does not enforce foreign keys; relationships above are by convention and are not constrained at the database level.
- Several control-plane tables are extended by later migrations (002 → `dq_rule`, `model_registry`; 012 → `data_repair_run` index; 017 → `strategy_iteration_result`). Treat the final shape as the merge of all referenced migrations.
