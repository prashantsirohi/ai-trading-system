# Pipeline

## Canonical stage order

`ai_trading_system.pipeline.orchestrator` supports this stage order:
1. `ingest`
2. `features`
3. `rank`
4. `execute`
5. `publish`

Any run may execute a subset, but the order is fixed.

## Entrypoints and default stage sets

Primary orchestrator:
- `python -m ai_trading_system.pipeline.orchestrator`

Compatibility wrapper:
- `python -m ai_trading_system.pipeline.daily_pipeline`
- delegates into the orchestrator with wrapper-specific defaults

Default stage sets differ by surface:
- CLI orchestrator default: `ingest,features,rank,events,execute,insight,publish`
- Daily wrapper default: `ingest,features,rank,events,execute,insight,publish`
- FastAPI pipeline request default: `ingest,features,rank,publish`
- React V2 pipeline action: `ingest,features,rank,publish`
- CLI and wrapper canary with the untouched default stage string: `ingest,features,rank`

Do not document one universal “full pipeline” default without naming the invoking surface.

## Shared execution model

For every stage attempt, the orchestrator:
- creates or updates `pipeline_run` and `pipeline_stage_run` rows in `data/control_plane.duckdb`
- creates a per-attempt output directory under `data[/research]/pipeline_runs/<run_id>/<stage>/attempt_<n>/`
- records materialized artifacts in `pipeline_artifact`
- runs DQ after `ingest`, `features`, and `rank`
- stops downstream stages on critical DQ failures

Publish failures are handled differently:
- `publish` can fail after upstream stages succeed
- the run status becomes `completed_with_publish_errors`

## Stage contracts

### `ingest`

Owner:
- `ai_trading_system.pipeline.stages.ingest`
- primary implementation path in `ai_trading_system.domains.ingest.daily_update_runner`

Inputs:
- domain-selected OHLCV store
- params including `batch_size`, `nse_primary`, `symbol_limit`, `include_delivery`, validation thresholds, and trust/DQ thresholds

Current default behavior in the orchestrated pipeline:
- runs `ai_trading_system.domains.ingest.daily_update_runner.run(..., symbols_only=True, nse_primary=True)`
- ingests NSE bhavcopy rows first
- fetches yfinance only for missing business dates
- records provider and validation lineage on market rows
- updates quarantine records for unresolved symbol-date gaps
- runs post-ingest reference-close validation unless disabled
- runs delivery collection inside the ingest stage unless disabled

Primary artifact:
- `ingest_summary.json`

Important output fields:
- `updated_symbols`
- `catalog_rows`
- `symbol_count`
- `latest_timestamp`
- `providers_used`
- `provider_counts_by_date`
- `nse_bhavcopy_dates`
- `yfinance_fallback_dates`
- `unresolved_dates`
- `quarantined_row_count`
- `trust_summary`
- `bhavcopy_validation_*`
- `delivery_*`

Block conditions:
- preflight failure when preflight is enabled
- reference validation failure when validation is required
- critical ingest DQ failure after stage completion

Retry behavior:
- no generic internal retry loop
- operator reruns the stage or pipeline
- one automatic repair path exists for unresolved-date DQ failure when `--auto-repair-quarantine` is enabled

### `features`

Owner:
- `ai_trading_system.pipeline.stages.features`
- compute path in `ai_trading_system.domains.ingest.daily_update_runner` and `ai_trading_system.domains.features.feature_store`

Inputs:
- current `_catalog`
- optional `updated_symbols` from the ingest artifact
- `full_rebuild`, `feature_tail_bars`, and `data_domain`

Behavior:
- runs `collectors.daily_update_runner.run(..., features_only=True)`
- computes technical features into parquet-backed feature storage
- operational mode defaults to incremental tail recompute unless `full_rebuild=True`
- research mode forces full rebuild behavior
- recomputes sector-strength artifacts through `features.compute_sector_rs.compute_all_symbols_rs`
- records a snapshot row in `_snapshots`

Primary artifact:
- `feature_snapshot.json`

Important output fields:
- `snapshot_id`
- `feature_rows`
- `feature_registry_entries`
- `feature_mode`
- `target_symbol_count`

Block conditions:
- missing feature snapshot id
- zero computed feature rows
- trust-window quarantine breadth above configured thresholds
- any other critical DQ failure

Retry behavior:
- operator rerun only

### `rank`

Owner:
- `ai_trading_system.pipeline.stages.rank`
- ranking core in `analytics/ranker.py`

Inputs:
- feature parquet outputs and OHLCV state
- params including `top_n`, `min_score`, breakout settings, pattern settings, and `ml_mode`

Behavior:
- blocks if trust status is `blocked` unless `allow_untrusted_rank=True`
- runs the core rank first
- optionally emits sidecar outputs for breakout, pattern, stock-scan, and sector-dashboard views
- records per-task status so matching outputs can be reused across retries when fingerprints match
- optionally builds an ML overlay when `ml_mode=shadow_ml`
- assembles `dashboard_payload.json` for operator surfaces

Primary artifacts:
- `ranked_signals.csv`
- `breakout_scan.csv`
- `pattern_scan.csv`
- `stock_scan.csv`
- `sector_dashboard.csv`
- `dashboard_payload.json`
- `rank_summary.json`
- optional `ml_overlay.csv`
- `task_status.json`

Block conditions:
- trust status `blocked`
- empty `ranked_signals`
- missing required rank columns
- critical DQ failure

Non-blocking degraded behavior:
- optional sidecar tasks may fail or time out
- failures are recorded in `degraded_outputs` and task status
- the stage can still succeed if core ranking and dashboard payload generation succeed

Retry behavior:
- operator rerun only
- helper logic can reuse prior task outputs when fingerprints match

### `execute`

Owner:
- `ai_trading_system.pipeline.stages.execute`
- execution service layer under `execution/`

Inputs:
- `ranked_signals.csv`
- optional `dashboard_payload.json`
- optional `breakout_scan.csv`
- optional `ml_overlay.csv`
- execution params such as strategy mode, capital, fixed quantity, and slippage

Behavior:
- reads rank outputs from the same run
- blocks when rank trust status is `blocked`
- can also block on `degraded` when `block_degraded_execution=True`
- optionally applies breakout linkage when `execution_breakout_linkage=soft_gate`
- uses `PaperExecutionAdapter`
- persists execution rows in `data/execution.duckdb`

Primary artifacts:
- `trade_actions.csv`
- `executed_orders.csv`
- `executed_fills.csv`
- `positions.csv`
- `execute_summary.json`

Current safety posture:
- this stage is paper execution only
- live broker components exist in the repo but are not wired into the orchestrated stage
- current UI-triggered “full pipeline” actions skip `execute`

Retry behavior:
- operator rerun only
- persistent rows remain in `data/execution.duckdb`

### `publish`

Owner:
- `ai_trading_system.pipeline.stages.publish`
- delivery manager in `ai_trading_system.domains.publish.delivery_manager`
- channel adapters in `publishers/` and `channel/`

Inputs:
- rank artifacts from the same `run_id`
- publish params such as `local_publish`, `publish_quantstats`, and QuantStats scope settings

Behavior:
- reads existing run artifacts only
- does not recompute upstream work
- retries each channel through `PublisherDeliveryManager`
- records delivery attempts in `publisher_delivery_log`
- dedupes successful prior deliveries by `run_id + channel + artifact hash`
- raises `PublishStageError` when one or more channels still fail after retries

Current channel roles:
- publish-of-record: `google_sheets_portfolio`, `google_sheets_dashboard`, `quantstats_dashboard_tearsheet`
- informational: `telegram_summary`
- diagnostic: `local_summary`

Primary artifact:
- `publish_summary.json`

Run-status behavior:
- upstream stages may succeed while publish fails
- final run status becomes `completed_with_publish_errors`

Retry behavior:
- rerun `publish` with the same `run_id`
- previously delivered channels are marked `duplicate` and skipped

## DQ enforcement points

DQ runs after:
- `ingest`
- `features`
- `rank`

Current critical gates include:
- provider coverage
- unresolved date breadth
- recent universe-wide jump anomaly
- features trust-window quarantine breadth

Critical failures stop downstream stages. High and medium failures are persisted but do not stop the pipeline.

## Trust enforcement points

Trust state is computed from `_catalog` and `_catalog_quarantine` and summarized by `analytics.data_trust.load_data_trust_summary()`.

Current trust states:
- `trusted`
- `degraded`
- `blocked`
- `legacy`
- `missing`

Stage enforcement:
- `rank` blocks on `blocked` unless `allow_untrusted_rank=True`
- `execute` blocks on `blocked`; it also blocks on `degraded` when `block_degraded_execution=True` unless `allow_untrusted_execution=True`
- `publish` does not hard-block on trust state; it includes trust state in downstream payloads

`degraded` is not the same as `blocked`. Current code allows downstream rank and publish behavior in some degraded cases.

## Preflight and repair caveats

Current code has two important operational caveats:
- `ai_trading_system.pipeline.preflight` requires Dhan credentials for `ingest` and `features`, even though the default orchestrated ingest path is `NSE bhavcopy -> yfinance fallback`
- auto-repair only exists for one ingest DQ failure path and is not a generic retry framework
