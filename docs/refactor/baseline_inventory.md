# Baseline Inventory

Phase 0 captures the current architecture and compatibility surface before any production refactor work begins.

## Runtime Boundaries

### Top-level layers

| Layer | Current responsibilities | Primary modules |
| --- | --- | --- |
| Core runtime | Shared contracts, env/bootstrap, path/logging facades, runtime config | `core/contracts.py`, `core/bootstrap.py`, `core/env.py`, `core/paths.py`, `core/logging.py`, `core/runtime_config.py` |
| Legacy runtime helpers | Concrete implementations still backing the `core` facades | `utils/data_domains.py`, `utils/logger.py` |
| Pipeline orchestration | Pipeline CLI, preflight, alerts, stage scheduling, retry metadata | `run/orchestrator.py`, `run/preflight.py`, `run/alerts.py`, `run/stages/*` |
| Analytics and ranking | Data quality, rank generation, ML overlays, monitoring, registry access | `analytics/*` |
| Execution domain | Paper execution adapters, order policy, portfolio state, execution store | `execution/*` |
| Publish domain | Delivery wrappers for Google Sheets, Telegram, dashboard, QuantStats | `run/publisher.py`, `publishers/*` |
| UI/API read layer | Execution API, control-center task orchestration, artifact-backed read models | `ui/execution_api/app.py`, `ui/services/execution_operator.py`, `ui/services/execution_data.py`, `ui/services/control_center.py` |
| Data collection and research | Ingestion jobs, repair tools, research workflows, backtests | `collectors/*`, `research/*` |

### Current runtime split

- `core/paths.py` is a compatibility facade that re-exports `utils.data_domains`.
- `core/logging.py` is a compatibility facade that re-exports `utils.logger`.
- Newer runtime entry points such as `run/orchestrator.py`, `run/preflight.py`, and `ui/services/execution_data.py` already import through `core.*`.
- Many analytics, collector, research, publisher, channel, and UI modules still import `utils.data_domains` and `utils.logger` directly.

## Pipeline Flow

### Orchestrator

- `run/orchestrator.py` owns run creation, stage retries, DQ evaluation, alert emission, and artifact registration.
- Stage outputs are persisted under `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`.
- Registry metadata lives in `data/control_plane.duckdb` via `analytics/registry/store.py`.
- `StageContext.output_dir()` in `core/contracts.py` is the shared artifact path authority for stage writes.

### Stage inventory

| Stage | Wrapper | Main downstream runtime touched | Produced artifacts |
| --- | --- | --- | --- |
| `ingest` | `run/stages/ingest.py` | `collectors.daily_update_runner`, optional bhavcopy and delivery collection | `ingest_summary.json` |
| `features` | `run/stages/features.py` | `collectors.daily_update_runner`, `_snapshots`, `_feature_registry` | `feature_snapshot.json` |
| `rank` | `run/stages/rank.py` | `analytics/ranker.py`, channel scans, dashboard payload building, optional ML overlay | `ranked_signals.csv`, `breakout_scan.csv`, `pattern_scan.csv`, `stock_scan.csv`, `sector_dashboard.csv`, `dashboard_payload.json`, optional `ml_overlay.csv` |
| `execute` | `run/stages/execute.py` | `execution/*`, `analytics/risk_manager.py` | `trade_actions.csv`, `executed_orders.csv`, `executed_fills.csv`, `positions.csv`, `execute_summary.json` |
| `publish` | `run/stages/publish.py` | `run/publisher.py`, `publishers/*`, `run.daily_pipeline.run_portfolio_analysis` | `publish_summary.json` |

### Data flow summary

1. `PipelineOrchestrator.run_pipeline()` creates or resumes a run, resolves domain paths, and optionally executes `PreflightChecker`.
2. Each stage receives a `StageContext` with the current artifact map loaded from `RegistryStore`.
3. `RegistryStore.record_artifact()` persists artifact metadata and content hashes after every stage.
4. `analytics.dq.DataQualityEngine` validates `ingest`, `features`, and `rank` stage outputs.
5. UI/API readers load the latest artifact files from the pipeline run directory and combine them with registry metadata from `control_plane.duckdb`.

## Artifact Producers and Consumers

### Rank artifacts

| Artifact | Producer | Current consumers |
| --- | --- | --- |
| `ranked_signals.csv` | `run/stages/rank.py` | `run/stages/execute.py`, `run/stages/publish.py`, `ui/services/execution_data.py`, `ui/services/execution_operator.py`, `publishers/quantstats_dashboard.py`, `ui/research/data_access.py`, `ui/execution/app.py`, `ui/research/app.py`, `ui/services/ml_workbench.py`, `ui/services/control_center.py` |
| `breakout_scan.csv` | `run/stages/rank.py` | `run/stages/execute.py`, `run/stages/publish.py`, `ui/services/execution_data.py`, `ui/services/execution_operator.py`, `publishers/quantstats_dashboard.py`, `ui/research/data_access.py`, `ui/execution/app.py` |
| `pattern_scan.csv` | `run/stages/rank.py` | `ui/services/execution_data.py`, `ui/services/execution_operator.py`, `ui/research/data_access.py` |
| `stock_scan.csv` | `run/stages/rank.py` | `run/stages/publish.py`, `ui/services/execution_data.py`, `ui/services/execution_operator.py` |
| `sector_dashboard.csv` | `run/stages/rank.py` | `run/stages/publish.py`, `ui/services/execution_data.py`, `ui/services/execution_operator.py`, `publishers/quantstats_dashboard.py`, `ui/research/data_access.py`, `ui/execution/app.py` |
| `dashboard_payload.json` | `run/stages/rank.py` | `run/stages/execute.py`, `run/stages/publish.py`, `ui/services/execution_data.py`, `ui/services/execution_operator.py`, `publishers/dashboard.py`, `ui/research/data_access.py`, `ui/research/app.py` |

### Execute artifacts

| Artifact | Producer | Current consumers |
| --- | --- | --- |
| `trade_actions.csv` | `run/stages/execute.py` | `ui/services/ml_workbench.py` |
| `executed_orders.csv` | `run/stages/execute.py` | `ui/services/ml_workbench.py` |
| `executed_fills.csv` | `run/stages/execute.py` | `ui/services/ml_workbench.py` |
| `positions.csv` | `run/stages/execute.py` | `ui/services/ml_workbench.py` |
| `execute_summary.json` | `run/stages/execute.py` | `ui/services/ml_workbench.py` |

### Publish artifacts

| Artifact | Producer | Current consumers |
| --- | --- | --- |
| `publish_summary.json` | `run/stages/publish.py` | Registry run details via `ui/services/control_center.py`, publish retry diagnostics, operator delivery logs |

## Representative Schema Snapshots

Representative fixtures for the current artifact contract live under `tests/fixtures/artifacts/`.

### Rank snapshot

- `ranked_signals.csv`
  - Required-by-DQ columns observed today: `symbol_id`, `composite_score`
  - Representative fixture columns: `symbol_id`, `exchange`, `close`, `composite_score`, `sector_name`
- `breakout_scan.csv`
  - Representative columns: `symbol_id`, `sector`, `setup_family`, `breakout_state`, `candidate_tier`, `breakout_score`
- `pattern_scan.csv`
  - Representative columns: `signal_id`, `symbol_id`, `pattern_family`, `pattern_state`, `pattern_score`
- `stock_scan.csv`
  - Representative columns: `symbol_id`, `close`, `category`
- `sector_dashboard.csv`
  - Representative columns: `Sector`, `RS_rank_pct`, `Quadrant`
- `dashboard_payload.json`
  - Representative top-level keys: `summary`, `metadata`, `warnings`, `ranked_signals`, `breakout_scan`, `pattern_scan`, `stock_scan`, `sector_dashboard`

### Execute snapshot

- `trade_actions.csv`
  - Representative columns: `action`, `symbol_id`, `exchange`, `side`, `quantity`, `requested_price`, `strategy_mode`, `reason`
- `executed_orders.csv`
  - Representative columns: `order_id`, `symbol_id`, `exchange`, `side`, `quantity`, `requested_price`, `strategy_mode`, `reason`
- `executed_fills.csv`
  - Representative columns: `fill_id`, `order_id`, `symbol_id`, `exchange`, `side`, `quantity`, `price`
- `positions.csv`
  - Representative columns: `symbol_id`, `exchange`, `quantity`, `avg_entry_price`, `last_fill_price`
- `execute_summary.json`
  - Representative top-level keys: `summary`, `run_date`, `parameters`, `positions_before`, `positions_after`
  - Representative summary keys: `execution_status`, `execution_enabled`, `preview_only`, `strategy_mode`, `data_trust_status`, `actions_count`, `order_count`, `fill_count`, `open_position_count`

### Publish snapshot

- `publish_summary.json`
  - Representative top-level keys: `rank_artifact_uri`, `rank_artifact_hash`, `targets`, `top_symbol`, `completed_at`
  - Optional key observed in current implementation: `failures`

## Execution API Route Inventory

| Route | Backing service | Primary sources |
| --- | --- | --- |
| `/api/execution/health` | `ui.services.execution_operator.get_execution_summary()["health"]` | `ui/services/execution_data.py` reading `data/ohlcv.duckdb`, `data/masterdata.db`, latest `rank/dashboard_payload.json` |
| `/api/execution/summary` | `ui.services.execution_operator.get_execution_summary()` | Latest payload, DB stats, recent runs, operator tasks |
| `/api/execution/ranking` | `ui.services.execution_operator.get_ranking_snapshot()` | Latest `ranked_signals.csv` from `ui/services/execution_data.py` |
| `/api/execution/market` | `ui.services.execution_operator.get_market_snapshot()` | Latest `breakout_scan.csv`, `sector_dashboard.csv`, execution health, dashboard payload summary |
| `/api/execution/workspace/pipeline` | `ui.services.execution_operator.get_pipeline_workspace_snapshot()` | Latest rank artifact set, dashboard payload, ops health from `control_plane.duckdb`, data trust summary |
| `/api/execution/shadow` | `ui.services.execution_operator.get_shadow_snapshot()` | Shadow-monitor outputs loaded through `ui/services/execution_data.py` |
| `/api/execution/runs` | `ui.services.control_center.get_recent_runs()` | `pipeline_run` rows in `data/control_plane.duckdb` |
| `/api/execution/runs/{run_id}` | `ui.services.control_center.get_run_details()` | `pipeline_run`, `pipeline_stage_run`, alerts, delivery logs in `data/control_plane.duckdb` |
| `/api/execution/tasks` | `ui.services.execution_operator.list_task_details()` | Operator task rows plus task log reconciliation in `data/control_plane.duckdb` |
| `/api/execution/tasks/{task_id}` | `ui.services.execution_operator.get_task_detail()` | Operator task metadata in `data/control_plane.duckdb` |
| `/api/execution/tasks/{task_id}/logs` | `ui.services.execution_operator.get_task_snapshot()` | Operator task logs in `data/control_plane.duckdb` |
| `/api/execution/tasks/{task_id}/events` | `ui.services.execution_operator.get_task_snapshot()` streamed | Operator task logs in `data/control_plane.duckdb` |
| `/api/execution/processes` | `ui.services.execution_operator.get_process_snapshot()` | Process discovery in `ui/services/control_center.py` |
| `/api/execution/pipeline/run` | `ui.services.execution_operator.run_pipeline_action()` | Launches `PipelineOrchestrator` through control-center task orchestration |
| `/api/execution/pipeline/publish-retry` | `ui.services.execution_operator.retry_publish_action()` | `find_latest_publishable_run()` plus pipeline action relaunch |
| `/api/execution/shadow/run` | `ui.services.execution_operator.run_shadow_action()` | Control-center background task launcher |
| `/api/execution/research/launch` | `ui.services.execution_operator.launch_research_action()` | Control-center background process launcher |
| `/api/execution/processes/{pid}/terminate` | `ui.services.execution_operator.terminate_process_action()` | Signal-based termination from control-center |
| `/api/execution/tasks/{task_id}/terminate` | `ui.services.execution_operator.terminate_task_action()` | Task reconciliation and optional subprocess termination |

## Mixed `core.*` and `utils.*` Usage

### Direct `utils.data_domains` imports

- Core facades and adapters: `core/paths.py`, `utils/data_config.py`
- Pipeline stages and tests: `run/stages/rank.py`, `run/stages/execute.py`, `tests/test_pipeline_orchestrator.py`
- Analytics: `analytics/rank_backtester.py`, `analytics/risk_manager.py`, `analytics/ml_engine.py`, `analytics/screener.py`, `analytics/alpha/dataset_builder.py`, `analytics/alpha/scoring.py`, `analytics/shadow_monitor.py`, `analytics/backtester.py`, `analytics/ranker.py`, `analytics/patterns/evaluation.py`
- Collectors: `collectors/daily_update_runner.py`, `collectors/dhan_ohlc_diagnostics.py`, `collectors/delivery_collector.py`, `collectors/nse_delivery_scraper.py`, `collectors/repair_ohlcv_window.py`, `collectors/reset_reingest_validate.py`, `collectors/dhan_collector.py`
- Research: `research/run_lightgbm_workflow.py`, `research/backfill_static_data.py`, `research/shadow_monitor.py`, `research/backtest_pipeline.py`, `research/plot_feature_importance.py`, `research/run_recipe.py`, `research/backfill_delivery_data.py`, `research/backtest_patterns.py`, `research/train_pipeline.py`, `research/prepare_training_dataset.py`
- Features/UI: `features/feature_store.py`, `ui/services/ml_workbench.py`, `ui/ml/app.py`, `ui/research/app.py`

### Direct `utils.logger` imports

- Core facade: `core/logging.py`
- Pipeline and UI: `run/alerts.py`, `ui/services/control_center.py`
- Utilities: `utils/pyarrow_utils.py`, `utils/data_config.py`, `utils/compact_features.py`
- Analytics: `analytics/rank_backtester.py`, `analytics/screener.py`, `analytics/risk_manager.py`, `analytics/lightgbm_engine.py`, `analytics/ranker.py`, `analytics/backtester.py`, `analytics/ml_engine.py`, `analytics/patterns/evaluation.py`, `analytics/alpha/dataset_builder.py`
- Collectors: `collectors/daily_update_runner.py`, `collectors/archive_nse_bhavcopy.py`, `collectors/masterdata.py`, `collectors/token_manager.py`, `collectors/ingest_full.py`, `collectors/nse_collector.py`, `collectors/delivery_collector.py`, `collectors/yfinance_collector.py`, `collectors/repair_ohlcv_window.py`, `collectors/zerodha_sector_collector.py`, `collectors/dhan_collector.py`, `collectors/compute_features_batch.py`, `collectors/nse_delivery_scraper.py`
- Research: `research/run_lightgbm_workflow.py`, `research/backfill_static_data.py`, `research/shadow_monitor.py`, `research/backtest_pipeline.py`, `research/plot_feature_importance.py`, `research/backfill_delivery_data.py`, `research/run_recipe.py`, `research/eval_pipeline.py`, `research/prepare_training_dataset.py`, `research/backtest_patterns.py`, `research/train_pipeline.py`
- Publishers and channels: `publishers/google_sheets.py`, `publishers/dashboard.py`, `publishers/quantstats_dashboard.py`, `channel/telegram_reporter.py`, `channel/breakout_scan.py`, `channel/stock_scan.py`, `channel/portfolio_analyzer.py`, `channel/ai_analyzer.py`, `channel/sector_dashboard.py`
- Features: `features/feature_store.py`, `features/compute_sector_rs.py`, `features/indicators.py`

## Runtime Duplication Hotspots

- Path resolution is duplicated conceptually between `core/paths.py` and `utils/data_domains.py`; today the core module is only a re-export layer.
- Logging is duplicated conceptually between `core/logging.py` and `utils/logger.py`; today the core module is only a re-export layer.
- Execution API route handlers are thin, but their read models still reach directly into raw artifacts through `ui/services/execution_data.py` rather than dedicated read-model modules.
- `run/stages/rank.py` and `run/stages/execute.py` still import `utils.data_domains` directly even though `core.paths` exists.
- Control-center task orchestration (`ui/services/control_center.py`) still logs via `utils.logger` while the orchestrator uses `core.logging`.

## Phase 0 Safety Net

- Representative artifact fixtures were added under `tests/fixtures/artifacts/` for rank, execute, and publish outputs.
- Smoke tests were added under `tests/smoke/` for:
  - orchestrator startup and artifact registration
  - execution API health, ranking, workspace, runs, tasks, and summary read paths
