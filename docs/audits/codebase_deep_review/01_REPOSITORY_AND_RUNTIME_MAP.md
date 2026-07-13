# Repository and Runtime Map

- **Purpose:** Record the audited repository inventory, runtime components, entrypoints, data flow, and dependency direction.
- **Audience:** Maintainers and reviewers orienting to the deployed system.
- **Last verified:** 2026-07-13
- **Source of truth:** `docs/SYSTEM_GUIDE.md` plus the cited runtime code, path helpers, and read-only environment inspection.

---

## Inventory

| Item | Observed count/state |
|---|---:|
| Python files | 760 |
| Python package LOC under `src/ai_trading_system` | 127,211 |
| TypeScript/JavaScript source files | 169 |
| React source LOC (excluding generated API types) | 25,032 |
| Test files named `test_*.py` | 261 |
| Python test functions | 1,511 |
| SQL migrations | 30 |
| API route decorators | 69 |
| API response models | 0 |
| Python files containing broad `except Exception` | 108 (348 sites) |
| `iterrows` / `pd.concat` sites in package | 103 / 99 |

Top-level active areas are `src/ai_trading_system`, `config`, `scripts`, `tools`, `tests`, and `web/execution-console-v2/ai-trading-dashboard-starter`. Root `analytics/` remains legacy. Runtime-looking `data/`, `reports/`, `logs/`, and `models/` trees are present locally but ignored; live operational storage resolved to `/Volumes/MacData/Trading/data` during this audit.

No generated database/Parquet/report artifact was found tracked by Git in the inspected filters. `.env`, `client_secret.json`, and `token.json` exist locally but are ignored.

## Component map

| Component | Purpose | Entry point | Inputs | Outputs | Data store | Downstream consumers | Owner layer |
|---|---|---|---|---|---|---|---|
| Orchestrator | Stage selection, attempts, DQ, lineage | `ai-trading-pipeline` / `pipeline.orchestrator:main` | CLI/API params, registry | run/stage state and artifacts | control plane | all stages, API | application/pipeline |
| Ingest | Trusted NSE OHLCV/delivery refresh | `pipeline.stages.ingest.IngestStage` | NSE, Dhan/yfinance fallback, master data | catalog, provenance, quarantine, summary | OHLCV + raw | features, DQ | ingest domain |
| Features | Technical/sector/valuation/earnings snapshots | seven `features_*` stages | catalog, prior features, master/fundamentals | Parquet/tables/snapshot | OHLCV + feature store | rank/research | features domain |
| Rank | Factors, stage, breakout/pattern and dashboards | `pipeline.stages.rank.RankStage` | catalog/features/prior rank | rank artifact family | attempt files + control plane | investigator/candidates/UI/publish | ranking domain |
| Investigator | Non-executable investigation lifecycle | `pipeline.stages.investigator` | rank and history | queue/lifecycle evidence | control plane + artifacts | UI, publish | investigator domain |
| Candidates/tracker/events | Selection, lifecycle, catalysts | respective stage classes | rank/fundamentals/events | shortlist and durable state | tracker/control plane | execute/insight | domain services |
| Execute | Paper order/fill/position/stop lifecycle | `pipeline.stages.execute.ExecuteStage` | ranked candidates, trust, portfolio | actions/orders/fills/positions | execution DB + artifacts | insight/UI | execution/risk |
| Publish | Deliver materialized output | `pipeline.stages.publish.PublishStage` | registered artifacts | channel logs/summary | control plane + external channels | operator | publishing domain |
| Research | Backtests, optimization, perf cohorts | research CLIs/API services | snapshots and research OHLCV | results/models/reports | research stores | operator/research UI | research domain |
| FastAPI | Operator read/control surface | `ai-trading-execution-api` | API requests, stores/artifacts | JSON/SSE/files/tasks | all read models; control writes | React | interface/UI |
| React | Operator console | Vite application | `/api/*` | browser UI | client cache | operator | interface/UI |

## Canonical call chains

### Ingest

`PipelineOrchestrator.run_pipeline` → `IngestStage.run` → `IngestOrchestrationService.run/run_default` → `daily_update_runner.run` → NSE primary `_run_nse_yfinance_daily_update` → missing business-date calculation → NSE normalization/symbol mapping → catalog/provenance/quarantine writes → bhavcopy validation → delivery collection → trust summary → DQ engine.

The alternate Dhan-primary runner remains in code. Current guidance states NSE is source-of-record, while `tests/test_daily_update_runner_sources.py::test_daily_update_runner_defaults_to_dhan_primary` indicates historical/default ambiguity that should be resolved in documentation and defaults.

### Features

Orchestrator expands `features` → `features_technical` → sector RS → valuation → stock valuation bands → sector earnings → phase 1 → snapshot. `FeaturesOrchestrationService.run_substage` resolves updated symbols and full/incremental mode, selects legacy or DuckDB batch compute, writes DuckDB/Parquet, then `features_snapshot` records the snapshot and DQ evaluates the logical `features` stage.

### Rank

`RankStage` → `RankOrchestrationService.run` → `StockRanker.rank_all` → `RankerInputLoader` → factor transforms → winsorization/sector demeaning/percentile scores → stage/penalty/stability → breakout/pattern/early-accumulation tasks → dashboard/artifact family → rank DQ.

### Execute

Registered `ranked_signals` → `ExecutionCandidateBuilder` → trust and Stage-2/breakout gates → persisted fills reconstructed by `PortfolioManager` → static heat gate → stop evaluation using `current_prices` → trade actions → `ExecutionService` → `PaperExecutionAdapter` → execution order/fill tables → positions and stop updates → execution artifacts.

### Publish

Registered rank/upstream artifacts → `PublishStage` dataset loading → channel handler selection → `PublisherDeliveryManager` → SHA-256 dedupe lookup → bounded retry/backoff → external/local handler → delivery log → publish summary. Exception: the default fundamental fallback refreshes read models and violates pure artifact consumption.

### Operator UI

React page/query module → FastAPI router → service/read-model function → registry/DuckDB/artifact → dictionary response/OpenAPI snapshot → handwritten/generated TypeScript contract → table/chart. The separation exists, but many services materialize pandas frames and all 69 routes lack response models.

## Persistent stores and observed live sizes

| Store | Resolved path | Observed size | Owner |
|---|---|---:|---|
| OHLCV | `$DATA_ROOT/ohlcv.duckdb` | 3,846,713,344 bytes | ingest/features |
| Control plane | `$DATA_ROOT/control_plane.duckdb` | 812,396,544 bytes | registry/orchestrator/read models |
| Execution | `$DATA_ROOT/execution.duckdb` | 3,682,304 bytes | execution |
| Candidate tracker | `$DATA_ROOT/candidate_tracker.duckdb` | 4,730,880 bytes | tracker |
| Master data | `$DATA_ROOT/masterdata.db` | not measured | ingest/shared |
| Feature store | `$DATA_ROOT/feature_store` | directory | features |
| Attempts | `$DATA_ROOT/pipeline_runs/<run>/<stage>/attempt_<n>` | directory | each stage |
| Perf tracker | `$DATA_ROOT/research.duckdb` | not measured | research/perf tracker |

## Entrypoints

The package exposes 22 console scripts in `pyproject.toml`, including pipeline, daily wrapper, API, healthcheck, bootstrap/repair commands, fundamental/valuation jobs, optimizer/promote, and research reports. Additional operational/research one-offs remain in `scripts/`, plus `tools/build_universe_index.py`, `audit_rank.py`, and `scripts/run_stage_gate_backtest.py` (which manipulates `sys.path` and imports root `analytics`).

Scheduled operation is external: shell helpers and runbooks exist, but no repository-owned scheduler or service unit defines a single authoritative schedule.

## Dependency direction and boundary violations

```text
React / FastAPI / CLI
        |
pipeline and application services
        |
domains: ingest, features, ranking, execution, publish, research
        |
platform contracts, paths, registry abstractions
        |
DuckDB / Parquet / provider SDKs / Google / Telegram / NSE
```

Observed violations include pipeline publish calling `daily_pipeline.run_portfolio_analysis`, publish refreshing fundamental read models, UI control services importing the orchestrator with a root-path fallback, ranking/business services owning raw SQL, and root research scripts importing legacy `analytics`.

## Import and packaging risk report

| Source | Imported/dependent target | Problem | Runtime impact | Recommended target |
|---|---|---|---|---|
| `scripts/run_stage_gate_backtest.py` | root `analytics.stage_gate_backtest` | root `sys.path` injection | installed package is not sufficient for this command | migrate into `ai_trading_system.research` |
| `ui/.../control_center.py` | orchestrator with root fallback | hides packaging failure | environment-dependent imports | direct package import; fail clearly |
| Telegram publisher | `telegram` | mandatory import, undeclared dependency | full test collection and channel import fail | `publish-telegram` extra or core dependency |
| `pyproject.toml` vs `requirements.txt` | divergent sets | no single dependency contract | non-reproducible environments | make `uv.lock` generated solely from project + extras |
| `market_intel @ ...@master` | moving Git branch | unrepeatable upgrade/install | builds change without project version change | pin commit/tag and expose provider extra |
