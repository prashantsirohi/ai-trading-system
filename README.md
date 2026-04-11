# AI Trading System

Production-oriented NSE technical research and execution platform with:
- staged pipeline orchestration (`ingest -> features -> rank -> execute -> publish`)
- DuckDB-backed control plane and artifact lineage
- rule-based data quality gating
- technical ranking and breakout scanning
- dual UI architecture (Streamlit research + React/FastAPI execution, with NiceGUI fallback during migration)
- publish channels (Google Sheets, Telegram, dashboard payload, QuantStats tear sheet)

## 0. Operator Quick Link

For an up-to-date, practical operations view of the runtime flow, trust model, and commands:
- [`docs/high_level_operational_data_flow.md`](docs/high_level_operational_data_flow.md)
- Visual system flow: [`3.1 Visual Data Flow Diagram`](docs/high_level_operational_data_flow.md#31-visual-data-flow-diagram)
- Trust decision flow: [`3.2 Data Trust Quarantine Decision Flow`](docs/high_level_operational_data_flow.md#32-data-trust-quarantine-decision-flow)
- SVG assets: [`operational_data_flow.svg`](docs/diagrams/operational_data_flow.svg), [`data_trust_decision_flow.svg`](docs/diagrams/data_trust_decision_flow.svg)

## 1. Architecture Overview

The repository is intentionally split into bounded modules:

- `run/`: orchestration and stage execution contracts
- `collectors/`: market and delivery ingestion
- `features/`: indicator and sector-strength computation
- `analytics/`: ranking, regime, DQ, registry, ML/shadow, backtesting
- `publishers/`: external delivery adapters
- `ui/`: operator and research interfaces
- `core/`: shared runtime contracts, env/path/bootstrap/logging
- `research/`: offline train/eval/backtest workflows

High-level runtime flow:

```text
Data Providers -> Ingest -> Features -> Rank -> Execute -> Publish
                     |         |          |         |         |
                     |         |          |         |         +--> Telegram / Sheets / QuantStats
                     |         |          |         +--> orders / fills / positions (paper/live adapters)
                     |         |          +--> ranked_signals / breakout_scan / sector_dashboard
                     |         +--> feature snapshots + sector leadership artifacts
                     +--> _catalog / _delivery

Control Plane (DuckDB):
pipeline_run, pipeline_stage_run, pipeline_artifact, dq_rule, dq_result,
publisher_delivery_log, pipeline_alert, model_registry, model_eval, model_deployment
```

## 2. Data Domains and Storage

The system uses two domains:

- `operational`: rolling live data for production monitoring and publish
- `research`: historical/static-oriented data for training and backtests

Primary paths (operational):
- `data/ohlcv.duckdb`
- `data/feature_store/`
- `data/pipeline_runs/`
- `data/control_plane.duckdb`

Primary paths (research):
- `data/research/research_ohlcv.duckdb`
- `data/research/feature_store/`
- `reports/research/`

Core tables:
- `ohlcv.duckdb::_catalog`: OHLCV history
- `ohlcv.duckdb::_delivery`: delivery percentage history
- `control_plane.duckdb::pipeline_*`: run/stage/artifact lineage
- `control_plane.duckdb::dq_*`: DQ rule + result history
- `control_plane.duckdb::publisher_delivery_log`: publish attempts and dedupe
- `control_plane.duckdb::model_*`: model governance and deployment trail

## 3. Staged Pipeline Design

Entrypoint:
- `python -m run.orchestrator`

Stage order:
- `ingest`
- `features`
- `rank`
- `execute`
- `publish`

Stage behavior:
- each stage writes explicit artifacts
- artifacts are versioned per `run_id/stage/attempt`
- stage attempts are tracked in `pipeline_stage_run`
- publish can be retried independently with the same `run_id`

Important runtime semantics:
- DQ severities are `critical`, `high`, `medium`, `low`
- only `critical` failures block downstream execution
- ingest/features/rank retries are operator-triggered reruns (not automatic loops)
- publish channels have retry/backoff + idempotent dedupe
- Telegram publish is informational delivery; Google Sheets + QuantStats are the publish-of-record outputs

## 4. Ingest and Delivery Collection

OHLCV ingestion:
- market data collectors write into `_catalog`
- dedupe/upsert semantics preserve key uniqueness expectations

Delivery ingestion:
- `collectors/delivery_collector.py`
- primary source: NSE archive MTO files
- fallback source: NSE security-wise endpoint
- outputs:
  - `_delivery` in DuckDB
  - `feature_store/delivery/NSE/*.parquet` features

Delivery can be disabled for constrained runs:
- `--skip-delivery-collect`

## 5. Feature and Sector Pipeline

Feature stage computes technical indicators and supporting artifacts:
- RSI, ADX, ATR, EMA, SMA, MACD, ROC, Supertrend, etc.
- incremental-tail updates for operational flows
- full rebuild option for schema/logic transitions

Sector leadership artifacts are produced for ranking context:
- sector relative strength
- stock vs sector performance context

## 6. Ranking Engine (Technical-Only)

Primary engine:
- `analytics/ranker.py::StockRanker`

Current factor model (cross-sectional, percentile-scored):
- relative strength: `0.25`
- volume intensity: `0.18`
- trend persistence: `0.15`
- proximity to highs: `0.17`
- delivery percentage: `0.10`
- sector strength: `0.15`

Outputs in rank stage:
- `ranked_signals.csv`
- `breakout_scan.csv`
- `stock_scan.csv`
- `sector_dashboard.csv`
- `dashboard_payload.json`
- `rank_summary.json`

Default ranking threshold:
- orchestrator `--min-score` default is `0.0` (full-universe scoring retained unless overridden)

## 7. Breakout Scan Architecture

Scanner:
- `channel/breakout_scan.py`

Canonical taxonomy (daily OHLC v2):
- `resistance_breakout_50d`
- `high_52w_breakout`
- `consolidation_breakout`
- `volatility_expansion_breakout`
- `volume_confirmed_breakout` (confirmation flag, not standalone trigger)

Legacy families remain available behind compatibility mapping:
- `base_breakout` -> `resistance_breakout_50d`
- `contraction_breakout` -> `consolidation_breakout`
- `supertrend_flip_breakout` -> `volatility_expansion_breakout`

Breakout rank semantics:
- `breakout_score` and `breakout_rank` are computed in breakout scan only
- main `composite_score` ranking remains unchanged in v1 rollout
- state output is explicit: `qualified`, `watchlist`, `filtered_by_regime`

### 7.1 Breakout Update (April 9, 2026)

Layered breakout evaluation (kept permissive at detection level):
- `breakout_detected`: structural setup detected
- `filtered_by_regime`: market/sector gate result
- `filtered_by_symbol_trend`: symbol trend-quality gate result

Symbol trend tiering (`candidate_tier`):
- `Tier A` = pass all 3 checks
- `Tier B` = fail exactly 1 check
- `Tier C` = fail 2+ checks

Core symbol trend checks:
- `close > sma200`
- `sma50_slope_20d_pct > 0`
- `near_52w_high_pct <= breakout_symbol_near_high_max_pct` (default `15.0`)

`symbol_trend_score`:
- `(pass_count / 3) * 100`

`breakout_score` contract (separate from composite rank):
- `+1` if `close > rolling_max_50`
- `+2` if `close > rolling_max_252`
- `+2` if consolidation/range-break condition is true
- `+1` if `volume_ratio_20 >= 1.5`
- `+2` if `rel_strength_score >= 80`

State mapping:
- if regime filter fails -> `filtered_by_regime`
- else if tier is `C` -> `filtered_by_symbol_trend`
- else if tier is `A` -> `qualified`
- else -> `watchlist` (`Tier B`)

Operational usage:
- `execution_breakout_linkage=soft_gate` allows only `Tier A` qualified breakouts
- dashboard publish sheet now shows breakouts as an explicit all-candidates section (`BREAKOUTS (all, unfiltered)`) for operator review

Uses:
- structural breakout conditions
- trend/ADX filters
- volume ratio conditions
- proximity-to-high context
- regime/bias and breadth-aware qualification gates
- sector RS thresholds for qualification

Execution labels include:
- `ACTIONABLE_BREAKOUT`
- `EARLY_BREAKOUT`
- `RELATIVE_STRENGTH_BREAKOUT`
- `WATCHLIST_BREAKOUT`
- `FILTERED_BREAKOUT`

## 8. DQ and Governance Control Plane

DQ engine:
- `analytics/dq/engine.py`

Governance store:
- `analytics/registry/store.py`

Persisted governance records:
- pipeline run + stage attempts + artifacts
- DQ rules + DQ outcomes
- publish delivery logs (delivered/retrying/failed/duplicate)
- pipeline alerts
- model registry, eval, deployment, rollback metadata

Alert behavior:
- alerts are persisted and logged
- no standalone alert fan-out dispatcher is wired directly in `AlertManager` today

## 9. Publish Architecture

Publish stage:
- `run/stages/publish.py`

Delivery manager:
- `run/publisher.py`
- dedupe key: `run_id + channel + artifact hash`
- retries with backoff for transient channel failures

Channels:
- Google Sheets portfolio + single dated dashboard sheet (`YYYY-MM-DD`) with compact Sector/Ranks/Breakouts/Breadth sections
- Telegram summary
- QuantStats dashboard tear sheet
- local summary mode (`--local-publish`)

Telegram delivery notes:
- sender performs a DNS precheck against `api.telegram.org` before sending
- DNS, SSL, timeout, and API failures are logged separately for operator diagnosis
- request timeouts are configurable via:
  - `TELEGRAM_CONNECT_TIMEOUT_SECONDS`
  - `TELEGRAM_READ_TIMEOUT_SECONDS`
  - `TELEGRAM_WRITE_TIMEOUT_SECONDS`
  - `TELEGRAM_POOL_TIMEOUT_SECONDS`
  - `TELEGRAM_SEND_ATTEMPTS`
  - `TELEGRAM_DNS_PRECHECK_ENABLED`

## 10. ML and Shadow Monitoring

ML engines:
- legacy XGBoost support (`analytics/ml_engine.py`)
- LightGBM workflow (`analytics/lightgbm_engine.py`, `research/train_lightgbm.py`, `research/run_lightgbm_workflow.py`)

Shadow monitor:
- `research/shadow_monitor.py`
- helpers in `analytics/shadow_monitor.py`
- compares technical baseline vs ML and blended overlays
- persists predictions/outcomes for weekly/monthly summary views

## 11. UI Architecture

Research UI (Streamlit):
- `ui/research/app.py`
- deep analytics, ranking explainability, breakout evidence, sector views, ML/shadow review

Execution UI (React + FastAPI):
- API: `ui/execution_api/app.py`
- React console: `web/execution-console/`
- route-based operator console for pipeline, control, ranking, market, runs, shadow, tasks, and processes
- default landing page now starts on the React `Pipeline` workspace backed by live rank artifacts
- durable task state in `control_plane.duckdb::operator_task*`
- migration plan: `docs/react-operator-migration-plan.md`

Legacy execution fallback (NiceGUI):
- `ui/execution/app.py`
- still available during migration, now reading the same durable task state

Shared UI services:
- `ui/services/`
- centralized query/control helpers to reduce duplicated business logic

## 12. Command Reference

Environment setup:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Primary pipeline run:

```bash
python -m run.orchestrator
```

Full operational run explicitly:

```bash
python -m run.orchestrator --data-domain operational
```

Synthetic smoke mode is removed. Use a real canary run instead:

```bash
python -m run.orchestrator --canary --symbol-limit 25 --local-publish
```

Canary run:

```bash
python -m run.orchestrator --canary --symbol-limit 25 --local-publish
```

Retry publish only:

```bash
python -m run.orchestrator --run-id <run_id> --stages publish
```

Skip delivery collection:

```bash
python -m run.orchestrator --skip-delivery-collect
```

Run the execution API:

```bash
PYTHONPATH=. ./.venv/bin/python -m ui.execution_api.app --port 8090
```

Run the React execution console:

```bash
cd web/execution-console
npm install
npm run dev
```

The React console now lands on the `Pipeline` workspace first and reads from:

- `GET /api/execution/workspace/pipeline`
- `GET /api/execution/ranking`
- `GET /api/execution/market`

Legacy NiceGUI execution fallback:

```bash
PYTHONPATH=. ./.venv/bin/python -m ui.execution.app --port 8080
```

Override ranking threshold:

```bash
python -m run.orchestrator --min-score 50
```

Breakout v2 controls (separate from composite rank):

```bash
python -m run.orchestrator \
  --breakout-engine v2 \
  --breakout-market-bias-allowlist BULLISH,NEUTRAL \
  --breakout-min-breadth-score 45 \
  --breakout-sector-rs-percentile-min 60 \
  --breakout-qualified-min-score 3
```

Optional execution linkage (default is informational-only / off):

```bash
python -m run.orchestrator --execution-breakout-linkage soft_gate
```

Research UI:

```bash
python -m streamlit run ui/research/app.py
```

Execution UI:

```bash
python -m ui.execution.app
```

Shadow refresh:

```bash
python -m research.shadow_monitor
```

## 13. QuantStats Tear Sheet Outputs

Default outputs:
- `reports/quantstats/dashboard_tearsheet_<run_id>.html`
- `reports/quantstats/dashboard_tearsheet_<run_id>_returns.csv`
- `reports/quantstats/dashboard_tearsheet_<run_id>_series.csv`
- `reports/quantstats/dashboard_tearsheet_<run_id>.json`

Optional:
- `reports/quantstats/dashboard_tearsheet_<run_id>_quantstats.html` (with `--quantstats-write-core-html`)

Return series construction uses consecutive ranked snapshots:
- reads `data/pipeline_runs/*/rank/attempt_*/ranked_signals.csv`
- selects prior-run top `N`
- computes overlap-based forward return
- aggregates equal-weight portfolio period returns

## 14. Testing and Validation

Representative test modules:
- `tests/test_pipeline_orchestrator.py`
- `tests/test_feature_incremental.py`
- `tests/test_quantstats_dashboard_publish.py`
- `tests/test_shadow_monitor.py`
- `tests/test_dashboard_helpers.py`

Run targeted suite:

```bash
python -m pytest -q tests
```

## 15. Known Operating Principles

- publish is non-authoritative; publish failures can end as `completed_with_publish_errors`
- research workflows should use the research domain by default
- generated runtime data (`data/`, `reports/`) should not be committed

## 16. Related Documentation

- [docs/architecture.md](docs/architecture.md)
- [docs/architecture_review.md](docs/architecture_review.md)
- [docs/architecture_target.md](docs/architecture_target.md)
- [docs/data-flow.md](docs/data-flow.md)
- [docs/dq_rules.md](docs/dq_rules.md)
- [docs/ops_runbook.md](docs/ops_runbook.md)
