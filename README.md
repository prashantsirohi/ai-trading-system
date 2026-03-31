# AI Trading System

A technical-only NSE trading pipeline with staged orchestration, DuckDB-backed lineage, data-quality gates, delivery-aware ranking, breakout scanning, and dashboard/publish surfaces.

## What It Does

- Ingests OHLCV into DuckDB
- Computes technical features and sector-strength artifacts
- Ranks stocks with a 6-factor technical model
- Produces breakout, stock-scan, and sector-dashboard artifacts
- Publishes summaries to local output and optional external channels
- Tracks runs, stage attempts, artifacts, DQ results, alerts, and publish delivery logs

## Current Technical Scope

The system currently focuses on technical screening and ranking only.

Included:
- relative strength
- volume intensity
- trend persistence
- proximity to highs
- delivery participation
- sector strength
- breakout scan
- bearish/bullish market regime detection

Not included:
- fundamental scoring
- Excel-driven screening logic

## Stage Pipeline

```text
ingest -> features -> rank -> publish
```

Main entrypoint:
- `python -m run.orchestrator`

Legacy wrapper:
- `python run/daily_pipeline.py`

## Main Components

### `run/`
- `orchestrator.py`
  - staged pipeline runner
- `stages/ingest.py`
  - OHLCV stage contract
- `stages/features.py`
  - feature and snapshot stage
- `stages/rank.py`
  - ranking, breakout, stock-scan, sector dashboard, dashboard payload
- `stages/publish.py`
  - publish retry, dedupe, delivery log integration

### `analytics/`
- `ranker.py`
  - 6-factor technical ranking engine
- `regime_detector.py`
  - directional market regime using ADX plus breadth
- `dq/engine.py`
  - DQ rule execution and severity gating
- `registry/store.py`
  - pipeline run/stage/artifact, DQ, alert, and model-governance persistence
- `rank_backtester.py`
  - research backtesting for technical factor studies

### `core/`
- `contracts.py`
  - shared stage and artifact contracts
- `env.py`
  - repo-local `.env` loading
- `paths.py`
  - operational/research path resolution
- `logging.py`
  - shared runtime logging context
- `runtime_config.py`
  - typed runtime credentials/config

### `publishers/`
- dedicated delivery adapters for:
  - Google Sheets
  - Telegram
  - dashboard payload publishing

### `collectors/`
- `dhan_collector.py`
  - operational OHLCV ingestion
- `delivery_collector.py`
  - delivery ingestion with archive plus security-wise fallback
- `nse_delivery_scraper.py`
  - NSE security-wise delivery backend

### `features/`
- `feature_store.py`
  - RSI, ADX, EMA, MACD, ATR, Bollinger Bands, ROC, Supertrend support
- `compute_sector_rs.py`
  - sector leadership from top-800 liquidity universe with operational fallback

### `ui/`
- `research/app.py`
  - Streamlit research UI
  - backtesting
  - LightGBM review
  - shadow monitor
  - charts and factor analysis
- `execution/app.py`
  - NiceGUI execution UI
  - live operational monitoring
  - latest ranked signals
  - breakout monitor
  - shadow comparison
- `services/`
  - shared read/query layer for both UIs

### `dashboard/`
- `app.py`
  - compatibility wrapper to the research UI

## Data Layout

Operational defaults:
- `data/ohlcv.duckdb`
- `data/feature_store/`
- `data/pipeline_runs/`

Research defaults:
- `data/research/research_ohlcv.duckdb`
- `data/research/feature_store/`
- `reports/research/`

Raw local data and generated outputs should not be committed.

## Ranking Model

The current production ranking is technical-only and uses these weights:

- relative strength: `0.25`
- volume intensity: `0.18`
- trend persistence: `0.15`
- proximity to highs: `0.17`
- delivery percentage: `0.10`
- sector strength: `0.15`

Factor scores are percentile-style `0-100` values computed cross-sectionally for the current universe.

## Breakout Logic

The rank stage also emits a dedicated breakout scan with setup families:

- `base_breakout`
  - breakout above a compact 30-bar base with volume and trend confirmation
- `contraction_breakout`
  - breakout after tighter recent contraction inside a broader structure
- `supertrend_flip_breakout`
  - actual bullish supertrend flip followed by breakout confirmation

Execution labels are regime-aware:

- `ACTIONABLE_BREAKOUT`
- `EARLY_BREAKOUT`
- `RELATIVE_STRENGTH_BREAKOUT`
- `COUNTER_TREND_BREAKOUT`

## UI Split

Research UI with Streamlit:

```bash
. .venv/bin/activate
python -m streamlit run ui/research/app.py
```

Execution UI with NiceGUI:

```bash
. .venv/bin/activate
python -m ui.execution.app
```

The research UI reads the latest operational rank artifacts plus research model outputs and shows:
- pipeline health
- ranked signals
- factor profile
- breakout scan
- sector dashboard
- charts with technical indicators
- regime and data freshness checks
- LightGBM model review
- shadow-monitor comparison

The execution UI focuses on live operations:
- latest operational payload
- operational health and freshness
- ranked signals and breakouts
- sector leadership
- shadow-monitor weekly/monthly challenger summaries
- process management for Streamlit, NiceGUI, pipeline, and shadow-monitor jobs
- one-click launch of the Streamlit research UI from the execution console

## Quick Start

Create and activate the virtual environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Repo-local runtime credentials are auto-loaded from `.env` by the orchestrator, dashboard, publish test, and the main channel integrations. In most cases you only need to activate `.venv`, not manually source `.env`.

Run tests:

```bash
python -m pytest -q \
  streamlit/test/test_pipeline_orchestrator.py \
  streamlit/test/test_feature_incremental.py \
  streamlit/test/test_training_dataset.py \
  streamlit/test/test_lightgbm_engine.py \
  streamlit/test/test_shadow_monitor.py
```

Run a smoke pipeline:

```bash
python -m run.orchestrator --smoke --local-publish
```

Run a real operational rank refresh:

```bash
python -m run.orchestrator --stages rank --skip-preflight --data-domain operational
```

Retry publish only:

```bash
python -m run.orchestrator --run-id <run_id> --stages publish
```

Run breakout-family backtest study:

```bash
python -m research.backtest_breakout_setups
```

Run shadow-monitor refresh:

```bash
python -m research.shadow_monitor
```

Run a canary:

```bash
python -m run.orchestrator --canary --symbol-limit 25 --local-publish
```

Test live publish channels:

```bash
python -m run.publish_test
```

## Governance and Metadata

The system persists:

- `pipeline_run`
- `pipeline_stage_run`
- `pipeline_artifact`
- `dq_rule`
- `dq_result`
- `publisher_delivery_log`
- `pipeline_alert`
- `model_registry`
- `model_eval`
- `model_deployment`

## Docs

- [`docs/architecture_target.md`](docs/architecture_target.md)
- [`docs/architecture_review.md`](docs/architecture_review.md)
- [`docs/dq_rules.md`](docs/dq_rules.md)
- [`docs/ops_runbook.md`](docs/ops_runbook.md)
- [`docs/data-flow.md`](docs/data-flow.md)

## Verified Status

Latest technical sanity checks completed successfully:
- compile checks passed
- `test/test_pipeline_orchestrator.py`: `13 passed`
- smoke pipeline completed
- operational rank artifacts refreshed successfully
- dashboard payload generated from live operational rank data

## Notes

- Smoke mode is synthetic and is meant only for orchestration verification.
- The real dashboard should be read from the latest operational rank artifacts, not from smoke output.
- Delivery collection uses archive data first and falls back to NSE security-wise delivery when needed.
