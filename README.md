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

### `dashboard/`
- `app.py`
  - Streamlit dashboard
  - ranking
  - charts
  - pipeline health
  - sector dashboard
  - breakout scan

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

The rank stage also emits a dedicated breakout scan using:

- close above prior 20-day range high
- volume expansion
- bullish supertrend state
- ADX confirmation
- near-high context

## Dashboard

Start Streamlit:

```bash
. .venv/bin/activate
python -m streamlit run dashboard/app.py
```

The dashboard reads the latest operational rank artifacts and shows:
- pipeline health
- ranked signals
- factor profile
- breakout scan
- sector dashboard
- charts with technical indicators
- regime and data freshness checks

## Quick Start

Create and activate the virtual environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Run tests:

```bash
python -m pytest -q test/test_pipeline_orchestrator.py
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
