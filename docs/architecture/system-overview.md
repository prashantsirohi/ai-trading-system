# System Overview

## Purpose

This repository runs a local, staged NSE workflow that:
- ingests OHLCV and delivery data into local stores
- computes technical and sector-strength features
- ranks symbols and assembles operator-facing payloads
- optionally writes paper-execution actions, orders, fills, and positions
- publishes selected outputs to Google Sheets, Telegram, local summaries, and QuantStats reports
- supports separate research and ML shadow-monitor flows alongside the operational pipeline

## Top-level runtime

Current operational flow:
1. `run.orchestrator` creates or resumes a pipeline run in `data/control_plane.duckdb`.
2. Stage wrappers under `run/stages/` execute `ingest -> features -> rank -> execute -> publish`.
3. OHLCV, delivery, trust, quarantine, and feature metadata live in the operational market store.
4. Per-attempt stage artifacts are written under `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`.
5. Operator UIs and APIs read control-plane records, pipeline artifacts, execution tables, and trust summaries.

## Operational vs research split

Operational domain:
- default domain for orchestrated runs
- writes to `data/ohlcv.duckdb`, `data/feature_store/`, `data/pipeline_runs/`, `reports/`, and `models/`
- backs publish, execution, and operator-console workflows

Research domain:
- selected by `data_domain="research"`
- writes to `data/research/research_ohlcv.duckdb`, `data/research/feature_store/`, `data/research/pipeline_runs/`, `reports/research/`, and `models/research/`
- used by research dataset, training, evaluation, recipe, and shadow-monitor flows

Current limitation:
- domain-aware path helpers exist
- several ranking, publish, and operator UI helpers still hardcode operational paths
- current publish and operator-console workflows should be treated as operational-only unless a code path explicitly says otherwise

## Current ingest source-of-record

The canonical operational pipeline path is:
- primary source: NSE bhavcopy
- fallback source: yfinance only for missing bhavcopy dates
- trust lineage stored in `_catalog` and `_catalog_provenance`
- unresolved symbol-date gaps stored in `_catalog_quarantine`

Alternate Dhan-primary ingestion still exists, but it is not the default orchestrated pipeline path.

## Major storage systems

Operational market store:
- `data/ohlcv.duckdb`
- stores OHLCV, delivery, trust lineage, quarantine state, and feature metadata

Control plane:
- `data/control_plane.duckdb`
- stores pipeline runs, stage attempts, artifacts, DQ results, publish logs, repair runs, model governance, and operator tasks

Execution store:
- `data/execution.duckdb`
- stores paper orders, fills, and trade notes from the execute stage

Reference store:
- `data/masterdata.db`
- SQLite store for symbol metadata, sector mapping inputs, and holidays

## UI surfaces and current status

FastAPI execution backend:
- `src/ai_trading_system/ui/execution_api/app.py`
- current operator API backend exposing JSON and SSE endpoints

React V2 execution console:
- `web/execution-console-v2/ai-trading-dashboard-starter/`
- single dashboard frontend that calls the FastAPI backend
- not served by the FastAPI app

## Legacy and non-canonical surfaces

These modules are present but are not the current runtime source of truth:
- `main.py`: retained compatibility shim that now exits fast with deprecation guidance to `python -m ai_trading_system.pipeline.orchestrator`
- `dashboard/`: compatibility wrappers that re-export current UI modules
- `config/settings.py`: legacy configuration model that does not describe the orchestrated runtime accurately enough to use as canonical documentation
