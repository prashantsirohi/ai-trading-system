# Architecture Overview

- **Purpose:** One-page mental model of the AI Trading System as it actually exists today.
- **Audience:** New engineers, operators, and reviewers who need ground truth before diving into a domain doc.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/pipeline/orchestrator.py:41`, `src/ai_trading_system/platform/db/paths.py`, `src/ai_trading_system/ui/execution_api/app.py`, `pyproject.toml [project.scripts]`, `web/execution-console-v2/`, `docs/_audit/current_code_truth_map.md`.

## Mission

A single-operator, NSE-focused trading and research system. It ingests Indian-equity price/volume data, computes technical features, ranks stocks, builds execution candidates, optionally enriches with corporate-action events and fundamentals, dispatches paper or live orders, and publishes the resulting narrative through multiple channels. Alongside the operational pipeline, an isolated research domain runs backtests, factor sweeps, optimizations, and model training without contaminating production data.

## 11-stage operational pipeline

Defined in `src/ai_trading_system/pipeline/orchestrator.py:41` (`PIPELINE_ORDER`):

```
ingest → features → rank → fundamentals* → candidates → events
    → execute → insight → narrative → publish → perf_tracker
```

`fundamentals` is in `OPTIONAL_STAGES` (`orchestrator.py:44`) and is skipped unless explicitly enabled.

See [operational_data_flow.md](./operational_data_flow.md) for the per-stage breakdown.

## Major subsystems

| Subsystem | Path | Role |
|---|---|---|
| Pipeline orchestrator | `src/ai_trading_system/pipeline/` | Stage wrappers, run/stage/attempt model, preflight, alerts, DQ engine, SQL migrations |
| Control plane (DuckDB) | `data/control_plane.duckdb` | Run lifecycle, artifacts, DQ results, model registry, pattern cache (`pipeline/registry.py:300`) |
| OHLCV store (DuckDB) | `data/ohlcv.duckdb` | Canonical operational price data (`platform/db/paths.py:98`) |
| Feature store | `data/feature_store/<symbol_id>/features_<start>_<end>.parquet` | Indicator snapshots written by the `features` stage |
| Domains | `src/ai_trading_system/domains/{ingest,features,ranking,candidates,fundamentals,catalysts,events,execution,publish,risk,strategy}/` | Business logic |
| Publishing | `src/ai_trading_system/domains/publish/` | Telegram, Google Sheets, QuantStats, PDF, daily-gainers, watchlist digest |
| UI backend | `src/ai_trading_system/ui/execution_api/` (FastAPI, port 8090) | 14 routers powering the React console |
| UI frontend | `web/execution-console-v2/ai-trading-dashboard-starter/` (Vite + React 18 + TS) | Operator workspace |
| Research isolation | `src/ai_trading_system/research/` + `DATA_DOMAIN=research` | Backtests, optimization, perf tracker — separate DuckDB |

## Storage at a glance

| Store | Path | Notes |
|---|---|---|
| Operational OHLCV | `data/ohlcv.duckdb` | `platform/db/paths.py:98` |
| Control plane | `data/control_plane.duckdb` | Runs/stages/artifacts/DQ/model registry/pattern cache |
| Execution ledger | `data/execution.duckdb` | Default path in `domains/execution/store.py:29`. The audit truth map mis-stated this as living inside `control_plane.duckdb`; the code today writes to a separate file. |
| Research OHLCV | `data/research/research_ohlcv.duckdb` | `platform/db/paths.py:111` (research domain re-roots under `data/research/`) |
| Feature store | `data/feature_store/<symbol_id>/*.parquet` | Per-symbol Parquet |
| Pipeline runs | `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/<artifact>` | All stage artifacts |

See [storage_and_lineage.md](./storage_and_lineage.md).

## Operational vs research separation

`platform/db/paths.py::resolve_data_domain()` honours `DATA_DOMAIN` (default `operational`). Setting `DATA_DOMAIN=research` re-roots OHLCV, feature store, pipeline runs, datasets, models, and reports under `data/research/` and `models/research/` (`paths.py:107-118`). Research recipes run standalone via the `ai-trading-research-recipe` entrypoint; they are not wired into the operational pipeline except via the optional ML overlay in the `rank` stage (loaded from the `model_registry` table).

## Trust & DQ in three sentences

Ingest computes a per-run trust summary with statuses `trusted / degraded / blocked / legacy / missing` (`domains/ingest/trust.py:1047-1066`). The DQ engine (`pipeline/dq/engine.py`) evaluates rules per stage and classifies failures into bands `green / amber / red_repairable / red_block`; `red_block` failures (from `HARD_FLOOR_RULES` such as `ingest_catalog_not_empty`, `features_snapshot_created`, `rank_artifact_not_empty`) raise `DataQualityCriticalError` regardless of `dq_mode`. In `relaxed` mode (default), non-hard-floor critical failures are downgraded from `red_repairable` to `amber` and stamped with `relaxed_from = red_repairable`. See [data_trust_and_dq.md](./data_trust_and_dq.md).

## UI surfaces

- **FastAPI execution console** — `src/ai_trading_system/ui/execution_api/app.py`, default `localhost:8090`, CORS `allow_origins=["*"]`, API-key middleware on all `/api/*` routes (`app.py:42-63`).
- **React V2 console** — `web/execution-console-v2/ai-trading-dashboard-starter/` (Vite + React 18 + TypeScript, TanStack Query/Table, OpenAPI types generated from the FastAPI app). Mock-data toggle for offline development.
- **Streamlit** — `grep -rni streamlit src/` returns exactly one hit: a comment inside `pipeline/migrations/013_events_enrichment_log.sql:7`. **No Streamlit usage in active code paths.**

See [ui_architecture.md](./ui_architecture.md).

## Planned & adjacent extension layers

Fundamentals, catalysts, and optimization are built today; their docs live alongside the rest:
- [../stages/fundamentals.md](../stages/fundamentals.md)
- [../stages/events.md](../stages/events.md) (catalyst trigger collection, LLM enrichment)
- [strategy-optimizer.md](../_legacy/archived_2026-05-16/architecture_strategy-optimizer.md) (Optuna-based research-domain optimizer)

**Live trading.** The Dhan adapter exists and ships orders, but order types are hardcoded MARKET + INTRADAY (`domains/execution/models.py`) and kill-switch / sandbox guardrails have not been audited. **Paper mode is the safe default; live trading is not certified production-ready.**

## Where to go next

- Full per-stage flow: [operational_data_flow.md](./operational_data_flow.md)
- Storage & lineage: [storage_and_lineage.md](./storage_and_lineage.md)
- Trust & DQ: [data_trust_and_dq.md](./data_trust_and_dq.md)
- UI architecture: [ui_architecture.md](./ui_architecture.md)
- Canonical layout & legacy: [target_architecture.md](./target_architecture.md)
