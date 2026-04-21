# Baseline Inventory

This snapshot captures the repository shape before any module moves into `src/`.

## Major folders

| Path | Purpose |
| --- | --- |
| `analytics/` | Ranking, DQ, ML, pattern, monitoring, and registry logic |
| `channel/` | Legacy report/channel entrypoints and publish helpers |
| `collectors/` | Ingest jobs, vendor collectors, repair scripts, and CLI tools |
| `config/` | Static config files and settings helpers |
| `core/` | Shared runtime contracts, env/bootstrap, path, logging, and config facades |
| `dashboard/` | Thin compatibility layer re-exporting UI modules |
| `data/` | Runtime databases, feature store, raw files, and pipeline artifacts |
| `docs/` | Architecture, operations, reference, and refactor notes |
| `execution/` | Execution models, store, adapters, policies, and service layer |
| `features/` | Feature computation and feature store logic |
| `publishers/` | Google Sheets, Telegram, dashboard, and QuantStats publishers |
| `research/` | Research workflows, backtests, training, and recipes |
| `run/` | Production pipeline entrypoints, preflight, alerts, and stage wrappers |
| `services/` | Domain orchestration for ingest, features, rank, execute, and publish |
| `tests/` | Unit, integration, smoke, and fixture coverage |
| `ui/` | Streamlit/FastAPI surfaces plus read models and operator services |
| `utils/` | Legacy helper modules still used by parts of the runtime |
| `src/` | New placeholder package root for future migration only |

## Runtime shape today

- `run/orchestrator.py` is the main staged pipeline entrypoint.
- Stage artifacts are written under `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`.
- `services/` contains newer orchestration logic, while many domain modules still live in their existing top-level packages.
- `core/paths.py` and `core/logging.py` act as facades, but direct `utils.*` imports still exist across the codebase.
- `main.py` is intentionally retained as a deprecated compatibility shim and is covered by `tests/test_main_entrypoint.py`.

## Stage wrappers

| Stage | Wrapper | Primary implementation path |
| --- | --- | --- |
| `ingest` | `run/stages/ingest.py` | `services/ingest/`, `collectors/` |
| `features` | `run/stages/features.py` | `services/features/`, `collectors/`, `features/` |
| `rank` | `run/stages/rank.py` | `services/rank/`, `analytics/`, `channel/` |
| `execute` | `run/stages/execute.py` | `execution/`, `services/execute/` |
| `publish` | `run/stages/publish.py` | `run/publisher.py`, `publishers/`, `services/publish/` |

## Noted cleanup follow-ups

- Tracked binary data observed: `data/masterdata.db`.
- This migration step does not delete or untrack local data; treat that as a separate, explicit follow-up.
