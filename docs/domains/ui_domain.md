# UI Domain

- **Purpose:** Operator-facing UI — FastAPI backend serving the React V2 console.
- **Audience:** Developer, operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/ui/execution_api/`](../../src/ai_trading_system/ui/execution_api/), [`web/execution-console-v2/`](../../web/execution-console-v2/)

---

## Responsibility

Surface pipeline state (runs, stages, artifacts, ranking, fundamentals, perf tracker) to a browser console, plus operator actions (trigger pipeline, retry stage, inspect artifacts).

## Package / module ownership

### FastAPI backend (`src/ai_trading_system/ui/execution_api/`)

| Module | Role |
|---|---|
| `app.py` | FastAPI app + lifespan + CLI entrypoint (`ai-trading-execution-api`). |
| `routes/` | 14 router modules (see below). |
| `services/` | Service-layer logic shared across routes. |
| `schemas/` | Pydantic request/response models. |

**Routers** (under `routes/`): `health.py`, `pipeline.py`, `runs.py`, `snapshots.py`, `artifacts.py`, `stocks.py`, `ranking_detail.py`, `fundamentals.py`, `insight.py`, `sectors.py`, `tasks.py`, `processes.py`, `backtest.py`, `perf_tracker.py`.

### React frontend (`web/execution-console-v2/`)

Vite + React + TypeScript. Has a mock-data toggle for offline dev. Consumes `/api/*` routes. See [`web/execution-console-v2/README.md`](../../web/execution-console-v2/README.md).

## Public contracts

- HTTP `/api/*` routes — full endpoint table in [`docs/reference/api_reference.md`](../reference/api_reference.md).
- API key auth via `configured_api_key()`.
- CORS: `allow_origins=["*"]` (per truth map).

## Storage ownership

None — UI is read-mostly. Writes go through pipeline + execution service layers.

## Dependencies

- Reads from `data/control_plane.duckdb`, `data/ohlcv.duckdb`, `data/research.duckdb`, and pipeline run artifacts.
- Imports services from operational domains (execution, ranking, fundamentals, perf_tracker).

## Extension points

- New endpoint: see [`docs/development/adding_new_api_endpoint.md`](../development/adding_new_api_endpoint.md).
- New React page: add under `web/execution-console-v2/src/`.

## Streamlit

**No Streamlit usage in active code paths** (per truth map). Any reference in older docs is stale.

## Known gaps

- API auth model (single shared key) is minimal — fine for local/single-operator but not multi-tenant.
- CORS allow-all is acceptable for localhost but should be tightened for any non-local deployment.

## See also

- [`docs/architecture/ui_architecture.md`](../architecture/ui_architecture.md)
- [`docs/reference/api_reference.md`](../reference/api_reference.md)
- [`docs/decisions/ADR-0005-react-operator-workspace.md`](../decisions/ADR-0005-react-operator-workspace.md)
