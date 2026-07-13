# UI Architecture

- **Purpose:** Describe the operator UI as it is built today — the FastAPI execution backend and the React V2 console — including auth, CORS, router layout, and frontend stack.
- **Audience:** Operators running the console; engineers adding routes or React views.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/ui/execution_api/app.py`, `src/ai_trading_system/ui/execution_api/routes/`, `web/execution-console-v2/ai-trading-dashboard-starter/package.json`, `pyproject.toml [project.scripts]`, `grep -rni streamlit src/`.

## FastAPI execution console — `ai-trading-execution-api`

- App factory: `src/ai_trading_system/ui/execution_api/app.py::create_app` (line 38).
- Launched by the `ai-trading-execution-api` console script (`pyproject.toml` → `ui.execution_api.app:main`).
- Default bind: `host=0.0.0.0`, `port=8090` (`app.py:74-78`).
- Uvicorn launch in `main()` (`app.py:81-88`); module: `ai_trading_system.ui.execution_api.app:app`.

### CORS

```
allow_origins=["*"]
allow_credentials=True
allow_methods=["*"]
allow_headers=["*", API_KEY_HEADER]
```

(`app.py:42-46`.) This is intentionally permissive — the API is fronted by the API-key middleware below.

### API key authentication

Middleware at `app.py:48-63`:

1. `OPTIONS` (CORS preflight) bypass auth.
2. Requests under `/api/*` must carry header `API_KEY_HEADER` matching `configured_api_key()` (`routes/_deps.py`). Missing config → 500 `Execution API key is not configured`; mismatch → 401 `Unauthorized`.
3. Non-`/api` paths are unauthenticated.

There is no per-route override; auth is global for `/api/*`.

### Routers

Mounted in `create_app()` via `for router in ALL_ROUTERS: app.include_router(router)` (`app.py:65-67`). `ALL_ROUTERS` is assembled in `routes/__init__.py`. Router modules under `src/ai_trading_system/ui/execution_api/routes/` (14 total, excluding `__init__.py` and `_deps.py`):

| Router file | Concern |
|---|---|
| `health.py` | Liveness / readiness |
| `pipeline.py` | Pipeline status & control surface |
| `runs.py` | Run history, run detail, attempt introspection |
| `snapshots.py` | Latest operational snapshot lookup |
| `artifacts.py` | Artifact registry browsing / download |
| `stocks.py` | Per-symbol detail |
| `ranking_detail.py` | Ranking explanation views |
| `fundamentals.py` | Fundamentals readmodel |
| `insight.py` | Insight stage output |
| `sectors.py` | Sector dashboard data |
| `tasks.py` | Operator task queue |
| `processes.py` | Process status / background jobs |
| `backtest.py` | Research backtest results |
| `perf_tracker.py` | Forward-return cohort tracker |

Exact path prefixes and method bodies are not catalogued here; see [`../reference/api_reference.md`](../reference/api_reference.md) (when populated) or read the router modules directly.

## React V2 console

- Location: `web/execution-console-v2/ai-trading-dashboard-starter/`.
- Stack (from `package.json`): React 18, Vite, TypeScript (`tsc -b && vite build`), TanStack Query, TanStack Table, Heroicons.
- API types: generated from FastAPI's OpenAPI schema via `openapi-typescript` (`gen:api` and `gen:api:live` scripts) into `src/types/api.gen.ts`. The repo ships a snapshot (`openapi.snapshot.json`) so the frontend can regenerate types without a running backend.
- Mock-data mode: documented in `web/execution-console-v2/ai-trading-dashboard-starter/README.md`; lets the console run fully offline against fixtures.
- Playwright config (`playwright.config.ts`) present for e2e tests.

The React app talks to FastAPI through the `/api/*` surface, sending the API key header. The repo includes a Vite proxy debugging note at `web/execution-console-v2/ai-trading-dashboard-starter/PROXY_ISSUES.md`.

## Streamlit status

`grep -rni streamlit src/` returns exactly one match: a comment line in `src/ai_trading_system/pipeline/migrations/013_events_enrichment_log.sql:7` ("Operators can inspect (via Streamlit / SQL) which triggers fired"). **No Streamlit usage in active code paths.** Any docs that imply a Streamlit operator UI are stale.

## Related

- [System Guide](../SYSTEM_GUIDE.md) — where the UI fits in the larger system.
- [operational_data_flow.md](./operational_data_flow.md) — what the UI is showing.
- [storage_and_lineage.md](./storage_and_lineage.md) — the DuckDB readmodels that back the routers (`ui/execution_api/services/readmodels/`).
