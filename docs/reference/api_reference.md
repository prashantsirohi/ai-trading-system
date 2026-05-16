# Execution API Reference

- **Purpose:** Complete catalog of FastAPI endpoints exposed by the execution console backend.
- **Audience:** Operator, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/ui/execution_api/app.py`, `src/ai_trading_system/ui/execution_api/routes/*.py`, `src/ai_trading_system/ui/execution_api/schemas/requests.py`, `src/ai_trading_system/ui/execution_api/routes/_deps.py`.

---

## Service bootstrap

| Item | Value | Source |
|---|---|---|
| CLI entry point | `ai-trading-execution-api` | `pyproject.toml [project.scripts]` → `ui.execution_api.app:main` |
| Default host | `0.0.0.0` | `app.py:76` |
| Default port | `8090` | `app.py:77` |
| App title | `AI Trading Execution API` (version `0.1.0`) | `app.py:39` |

### CORS

Configured at `app.py:40-46`:

```
allow_origins=["*"]
allow_credentials=True
allow_methods=["*"]
allow_headers=["*", API_KEY_HEADER]
```

Wide-open origins; tighten before exposing on a public network.

### Authentication

- Middleware: `app.py:48-63`. Only paths starting with `/api` are gated.
- Header: `x-api-key` (constant `API_KEY_HEADER` at `routes/_deps.py:19`).
- Server reads `EXECUTION_API_KEY` env var via `configured_api_key()` at `routes/_deps.py:28-35`. Blank/unset -> every `/api/*` request returns **HTTP 500** with `{"detail": "Execution API key is not configured"}`.
- Mismatched key -> **HTTP 401** `{"detail": "Unauthorized"}`.
- `OPTIONS` (CORS preflight) bypasses auth.

### Project root

Resolved per-request via `project_root()` (`routes/_deps.py:22-25`), honoring optional `AI_TRADING_PROJECT_ROOT` env override; default is the repo root (5 levels up from `_deps.py`).

### Router registration

All 14 routers are listed in `routes/__init__.py:3-35` and mounted in order via `for router in ALL_ROUTERS: app.include_router(router)` (`app.py:65-66`). Each router declares its own `prefix=` — there is no app-level prefix.

---

## Router: health

- **Source:** `src/ai_trading_system/ui/execution_api/routes/health.py`
- **Prefix:** `/api/execution` (tags `["health"]`) — `health.py:15`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/summary` | Full execution summary (delegates to `get_execution_summary`). | — | `dict[str, Any]` |
| GET | `/api/execution/health` | Returns just the `health` block from the summary. | — | `dict[str, Any]` |

## Router: pipeline (actions)

- **Source:** `src/ai_trading_system/ui/execution_api/routes/pipeline.py`
- **Prefix:** `/api/execution` (tags `["actions"]`) — `pipeline.py:22`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| POST | `/api/execution/pipeline/run` | Kick off a pipeline run (delegates to `run_pipeline_action`). | `PipelineRunRequest` | `{"task": ...}` |
| POST | `/api/execution/pipeline/publish-retry` | Re-trigger publish for a prior run. Raises 400 on `ValueError`. | `PublishRetryRequest` | `{"task": ...}` |
| POST | `/api/execution/shadow/run` | Launch a shadow / backfill refresh. | `ShadowRunRequest` | `{"task": ...}` |

Request schemas (from `schemas/requests.py`):

- **`PipelineRunRequest`**: `label: str = "Execution API pipeline run"`, `stages: list[str]` (default `["ingest","features","rank","publish","perf_tracker"]`), `params: dict[str, Any] = {}`, `run_id: str | None`, `run_date: str | None`.
- **`PublishRetryRequest`**: `local_publish: bool = False`, `run_id: str | None`.
- **`ShadowRunRequest`**: `label: str = "Shadow refresh"`, `backfill_days: int = 0`, `prediction_date: str | None`.

## Router: runs

- **Source:** `src/ai_trading_system/ui/execution_api/routes/runs.py`
- **Prefix:** `/api/execution/runs` (tags `["runs"]`) — `runs.py:20`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/runs` | List recent runs. Query: `limit: int = 20` (1–200). | — | `{"runs": [...]}` |
| GET | `/api/execution/runs/{run_id}` | Run details. 404 on unknown id. | — | `dict[str, Any]` |
| GET | `/api/execution/runs/{run_id}/dq` | DQ rule results for a run; optional `severity`, `stage` query filters. | — | `dict[str, Any]` |
| GET | `/api/execution/runs/{run_id}/artifacts` | Artifact registry grouped by stage. | — | `dict[str, Any]` |

## Router: snapshots

- **Source:** `src/ai_trading_system/ui/execution_api/routes/snapshots.py`
- **Prefix:** `/api/execution` (tags `["snapshots"]`) — `snapshots.py:24`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/ranking` | Ranking snapshot. Query: `limit=25` (1–2500), `stage2_only=False`, `stage2_min_score: float|None` (0–100). | — | `dict[str, Any]` |
| GET | `/api/execution/market` | Market snapshot. Query: `limit=25` (1–200). | — | `dict[str, Any]` |
| GET | `/api/execution/market/breadth` | Historical breadth. Query: `limit=0` (0–10000; 0 = all). | — | `dict[str, Any]` |
| GET | `/api/execution/workspace/pipeline` | Pipeline workspace snapshot. Query: same filters as `/ranking`. | — | `dict[str, Any]` |
| GET | `/api/execution/shadow` | Shadow snapshot. | — | `dict[str, Any]` |
| GET | `/api/execution/workspace/snapshot` | Slim Control Tower payload. Query: `top_n=3` (1–10). | — | `dict[str, Any]` |

## Router: artifacts

- **Source:** `src/ai_trading_system/ui/execution_api/routes/artifacts.py`
- **Prefix:** `/api/execution/artifacts` (tags `["artifacts"]`) — `artifacts.py:27`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/artifacts/{run_id}/{stage}/{name}` | Download a registry-resolved artifact file. 404 if unknown, 400 if path escapes `pipeline_runs_dir`. | — | `FileResponse` (binary; `Content-Type` guessed by `mimetypes`). |

Safety logic lives in `resolve_artifact_path()` (registry lookup + path-containment check).

## Router: stocks

- **Source:** `src/ai_trading_system/ui/execution_api/routes/stocks.py`
- **Prefix:** `/api/execution/stocks` (tags `["stocks"]`) — `stocks.py:27`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/stocks/{symbol}` | Stock detail (fundamentals + quote + ranking + lifecycle). Always 200; UI inspects `available`. | — | `dict[str, Any]` |
| GET | `/api/execution/stocks/{symbol}/ohlcv` | OHLCV + delivery candles. Query (aliases shown): `from` (ISO date), `to` (ISO date), `interval="daily"` (only `daily` supported), `limit: int|None` (1–2000). | — | `dict[str, Any]` |

## Router: ranking_detail

- **Source:** `src/ai_trading_system/ui/execution_api/routes/ranking_detail.py`
- **Prefix:** `/api/execution/ranking` (tags `["ranking-detail"]`) — `ranking_detail.py:28`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/ranking/{symbol}` | Full ranked row + lifecycle + curated factors. Query: `run_id` (pin to a specific run). | — | `dict[str, Any]` |
| GET | `/api/execution/ranking/{symbol}/history` | Historical rank positions. Query: `limit=20` (1–200). | — | `dict[str, Any]` |

Note: the **list** form `GET /api/execution/ranking` is served by the `snapshots` router; no path-collision because this router only declares `{symbol}`-suffixed paths.

## Router: fundamentals

- **Source:** `src/ai_trading_system/ui/execution_api/routes/fundamentals.py`
- **Prefix:** `/api/execution/fundamentals` (tags `["fundamentals"]`) — `fundamentals.py:13`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/fundamentals/latest` | Latest fundamentals snapshot. Query: `limit=25` (1–100). | — | `dict[str, Any]` |

## Router: insight

- **Source:** `src/ai_trading_system/ui/execution_api/routes/insight.py`
- **Prefix:** `/api/execution` (tags `["insight"]`) — `insight.py:13`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/insight/latest` | Latest event-aware insight payload. | — | `dict[str, Any]` |

## Router: sectors

- **Source:** `src/ai_trading_system/ui/execution_api/routes/sectors.py`
- **Prefix:** `/api/execution/sectors` (tags `["sectors"]`) — `sectors.py:18`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/sectors` | All sectors with RS, momentum, quadrant, stage distribution. | — | `dict` (shape per `get_sectors_with_stage`) |
| GET | `/api/execution/sectors/{sector}/constituents` | All NSE stocks in the sector with latest price/technicals/stage. | — | `dict` (shape per `get_sector_constituents`) |

## Router: tasks

- **Source:** `src/ai_trading_system/ui/execution_api/routes/tasks.py`
- **Prefix:** `/api/execution/tasks` (tags `["tasks"]`) — `tasks.py:21`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/tasks` | List recent tasks. Query: `limit=50` (1–300). | — | `{"tasks": [...]}` |
| GET | `/api/execution/tasks/{task_id}` | Task detail. 404 on unknown. | — | `{"task": ...}` |
| GET | `/api/execution/tasks/{task_id}/logs` | Polling-style log snapshot. Query: `after=0`, `limit=300` (1–1000). | — | `dict[str, Any]` |
| GET | `/api/execution/tasks/{task_id}/events` | **Server-Sent Events** stream of task snapshots until terminal status. Query: `cursor=0`. | — | `text/event-stream` (`data: {...}\n\n`) |
| POST | `/api/execution/tasks/{task_id}/terminate` | Terminate a task. 404 on unknown. | — | `dict[str, Any]` |

Terminal statuses (closes the SSE stream): `completed`, `completed_with_publish_errors`, `failed`, `terminated` (`tasks.py:24-31`).

## Router: processes

- **Source:** `src/ai_trading_system/ui/execution_api/routes/processes.py`
- **Prefix:** `/api/execution/processes` (tags `["processes"]`) — `processes.py:16`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/processes` | Snapshot of tracked background processes. | — | `dict[str, Any]` |
| POST | `/api/execution/processes/{pid}/terminate` | Terminate process by PID. | — | `dict[str, Any]` |

## Router: backtest

- **Source:** `src/ai_trading_system/ui/execution_api/routes/backtest.py`
- **Prefix:** `/api/execution/backtest` (tags `["backtest"]`) — `backtest.py:19`

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/backtest/profiles` | List risk profiles under `config/risk_profiles/*.yaml`. | — | `{"profiles": [...]}` |
| POST | `/api/execution/backtest/run` | Run engine-driven backtest; returns summary + trade rows inline. | `BacktestRunRequest` | `dict[str, Any]` |
| POST | `/api/execution/backtest/winner-capture` | Yearly top-gainer capture analysis vs research dynamic rankings. | `WinnerCaptureRequest` | `dict[str, Any]` |

Request schemas (defined inline in `backtest.py:37-52`):

- **`BacktestRunRequest`**: `profile: str` (required), `data_source: str = "pipeline_replay"` (`pipeline_replay` or `research_dynamic`), `from_date`, `to_date` (ISO strings; parsed via `_parse_iso` -> 400 on bad format), `equity: float = 1_000_000.0` (>=0), `persist: bool = True`, `custom_config: dict|None`.
- **`WinnerCaptureRequest`**: `year: int` (1990–2100, required), `exchange: str = "NSE"`, `top_gainers: int = 50` (1–500), `rank_cutoff: int = 50` (1–500), `persist: bool = True`.

## Router: perf_tracker

- **Source:** `src/ai_trading_system/ui/execution_api/routes/perf_tracker.py`
- **Prefix:** `/api/execution/perf-tracker` (tags `["perf-tracker"]`) — `perf_tracker.py:31`
- **Backing store:** `data/research.duckdb` (via `open_research_db(read_only=True)`).

| Method | Path | Purpose | Request schema | Response schema |
|---|---|---|---|---|
| GET | `/api/execution/perf-tracker/coverage` | Date-range + row-count summary. | — | `{"first_date","last_date","dates","rows"}` |
| GET | `/api/execution/perf-tracker/cohorts` | Forward returns + hit rates per cohort band. Query: `lookback_days=90` (0–10000; 0=all). | — | `{"lookback_days", "cohorts":[...]}` |
| GET | `/api/execution/perf-tracker/buckets` | Watchlist bucket attribution. Query: `lookback_days=90` (0–10000). | — | `{"lookback_days", "buckets":[...]}` |
| GET | `/api/execution/perf-tracker/factor-ic` | Spearman IC per factor x window. Query: `windows` (comma ints; default `30,90,180`); 400 on bad input. | — | `{"windows":[...], "factors":[...]}` |
| GET | `/api/execution/perf-tracker/drift` | Factors whose recent IC dropped vs baseline. Query: `recent_window=30`, `baseline_window=180`, `threshold_pct=30.0`. | — | `{"recent_window","baseline_window","threshold_pct","factors":[...], "flagged":[...]}` |

Cohort bands (`perf_tracker.py:34-40`): `top-10` (1–10), `top-50` (1–50), `top-200` (1–200), `51-200`, `201+` (201–10_000_000).

Factor columns scanned (`perf_tracker.py:42-50`): `factor_rs`, `factor_vol`, `factor_trend`, `factor_prox`, `factor_deliv`, `factor_sector`, `factor_momentum_accel`.

---

## Response schemas

Response bodies are typed as `dict[str, Any]` (or `FileResponse`/`StreamingResponse`) in the route signatures — there are no Pydantic response models. Shapes are determined by the service-layer functions imported from `ui/execution_api/services/**`. Treat the JSON returned by each handler as the contract; consumers should pin to the React console’s usage rather than infer from type hints.

## Error responses

| Status | Source | Trigger |
|---|---|---|
| 400 | `pipeline.py:47`, `artifacts.py:42`, `backtest.py:28`, `perf_tracker.py:202,205` | `ValueError` / unsafe artifact path / bad ISO date / bad `windows` query. |
| 401 | `app.py:62` | `/api/*` request with missing or mismatched `x-api-key`. |
| 404 | `runs.py:33`, `tasks.py:44,58`, `artifacts.py:37` | Unknown `run_id`, `task_id`, or artifact not in registry. |
| 500 | `app.py:55-59` | `EXECUTION_API_KEY` unset on the server. |
