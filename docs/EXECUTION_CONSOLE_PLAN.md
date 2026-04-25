# Execution Console Rewiring — Plan

This plan tracks the rewiring of the operator-facing **execution console** (FastAPI
backend + React/Vite frontend) into a clean, layered, lint-enforced architecture
fronted by a richer UI based on the *Gemini Canvas V2* design.

It is intentionally narrower than `docs/EXECUTION_PLAN.md`, which covers the
pattern/pipeline domain. This doc owns everything under
`src/ai_trading_system/ui/execution_api/` and `web/execution-console-v2/`.

> **Architectural target.** Three layers, enforced by an AST lint test:
>
> ```
> HTTP (routes/)  →  service (services/)  →  readmodel (services/readmodels/)  →  domain
> ```
>
> Only the HTTP layer is allowed to import `fastapi`/`uvicorn`/`starlette`.
> Services delegate to readmodels; readmodels are pure functions over the
> control-plane DuckDB and the on-disk run artifacts.

---

## Status snapshot

| PR | Phase | Title | Status |
|----|-------|-------|--------|
| #1 | 1 — Move | `interfaces/api/` → `ui/execution_api/` with deprecation shims | ✅ shipped (`f28fe00`) |
| #2 | 1 — Split | Split `app.py` into routers + extract pydantic request schemas | ✅ shipped (`3eef916`) |
| #3-A | 1 — Boundary | AST-based layer-boundary lint + canonical entry-point paths | ✅ shipped (`ab4afdd`) |
| #3-B | 1 — Frontend prep | env-driven config, react-query hooks, shared primitives, openapi codegen | ✅ shipped (`a8c3d38`) |
| #4 | 2a — Backend | Runs introspection: `/runs/{id}/dq`, `/runs/{id}/artifacts`, gated download | ✅ shipped |
| #5 | 2a — Backend | Stocks domain: `/stocks/{symbol}`, `/stocks/{symbol}/ohlcv` | ✅ shipped |
| #6 | 2a — Backend | Ranking detail: `/ranking/{symbol}`, `/ranking/{symbol}/history`, lighter `/workspace/snapshot` | ✅ shipped |
| #7 | 2b — Frontend | Control Tower view + shared chrome (TopBar, command bar, regime/breadth strip) | 📋 future |
| #8 | 2b — Frontend | Ranking view (expandable rows, factor bars, lifecycle visual, comparison tray, score decomposition) | 📋 future |
| #9 | 2b — Frontend | Patterns + Sectors views (funnel, pattern cards, leadership chart, rotation heatmap, drill-down) | 📋 future |
| #10 | 2b — Frontend | Execution view (eligible/watchlist/blocked buckets, orders table, capital widget, risk dashboard) | 📋 future |
| #11 | 2b — Frontend | Runs audit (history table, detail pane, timeline, artifacts, DQ modal, replay/retry) | 📋 future |
| #12 | 2b — Frontend | Stock detail workspace + compare modal (modal/drawer layer, keyboard shortcuts) | 📋 future |

Legend: ✅ shipped · ⏳ in flight · 📋 future.

---

## Phase 1 — Backend rewiring (shipped)

### PR #1 — Move (`f28fe00`)

- `git mv` 11 files from `src/ai_trading_system/interfaces/api/` to `src/ai_trading_system/ui/execution_api/`.
- Created deprecation shims at the old paths via the `sys.modules[__name__] = _real_module` pattern so external callers (Streamlit dashboard, tests, repo-root `ui/*` shims) keep working with a `DeprecationWarning`.
- Rewired `ui/*` shims at the repo root and updated tests.

### PR #2 — Split + schemas (`3eef916`)

- Extracted 4 pydantic request models into `services/schemas/requests.py` (`PipelineRunRequest`, `PublishRetryRequest`, `ShadowRunRequest`, `ResearchLaunchRequest`).
- Created `routes/_deps.py` with `project_root()`, `configured_api_key()`, `API_KEY_HEADER`.
- Split `app.py` (previously monolithic) into 6 routers under `routes/`:
  `health.py`, `snapshots.py`, `runs.py`, `tasks.py`, `processes.py`, `pipeline.py` — covering 19 endpoints.
- `routes/__init__.py` exposes `ALL_ROUTERS` for the bootstrap.
- `app.py` reduced to 88 lines: CORS middleware, API-key middleware, `include_router()` loop.

### PR #3-A — Boundary lint + entry points (`ab4afdd`)

- `tests/lint/test_layer_boundaries.py` — AST walk asserting that
  `fastapi`/`uvicorn`/`starlette` are **not** imported in:
  - `ai_trading_system.domains`
  - `ai_trading_system.pipeline`
  - `ai_trading_system.platform`
  - `ai_trading_system.research`
  - `ai_trading_system.ui.execution_api.services`
  - `ai_trading_system.ui.execution_api.schemas`
  Companion sanity check verifies the HTTP layer **does** import `fastapi`.
- `pyproject.toml`: `[project.scripts]` entry point updated to canonical `ai_trading_system.ui.execution_api.app:main`.
- `app.py`: `uvicorn.run("ai_trading_system.ui.execution_api.app:app", ...)`.

### PR #3-B — Frontend prep (`a8c3d38`)

- Env-driven config: `src/lib/api/client.ts` reads `VITE_EXECUTION_API_BASE_URL`, `VITE_EXECUTION_API_KEY`, `VITE_USE_MOCK_API`, `VITE_DEFAULT_REFETCH_INTERVAL_MS` via `readBoolean/readNumber/readString` helpers.
- `vite.config.ts` now uses `loadEnv()` to honour `VITE_EXECUTION_PROXY_TARGET`.
- `src/vite-env.d.ts` types `ImportMetaEnv`.
- `.env.example` documents every knob.
- Shared primitives: `StatusBadge` + `STATUS_TO_TONE` map + `statusTone()` helper, `EmptyState`, `ErrorState`, `lib/utils/text.ts`.
- React-query layer: `lib/queries/keys.ts` factory under `['execution', <domain>]`; typed hooks `usePipelineWorkspace`, `useRanking`, `useRecentRuns`, `usePatterns`, `useSectors`, `useShadow`, `useRefreshAll`.
- Migrated 7 pages to the new hooks.
- Codegen: `openapi-typescript@^7.4.4` + `gen:api` (offline against `openapi.snapshot.json`) + `gen:api:live` scripts. `src/types/api.gen.ts` (~1018 lines) checked in.

---

## Phase 2a — New backend endpoints (in flight)

### PR #4 — Runs introspection (⏳ in flight)

**Target endpoints:**

- `GET /api/execution/runs/{run_id}/dq?severity=&stage=` — DQ results with per-severity aggregates.
- `GET /api/execution/runs/{run_id}/artifacts` — artifact list grouped by stage, with `download_url` per row.
- `GET /api/execution/artifacts/{run_id}/{stage}/{name}` — gated artifact download (`FileResponse`).

**Security posture (3-layer gate on download path):**

1. URL-segment regex allow-list `^[A-Za-z0-9._-]+$` blocks `..`, slashes, absolute paths.
2. Registry lookup: must match a `pipeline_artifact` row whose URI's basename equals `name`.
3. Resolved path must `Path.resolve().relative_to(pipeline_runs_dir)`.

**Errors:**

- `ArtifactNotFoundError` → HTTP 404.
- `UnsafeArtifactPathError` → HTTP 400.

**Data sources:** `data/control_plane.duckdb` tables `dq_result` and `pipeline_artifact` (defined in `sql/migrations/001_pipeline_governance.sql`).

**Done:**
- Readmodel `services/readmodels/runs_introspection.py` with `get_dq_results_for_run`, `get_artifacts_for_run`, `resolve_artifact_path`, `ArtifactRecord`, `ArtifactNotFoundError`, `UnsafeArtifactPathError`.
- `routes/runs.py` extended with `/{run_id}/dq` + `/{run_id}/artifacts`.
- `routes/artifacts.py` for the gated download (404 on missing record / 400 on path escape).
- `routes/__init__.py` registers the new `artifacts` router.
- `tests/test_execution_api_runs_introspection.py` — 12 tests covering happy path, severity/stage filters, missing DB, missing run, basename mismatch, path traversal at the URL layer, registry-but-file-missing, and registry-points-outside-sandbox.
- `tests/lint/test_layer_boundaries.py` continues to pass (no `fastapi`/`uvicorn`/`starlette` import in the new readmodel).

### PR #5 — Stocks domain (✅ shipped)

**Endpoints:**

- `GET /api/execution/stocks/{symbol}` — fundamentals (from canonical `symbols` master), latest quote (`_catalog` + `_delivery` LEFT JOIN), ranking position (1-based row in `ranked_signals`), lifecycle chips (`rank → breakout → pattern → execution`).
- `GET /api/execution/stocks/{symbol}/ohlcv?from=&to=&interval=&limit=` — daily candles + delivery, ascending chronological order.

**Done:**

- Readmodel `services/readmodels/stock_detail.py` — `get_stock_detail`, `get_stock_ohlcv`, plus helpers (`_lifecycle`, `_frame_row_for_symbol`, `_rank_position`).
- Routes `routes/stocks.py` registered in `routes/__init__.py`.
- Permissive degradation: every block can be `None` independently; `available` is `True` when *any* block is populated, so a metadata-only payload (no rank frames yet) still renders.
- DuckDB casts (`CAST(? AS DATE)` + `< CAST(? AS DATE) + INTERVAL 1 DAY`) survive the strict timestamp/varchar binder.
- Invalid date strings degrade silently to "no filter" rather than 4xx.
- 9 tests covering happy path, unknown symbol, metadata-only fallback, full history, date-range filter, limit (most-recent retained), invalid dates, unknown symbol on OHLCV, missing DB.

### PR #6 — Ranking detail (✅ shipped)

**Endpoints:**

- `GET /api/execution/ranking/{symbol}?run_id=` — full per-symbol ranked row + lifecycle + decision + curated factor block. Optionally pinned to a specific run.
- `GET /api/execution/ranking/{symbol}/history?limit=N` — historical rank position across the most recent N runs (sparkline data, newest first, gaps allowed when symbol absent from a run).
- `GET /api/execution/workspace/snapshot?top_n=N` — slim Control Tower payload (top-N actions + sector leaders + counts) without the heavy `/workspace/pipeline` payload.

**Done:**

- Readmodel `services/readmodels/ranking_detail.py` — `get_ranking_detail`, `get_ranking_history`, `get_workspace_snapshot_compact`, plus helpers (`_extract_factor_block`, `_categorise_factor`, `_decision_from_category`, `_resolve_rank_attempt_dir`, `_load_snapshot_for_run`, `_walk_historical_runs`, `_infer_run_date`).
- Factor extraction: regex-categorises numeric `*_score` / factor columns from `ranked_signals` into the four Canvas buckets (`rs`, `volume`, `trend`, `sector`); anything else lands in `other`. Each bucket's `value` is the max of its contributors so the bar magnitude is representative.
- History walker: walks all `pipeline_runs/*/rank/attempt_*/` dirs, deduplicating to the most-recent attempt per run, sorted by mtime descending. Symbol absent from a run surfaces as `rank_position: null` so the UI can render a continuous timeline with gaps.
- Run-pinned mode: `_load_snapshot_for_run(ctx, run_id)` resolves `pipeline_runs/{run_id}/rank/attempt_*/` and returns a `LatestOperationalSnapshot` with that pinned context.
- Routes `routes/ranking_detail.py` registered in `routes/__init__.py`. The new router's prefix `/api/execution/ranking` does not collide with the existing list endpoint at `/api/execution/ranking` (no path param) — FastAPI routes the exact path first.
- `routes/snapshots.py` extended with `GET /workspace/snapshot`.
- 13 tests covering: latest happy path, run-pinned, unknown run/symbol, factor categorisation, history newest-first ordering, history gaps when symbol missing, missing runs dir, compact workspace top actions, compact workspace empty, list-endpoint regression check.

### Backend Phase 2a complete

With PR #6 shipped, all backend endpoints needed for the Phase 2b frontend uplift are in place. Phase 2b can now begin.

---

## Phase 2b — Frontend uplift (planned)

Ports the *Gemini Canvas V2* design (`/Users/prashant/Downloads/Gemini Canvas V2.docx` — extracted to a single React reference file) into the real `web/execution-console-v2/` dashboard.

Each PR is bounded so it can ship independently behind feature flags or a route-level toggle.

### PR #7 — Control Tower + shared chrome (📋 future)

**Scope.** Top-level shell + the new home page.

- New `ControlTowerPage` (replaces or sits alongside the current dashboard landing).
- Decision Summary banner (top-3 actions, click-through to stock detail).
- Trust banner: System Trusted / Degraded badge + Active Quarantines + Fallback Ratio + Market State (Risk-On/Off, breadth, %>200SMA).
- Pipeline Stage Flow visualizer (existing data, redesigned with status pill chain + DQ-warn affordance).
- Output Summary cards (Top Ranked / Breakouts / Pattern Setups / Sector Leaders) with sparklines.
- Shared chrome: TopBar (refresh-all, env badge, last-updated), Sidebar nav, command bar (`Cmd+K`/`/`).
- Adds `framer-motion` + `recharts` + `@heroicons` already in `package.json`; uses existing react-query hooks.

**Acceptance.** Replaces the existing `pages/DashboardPage.tsx` (or equivalent). Lighthouse perf ≥ 80 on `dev` build. Zero new typecheck or lint errors.

### PR #8 — Ranking view (📋 future)

**Scope.** The richest single view in the design.

- Filter chip bar (All Tiers / Tier A / Breakouts Only / Patterns Active) + search.
- Expandable row: factor progress bars (RS / Volume / Trend / Sector), tier badge, rank-pos chip.
- Expanded panel: Model Explanation (strongest / catalyst / limiting factor), score decomposition (`base → penalty → final`), Lifecycle Visual (`rank → breakout → pattern → execution` chain), Verdict banner, mini auto-chart with pattern overlay.
- Comparison tray (fixed footer, up to 3 symbols) → Compare Factors modal (PR #12 finishes the modal).
- Depends on PR #6 endpoints for the per-symbol explanation payload.

### PR #9 — Patterns + Sectors views (📋 future)

**Patterns:**

- Pipeline conversion funnel (Universe → Pattern Found → Qualified (RS>70) → Execution Ready) with conversion percentages.
- Pattern cards: type-specific SVG glyphs (Cup & Handle, VCP, High Tight Flag, fallback), urgency heat (`🔥 IMMINENT` / `⚠️ NEAR` / `⏳ EARLY`), quality tier label, RS, distance-to-breakout, failure risk + reason.

**Sectors:**

- Early Leader Detection banner (emerging sector callout).
- Sector Leadership chart with capital-flow / breadth sub-bar + per-sector stock count.
- Sector Rotation Heatmap (D-5..D-1 dot grid).
- Selected sector drill-down: auto-generated narrative, RS / breakouts / breadth stats, top constituents, relative-performance line chart vs NIFTY.

### PR #10 — Execution view (📋 future)

**Scope.** Operator-facing trade routing.

- Execution State banner (Live Mode disabled vs enabled, striped warning overlay in Preview), Trust badge, Capital Usage bar.
- Three buckets: **Eligible** (green) / **Watchlist** (amber) / **Blocked** (red), each card click-through to stock detail.
- Execution Orders table (Symbol / Entry / Stop / Target / R:R / Size% / Confidence) with row→detail.
- Live Timeline strip (compact, per-symbol stage progression).
- Right rail: Capital Allocation widget + Portfolio Risk dashboard (concentration, top-sector exposure, est. max drawdown).

### PR #11 — Runs audit (📋 future)

**Scope.** Operator post-mortem surface. Consumes PR #4 endpoints directly.

- KPI strip (latest status / last successful run / failed-runs 24h / publish errors 24h).
- Split pane: history table (left, filterable: All / Production / Research / Failed) + detail pane (right).
- Detail pane: header (run id, domain, trust, data lag, params, copy-id, replay), Verdict + Root-Cause banner, Pipeline Stage Timeline with per-stage attempt + warnings + artifacts + retryable affordance, DQ modal (failed rules + severity + symbols + impact + blocked flag), Artifacts list with download links (consumes PR #4 `/runs/{id}/artifacts`), Publish channels grid, Comparison vs prev run.
- Cross-jump: Open Ranking / Patterns / Execution buttons in detail pane.

### PR #12 — Stock detail workspace + compare modal (📋 future)

**Scope.** The modal/drawer layer the Canvas keyboard shortcuts assume.

- Full-screen Stock Detail Workspace (opens via row click, badge click, command bar, or `c`/`e`/`s` shortcuts).
- Tabs/sections: Overview, Auto-Chart (full size), Factor Decomposition, Pattern History, Decision Trace, Risk & Scenarios.
- Compare Factors modal (up to 3 symbols, tabular factor diff with Absolute/Relative toggle).
- Command bar (`Cmd+K` / `/`): symbol search, tab jump, recent runs jump.
- Keyboard shortcuts: `Esc` close, `c` toggle compare on current symbol, `e` jump to Execution, `s` jump to Sectors.
- Depends on PR #5 (stock detail endpoint) + PR #6 (per-symbol ranking detail).

---

## Phase 3 — Deferred / out-of-scope

These were considered and consciously deferred:

- Pydantic *response* models (currently routes return `dict[str, Any]`). Easy follow-up once the codegen flow is stable enough that wire compatibility is the primary regression risk.
- Streaming / SSE for run progress. Not in design; current design polls via `useRecentRuns(refetchInterval)`.
- Authentication beyond static API key. The Canvas design does not surface auth UI.
- Multi-user collaboration (presence indicators, comments). Out of scope.

---

## How to extend this plan

When you ship a PR, update **only**:

1. The status row in the snapshot table (`📋 future` → `⏳ in flight` → `✅ shipped (commit-sha)`).
2. The "Done so far" / "Remaining" lists if applicable.

Add new PRs by appending rows to the snapshot table and a new `### PR #N` section under the appropriate phase. Do not silently merge phases — each phase should remain individually shippable and revertible.
