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
| #7 | 2b — Frontend | Control Tower view + shared chrome (TopBar, command bar, regime/breadth strip) | ✅ shipped |
| #8 | 2b — Frontend | Ranking view (expandable rows, factor bars, lifecycle visual, comparison tray, score decomposition) | ⏳ in flight |
| #9 | 2b — Frontend | Patterns + Sectors views (funnel, pattern cards, leadership chart, rotation heatmap, drill-down) | ⏳ in flight |
| #10 | 2b — Frontend | Execution view (eligible/watchlist/blocked buckets, orders table, capital widget, risk dashboard) | ⏳ in flight |
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

### PR #7 — Control Tower + shared chrome (✅ shipped)

**Endpoints consumed:** `/api/execution/workspace/snapshot` (PR #6) + the existing `/api/execution/workspace/pipeline` (used by `usePipelineWorkspace` for the trust pill).

**Done:**

- `lib/api/workspace.ts` + `lib/queries/index.ts` `useWorkspaceSnapshot(topN=3)` hook keyed under `['execution', 'workspace-snapshot', topN]`.
- `pages/ControlTowerPage.tsx` — the new landing route at `/`. Composes Decision Summary banner, Trust banner, Output Summary cards. Falls back to a graceful empty state when the snapshot is unavailable.
- `components/control-tower/DecisionSummaryBanner.tsx` — top-N action chips with verdict-tone colour (green/amber/rose/blue).
- `components/control-tower/TrustBanner.tsx` — system-trust pill (Trusted / Degraded / Blocked / No Run Yet) + counter strip (top sector / ranked / breakouts / patterns) + animated heartbeat dot when trusted.
- `components/control-tower/OutputSummaryCards.tsx` — 4-card navigation row with hover glow keyed off card tone (blue / emerald / purple / amber). Cards click through to `/ranking`, `/patterns`, `/sectors`.
- `components/control-tower/CommandBar.tsx` — modal command palette with type-to-filter scoring (exact / starts-with / contains tiers). Arrow keys navigate, Enter selects, Esc closes. Click-outside also closes.
- `components/control-tower/icons.tsx` — six inline SVG glyphs (`TargetIcon`, `ArrowUpRightIcon`, `ShieldCheckIcon`, `ShieldAlertIcon`, `CommandIcon`) so the design doesn't pull in `lucide-react` as a second icon library.
- `lib/hooks/useCommandBar.ts` — global keyboard hook bound to ⌘K / Ctrl+K (always) and `/` (only when not typing in an input).
- `components/layout/AppLayout.tsx` — mounts the command bar once at the shell level so every page inherits the shortcut.
- `components/layout/Sidebar.tsx` — adds a "Control Tower" entry pointing at `/`. Uses NavLink `end` prop on the home route so it doesn't stay highlighted on every nested path.
- `components/layout/TopBar.tsx` — adds a colour-coded trust pill on the left and a `⌘K` command button on the right; the existing Refresh + Retry Publish buttons stay in place.
- `App.tsx` — `/` routes to `ControlTowerPage` (replacing the previous `<Navigate to="/pipeline">`); a catch-all sends unknown paths back to `/`.

**Verification:** `tsc -b --noEmit` clean. Production build (`npm run build`) succeeds (1659 modules transformed). Existing Playwright e2e specs at `tests/e2e/pipeline-and-ranking.spec.ts` are unaffected because they navigate directly to `/pipeline` and `/ranking`.

**Deliberately out of scope (Canvas design parity gaps):**

- Pipeline Stage Flow visualiser. Will land in PR #11 (Runs audit) where the timeline payload data lives.
- Sparklines on the summary cards. Will land in PR #11 once `/ranking/{symbol}/history` is consumed at scale.
- Live regime / breadth indicators. Need a new backend endpoint not yet in scope; tracked as a follow-up.

### PR #8 — Ranking view (⏳ in flight)

**Endpoints consumed:** `/api/execution/ranking?limit=` (existing list, used for the table feed) + `/api/execution/ranking/{symbol}` and `/api/execution/ranking/{symbol}/history` (PR #6, used lazily by the expanded row).

**Done:**

- `lib/api/ranking.ts` extended with `getRankingDetail(symbol, runId?)` and `getRankingHistory(symbol, limit)` plus a typed camelCase shape (`RankingDetail`, `LifecycleStage`, `FactorBlock`, `RankingDetailDecision`, `RankingHistoryPoint`). Snake_case→camelCase mapping happens at the fetch layer.
- `lib/queries/keys.ts` factories: `rankingDetail(symbol, runId)` and `rankingHistory(symbol, limit)` keyed under `['execution', 'ranking-detail' | 'ranking-history', …]`.
- `lib/queries/index.ts` hooks `useRankingDetail` + `useRankingHistory`. Both gate on `enabled: Boolean(symbol)` so unmounted/empty rows don't fire.
- `lib/mock/rankingDetails.ts` provides backend-shaped fallbacks (`getRankingDetailFallback`, `getRankingHistoryFallback`) keyed off the existing `rankingMock`. Verdict + lifecycle states are derived from tier and breakout state so the offline view stays representative.
- `components/ranking/FilterChipBar.tsx` — pill bar (All / Tier A / Breakouts / Patterns) with a search input that matches symbol or sector. Surfaces `matched / total` count.
- `components/ranking/TierBadge.tsx` — small circular pill, emerald/amber/rose by tier.
- `components/ranking/FactorBars.tsx` — RS / Volume / Trend / Sector bars in two variants: `inline` (compact, 4-column grid for the row) and `expanded` (large 2-column grid for the expansion panel). Falls back to row-derived values when the backend factor block is empty.
- `components/ranking/LifecycleVisual.tsx` — four-stage chain (`rank → breakout → pattern → execution`). State per stage drives the dot + ring colour (complete / active / blocked / pending).
- `components/ranking/VerdictBanner.tsx` — emerald/amber/rose/blue verdict pill + confidence + reason from the decision block.
- `components/ranking/ScoreDecomposition.tsx` — three-tile `base → penalty → final` strip. Base = mean of the four canvas factor values; penalty = whatever's required to reach the published composite; final = the composite itself.
- `components/ranking/ModelExplanation.tsx` — three-up callout: strongest factor (emerald), catalyst (blue), limiting factor (rose). Catalyst text follows breakout > pattern > maintenance precedence.
- `components/ranking/MiniChart.tsx` — recharts area sparkline of historical rank position with the y-axis reversed so a *higher* line means *better* rank. Drops runs where the symbol was absent so the line stays continuous.
- `components/ranking/ExpandedRowPanel.tsx` — composes Verdict / Model Explanation / Score Decomposition / Factor Bars / Lifecycle / MiniChart. Uses the row's data as fallback so the panel never blanks out while the live detail is loading.
- `components/ranking/ComparisonTray.tsx` — fixed bottom footer chip stack (max 3 symbols). "Compare Factors" CTA enables once two symbols are pinned. Triggers a non-blocking notice that the modal lands in PR #12 — the selection is retained.
- `components/tables/RankingTable.tsx` — full rewrite. Sortable columns (`#`, `Ticker`, `Score`, inline factor bars, `Sector`, `Pattern`, `Breakout`, expand caret). Each row stamps `data-symbol` + `data-expanded` for e2e coverage. Expansion panel renders inline (`<tr><td colSpan>…</td></tr>`). The previous row-click → drawer behaviour is gone (the drawer is superseded by the expansion panel; PR #12 introduces the full Stock Detail Workspace).
- Backwards-compatible: `expandedSymbol` / `onToggleExpand` / `comparedSymbols` / `onToggleCompare` are optional props on `RankingTable`. When omitted, the table renders as a slim non-interactive list — keeps `PipelinePage`'s "Top Ranked Candidates" embed working unchanged.
- `pages/RankingPage.tsx` rewritten to own filter / expand / compare state and compose `FilterChipBar` + `RankingTable` + `ComparisonTray`. Loading / error / empty states preserved.
- `tests/e2e/pipeline-and-ranking.spec.ts` updated: the ranking smoke test now asserts the expansion panel renders Verdict / Model Explanation / Score Decomposition / Lifecycle / Factor Bars headings, plus a third spec that adds a symbol to the compare tray.

**Verification:** `tsc -b --noEmit` clean. Production build (`npm run build`) succeeds (1668 modules transformed).

**Deliberately out of scope:**

- Compare Factors modal — lands in PR #12 alongside the full Stock Detail Workspace; the tray retains its selection across that boundary.
- The legacy `components/drawers/SymbolDetailDrawer.tsx` is left untouched but no longer wired in. PR #12 will retire it in favour of the workspace.

### PR #9 — Patterns + Sectors views (⏳ in flight)

**Endpoints consumed:** `/api/execution/ranking?limit=` + `/api/execution/market` (existing — no new backend in this PR). Stage counts and per-sector constituent counts are composed client-side from the ranking feed.

**Patterns — done:**

- `components/patterns/PatternIcons.tsx` — inline SVGs for Cup & Handle, VCP, High-Tight Flag, Round Bottom, Flat Base, Tight Flag, plus a generic fallback. Helper `patternIconFor(pattern)` maps row pattern strings to a glyph component.
- `components/patterns/PipelineFunnel.tsx` — four-stage strip (Universe → Pattern Found → Qualified (RS>70) → Execution Ready) with width proportional to count and a `% thru` caption between stages.
- `components/patterns/PatternCard.tsx` — Canvas-style card per symbol: type glyph, urgency pill (🔥 Imminent / ⚠️ Near / ⏳ Early), tier + RS + sector-RS micro-stats, derived distance-to-breakout, and failure-risk assessment with rationale. Card click feeds the parent's "selected" state with a hint that the full pattern detail lands in PR #12.
- `pages/PatternsPage.tsx` rewritten to compose the funnel + a filter bar (All / Imminent / Qualified) over the pattern card grid.
- Funnel inputs derived purely client-side: `Universe = useRanking.rows.length`, `Pattern Found = patterns rows with pattern !== 'N/A'`, `Qualified = … && rs > 70`, `Execution Ready = … && breakout`. Documented in `pages/PatternsPage.tsx`.

**Sectors — done:**

- `components/sectors/EarlyLeaderBanner.tsx` — surfaces the strongest sector with positive momentum that isn't yet in the top-2 by RS rank (where rotation alpha lives). Renders nothing when no sector qualifies.
- `components/sectors/SectorLeadershipChart.tsx` — replaces the old single-bar chart with a per-sector card showing quadrant pill, ranked-constituent count, RS rank, capital-flow bar (RS), and breadth proxy bar (`1 - momentumRank/maxMomentumRank`). Click selects the sector for drill-down.
- `components/sectors/SectorRotationHeatmap.tsx` — D-5..D-1 dot grid composed client-side from `rs100 → rs50 → rs20 → rs → momentum-adjusted rs`. Tone scale is emerald/emerald-soft/amber/rose/rose. Documented as a synthetic-but-directional read until a per-day history endpoint lands.
- `components/sectors/SectorDrilldown.tsx` — narrative paragraph (trend × breadth × breakouts), 4-stat row (RS / Momentum / Constituents / Breakouts), recharts line chart over the 4-step rolling-RS series, and top-3 ranked constituents pulled from the ranking feed by sector match.
- `pages/SectorsPage.tsx` rewritten to stack EarlyLeaderBanner + a 2-column grid (Leadership chart + Rotation heatmap) + Drill-down. Auto-selects the top-ranked sector on first load; selection synchronises between the chart and the heatmap.

**Verification:** `tsc -b --noEmit` clean. Production build (`npm run build`) succeeds (≈1670 modules transformed).

**Deliberately out of scope:**

- Per-day historical RS rank endpoint. Heatmap proxies from the rolling RS columns we already have until a true history endpoint ships.
- Relative-performance line vs NIFTY. Need a baseline-symbol endpoint not yet in scope; the drill-down currently plots the sector's own rolling RS as a temporary stand-in.
- Pattern detail modal — lands in PR #12 alongside the Stock Detail Workspace.

### PR #10 — Execution view (⏳ in flight)

**Endpoints consumed:** `/api/execution/ranking?limit=` + `/api/execution/workspace/snapshot` (existing). No new backend in this PR — every per-order field is derived in `components/execution/derive.ts` until a routing endpoint lands.

**Done:**

- New `VITE_EXECUTION_MODE` env var (`preview` | `live`) + `EXECUTION_MODE` constant in `lib/api/client.ts`. `.env.example` and `vite-env.d.ts` updated. The toggle is cosmetic (it disables the orders table and stripes the banner) — actual gating still lives in the upstream trust pipeline.
- `components/execution/derive.ts` — pure helpers `bucketFor(row)`, `deriveOrder(row)`, `deriveExecution(rows)`. Bucketing rule: Eligible = Tier-A + breakout, Blocked = Tier-C or sectorStrength<55, otherwise Watchlist. Per-order stop/target/size/confidence computed from price + tier + score + sector strength. Aggregates: capital used %, top-sector exposure, single-symbol concentration, naive estimated max drawdown.
- `components/execution/ExecutionStateBanner.tsx` — Preview-mode striped overlay, mode pill, trust pill, capital-used progress bar, eligible-count caption.
- `components/execution/BucketColumns.tsx` — three colour-coded columns (emerald / amber / rose) of routable / watchlist / blocked symbol cards. Each card surfaces tier, RS, score, sector, and breakout flag.
- `components/execution/OrdersTable.tsx` — sortable order plan (Symbol / Entry / Stop / Target / R:R / Size% / Confidence). Confidence column ships with an inline 0-100 progress bar tone-keyed at 75/55. Row click is exposed for the future Stock Detail Workspace (PR #12). The whole table dims when `EXECUTION_MODE === 'preview'`.
- `components/execution/LiveTimeline.tsx` — per-symbol four-dot stage progression (`rank → breakout → pattern → execution`). Active stage pulses; blocked symbols flash rose. Renders the top 8 ranked symbols.
- `components/execution/CapitalWidget.tsx` — segmented horizontal bar coloured by symbol with a per-name legend and an "Available" footer row.
- `components/execution/PortfolioRiskDashboard.tsx` — three threshold gauges: Concentration, Top Sector, Est. Max Drawdown. Each renders a value + tone (`Healthy` / `Watch` / `Hot`) keyed off conservative thresholds documented in code.
- `pages/ExecutionPage.tsx` — full rewrite composing the banner, buckets, orders table, live timeline, and the right-rail capital + risk widgets. Loading / error / empty states preserved. Default capital ceiling is 30% (configurable in code; backend policy will own this once a routing endpoint exists).

**Verification:** `tsc -b --noEmit` clean. Production build (`npm run build`) succeeds (≈1690 modules transformed).

**Deliberately out of scope:**

- Real `/api/execution/orders` (or equivalent) endpoint — orders are derived heuristically from the ranking feed today. Every derivation in `derive.ts` is documented so the swap-out is mechanical when the backend exists.
- Live "submit" affordance / order routing button. The page deliberately *only* visualises the plan in Preview mode.
- Per-symbol risk-adjusted size policy from the broker side. Today's size% is `min(6, 6 × tierWeight × sectorStrength/100)`.
- Cross-jump from order rows to the Stock Detail Workspace — the click handler is wired but the workspace itself lands in PR #12.

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
