# Phase 4B Read-Only Operator Dashboard

- **Purpose:** Run and verify the API-only Phase 4B operator dashboard.
- **Audience:** Operators, frontend developers, reviewers, and deployers.
- **Last verified:** 2026-07-15
- **Source of truth:** `web/execution-console-v2/ai-trading-dashboard-starter/src/phase4/`.
- **Backend contract:** `phase4a-api-schema-v1` under `/api/v1`.

## Architecture and boundary

Phase 4B reuses the repository's React 18, TypeScript, Vite, React Router,
TanStack Query/Table, CSS, Vitest, Testing Library, axe-core, and Playwright
stack. It is a separately hosted static build; FastAPI does not serve or write
the build. Vite proxies same-origin `/api` requests for local development.

The production import graph begins at `src/App.tsx` and imports only the Phase
4B application. Its client exposes one `get` method, accepts only `/api/v1/`
paths, uses abortable bounded-time requests, retries only transient GET failures,
and supports request IDs, bearer/API-key headers, ETags, 304, and typed errors.
It has no database, DuckDB, artifact, DATA_ROOT, filesystem, broker, pipeline,
or Python-domain access. Older console source files remain for history but are
not reachable from the Phase 4B build.

The browser never recomputes stages, rank, scan routing, candidate lifecycle,
governance authority, calibration, readiness, freshness, or lineage. It renders
`data`, `meta.partial`, `meta.limitations`, `meta.freshness`, `meta.lineage`, and
`meta.pagination` as returned by Phase 4A.

## Authentication and security

The login accepts a bearer token or API key. The value stays in React page
memory and is cleared by sign-out or page reload. It is never written to browser
storage, shown after authentication, logged, or placed in a URL. Distinct states
exist for 401, 403, 404, 409, 429, 503, timeout, and unexpected errors. Backend
text is rendered as ordinary React text; the app uses no unsafe HTML, `eval`,
dynamic code execution, or backend-provided external links.

Vite `VITE_*` values are public bundle configuration. A build-time
`VITE_PHASE4_API_KEY` is acceptable only for disposable local fixture work.
Production/internal deployment must use session entry or a secure same-origin
reverse proxy and must not embed a durable secret.

## Local fixture workflow

Terminal one:

```bash
export PHASE4_API_KEY='local-test-key'
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.serve_phase4_api \
  --fixture-profile small_fixture --host 127.0.0.1 --port 8765
```

Terminal two:

```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm install
VITE_PHASE4_API_BASE_URL=http://127.0.0.1:8765 \
  npm run dev -- --host 127.0.0.1
```

Open `http://127.0.0.1:5173/` and enter `local-test-key`. “Reload displayed
data” re-fetches GET responses; it does not refresh a source or run a pipeline.

## Route and endpoint inventory

| Dashboard route | Primary Phase 4A endpoints |
|---|---|
| `/` | readiness, market stage, routing, candidates, positions, alerts, conflicts, calibration, performance |
| `/market` | market stage, sectors, symbol-scoped stocks |
| `/routing`, `/routing/:id` | routing collection/conflicts/detail |
| `/candidates`, `/candidates/:id` | candidates/detail, snapshots, decisions, outcomes |
| `/positions`, `/positions/:id` | coverage/detail, missing data, recovery proposals |
| `/alerts`, `/alerts/:id`, `/incidents/:id` | alerts and incidents collections/details |
| `/governance` | corrections, impacts, conflicts, membership history, routing conflicts |
| `/calibration` | summary, manifest, coverage, exclusions |
| `/performance` | latest, runs, baselines |
| `/readiness` | system readiness, limitations, and readiness checks |

Routing and candidate collections use API cursor pagination. The stock explorer
requires a symbol because Phase 4A does not expose a paginated stocks collection;
the browser therefore never loads the full stock universe. Filters, tabs,
cursors where practical, and supported `as_of` dates are URL-addressable.

## Operator semantics

Every major page shows freshness, lineage, partial state, and limitations.
Unknown freshness is unknown, never fresh. Stale data stays visible with
API-provided semantic time. Partial totals are labelled available records.
Empty, unavailable, conflict, authentication, authorization, rate-limit, and
unexpected states are distinct.

The persistent banner reads “Development view only — production readiness is
blocked” whenever `phase4_production_ready` is false. It cannot be dismissed.
The readiness page preserves these production blockers:

- `SINGLE_YEAR_CONCENTRATION`
- `COPIED_REALISTIC_BASELINE_MISSING`
- `OPERATOR_MIGRATIONS_NOT_APPLIED`
- `EMPTY_REAL_PHASE3B_HISTORY`

Governance conflicts never receive a frontend-selected winner. Routing shows
the winning and every retained reason. Position detail shows positive-action
suppression. Recovered candidates show that pre-entry history is unavailable.
Missing outcomes are unavailable rather than zero or failure. Fixture
performance is never presented as a copied-realistic baseline.

## Accessibility, testing, and build

The dashboard targets WCAG 2.1 AA with semantic landmarks/headings, labelled
controls, table captions and headers, text-plus-color status, visible focus,
keyboard rows, skip navigation, reduced motion, and horizontal table scrolling.
Automated axe checks cover login, conflict/partial state, and position tables.
Layouts target 1440, 1280, and 1024 pixel operator widths.

```bash
PYTHONPATH=src ./.venv/bin/python scripts/export_phase4_openapi.py
cd web/execution-console-v2/ai-trading-dashboard-starter
npm run gen:api
npm run check:api
npm run typecheck
npm run lint
npm test
npm run build
npm run test:e2e
```

The OpenAPI exporter constructs only the deterministic fixture-mode app.
`check:api` fails on backend/snapshot drift. Playwright asserts every observed
`/api/v1` request uses GET. Vite emits hashed assets and no source maps by
default.

## Deployment and non-goals

Bind development services to `127.0.0.1`. A production static host should set a
restrictive Content Security Policy and proxy `/api/v1` securely. Phase 4B adds
no pipeline/rebuild controls, candidate mutations, alert resolution, recovery
approval/execution, notes, watchlist writes, overrides, calibration/benchmark
triggers, migrations, broker/order/position/stop/allocation controls, ranking or
routing logic, or Phase 4C behavior. Production readiness remains false.
