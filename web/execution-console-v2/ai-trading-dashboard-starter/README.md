# AI Trading Dashboard Starter

A Vite + React + TypeScript + Tailwind starter for your trading operator dashboard.

## Included
- Page-based structure for Pipeline, Ranking, Patterns, Sectors, Execution, Runs, Shadow, Research
- Reusable layout and card components
- Typed mock API modules
- API wrapper modules ready to swap from mock to real FastAPI endpoints
- TanStack Table starter on Ranking
- Recharts placeholder chart on Sectors
- Framer Motion page transitions

## Run
```bash
npm install
npm run dev
```

Default local URL: `http://127.0.0.1:5173/`.

## Environment
Copy `.env.example` to `.env` and adjust values when connecting to live APIs.

- `VITE_USE_MOCK_API=false` (default) enables fetch calls to backend endpoints.
- `VITE_USE_MOCK_API=true` forces full mock mode.
- `VITE_EXECUTION_API_BASE_URL` defaults to empty, which keeps browser requests same-origin (`/api/...`).
- `VITE_EXECUTION_PROXY_TARGET` controls the Vite dev proxy target (default `http://127.0.0.1:8090`).
- `VITE_EXECUTION_API_KEY` is injected by the Vite proxy for `/api/execution/*` requests in local dev.

## UI E2E Testing (Playwright)
Permanent UI regression tests are configured for iterative UI improvements.

```bash
npm run test:e2e
```

Additional modes:

```bash
npm run test:e2e:ui
npm run test:e2e:headed
```

Notes:
- Playwright config: `playwright.config.ts`
- Tests: `tests/e2e/*.spec.ts`
- E2E runs in forced mock mode (`VITE_USE_MOCK_API=true`) for deterministic UI checks.

## Swap mock data for real APIs
The UI currently reads from `src/lib/api/*`, which in turn reads from `src/lib/mock/*`.
Later, replace the internals of those API modules with calls to your FastAPI endpoints like:

- `/api/execution/workspace/pipeline`
- `/api/execution/ranking`
- `/api/execution/runs`
- `/api/execution/patterns`
- `/api/execution/sectors`
- `/api/execution/shadow`

## Suggested next steps
- Hook `ranking.ts` to `/api/execution/ranking`
- Add symbol detail drawer fed by combined rank/pattern/breakout data
- Replace Recharts price mock with TradingView Lightweight Charts
- Add React Query once backend integration begins
