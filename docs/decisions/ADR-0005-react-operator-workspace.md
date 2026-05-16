# ADR-0005: React Operator Workspace

- **Purpose:** Record the decision to move the operator workspace to React + FastAPI, replacing earlier UI surfaces.
- **Audience:** Developer, operator.
- **Last verified:** 2026-05-16
- **Source of truth:** Code paths cited inline (file references in the Decision section) + [`docs/_audit/current_code_truth_map.md`](../_audit/current_code_truth_map.md).
- **Status:** Accepted; React V2 + FastAPI is live (`web/execution-console-v2/` + `ai-trading-execution-api`). Streamlit usage in active code paths: none confirmed.

---

## Context

Earlier operator UI was Streamlit-based (or partially CLI). Streamlit's strengths (zero-boilerplate dashboards) became liabilities as the workspace grew:

- Multi-page state was awkward.
- Real-time updates required hacks.
- No clean separation between UI and data access.
- Hard to write fast cohesive workflows for an operator who runs the same drill-down many times a day.

The system simultaneously needed a programmatic API surface for: external tooling, future automation, mobile dashboards, etc.

## Decision

Build a 3-layer operator console:

```
HTTP (routes/)  →  service (services/)  →  readmodel (services/readmodels/)  →  domain
```

- Backend: FastAPI at `src/ai_trading_system/ui/execution_api/` (CLI: `ai-trading-execution-api`, default port 8090, auth via `EXECUTION_API_KEY` env var sent as `x-api-key` header).
- Frontend: React + Vite + TypeScript at `web/execution-console-v2/`.
- AST-lint enforces the layer split (see `tests/lint/`).

A previously-detailed phased rollout (PR #1 through #12) lived in `docs/EXECUTION_CONSOLE_PLAN.md`, which has been archived to `docs/_legacy/`. The plan is now realized — Control Tower, Ranking, Patterns, Sectors, Execution, Runs Audit, Stock Detail views are all shipped.

## Consequences

**Positive:**
- Clean layering: routes don't touch DuckDB; services delegate to readmodels.
- React V2 supports rich workspace patterns: keyboard shortcuts, comparison tray, drill-down, etc.
- The FastAPI surface doubles as a programmatic API for automation.
- Mock-data mode in the React app supports offline UI dev.

**Negative:**
- More dependencies than Streamlit (Node toolchain, Vite, React, openapi-codegen).
- Two processes to manage: backend + frontend dev server. Mitigated by docs in [`docs/architecture/ui_architecture.md`](../architecture/ui_architecture.md) and [`docs/domains/ui_domain.md`](../domains/ui_domain.md).
- CORS is `allow_origins=["*"]` — acceptable for local single-operator deployment but must be tightened before any non-local deployment.

## See also

- [`docs/architecture/ui_architecture.md`](../architecture/ui_architecture.md)
- [`docs/domains/ui_domain.md`](../domains/ui_domain.md)
- [`docs/reference/api_reference.md`](../reference/api_reference.md)
