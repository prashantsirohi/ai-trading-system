# Phase 4A Read-Only API

- **Purpose:** Operate and verify Phase 4A without mutating operator state.
- **Audience:** Operators, API clients, and Phase 4B developers.
- **Last verified:** 2026-07-15
- **Source of truth:** `src/ai_trading_system/interfaces/api/` and `interfaces/cli/serve_phase4_api.py`.
- **API schema:** `phase4a-api-schema-v1`

## Boundary and readiness

Phase 4A exposes canonical Phase 3 state under `/api/v1`. It has no POST, PUT,
PATCH, or DELETE business operation and cannot run pipelines, migrations,
calibration builds, recovery actions, alert acknowledgements, order simulation,
or broker synchronization. It does not implement a UI.

Development readiness is true and production readiness is false. The API keeps
`SINGLE_YEAR_CONCENTRATION`, `COPIED_REALISTIC_BASELINE_MISSING`,
`OPERATOR_MIGRATIONS_NOT_APPLIED`, and `EMPTY_REAL_PHASE3B_HISTORY` visible.
Production readiness false alone does not make API health unready.

## Endpoint and source contracts

The 41 GET operations cover system/health, market and stage state, routing,
candidates, position coverage, alerts/incidents, governance,
calibration/readiness, and performance. OpenAPI is at `/openapi.json` and
`/docs` unless disabled. Governed rows are authoritative, followed by immutable
promoted evidence and then summaries; inconsistent versions are not mixed.

## Authentication and tracing

Live and ready health are public and disclose no rows. Other routes require
`Authorization: Bearer <key>` or `X-API-Key: <key>` by default. Keys come from
`PHASE4_API_KEY`, use constant-time comparison, and are not logged. Bypass
requires explicit local-development mode.

Valid `X-Request-ID` values are propagated; invalid values are replaced with a
UUID. Structured logs contain request ID, route, status, duration, and
authentication state—not credentials, SQL, source paths, or payloads. The
in-memory limit defaults to 120 requests per minute per credential;
distributed limiting remains a deployment concern.

## Pagination, as-of, freshness, and ETags

Collection cursors bind sort field, direction, stable identity, and filter
hash. The default limit is 50 and maximum is 500. Filters and sorts are
allowlisted; unknown query parameters and cursor/filter mismatches return
`INVALID_ARGUMENT`.

`as_of` accepts an ISO date or timezone-aware RFC3339 timestamp. Dates mean end
of day UTC. Naive, malformed, and future values return `INVALID_AS_OF`.
Universal stages use canonical effective-time and recorded-availability
resolution, preventing late corrections from leaking backward. Freshness uses
semantic lineage rather than file modification time.

Versioned details use semantic ETags and return 304 for matching
`If-None-Match`. Response-generation timestamps do not affect ETags.

## Partial and unavailable data

- Existing empty collections return `data: []`, `partial: true`, and
  `SOURCE_EMPTY`.
- Missing optional migrations return `SOURCE_NOT_MIGRATED`; no table is made.
- Missing artifacts or realistic baselines remain unavailable with their named
  limitation rather than zero metrics.
- Missing details return 404 `RESOURCE_NOT_FOUND`.
- Governance conflicts remain conflict metadata, never an authoritative stage.

Errors omit stack traces, SQL, paths, credentials, and raw exceptions.

## Safe fixture smoke test

```bash
export PHASE4_API_KEY='local-test-key'
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.serve_phase4_api \
  --fixture-profile small_fixture --host 127.0.0.1 --port 8765

curl -s http://127.0.0.1:8765/api/v1/health/live
curl -s http://127.0.0.1:8765/api/v1/health/ready
curl -s -H "Authorization: Bearer $PHASE4_API_KEY" \
  http://127.0.0.1:8765/api/v1/system/readiness
```

For copied-store verification, make a backup-derived regular-file copy outside
`DATA_ROOT`, hash it, serve it with `--fixture-profile copied_store
--copied-control-plane /path/to/copy`, exercise the API, and compare the hash.
The command rejects symlinks and the configured operator store.

## Non-goals

Phase 4A has no dashboard, charts, saved views, watchlists, preferences, action
buttons, candidate edits, recovery approval, alert acknowledgement, refresh
control, pipeline trigger, execution action, or broker import.
