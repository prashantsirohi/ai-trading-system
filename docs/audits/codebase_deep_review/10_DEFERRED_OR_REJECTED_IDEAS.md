# Deferred or rejected ideas

- **Purpose:** Record proposals intentionally postponed or rejected and the evidence required to revisit them.
- **Audience:** Technical leads and maintainers evaluating broader changes.
- **Last verified:** 2026-07-13
- **Source of truth:** Current constraints, confirmed findings, and the target architecture in this audit.

---

## Deferred until prerequisites exist

| Idea | Decision | Why now is premature | Revisit when |
|---|---|---|---|
| Parallel feature/ranking workers | defer | current stores lack cross-process writer coordination and rank inputs lack one immutable snapshot | AUD-001 and AUD-003 resolved; deterministic merge benchmark exists |
| Major DuckDB upgrade | defer | copied-store migration/concurrency suite is missing | Phase 2 recovery and compatibility suite pass |
| pandas/NumPy major upgrade | defer | factor/backtest golden parity and model compatibility gates are incomplete | point-in-time/golden tests and locked install are green |
| Live broker adapter enablement | reject for current state | idempotency, batch risk, partial fills, reconciliation, and centralized interlock are incomplete | every go-live gate in `05` passes and operator explicitly approves |
| Automatic repair of a locked live database | reject | terminating/overriding an active owner risks corruption or interrupting trading operations | use maintenance window, owner coordination, and copied backup |
| Runtime-generated market data to pass trust gates | reject | violates the source-of-record contract and can hide provider failures | never for operational correctness; synthetic shapes only for isolated performance tests |
| Large repository-wide lint auto-fix | defer | 179 auto-fixes create review noise and can overlap operator work | correctness patches land; execute mechanical batches by subsystem |
| Full strict typing in one change | defer | 19 errors already exist in the critical slice and boundary `Any` is widespread | ratchet critical modules incrementally |
| Rewrite the React application | reject | current app builds and the main issues are bundle/page boundaries and test depth | improve route splitting and contracts incrementally |
| Replace DuckDB with a client/server database | defer | single-writer ownership and query-shape fixes have not been tested; migration cost is high | measured concurrency/scale still exceeds a coordinated DuckDB design |

## Rejected architectural directions

### Microservices by pipeline stage

Rejected. The stages share schemas, artifacts, time semantics, and a single operator deployment. Splitting services would turn existing in-process coupling into network and version coupling before contracts are stable. A modular monolith with explicit ports, immutable artifacts, and a writer coordinator addresses the confirmed failures with less operational burden.

### Generic “repository” abstraction over all stores

Rejected. Control-plane attempts, market observations, feature frames, and execution state have different consistency and query requirements. Use domain-specific ports and repositories. A universal CRUD layer would obscure invariants such as point-in-time reads and monotonic order transitions.

### One global database for all domains

Rejected. Separate stores provide useful failure, retention, backup, and access boundaries. The problem is ambiguous writer ownership and documentation drift, not the existence of multiple stores.

### Publish as a catch-all repair stage

Rejected. Publish retries must be safe and artifact-driven. Upstream read-model refreshes belong in their owning stage or an explicit materialization stage with separate DQ and lineage.

### “Latest row” as a universal historical-data API

Rejected. Operational latest-state queries and historical as-of queries need different explicit interfaces. Reusing latest-row helpers for dated ranks caused the confirmed lookahead path.

### Enabling live placement behind only an environment flag

Rejected. A flag is configuration, not a safety case. Live mode needs authenticated capability, an operator interlock, idempotent durable submission, reconciliation, fill-driven risk/stops, audit events, and rehearsed rollback.

## Ideas needing measured evidence

- Replace pandas transformations with Polars or SQL only after per-stage profiles identify conversion/CPU cost and golden parity exists.
- Introduce Redis or another cache only after cache keys include snapshot lineage and local immutable caching proves insufficient.
- Use a task queue only after the single-host writer coordinator and recovery model are stable.
- Adopt event sourcing for execution only if the explicit state machine and append-only audit requirements cannot be met more simply.
- Partition or compact market data only after `EXPLAIN ANALYZE` on a copied production-shaped store identifies the actual pruning failure.

## Scope intentionally not claimed

This review did not perform a live database migration, mutate broker state, terminate the process holding the OHLCV lock, run a real-data publishing canary, query external vulnerability registries, or prove the latest dependency versions. Those actions require explicit operator authorization, safe copies/maintenance windows, or network metadata transfer. The roadmap names the evidence needed before acting.
