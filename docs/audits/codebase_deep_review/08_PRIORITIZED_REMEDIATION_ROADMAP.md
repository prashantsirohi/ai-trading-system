# Prioritized remediation roadmap

- **Purpose:** Sequence confirmed remediation by safety dependency, exit criterion, ownership, and effort.
- **Audience:** Operators and engineering leads planning implementation.
- **Last verified:** 2026-07-13
- **Source of truth:** Confirmed findings `AUD-001` through `AUD-017` and their required verification gates.

---

## Sequencing principle

Correctness and safety invariants come before module reshaping or dependency upgrades. Each phase must leave the system runnable, preserve external data paths, and use temporary/copied stores for tests and migrations.

## Phase 0 — stabilize and make evidence reproducible (days 1–5)

| Work | Findings | Exit criterion | Size |
|---|---|---|---:|
| Declare optional integrations correctly and make full test collection work in a clean locked environment | AUD-007 | wheel/locked install collects all tests | S |
| Fix confirmed undefined names and establish no-new-error Ruff baseline | AUD-007, AUD-012 | critical modules lint clean; baseline recorded | S |
| Add point-in-time, artifact-promotion, batch-risk, and idempotency failing tests | AUD-001/2/4/5 | tests reproduce each defect | M |
| Correct current execution/storage/API docs and retire stale truth map | documentation drift | docs check passes; no contradictory current claims | S |
| Remove OAuth token-prefix output and enforce/warn on secret file permissions | AUD-010 | no token material printed; documented 0600 check | S |
| Define maintenance-window copied-data canary procedure | AUD-003 | safe, read-only/disposable workflow reviewed | S |

## Phase 1 — correctness and execution safety (weeks 1–3)

| Work | Findings | Exit criterion | Size |
|---|---|---|---:|
| Build one cutoff-aware `RankInputSnapshot` and route all factors through it | AUD-001, AUD-013 | appending future data cannot change historical output | L |
| Add written → DQ-passed → promoted artifact lifecycle | AUD-002, AUD-017 | failed attempts never resolve as authoritative | M |
| Reserve/recompute risk for every accepted batch order | AUD-004 | sequential/concurrent batches cannot breach limits | M |
| Durable unique submission intent and broker reconciliation | AUD-005 | retry/crash never duplicates an intent | L |
| Drive position/stop lifecycle from confirmed cumulative fills | AUD-016 | partial/open/cancel paths preserve correct stop state | L |
| Parameterize feature-reader and loader query values | AUD-008 | SQL policy tests pass | S |

## Phase 2 — persistence, recovery, and publishing (weeks 3–6)

| Work | Findings | Exit criterion | Size |
|---|---|---|---:|
| One writer coordinator per DuckDB store with inter-process protocol | AUD-003 | contention test has zero lock failures | L |
| Atomic artifact write, fsync/promote, and hash revalidation | AUD-017 | fault injection cannot expose partial/mutated artifacts | M |
| Remove fundamental recomputation from publish | AUD-006 | publish retry works with upstream stores read-only | M |
| Add retention/compaction and bounded API queries | AUD-013 | production-shaped history stays within latency/RSS budgets | M |
| Backup/restore and failed-attempt recovery drills | AUD-002/3/17 | operator runbook successfully rehearsed | M |

## Phase 3 — contracts and maintainability (weeks 6–10)

| Work | Findings | Exit criterion | Size |
|---|---|---|---:|
| Consolidate validated settings and path composition | AUD-009, AUD-015 | no runtime path/env reads outside approved adapters | L |
| Split registry, orchestrator, feature store, ranking service, and dashboard at identified seams | AUD-012 | modules have single ownership and dependency tests | XL |
| Add typed API requests/responses and generated client | AUD-011 | OpenAPI snapshot stable; generic boundary `Any` removed | L |
| Introduce domain ports for stores/providers/brokers/delivery | AUD-012 | domain layer has no infrastructure/framework imports | L |
| Ratchet type checking through execution/pipeline/ranking/API | AUD-007 | critical set clean under agreed mypy profile | M |

## Phase 4 — performance and operator experience (weeks 10–14)

| Work | Findings | Exit criterion | Size |
|---|---|---|---:|
| Reuse immutable rank/feature input snapshots and benchmark query layout | AUD-013 | agreed 10x fixture targets pass | L |
| Add pagination/read models and query telemetry | AUD-013 | p95 and RSS budgets enforced | M |
| Route-split React app and split oversized pages | AUD-014 | entry bundle budget met; critical flows pass | M |
| Expand frontend component/Playwright coverage | AUD-014 | run/preview/execute/cancel/error flows protected | M |
| Add stage duration, rows, RSS, lock, and retry telemetry | AUD-003/13 | bottlenecks observable without secret leakage | M |

## Phase 5 — controlled modernization and live-readiness review (after stable baselines)

| Work | Exit criterion | Size |
|---|---|---:|
| Upgrade dependencies in compatible groups using the matrix in `04` | full Python/UI/integration/performance gates pass | L |
| Remove compatibility facades and obsolete requirements contract | clean wheel install and supported operator migration | M |
| Paper shadow and failure-recovery campaign | operator-defined stable period with reconciled outcomes | L |
| Independent live-mode security/safety review | all go-live gates in `05` pass | L |

Live broker placement is explicitly outside this roadmap’s automatic completion. It requires a separate operator decision after Phase 5 evidence.

## Ownership suggestion

| Stream | Accountable owner |
|---|---|
| data time semantics and ranking | ranking/data lead |
| artifact lifecycle and DuckDB coordination | platform/pipeline lead |
| orders, risk, fills, stops | execution/risk lead |
| API/auth/operator controls | application/security lead |
| UI contracts and bundle | frontend lead |
| CI, packaging, docs governance | repository maintainer |

## Phase gates

Do not begin broad refactoring until Phase 1 correctness tests pass. Do not upgrade DuckDB before copied-store migration and concurrency tests exist. Do not enable concurrent feature/rank work before writer serialization and deterministic artifact merging exist. Do not enable live execution on the basis of paper happy-path tests.
