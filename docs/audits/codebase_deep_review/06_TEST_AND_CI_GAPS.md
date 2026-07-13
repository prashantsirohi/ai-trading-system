# Test and CI gaps

- **Purpose:** Record verification results and define the test and CI gates needed for safe remediation.
- **Audience:** Maintainers, reviewers, and release engineers.
- **Last verified:** 2026-07-13
- **Source of truth:** Test, build, lint, type-check, packaging, and environment checks run during this audit.

---

## Current evidence

The repository has substantial Python coverage by count—261 `test_*.py` files and 1,511 test functions—and good targeted tests around trust, paths, execution persistence, and operator tasks. Quantity does not close the highest-risk semantic gaps.

Verification performed during this review:

| Check | Result |
|---|---|
| Critical targeted Python set | 56 passed in 4.95 s |
| Full Python collection | failed after 17.89 s because `telegram` is not installed/declared in the primary project contract |
| Package import | passed from `src` |
| Orchestrator `--help` | passed with `PYTHONPATH=src` |
| Environment dependency consistency | `uv pip check` passed for 104 installed packages |
| Critical execution mypy slice | failed: 19 errors in 5 files |
| Repository Ruff scan | 274 issues, 179 auto-fixable; includes several undefined names |
| React production build | passed; large-chunk warning |
| React unit tests | 1 file / 3 tests passed |
| Dependency vulnerability query | not completed; registry access required external metadata transfer and was not authorized |
| Real-data canary/profile | not run; live OHLCV store had a conflicting lock and the pipeline can mutate live stores |

The collection failure is a release-blocking test reproducibility defect even though the installed environment is internally consistent: the lock/project declaration does not describe everything imported by the tests.

## Critical behavior matrix

| Behavior | Existing confidence | Missing proof | Required test |
|---|---|---|---|
| DATA_ROOT propagation | strong targeted tests | scattered non-core paths | scan/contract test for runtime path construction |
| trust/DQ blocking | strong unit tests | artifact visibility after DQ failure | failed-attempt then retry integration test |
| historical ranking | broad ranking tests | no future-row exclusion proof | append future data and assert old-date rank/result unchanged |
| rank stability | partial | deterministic ties and snapshot consistency | golden factor/rank fixtures across shuffled input |
| DuckDB serialization | single-process confidence | independent instances/processes | multi-process writer and API/pipeline contention test |
| portfolio heat | single-order checks | cumulative batch/concurrent reservation | two individually valid orders jointly exceed cap |
| order submission | persistence tests | idempotent retry/crash recovery | fault before/after broker acknowledgement |
| stop/trailing behavior | current-price and persistence coverage | partial fills/open orders/stale prices | state-machine table tests |
| publish retry | delivery dedupe exists | no upstream-mutation prohibition | retry publish with upstream stores read-only |
| API schemas | route tests | no response contract/version guard | OpenAPI snapshot and invalid payload suite |
| frontend | 3 unit tests | route behavior, mutation confirmation, accessibility | component/API mocks plus Playwright critical flows |
| packaging | import smoke | clean install/full collection | build wheel, install into empty env, run smoke/tests |

## Required test additions by priority

### P0/P1

1. Point-in-time rank invariant covering latest price, returns, volume, delivery, membership, and every factor input.
2. Artifact promotion test proving failed DQ attempts never appear in the consumable artifact map.
3. Cross-process DuckDB writer serialization and crash recovery.
4. Batch/concurrent portfolio heat reservation.
5. Broker submission idempotency with timeouts and unknown outcomes.
6. Fill-driven stop lifecycle including partial fills.
7. Clean-environment install and full pytest collection.

### P2

- API OpenAPI snapshots and typed response fixtures;
- bounded pagination/property tests;
- publish read-only retry and atomic artifact failure injection;
- config precedence and secret-redaction tests;
- frontend tests for run creation, preview/execute confirmation, cancellation, and error recovery;
- performance regression baselines for rank/features/API.

## CI gate design

```text
fast PR gate
  -> docs check + formatting/lint delta
  -> unit and invariant tests
  -> type-check ratchet for touched modules
  -> frontend type/build/unit
  -> package wheel + clean install smoke

integration gate
  -> temporary DuckDB migrations
  -> pipeline stage contracts and DQ promotion
  -> multi-process persistence tests
  -> paper execution fault injection
  -> Playwright critical operator flows

scheduled/release gate
  -> production-shaped copied-data canary
  -> performance/bundle budgets
  -> dependency and secret scans
  -> backup/restore and migration rehearsal
```

All database tests must use temporary directories. The copied-data canary must be explicitly read-only or point to disposable copies; it must not use generated market observations to satisfy production trust checks.

## Lint and type adoption

Do not make a 274-issue cleanup part of correctness patches. Establish a baseline and enforce zero new Ruff errors, immediately fixing true undefined names and security-relevant warnings. Ratchet mypy by critical module: execution first, then pipeline contracts/registry, ranking input contracts, data trust, and API schemas. Remove broad `Any` at boundaries before attempting strict mode repository-wide.

## Coverage policy

Line coverage alone is insufficient for trading logic. Require invariant/branch coverage for:

- no future observation can influence a dated result;
- no failed or quarantined attempt can become authoritative;
- no retry can create an additional broker intent;
- no accepted batch can exceed configured projected risk;
- preview cannot mutate broker/execution state;
- all registered artifacts are immutable and hash-verifiable.

Track conventional line/branch coverage as a trend, but block releases on those semantic invariants and clean-environment reproducibility.
