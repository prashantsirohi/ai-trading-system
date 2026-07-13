# Documentation drift

- **Purpose:** Identify contradictions, stale contracts, missing runbooks, and documentation governance improvements.
- **Audience:** Documentation owners, operators, and system maintainers.
- **Last verified:** 2026-07-13
- **Source of truth:** Current runtime code, `docs/SYSTEM_GUIDE.md`, and the documents named in the drift register.

---

## Verdict

`docs/SYSTEM_GUIDE.md` and `docs/architecture/operational_data_flow.md` are useful current orientation documents. Drift is concentrated in older audit/reference material and in execution/storage details that changed after their last verification. The primary risk is not missing prose; it is contradictory current-looking prose.

## Drift register

| Document/claim | Runtime truth found | Status | Action |
|---|---|---|---|
| `docs/SYSTEM_GUIDE.md`: 13 logical stages and seven feature substages | matches current orchestrator shape | current | retain; update only with system changes |
| `docs/architecture/operational_data_flow.md` | broadly matches stage flow and ownership | current | retain |
| `docs/_audit/current_code_truth_map.md`: 11 stages and old flow | code/guide expose 13 logical stages and expanded feature DAG | stale/contradictory | archive or regenerate; do not label current |
| same truth map: repo-local `data/...` examples | runtime resolves external `DATA_ROOT` | unsafe drift | replace with helper-derived terminology |
| `docs/architecture/data_trust_and_dq.md`: control plane at `data/control_plane.duckdb` | path helpers place stores below configured runtime root | stale path | update location wording and verification date |
| `docs/architecture/execution_policy.md`: trailing stops not implemented | execution service contains trailing-stop update logic and pipeline calls it outside preview | stale behavior | document actual algorithm and remaining fill-state risks |
| same execution policy: execution records in control plane | separate execution store is used | stale ownership | update persistence table |
| API reference: 14 routers | current app includes additional optimization/investigator/control routes | stale inventory | generate router/endpoint summary from OpenAPI |
| API reference request defaults | current schemas differ in included default stages/options | stale interface | source examples from schema tests |
| feature-stage detailed docs describe a single stage/attempt | current pipeline exposes seven feature substages | incomplete | document substage contracts and recovery granularity |
| archived legacy docs | intentionally historical | safe only when visibly archived | keep archive banner and remove current cross-links |
| package migration audit | useful transition context | historical | preserve as decision record, not runtime guide |

## Terminology inconsistencies

Standardize the following:

| Preferred term | Avoid/clarify |
|---|---|
| logical stage vs feature substage | using “stage” for both without qualification |
| control-plane store | registry DB, pipeline DB, catalog DB unless naming an exact schema |
| execution store | implying orders/fills/stops live in control plane |
| registered artifact | “latest file” without attempt/run identity |
| effective/cutoff date | generic “date” in point-in-time computations |
| preview | dry-run when the path also performs persistence; define mutation contract |
| `DATA_ROOT`-resolved path | repo-local `data/` path in current operator instructions |

## Missing current documentation

1. A point-in-time data contract for historical features/ranks/backtests.
2. An artifact promotion contract separating written, DQ-passed, and authoritative states.
3. A store ownership and writer-serialization contract covering API/background/pipeline processes.
4. An execution order/fill/stop state machine with idempotency and reconciliation.
5. A generated API/OpenAPI reference and authorization matrix.
6. A dependency/install contract defining pyproject, lock, optional integrations, and clean setup.
7. Recovery runbooks for locked DuckDB stores, failed attempts, corrupt artifacts, and broker-unknown outcomes.

## Documentation governance

- Keep `SYSTEM_GUIDE.md` as the sole high-level current map.
- Add `status: current|historical`, owner, and last-verified date to detailed architecture documents.
- Validate referenced file paths and commands in `scripts/check_docs.py` where practical.
- Generate volatile inventories—API routes, stage names, migrations, configuration keys—from code or tests.
- Require the guide and relevant detailed contract in the same change when ownership, stages, safety invariants, commands, or interfaces change.
- Store architectural decisions as dated ADRs; do not mutate historical decisions into current manuals.

This audit intentionally does not edit the drifted documents because its requested deliverable is diagnosis and roadmap, not an architectural behavior change. Their correction is Phase 0 work.
