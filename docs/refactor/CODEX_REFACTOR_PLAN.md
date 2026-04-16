# CODEX_REFACTOR_PLAN.md
## AI Trading System – Codex Refactor Plan

### Version: 1.1
### Maintained By: Prashant Sirohi
### Purpose: Guide Codex through a structured architectural refactor of the AI Trading System.

---

## Objective

Refactor the `ai-trading-system` to:

- eliminate architectural drift
- improve modularity and maintainability
- introduce stable contracts between layers
- strengthen execution safety and observability
- prepare the system for production-grade scalability

This refactor must preserve current functionality and artifact compatibility.

---

## Target Architecture

### Architectural Principles

- clear separation of concerns
- artifact-driven pipeline with strong lineage
- thin stage wrappers and modular services
- unified runtime infrastructure
- stable UI read models
- execution safety and trust gating
- incremental and reversible changes

---

## Artifact Compatibility Rules

Codex must preserve the current artifact contract unless a phase explicitly allows additive extension.

### Required compatibility guarantees

- preserve existing artifact filenames
- preserve artifact folder layout under `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`
- preserve required CSV columns used by downstream stages and publish flows
- preserve current JSON payload keys used by UI and API consumers
- additive columns or JSON keys are allowed only when they do not break existing consumers
- publish flows must continue to work against existing rank artifacts
- rerun semantics and stage retry behavior must remain intact

### Compatibility interpretation

Allowed:
- internal module refactors
- extraction into services
- adding tests and adapters
- adding backward-compatible helper fields

Not allowed:
- renaming `ranked_signals.csv`, `breakout_scan.csv`, `pattern_scan.csv`, `sector_dashboard.csv`, `dashboard_payload.json`, `execute_summary.json`, `publish_summary.json`
- changing the meaning of existing required fields without a compatibility shim
- bypassing existing trust or DQ safety behavior

---

## API Compatibility Rules

Codex must keep the execution API stable during refactoring.

### Existing API surfaces to preserve

- `/api/execution/health`
- `/api/execution/summary`
- `/api/execution/ranking`
- `/api/execution/workspace/pipeline`
- `/api/execution/runs`
- `/api/execution/tasks`

### API change policy

- response shape may only change additively unless explicitly approved
- existing routes must remain callable
- data loading may move behind read models, but route behavior must remain stable
- if a route is internally migrated, old data loading paths must remain supported until tests pass

---

## File Ownership Map

This map tells Codex where structural refactoring should happen.

### Runtime foundation
- `utils/data_domains.py` → migrate responsibilities into `core/paths.py`
- `utils/logger.py` → migrate responsibilities into `core/logging.py`
- duplicated runtime helpers → consolidate under `core/`

### Stage orchestration
- `run/stages/ingest.py`
- `run/stages/features.py`
- `run/stages/rank.py`
- `run/stages/execute.py`
- `run/stages/publish.py`

These must become thin wrappers that call service-layer orchestration.

### Service extraction targets
- `services/ingest/`
- `services/features/`
- `services/rank/`
- `services/execute/`
- `services/publish/`

### Rank domain split target
- `analytics/ranker.py` → split into:
  - `services/rank/input_loader.py`
  - `services/rank/factors.py`
  - `services/rank/composite.py`
  - `services/rank/contracts.py`
  - `services/rank/dashboard_payload.py`

### UI read models
- `ui/services/readmodels/rank_snapshot.py`
- `ui/services/readmodels/pipeline_status.py`
- `ui/services/readmodels/latest_operational_snapshot.py`
- `ui/execution_api/*`
- UI-facing loaders must use read models rather than reading raw artifacts ad hoc

### Execute domain
- `execution/autotrader.py`
- `execution/policies.py`
- `execution/service.py`
- `execution/store.py`
- introduce `services/execute/candidate_builder.py`

### Publish domain
- `run/publisher.py`
- `publishers/telegram.py`
- `publishers/google_sheets.py`
- `publishers/dashboard.py`
- extract rendering from delivery

---

## Refactor Phases

## Phase 0 — Baseline Inventory and Safety Net
**Goal:** Establish a stable starting point.

### Scope
- document current architecture
- snapshot artifact schemas
- add smoke tests for orchestrator and execution API
- identify mixed usage of `core.*` and `utils.*`

### Files likely to change
- `docs/refactor/baseline_inventory.md`
- `tests/fixtures/artifacts/*`
- `tests/smoke/*`
- optionally small non-invasive test helpers

### Tasks
- inventory current pipeline stages, services, utilities, and data flow
- record current artifact producers and consumers
- record current execution API routes and their backing data sources
- identify all imports of `utils.data_domains` and `utils.logger`
- add smoke tests for orchestrator startup path and execution API health/ranking/workspace endpoints
- capture representative artifact fixtures for test coverage

### Entry criteria
- repository builds and current test environment can run

### Exit criteria
- `docs/refactor/baseline_inventory.md` exists
- smoke tests exist and pass in current architecture
- artifact fixtures exist for rank, execute, and publish outputs
- runtime duplication hotspots are documented

### Rollback strategy
- tests and docs only in this phase; no rollback complexity expected

### Deliverables
- `docs/refactor/baseline_inventory.md`
- `tests/fixtures/artifacts/`
- smoke tests under `tests/smoke/`

### Codex Prompt – Phase 0
Read `docs/refactor/CODEX_REFACTOR_PLAN.md` and execute only Phase 0.

Tasks:
1. Inventory current architecture and runtime boundaries.
2. Snapshot artifact schemas from representative outputs.
3. Add smoke tests for orchestrator and execution API.
4. Identify all imports of `utils.data_domains` and `utils.logger`.

Constraints:
- Do not change business logic.
- Do not rename artifacts.
- Do not remove legacy modules.
- Keep changes documentation and test focused.

Outputs:
- `docs/refactor/baseline_inventory.md`
- `tests/fixtures/artifacts/*`
- `tests/smoke/test_orchestrator_smoke.py`
- `tests/smoke/test_execution_api_smoke.py`

---

## Phase 1 — Unify Runtime Foundation
**Goal:** Standardize shared infrastructure.

### Scope
- replace legacy runtime helpers with unified `core` infrastructure
- remove duplicated path and logging logic

### Files likely to change
- `utils/data_domains.py`
- `utils/logger.py`
- `core/paths.py`
- `core/logging.py`
- import sites across pipeline stages and services

### Tasks
- migrate `utils.data_domains` responsibilities into `core.paths`
- migrate `utils.logger` responsibilities into `core.logging`
- add compatibility shims where needed for one phase
- update imports incrementally
- remove duplicated runtime logic only after tests pass

### Entry criteria
- Phase 0 complete
- smoke tests passing

### Exit criteria
- no active imports of `utils.data_domains`
- no active imports of `utils.logger`
- runtime path and logging behavior is covered by tests

### Rollback strategy
- keep temporary compatibility wrappers until all imports are migrated
- do not delete old module in same change set where new implementation first appears

### Codex Prompt – Phase 1
Read `docs/refactor/CODEX_REFACTOR_PLAN.md` and execute only Phase 1.

Tasks:
1. Consolidate runtime path logic into `core/paths.py`.
2. Consolidate runtime logging into `core/logging.py`.
3. Migrate imports gradually with compatibility shims.
4. Extend tests as needed.

Constraints:
- Do not alter stage behavior.
- Preserve environment-variable behavior.
- Keep rollback simple.

Outputs:
- updated `core/paths.py`
- updated `core/logging.py`
- migrated imports
- passing smoke tests

---

## Phase 2 — Introduce UI Read Models
**Goal:** Stabilize UI/API data contracts.

### Scope
- create read models for UI and API consumption
- stop ad hoc artifact loading inside UI routes and handlers

### Files likely to change
- `ui/services/readmodels/`
- `ui/execution_api/*`
- UI service loaders
- read-only tests and API snapshot tests

### Tasks
- create `ui/services/readmodels/rank_snapshot.py`
- create `ui/services/readmodels/pipeline_status.py`
- create `ui/services/readmodels/latest_operational_snapshot.py`
- refactor UI services and API routes to consume read models
- add response snapshot tests for core operator endpoints

### Entry criteria
- Phase 1 complete
- runtime infrastructure stable

### Exit criteria
- operator routes use read models for artifact-backed responses
- API response tests pass for health, ranking, workspace pipeline, and runs
- UI no longer reads artifacts directly outside controlled read model paths

### Rollback strategy
- keep legacy loading path behind helper wrapper until snapshots pass
- do not delete old loader in same step as route migration

### Codex Prompt – Phase 2
Read `docs/refactor/CODEX_REFACTOR_PLAN.md` and execute only Phase 2.

Tasks:
1. Create UI read models under `ui/services/readmodels/`.
2. Refactor API routes to use the read models.
3. Add response snapshot tests.
4. Preserve route behavior and shape.

Constraints:
- Do not change endpoint URLs.
- Only additive response changes are allowed.
- Keep artifact compatibility intact.

Outputs:
- `ui/services/readmodels/rank_snapshot.py`
- `ui/services/readmodels/pipeline_status.py`
- `ui/services/readmodels/latest_operational_snapshot.py`
- updated API route implementations
- API snapshot tests

---

## Phase 3 — Thin Stage Wrappers
**Goal:** Move business logic into services.

### Scope
- extract stage logic into service modules
- ensure stage files orchestrate workflows only

### Files likely to change
- `run/stages/*`
- `services/ingest/*`
- `services/features/*`
- `services/rank/*`

### Tasks
- extract ingest orchestration service
- extract features orchestration service
- extract rank orchestration service
- extract dashboard payload construction into dedicated service module
- keep stage wrappers thin and readable

### Entry criteria
- Phase 2 complete

### Exit criteria
- stage wrappers mostly coordinate inputs, outputs, and status only
- service modules contain business workflows
- no artifact behavior regression in tests

### Rollback strategy
- keep existing stage wrapper signatures unchanged
- preserve original stage entrypoints until service extraction stabilizes

### Codex Prompt – Phase 3
Read `docs/refactor/CODEX_REFACTOR_PLAN.md` and execute only Phase 3.

Tasks:
1. Extract orchestration services for ingest, features, and rank.
2. Move dashboard payload construction into a dedicated service.
3. Keep stage wrappers thin.
4. Preserve artifact outputs and stage summaries.

Constraints:
- Do not change orchestrator CLI contract.
- Do not rename stage outputs.
- Keep changes small and testable.

Outputs:
- service modules under `services/ingest/`, `services/features/`, `services/rank/`
- thinner `run/stages/*`
- updated tests

---

## Phase 4 — Normalize Ranking Domain Model
**Goal:** Improve clarity and testability.

### Scope
- split ranking logic into explicit components
- externalize factor weights and contracts

### Files likely to change
- `analytics/ranker.py`
- `services/rank/*`
- factor config files
- rank tests

### Tasks
- split `analytics/ranker.py` into input loader, factor calculators, composite scorer, and contracts
- externalize factor weights into configuration
- preserve current ranking artifact schema and ordering semantics
- add unit tests for factor and composite behavior

### Entry criteria
- Phase 3 complete

### Exit criteria
- ranking logic is modular and unit-testable
- factor weights are not hardcoded in a monolith
- rank outputs remain backward-compatible

### Rollback strategy
- keep old ranker facade until modular components are verified
- use adapter layer if needed to keep current callers stable

### Codex Prompt – Phase 4
Read `docs/refactor/CODEX_REFACTOR_PLAN.md` and execute only Phase 4.

Tasks:
1. Split ranking responsibilities into explicit modules.
2. Externalize factor weights.
3. Preserve rank artifact contract.
4. Add targeted unit tests.

Constraints:
- Do not change ranking file names.
- Preserve composite score semantics unless explicitly documented.
- Keep caller interfaces stable via facade if needed.

Outputs:
- modular rank domain files
- external factor-weight config
- unit tests for ranking components

---

## Phase 5 — Decouple Execution Contracts
**Goal:** Introduce a normalized execution interface.

### Scope
- standardize execution inputs while preserving current outputs

### Files likely to change
- `execution/*`
- `services/execute/*`
- tests around execute stage inputs/outputs

### Tasks
- create `ExecutionCandidateBuilder`
- standardize execution candidate input structure
- preserve existing `trade_actions.csv`, `executed_orders.csv`, `executed_fills.csv`, `positions.csv`, and `execute_summary.json`
- keep trust-gating behavior unchanged

### Entry criteria
- Phase 4 complete

### Exit criteria
- execution input preparation is normalized and testable
- execution stage still emits current artifacts
- trust and preview safeguards remain intact

### Rollback strategy
- keep current autotrader entrypoints intact while candidate builder is introduced
- switch internal wiring only after execution tests pass

### Codex Prompt – Phase 5
Read `docs/refactor/CODEX_REFACTOR_PLAN.md` and execute only Phase 5.

Tasks:
1. Introduce `ExecutionCandidateBuilder`.
2. Normalize execution input contracts.
3. Preserve current execute artifact outputs.
4. Extend tests for trust and preview safety.

Constraints:
- Do not enable new live trading paths.
- Do not weaken trust gating.
- Preserve current execution artifacts.

Outputs:
- `services/execute/candidate_builder.py`
- updated execute wiring
- execute-stage tests

---

## Phase 6 — Guarantee Data Integrity at Write Time
**Goal:** Eliminate repair-on-read logic.

### Scope
- validate and normalize data during write paths
- strengthen repair tooling and DQ clarity

### Files likely to change
- ingest services and collectors
- DQ checks
- maintenance or repair scripts under `scripts/`

### Tasks
- add validation at ingest write boundaries
- add explicit schema repair or migration scripts
- reduce read-time repair assumptions where safe
- strengthen DQ checks without breaking current pipeline semantics

### Entry criteria
- Phase 5 complete

### Exit criteria
- write-time validation exists for critical write paths
- repair behavior is explicit and scriptable
- read-time repair reliance is reduced and documented

### Rollback strategy
- validation should fail closed with clear diagnostics
- keep existing repair utilities until write-time guarantees are proven

### Codex Prompt – Phase 6
Read `docs/refactor/CODEX_REFACTOR_PLAN.md` and execute only Phase 6.

Tasks:
1. Add validation at ingest write time.
2. Add explicit schema repair or migration scripts.
3. Reduce repair-on-read assumptions.
4. Improve DQ clarity.

Constraints:
- Do not remove trust lineage.
- Do not weaken quarantine behavior.
- Keep failure modes observable.

Outputs:
- strengthened ingest validation
- repair scripts under `scripts/`
- updated DQ tests or fixtures

---

## Phase 7 — Publish Layer Cleanup
**Goal:** Separate rendering from delivery.

### Scope
- split payload building from delivery logic
- preserve retry-safe and idempotent behavior

### Files likely to change
- `run/publisher.py`
- `publishers/*`
- `services/publish/*`

### Tasks
- extract `telegram_summary_builder.py`
- extract `publish_payloads.py`
- keep delivery manager responsible for retries, dedupe, and delivery state
- add tests around duplicate and retry-safe behavior

### Entry criteria
- Phase 6 complete

### Exit criteria
- rendering concerns are separated from channel delivery
- retry-safe behavior remains intact
- publish artifacts and delivery logs remain compatible

### Rollback strategy
- preserve existing delivery manager interface
- move render logic first, then rewire delivery calls

### Codex Prompt – Phase 7
Read `docs/refactor/CODEX_REFACTOR_PLAN.md` and execute only Phase 7.

Tasks:
1. Separate publish rendering from delivery.
2. Extract Telegram summary building.
3. Preserve dedupe and retry-safe delivery behavior.
4. Add publish-path tests.

Constraints:
- Do not change publish-only retry semantics.
- Do not change channel identifiers or dedupe keys without compatibility.
- Preserve delivery logging.

Outputs:
- `services/publish/telegram_summary_builder.py`
- `services/publish/publish_payloads.py`
- updated publish tests

---

## Phase 8 — Documentation Alignment
**Goal:** Ensure consistency between docs and code.

### Scope
- update docs after structural work is complete
- record migration notes and final architecture

### Files likely to change
- `README.md`
- `docs/refactor/final_architecture.md`
- migration notes and developer guides

### Tasks
- update README to reflect new service layout and runtime foundations
- write final architecture summary
- add migration notes for maintainers
- document compatibility decisions and shims removed

### Entry criteria
- Phases 0–7 complete

### Exit criteria
- documentation matches repository structure and behavior
- migration notes exist for maintainers and future Codex runs

### Rollback strategy
- documentation only; no rollback complexity expected

### Codex Prompt – Phase 8
Read `docs/refactor/CODEX_REFACTOR_PLAN.md` and execute only Phase 8.

Tasks:
1. Align repository documentation with the final refactor state.
2. Add final architecture documentation.
3. Add migration notes and removed-shim notes.

Constraints:
- Documentation must reflect implemented code, not aspirational structure.

Outputs:
- updated `README.md`
- `docs/refactor/final_architecture.md`
- migration notes

---

## Target Directory Structure

```text
ai-trading-system/
├── core/
├── run/
├── analytics/
├── services/
│   ├── ingest/
│   ├── features/
│   ├── rank/
│   ├── execute/
│   └── publish/
├── ui/
│   └── services/
│       └── readmodels/
├── docs/
│   └── refactor/
│       ├── CODEX_REFACTOR_PLAN.md
│       ├── baseline_inventory.md
│       └── final_architecture.md
├── tests/
└── scripts/
```

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Unified runtime infrastructure | ✅ |
| Thin stage wrappers | ✅ |
| Stable UI read models | ✅ |
| Execution safety boundaries | ✅ |
| Removal of legacy utilities | ✅ |
| Artifact compatibility preserved | ✅ |
| Full pipeline operational | ✅ |

---

## Phase Deliverable Template

For every phase, Codex should explicitly report:

- files changed
- tests added or updated
- compatibility risks checked
- shims introduced or removed
- exit criteria satisfied
- follow-up risks for next phase

---

## Codex Operating Instructions

When executing this plan, Codex must:

1. work sequentially through phases
2. implement only one phase at a time
3. avoid breaking pipeline compatibility
4. preserve artifact formats
5. add tests before removing legacy code
6. keep changes small and reviewable
7. document all architectural modifications
8. prefer adapters and shims before hard deletions
9. preserve trust, DQ, and retry-safe delivery behavior

---

## Non-Goals

Codex must NOT:

- rewrite the entire repository
- change artifact filenames
- replace DuckDB or Parquet
- introduce live broker execution
- modify trading strategies during structural refactoring
- merge research and operational domains

---

## Recommended Immediate Next Command

To begin safely:

`Read docs/refactor/CODEX_REFACTOR_PLAN.md and execute only Phase 0.`

---

## License

This refactor plan is part of the AI Trading System repository and is intended for use with Codex and automated development workflows.

