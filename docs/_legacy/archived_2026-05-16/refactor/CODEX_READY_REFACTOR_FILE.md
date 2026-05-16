# CODEX_READY_REFACTOR_EXECUTION_PLAN.md

## Purpose

This document is the **canonical Codex execution playbook** for migrating the repository into a cleaner `src`-based, domain-oriented structure **without changing runtime behavior**.

It is designed to remove ambiguity during Codex-assisted refactoring and to protect the existing operational pipeline contracts:

`ingest -> features -> rank -> execute -> publish`

This plan follows one core rule:

> **Move first, stabilize second, decompose third.**

Do **not** ask Codex to execute the entire migration in one pass.

---

## Target Structure

```text
src/ai_trading_system/
├── pipeline/
│   ├── stages/
│   ├── dq/
│   ├── contracts.py
│   ├── context.py
│   ├── registry.py
│   └── orchestrator.py
├── domains/
│   ├── ingest/
│   ├── features/
│   ├── ranking/
│   ├── execution/
│   └── publish/
├── interfaces/
│   ├── api/
│   ├── streamlit/
│   └── shared_viewmodels/
├── platform/
│   ├── config/
│   ├── logging/
│   ├── db/
│   ├── utils/
│   └── storage/
└── research/
```
repo/
├── src/ai_trading_system/
├── tests/
├── docs/
├── scripts/
├── data/
├── reports/
├── logs/
├── web/
└── pyproject.toml

Keep `web/execution-console/` unchanged during this migration unless there is an explicit later task for it.

---

## Global Non-Negotiable Rules

These rules must be pasted at the top of **every Codex session prompt**.

```text
Important constraints:
- This is a behavior-preserving migration, not a redesign.
- Do not change business logic unless explicitly required to preserve imports after path moves.
- Do not change CLI flags, API response shapes, DuckDB schema, artifact filenames, artifact folder layout, environment variable names, or publish dedupe behavior.
- Do not add features, security middleware, new validation rules, or architectural improvements unless explicitly requested in this session.
- Do not rename public JSON keys, CSV columns, stage names, or run metadata fields.
- Do not change default execution mode, trust gating, DQ severity behavior, or publish retry semantics.
- Do not move web/execution-console in this migration unless explicitly requested.
- Use compatibility re-export shims at old import paths whenever moving modules.
- Prefer minimal edits over “cleanup” edits.
- If a file is oversized, do not split it unless this session explicitly asks for decomposition.
- If uncertain whether something is a public contract, assume it is and preserve it.
- Before changing any file, list why it must change.
- After changes, produce: changed files list, old path -> new path map, test results, and known risks.
```

---

## Core Migration Rule

For every major area:

1. **Move package/module paths only**
2. **Add shims at old import paths**
3. **Update imports only as needed**
4. **Run tests and canary validation**
5. **Only then decompose oversized files**
6. **Only after all consumers are migrated, remove shims**

---

## Protected Runtime Invariants

### Runtime behavior
- CLI entrypoints must keep working
- stage sequence remains `ingest -> features -> rank -> execute -> publish`
- publish retry semantics remain idempotent
- trust and DQ gating behavior remain intact

### Artifact contracts
The following filenames and output locations must remain unchanged during migration:

- `data/pipeline_runs/<run_id>/ingest/attempt_<n>/ingest_summary.json`
- `data/pipeline_runs/<run_id>/features/attempt_<n>/feature_snapshot.json`
- `data/pipeline_runs/<run_id>/rank/attempt_<n>/ranked_signals.csv`
- `data/pipeline_runs/<run_id>/rank/attempt_<n>/breakout_scan.csv`
- `data/pipeline_runs/<run_id>/rank/attempt_<n>/pattern_scan.csv`
- `data/pipeline_runs/<run_id>/rank/attempt_<n>/stock_scan.csv`
- `data/pipeline_runs/<run_id>/rank/attempt_<n>/sector_dashboard.csv`
- `data/pipeline_runs/<run_id>/rank/attempt_<n>/dashboard_payload.json`
- `data/pipeline_runs/<run_id>/execute/attempt_<n>/execute_summary.json`
- `data/pipeline_runs/<run_id>/publish/attempt_<n>/publish_summary.json`

### Contracts that must not change
- DuckDB schema
- API response shapes
- environment variable names
- dedupe-key semantics
- run/stage metadata semantics

---

## Validation Requirements for Every Session

Add this block to each Codex prompt when strict validation is desired:

```text
Validation requirements:
- Run relevant tests
- If pipeline code changed, run at least a canary pipeline validation
- If rank/publish paths changed, compare artifact names and key schemas
- If API paths changed, verify route list/startup still works
- Report any intentionally deferred risk
- If something seems risky, stop and document instead of guessing
```

---

## Ambiguity Killers

Use these exact phrases when needed.

### Stop opportunistic cleanup
```text
Do not perform opportunistic cleanup.
Only touch files required for this session’s scope.
```

### Preserve all external contracts
```text
If a field, filename, path, or key might be externally consumed, preserve it exactly.
```

### Stop premature decomposition
```text
Do not split large files in this session unless decomposition is explicitly in scope.
```

### Prevent behavior drift
```text
Preserve current behavior even if the existing implementation is imperfect.
Behavior fixes belong in separate follow-up tasks.
```

### Force safe deferral
```text
If the requested move cannot be done safely without behavior change, stop at the safest partial completion and document the blocker.
```

---

# Session-by-Session Execution Checklist

---

## Session 0 — Baseline Capture and Packaging Prep

### Goal
Prepare the repo for migration without changing runtime behavior.

### Scope
- `src/` package layout setup
- packaging config
- baseline artifact capture
- remove obviously broken or dead items only if already confirmed
- no domain moves yet

### Codex Prompt

```text
Task: Prepare the repository for a src-based migration without changing runtime behavior.

Primary goals:
1. Create src/ai_trading_system/ package root.
2. Configure packaging/import resolution for src layout.
3. Capture migration baseline documentation.
4. Do not move domain code yet.

Important constraints:
- This is a behavior-preserving migration, not a redesign.
- Do not change business logic unless explicitly required to preserve imports after path moves.
- Do not change CLI flags, API response shapes, DuckDB schema, artifact filenames, artifact folder layout, environment variable names, or publish dedupe behavior.
- Do not add features, security middleware, new validation rules, or architectural improvements unless explicitly requested in this session.
- Do not rename public JSON keys, CSV columns, stage names, or run metadata fields.
- Do not change default execution mode, trust gating, DQ severity behavior, or publish retry semantics.
- Do not move web/execution-console in this migration unless explicitly requested.
- Use compatibility re-export shims at old import paths whenever moving modules.
- Prefer minimal edits over cleanup edits.
- If uncertain whether something is a public contract, assume it is and preserve it.

Session scope:
- Create src/ai_trading_system/__init__.py
- Update pyproject.toml or equivalent for src layout
- Ensure tests can still resolve imports
- Capture baseline docs:
  - docs/refactor/baseline_inventory.md
  - docs/refactor/baseline_artifact_contracts.md
  - docs/refactor/baseline_import_graph.md
- If main.py is confirmed broken and unused, mark for removal or remove only with clear justification
- If committed runtime data should be untracked, document that separately; do not delete local runtime data blindly
- Fix only clearly safe forward-compat issues already identified

Do not:
- Move pipeline/domain/interface modules yet
- Refactor oversized files
- Rename imports repo-wide unless needed for packaging

Required output:
1. Summary of changes
2. Exact files changed
3. Any file removed and why
4. Baseline docs created
5. Test results
6. Risks or follow-ups
```

### Success Criteria
- tests still resolve imports
- no operational code moved
- baseline docs exist

---

## Session 1 — Platform Move Only

### Goal
Move platform-level helpers first because everything else depends on them.

### Scope
- config
- logging
- paths
- env/bootstrap/runtime helpers
- shims only, no redesign

### Codex Prompt

```text
Task: Migrate platform-level modules into src/ai_trading_system/platform/ using compatibility shims.

Important constraints:
- This is a behavior-preserving migration, not a redesign.
- Do not change logic in logger, paths, env loading, runtime config, or config parsing.
- Do not introduce a global DB abstraction refactor in this session.
- Do not update every consumer repo-wide unless required for moved files.
- Keep old import paths working through re-export shims.

Move only these categories:
- config/* -> platform/config/*
- core/logging.py -> platform/logging/logger.py
- core/paths.py -> platform/db/paths.py
- utils/env.py and core/env.py -> platform/utils/env.py
- core/bootstrap.py -> platform/utils/bootstrap.py
- core/runtime_config.py -> platform/utils/runtime_config.py

Required approach:
1. Move files to new platform paths.
2. Leave old-path shims that re-export from the new path.
3. Update only moved-file internal imports if necessary.
4. Do not perform broad consumer rewrites yet unless clearly safe.

Required outputs:
- docs/refactor/session1_platform_migration.md
- old path -> new path map
- list of shims added
- test results
- any unresolved import hotspots
```

### Success Criteria
- old imports still work
- new paths exist
- no runtime contract changes

---

## Session 2 — Pipeline Contracts, Registry, DQ, and Stage Wrappers

### Goal
Stabilize the pipeline core before domains move.

### Scope
- `pipeline/contracts.py`
- `pipeline/registry.py`
- `pipeline/dq/engine.py`
- `pipeline/stages/*`
- optionally `pipeline/orchestrator.py` as a path move only

### Codex Prompt

```text
Task: Move pipeline core modules into src/ai_trading_system/pipeline/ with minimal logic changes.

Important constraints:
- Preserve StageContext, StageResult, StageArtifact, PipelineStageError, and related contract behavior exactly.
- Do not redesign orchestrator flow.
- Do not change stage ordering, stage names, run metadata semantics, or artifact output paths.
- Do not move domain business logic in this session.
- If moving orchestrator, do it as a path move only.

Session scope:
- core/contracts.py -> pipeline/contracts.py
- analytics/registry/store.py -> pipeline/registry.py
- analytics/dq/engine.py -> pipeline/dq/engine.py
- run/stages/*.py -> pipeline/stages/*.py
- optionally run/orchestrator.py -> pipeline/orchestrator.py, but only if safe

Required approach:
1. Move files.
2. Add old-path shims.
3. Update imports needed for moved files.
4. Keep run entrypoints operational.

Do not:
- Extract new helpers from orchestrator yet
- Merge alerts/preflight/publish helpers
- Redesign DQ rule behavior
- Change registry schema handling

Required outputs:
- docs/refactor/session2_pipeline_core.md
- contract invariants checked
- list of moved files and shims
- tests run
- note whether orchestrator was moved or deferred
```

### Success Criteria
- `StageContext` remains importable throughout
- stage wrappers still work
- DQ and registry behavior preserved

---

## Session 3 — Orchestrator Extraction Only

### Goal
Only after pipeline core is stable, extract context-building logic into a helper.

### Codex Prompt

```text
Task: Extract pipeline context-building logic into pipeline/context.py without changing orchestrator behavior.

Important constraints:
- This is a focused extraction, not a redesign.
- Do not change run lifecycle behavior, stage sequencing, artifact path logic, logging semantics, or exception semantics.
- Do not merge unrelated helpers into orchestrator.
- Preserve CLI surface and entrypoint behavior.

Session scope:
- Identify the logic inside orchestrator that constructs StageContext or equivalent per-stage context
- Extract it into pipeline/context.py
- Make orchestrator call the extracted helper
- Keep signatures and outputs equivalent

Do not:
- Refactor other orchestrator responsibilities
- Change parameter names unless strictly required
- Move publish/alerts/preflight code in this session

Required outputs:
- docs/refactor/session3_orchestrator_context.md
- before/after call flow summary
- exact extracted helper name and parameters
- tests and canary results
```

### Success Criteria
- context helper extracted safely
- orchestrator behavior unchanged
- CLI surface unchanged

---

## Session 4 — Ingest Path Move Only

### Goal
Move ingest modules into `domains/ingest/` but do not decompose large files yet.

### Codex Prompt

```text
Task: Move ingest-related modules into src/ai_trading_system/domains/ingest/ using path moves and shims only.

Important constraints:
- Do not decompose dhan_collector.py or daily_update_runner.py in this session.
- Do not change provider selection logic, fallback semantics, quarantine behavior, provenance logic, or trust status computation.
- Do not remove cross-domain calls yet unless necessary to preserve imports.
- Keep collectors/daily_update_runner.py working as an entrypoint or shim.

Move only:
- services/ingest/orchestration.py -> domains/ingest/service.py
- analytics/data_trust.py -> domains/ingest/trust.py
- collectors/nse_collector.py -> domains/ingest/providers/nse.py
- collectors/dhan_collector.py -> domains/ingest/providers/dhan.py
- collectors/yfinance_collector.py -> domains/ingest/providers/yfinance.py
- collectors/delivery_collector.py -> domains/ingest/delivery.py
- collectors/repair_ohlcv_window.py -> domains/ingest/repair.py
- collectors/masterdata.py -> domains/ingest/masterdata.py

Approach:
1. Move files only.
2. Leave old-path shims.
3. Keep daily_update_runner as a thin entrypoint/shim if needed.
4. Update only necessary imports.

Required outputs:
- docs/refactor/session4_ingest_paths.md
- moved files map
- list of legacy collector entrypoints still kept
- tests and ingest-only validation notes
```

### Success Criteria
- ingest still produces same outputs and trust behavior
- provider order unchanged
- quarantine logic unchanged

---

## Session 5 — Ingest Decomposition

### Goal
Decompose ingest internals safely after path stabilization.

### Codex Prompt

```text
Task: Decompose ingest internals after path stabilization, while preserving behavior.

Important constraints:
- Preserve ingest outputs, trust state semantics, fallback behavior, quarantine/provenance handling, and CLI behavior.
- Do not change database schema or artifact paths.
- Extract responsibilities incrementally; do not rewrite everything at once.

Target decomposition:
- Extract DB read/write logic into domains/ingest/repository.py
- Extract Dhan auth/token logic into a dedicated module if clearly separable
- Reduce daily_update_runner.py to a thin app/CLI wrapper
- Keep provider networking logic inside provider modules

Do not:
- Introduce new provider abstractions unless required
- Change API payload parsing behavior
- Move feature computation behavior here or redesign ingest/features boundary beyond safe separation

Required outputs:
- docs/refactor/session5_ingest_decomposition.md
- list of extracted classes/functions
- list of preserved public entrypoints
- tests and ingest artifact parity notes
```

### Success Criteria
- repository extraction done safely
- public ingest entrypoints preserved
- artifacts and trust behavior unchanged

---

## Session 6 — Features Path Move Only

### Goal
Move features modules without splitting `feature_store.py` yet.

### Codex Prompt

```text
Task: Move features modules into src/ai_trading_system/domains/features/ using path moves and shims only.

Important constraints:
- Do not split feature_store.py in this session.
- Do not change snapshot semantics, Parquet layout, DuckDB metadata behavior, or downstream rank inputs.
- Do not change feature names, table names, or snapshot/output contracts.

Move only:
- services/features/orchestration.py -> domains/features/service.py
- features/feature_store.py -> domains/features/feature_store.py
- features/indicators.py -> domains/features/indicators.py
- features/compute_sector_rs.py -> domains/features/sector_rs.py

Approach:
1. Move files only.
2. Add shims at old paths.
3. Update imports only as needed.

Required outputs:
- docs/refactor/session6_features_paths.md
- moved file map
- tests and feature-stage validation notes
```

### Success Criteria
- old imports still work
- feature outputs unchanged
- no feature internals split yet

---

## Session 7 — Features Decomposition

### Goal
Split `feature_store.py` after stabilization.

### Codex Prompt

```text
Task: Decompose domains/features/feature_store.py into clearer internal modules while preserving external behavior.

Important constraints:
- Preserve feature outputs, snapshot behavior, registry behavior, Parquet layout, DuckDB writes, and downstream compatibility.
- Keep the public entrypoint/module path stable even if internals are split.
- Do not rename output columns or public methods unless absolutely necessary.

Target decomposition:
- Pure indicator computations -> domains/features/indicators.py
- Snapshot/Parquet logic -> domains/features/snapshot.py
- Registry/metadata DB logic -> domains/features/repository.py

Required cleanup:
- Replace bare except: only where touched, without changing expected error handling behavior unexpectedly

Do not:
- Redesign feature computation algorithms
- Change snapshot IDs, partitioning conventions, or metadata table schema

Required outputs:
- docs/refactor/session7_features_decomposition.md
- extracted responsibilities map
- public interface preserved list
- tests and feature artifact parity notes
```

### Success Criteria
- public features interface preserved
- internals cleaner
- artifact parity maintained

---

## Session 8 — Ranking Path Move Only

### Goal
Move ranking-related code into one domain without merging internals yet.

### Codex Prompt

```text
Task: Move ranking-related modules into src/ai_trading_system/domains/ranking/ using path moves and shims only.

Important constraints:
- Do not redesign ranking internals in this session.
- Do not merge factor modules yet.
- Preserve rank artifact filenames, payload JSON structure, breakout/pattern outputs, and score columns.
- Keep ranking behavior unchanged.

Move only:
- services/rank/orchestration.py -> domains/ranking/service.py
- analytics/ranker.py -> domains/ranking/ranker.py
- channel/breakout_scan.py -> domains/ranking/breakout.py
- channel/stock_scan.py -> domains/ranking/stock_scan.py
- channel/sector_dashboard.py -> domains/ranking/sector_dashboard.py
- analytics/patterns/* -> domains/ranking/patterns/*
- analytics/regime_detector.py -> domains/ranking/regime_detector.py
- analytics/screener.py -> domains/ranking/screener.py
- services/rank/dashboard_payload.py -> domains/ranking/payloads.py

Approach:
1. Move files only.
2. Add shims.
3. Update imports only as needed.

Required outputs:
- docs/refactor/session8_ranking_paths.md
- moved file map
- tests and rank artifact validation notes
```

### Success Criteria
- rank outputs preserved
- old imports still work
- ranking internals not merged prematurely

---

## Session 9 — Ranking Cleanup

### Goal
Clean ranking internals only after path stabilization.

### Codex Prompt

```text
Task: Simplify ranking internals after path stabilization, while preserving outputs and behavior.

Important constraints:
- Preserve ranked_signals.csv, breakout_scan.csv, pattern_scan.csv, stock_scan.csv, sector_dashboard.csv, and dashboard_payload.json contracts.
- Preserve score computation outputs unless a change is required to keep behavior equivalent after relocation.
- Do not rename public payload keys or artifact columns.

Potential cleanup scope:
- Merge tightly coupled factor modules if useful
- Clarify internal service/engine boundaries
- Keep a stable public ranking entrypoint

Do not:
- Change factor weights
- Change score normalization rules
- Change breakout qualification semantics
- Change pattern output schema

Required outputs:
- docs/refactor/session9_ranking_cleanup.md
- internal module simplification notes
- list of preserved output contracts
- tests and artifact parity notes
```

### Success Criteria
- cleanup stays internal
- contracts unchanged
- rank artifacts remain stable

---

## Session 10 — Execution Move

### Goal
Move execution almost 1:1 because it is already well-structured.

### Codex Prompt

```text
Task: Move execution modules into src/ai_trading_system/domains/execution/ with minimal changes.

Important constraints:
- This should be a mostly 1:1 move.
- Preserve trust blocking behavior, preview/live semantics, adapter behavior, positions/orders/fills persistence, and output artifacts.
- Do not redesign execution models.

Move:
- execution/* -> domains/execution/*
- services/execute/candidate_builder.py -> domains/execution/candidate_builder.py

Approach:
1. Move files only.
2. Add shims.
3. Update imports only as needed.

Required outputs:
- docs/refactor/session10_execution_move.md
- moved file map
- tests and execute-stage validation notes
```

### Success Criteria
- execution behavior unchanged
- outputs preserved
- minimal diff beyond path changes

---

## Session 11 — Publish Path Move Only

### Goal
Move publish logic carefully because publish is retry-safe and artifact-driven.

### Codex Prompt

```text
Task: Move publish-related modules into src/ai_trading_system/domains/publish/ using path moves and shims only.

Important constraints:
- Preserve publish retry semantics, dedupe behavior, channel selection behavior, and publish artifact contracts.
- Do not redesign delivery state persistence in this session.
- Do not change message shaping or payload contracts unless required for imports.

Move only:
- run/publisher.py -> domains/publish/delivery_manager.py
- publishers/google_sheets.py -> domains/publish/channels/google_sheets.py
- channel/telegram_reporter.py and/or publishers/telegram.py -> domains/publish/channels/telegram.py
- publishers/quantstats_dashboard.py -> domains/publish/channels/quantstats.py
- publish-stage orchestration into domains/publish/service.py

Approach:
1. Move files only.
2. Add shims.
3. Update imports only as needed.

Required outputs:
- docs/refactor/session11_publish_paths.md
- moved file map
- tests and publish validation notes
```

### Success Criteria
- publish retry/dedupe preserved
- outputs stable
- no internal publish redesign yet

---

## Session 12 — Publish Cleanup

### Goal
Only after path stabilization, clean internal boundaries.

### Codex Prompt

```text
Task: Clean internal publish structure after path stabilization while preserving external behavior.

Important constraints:
- Preserve dedupe semantics, retry behavior, channel outcomes, and publish_summary.json behavior.
- Keep channel payload contracts stable.

Possible cleanup:
- Isolate publish repository/state persistence if scattered
- Clarify service vs delivery manager responsibilities
- Reduce shim dependence

Do not:
- Change hashing logic
- Change retry policy behavior
- Change channel result naming or external identifiers unless strictly necessary

Required outputs:
- docs/refactor/session12_publish_cleanup.md
- internal cleanup summary
- preserved contract notes
- tests and publish parity notes
```

### Success Criteria
- publish behavior identical
- internal structure clearer
- no contract drift

---

## Session 13 — Interfaces Move as Units

### Goal
Move API and Streamlit code as units. Do not add new middleware or split the giant UI file yet.

### Codex Prompt

```text
Task: Move interface modules into src/ai_trading_system/interfaces/ as units, without redesign.

Important constraints:
- Do not add auth middleware in this session.
- Do not split the large Streamlit app file in this session.
- Do not change API response shapes, route names, query parameters, or UI payload expectations.
- Move as units first.

Move:
- ui/execution_api/* -> interfaces/api/*
- ui/research/* -> interfaces/streamlit/research/*
- ui/execution/* -> interfaces/streamlit/execution/*
- ui/services/* -> interfaces/api/services/* or interfaces/shared_viewmodels/* only where clearly appropriate

Approach:
1. Move units mostly as-is.
2. Add shims if old imports exist.
3. Update imports only as required.

Required outputs:
- docs/refactor/session13_interfaces_move.md
- moved file map
- API route preservation notes
- tests and startup validation notes
```

### Success Criteria
- interfaces moved safely
- routes preserved
- UI files not prematurely broken apart

---

## Session 14 — Interfaces Cleanup

### Goal
After stabilization, clean interface boundaries.

### Codex Prompt

```text
Task: Clean interface internals after path stabilization while preserving API/UI behavior.

Important constraints:
- Preserve API route contracts, response shapes, and UI-consumed payloads.
- Do not add new auth, RBAC, or middleware in this session.
- Do not redesign frontend behavior.

Possible scope:
- Separate API service helpers from UI/shared presentation helpers
- Reduce coupling to legacy paths
- Keep presentation payload builders stable

Required outputs:
- docs/refactor/session14_interfaces_cleanup.md
- cleanup summary
- preserved route/payload notes
- tests and startup validation notes
```

### Success Criteria
- cleanup stays internal
- no route/payload drift
- middleware/features still untouched

---

## Session 15 — Research Move Last

### Goal
Move research after operational paths are stable.

### Codex Prompt

```text
Task: Move research-related code into src/ai_trading_system/research/ after operational pipeline stabilization.

Important constraints:
- Preserve research script behavior and importability.
- Do not mix research modules back into operational domains.
- Do not change training/evaluation/report behavior in this session.

Move:
- research/ -> research/
- research-oriented analytics modules into research/ where clearly non-operational

Do not:
- Move operational ranking/feature modules just because research imports them
- Redesign research workflows

Required outputs:
- docs/refactor/session15_research_move.md
- moved file map
- tests and research import validation notes
```

### Success Criteria
- research paths updated
- operational modules remain separate
- no behavior redesign

---

## Session 16 — Remove Shims and Finalize

### Goal
Only after all consumers are migrated.

### Codex Prompt

```text
Task: Remove legacy import shims and finalize the migration.

Important constraints:
- Only remove a shim if no remaining imports depend on it.
- Do not remove a shim “because it should be unused”; verify actual usage.
- Do not change behavior during cleanup.

Required work:
- Find remaining legacy imports
- Update them to new canonical paths
- Remove unused shims
- Update docs/README/AGENTS.md
- Produce final migration map

Required outputs:
- docs/refactor/final_migration_report.md
- list of removed shims
- remaining legacy-path usage (should be zero or documented)
- final tests and canary validation
```

### Success Criteria
- no legacy imports remain
- shims removed safely
- final docs updated

---

# Recommended Practical Grouping

Instead of running 17 tiny sessions, use these **8 practical Codex runs**:

1. Session 0 + Session 1  
2. Session 2 + Session 3  
3. Session 4 + Session 5  
4. Session 6 + Session 7  
5. Session 8 + Session 9  
6. Session 10 + Session 11 + Session 12  
7. Session 13 + Session 14  
8. Session 15 + Session 16  

This balances safety with speed.

---

## Final Notes

This plan is intentionally conservative.

It is optimized to:
- preserve operational behavior
- avoid silent contract drift
- stop opportunistic cleanup
- reduce ambiguity for Codex
- let you validate progress phase by phase

If a requested move cannot be completed safely inside a session, the correct action is:

> **stop at the safest partial completion and document the blocker**

That is preferred over speculative completion.
