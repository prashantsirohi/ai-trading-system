# Legacy Documentation Archive

- **Purpose:** Hold historical documentation that has been replaced by the new docs structure. Nothing here is current. Read for context only.
- **Audience:** Anyone investigating "why was this designed this way" or "what did the old version say".
- **Last verified:** 2026-07-13

> **Do not link to files in this directory from current docs.** If you find content here that is still relevant, migrate it into the appropriate current doc and link from there.

## How to recover an archived doc from git

Each archive subdirectory is named `archived_<YYYY-MM-DD>/` for the date of the archival wave. Recover any file with:

```bash
git log --diff-filter=D --summary -- docs/archive/<old-path>      # find when it moved
git show <commit>:docs/archive/<old-path>                          # view old content
```

For files moved (not deleted), the current location is `docs/_legacy/archived_<date>/<file>`.

## Archival waves

### `archived_2026-07-13/` — Stale truth-map retirement

`docs/_audit/current_code_truth_map.md` was archived after the July deep review confirmed that its pipeline, persistence, and API claims no longer matched runtime code. The original path now contains only a tombstone pointing to `docs/SYSTEM_GUIDE.md`.

### `archived_2026-05-16/` — Documentation cleanup wave

Triggered by the docs cleanup task in branch `claude/optimistic-dijkstra-b7054f`. Inventory and rationale:

- `docs/_audit/documentation_inventory.md` — full inventory and per-file action.
- `docs/_audit/stale_reference_report.md` — stale claims found and resolved.
- `docs/_audit/current_code_truth_map.md` — code-truth snapshot used to rewrite docs.

#### Files moved in this wave

From `docs/archive/` → `docs/_legacy/archived_2026-05-16/`:

| Old path | New path | Replaced by |
|---|---|---|
| `docs/archive/README.md` | `docs/_legacy/archived_2026-05-16/archive_index.md` | This file |
| `docs/archive/architecture.md` | `docs/_legacy/archived_2026-05-16/architecture_iceberg_lite.md` | `docs/architecture/storage_and_lineage.md` |
| `docs/archive/data-flow.md` | `docs/_legacy/archived_2026-05-16/data-flow.md` | `docs/architecture/operational_data_flow.md` |
| `docs/archive/database.md` | `docs/_legacy/archived_2026-05-16/database.md` | `docs/reference/database_schema.md` |
| `docs/archive/dhan_ohlc_isolation_strategy.md` | `docs/_legacy/archived_2026-05-16/dhan_ohlc_isolation_strategy.md` | `docs/reference/data_sources.md` (NSE primary, Dhan fallback) |
| `docs/archive/dq_rules.md` | `docs/_legacy/archived_2026-05-16/dq_rules.md` | `docs/architecture/data_trust_and_dq.md` |
| `docs/archive/high_level_operational_data_flow.md` | `docs/_legacy/archived_2026-05-16/high_level_operational_data_flow.md` | `docs/architecture/operational_data_flow.md` |
| `docs/archive/ohlcv_reset_reingest_runbook.md` | `docs/_legacy/archived_2026-05-16/ohlcv_reset_reingest_runbook.md` | `docs/runbooks/data_repair.md` |
| `docs/archive/ops_runbook.md` | `docs/_legacy/archived_2026-05-16/ops_runbook.md` | `docs/runbooks/daily_operations.md` + `docs/runbooks/troubleshooting.md` |

In the Phase 3–5 wave on the same day, five more docs joined the archive after their content was migrated:

| Old path | New path | Replaced by |
|---|---|---|
| `docs/AS_IS_DESIGN.md` | `docs/_legacy/archived_2026-05-16/AS_IS_DESIGN.md` | `docs/architecture/target_architecture.md` |
| `docs/architecture/pipeline.md` | `docs/_legacy/archived_2026-05-16/architecture_pipeline.md` | `docs/architecture/operational_data_flow.md` + `docs/stages/*.md` |
| `docs/architecture/system-overview.md` | `docs/_legacy/archived_2026-05-16/architecture_system-overview.md` | `docs/architecture/overview.md` |
| `docs/architecture/module-map.md` | `docs/_legacy/archived_2026-05-16/architecture_module-map.md` | `docs/architecture/target_architecture.md` + `docs/domains/*.md` |
| `docs/research_backtesting.md` | `docs/_legacy/archived_2026-05-16/research_backtesting.md` | `docs/domains/research_domain.md` |

In the Phase 6–7 wave on the same day, 13 more docs joined the archive after their content was migrated to reference + runbooks:

| Old path | New path | Replaced by |
|---|---|---|
| `docs/operations/runbook.md` | `docs/_legacy/archived_2026-05-16/operations_runbook.md` | `docs/runbooks/daily_operations.md` + `weekly_operations.md` |
| `docs/operations/troubleshooting.md` | `docs/_legacy/archived_2026-05-16/operations_troubleshooting.md` | `docs/runbooks/troubleshooting.md` + `dq_failure_response.md` + `publish_retry.md` + `data_repair.md` |
| `docs/operations/installation.md` | `docs/_legacy/archived_2026-05-16/operations_installation.md` | `docs/runbooks/deployment_mac_mini.md` |
| `docs/operations/configuration.md` | `docs/_legacy/archived_2026-05-16/operations_configuration.md` | `docs/reference/configuration.md` |
| `docs/operations/market_intel_runner.md` | `docs/_legacy/archived_2026-05-16/operations_market_intel_runner.md` | `docs/domains/catalyst_intelligence_domain.md` (with runbook fragments under `docs/runbooks/`) |
| `docs/interfaces/api.md` | `docs/_legacy/archived_2026-05-16/interfaces_api.md` | `docs/reference/api_reference.md` |
| `docs/interfaces/ui.md` | `docs/_legacy/archived_2026-05-16/interfaces_ui.md` | `docs/architecture/ui_architecture.md` + `docs/domains/ui_domain.md` |
| `docs/architecture/data-model.md` | `docs/_legacy/archived_2026-05-16/architecture_data-model.md` | `docs/reference/database_schema.md` + `docs/architecture/storage_and_lineage.md` |
| `docs/architecture/pattern-scan.md` | `docs/_legacy/archived_2026-05-16/architecture_pattern-scan.md` | `docs/reference/breakout_and_patterns.md` + `docs/stages/rank.md` |
| `docs/architecture/strategy-optimizer.md` | `docs/_legacy/archived_2026-05-16/architecture_strategy-optimizer.md` | `docs/domains/optimization_domain.md` |
| `docs/fundamental_layer.md` | `docs/_legacy/archived_2026-05-16/fundamental_layer.md` | `docs/domains/fundamentals_domain.md` + `docs/stages/fundamentals.md` |
| `docs/risk_engine_runbook.md` | `docs/_legacy/archived_2026-05-16/risk_engine_runbook.md` | `docs/reference/execution_policy.md` + `docs/stages/execute.md` |
| (directory) `docs/operations/` | removed | content lives under `docs/runbooks/` and `docs/reference/` |
| (directory) `docs/interfaces/` | removed | content lives under `docs/reference/api_reference.md` and `docs/architecture/ui_architecture.md` |

From `docs/refactor/` → `docs/_legacy/archived_2026-05-16/refactor/` (frozen historical baselines, no ongoing operational use):

| Old path | New path | Notes |
|---|---|---|
| `docs/refactor/baseline_inventory.md` | `docs/_legacy/archived_2026-05-16/refactor/baseline_inventory.md` | Pre-refactor folder snapshot. References `services/` as empty placeholder; no longer accurate. |
| `docs/refactor/baseline_import_graph.md` | `docs/_legacy/archived_2026-05-16/refactor/baseline_import_graph.md` | Pre-refactor import traces. |
| `docs/refactor/batch1_legacy_audit.md` | `docs/_legacy/archived_2026-05-16/refactor/batch1_legacy_audit.md` | Phase 1a refactor audit. |
| `docs/refactor/batch4_legacy_audit.md` | `docs/_legacy/archived_2026-05-16/refactor/batch4_legacy_audit.md` | Phase 4 refactor audit. |

In the Phase 11 final wave, 5 more docs joined the archive after their content was migrated to ADRs + development docs:

| Old path | New path | Replaced by |
|---|---|---|
| `docs/EXECUTION_CONSOLE_PLAN.md` | `docs/_legacy/archived_2026-05-16/EXECUTION_CONSOLE_PLAN.md` | `docs/decisions/ADR-0005-react-operator-workspace.md` + `docs/architecture/ui_architecture.md` |
| `docs/refactor/CODEX_READY_REFACTOR_FILE.md` | `docs/_legacy/archived_2026-05-16/refactor/CODEX_READY_REFACTOR_FILE.md` | `docs/development/legacy_cleanup_plan.md` |
| `docs/refactor/final_architecture.md` | `docs/_legacy/archived_2026-05-16/refactor/final_architecture.md` | `docs/architecture/target_architecture.md` |
| `docs/refactor/baseline_artifact_contracts.md` | `docs/_legacy/archived_2026-05-16/refactor/baseline_artifact_contracts.md` | `docs/reference/artifacts.md` |
| `docs/refactor/collectors_canonical_map.md` | `docs/_legacy/archived_2026-05-16/refactor/collectors_canonical_map.md` | `docs/development/package_migration.md` |
| (directory) `docs/refactor/` | removed | content lives under `docs/development/` and `docs/decisions/` |

#### Files retained for content migration (NOT yet archived) — RESOLVED 2026-05-16

**All files in this list have now been archived in Waves 2, 3, or 4.** See sections above.

Original list (kept for traceability):

These docs contain current-or-partially-current content that must be folded into the new structure during Phases 3–9 **before** they can be archived:

- `docs/AS_IS_DESIGN.md` — Revision-2 corrections to be folded into `docs/architecture/target_architecture.md`.
- `docs/EXECUTION_CONSOLE_PLAN.md` — UI rewiring rationale to be folded into `docs/decisions/ADR-0005-react-operator-workspace.md` + `docs/architecture/ui_architecture.md`.
- `docs/fundamental_layer.md` — to be folded into `docs/domains/fundamentals_domain.md`.
- `docs/research_backtesting.md` — to be folded into `docs/domains/research_domain.md`.
- `docs/risk_engine_runbook.md` — to be folded into `docs/reference/execution_policy.md` (+ runbook portions).
- `docs/architecture/{system-overview,pipeline,module-map,data-model,pattern-scan,strategy-optimizer}.md` — content migration to corresponding new pages (architecture/ overview + operational_data_flow + storage_and_lineage; reference/ ranking_factors + breakout_and_patterns; domains/ optimization_domain).
- `docs/operations/*` — to be folded into `docs/runbooks/*` and `docs/reference/configuration.md`.
- `docs/interfaces/api.md` — to be folded into `docs/reference/api_reference.md`.
- `docs/interfaces/ui.md` — to be folded into `docs/architecture/ui_architecture.md` + `docs/domains/ui_domain.md`.
- `docs/refactor/CODEX_READY_REFACTOR_FILE.md`, `docs/refactor/final_architecture.md`, `docs/refactor/baseline_artifact_contracts.md`, `docs/refactor/collectors_canonical_map.md` — to be folded into `docs/development/legacy_cleanup_plan.md`, `docs/architecture/target_architecture.md`, `docs/reference/artifacts.md`, `docs/development/package_migration.md`.

These will move into `docs/_legacy/archived_2026-05-16/` once the migration in Phases 3–9 is complete.
