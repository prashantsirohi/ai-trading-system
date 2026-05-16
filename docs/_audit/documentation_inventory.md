# Documentation Inventory

- **Purpose:** Catalog every documentation-like file in the repo, classify currency, and propose an action for the upcoming cleanup.
- **Audience:** Operator, doc reviewers, future cleanup phases.
- **Last verified:** 2026-05-16
- **Source of truth:** Filesystem scan of repo at branch `claude/optimistic-dijkstra-b7054f`; cross-checked against `current_code_truth_map.md`.
- **Status:** Phase 0 deliverable. No docs have been modified or moved yet.

---


> **Note for `scripts/check_docs.py`:** This file intentionally lists stale terms (the whole point of an audit). Forbidden-term checks below this banner are not enforced — the report itself is the documentation of the stale claims being retired.

## Method

Every `*.md` file outside `.git/`, `node_modules/`, `data/`, `.claude/` was opened and skimmed (full read for files <300 lines). No `*.docx`, `*.pdf`, or `*.txt` documentation files were found. Classification uses:

- **CURRENT** — Matches code as of today.
- **PARTIALLY_CURRENT** — Useful but contains stale claims; needs migration before retire.
- **LEGACY** — Describes a prior state; superseded.
- **DUPLICATE** — Same content also exists elsewhere.
- **UNKNOWN** — Could not be classified without deeper code reading.

Action options: `KEEP`, `MIGRATE_CONTENT_THEN_ARCHIVE`, `ARCHIVE_AS_IS`, `DELETE_CANDIDATE`.

Replacement target column references the **new** docs structure defined in the task spec (e.g., `docs/architecture/overview.md`). Targets are proposed, not yet created.

---

## Root-level

| Path | Title / Purpose | Classification | Key stale references | Replacement target | Action |
|---|---|---|---|---|---|
| `README.md` | Project entrypoint; high-level intro and doc nav | CURRENT | None significant — already concise and references docs/ | `README.md` (rewrite per Phase 2 §4) | KEEP (light rewrite later) |
| `AGENTS.md` | LLM behavioral guidelines | DUPLICATE | Byte-identical to `claude.md` | n/a — keep one canonical copy | DELETE_CANDIDATE (after confirming with operator which name to keep) |
| `claude.md` | LLM behavioral guidelines | DUPLICATE | Byte-identical to `AGENTS.md` | n/a | DELETE_CANDIDATE (mirror of AGENTS.md) |
| `.docs-pr-checklist.md` | PR checklist enforcing doc sync | CURRENT | None | `docs/development/docs_update_checklist.md` (Phase 10) | MIGRATE_CONTENT_THEN_ARCHIVE (move/rename into docs/) |

Note: `AGENTS.md` and `claude.md` cannot both stay. The CLAUDE.md project guidelines at the root of this worktree is a *different* file from `claude.md` — `CLAUDE.md` here lives in the worktree only and is sourced by the harness.

---

## docs/ — top-level

| Path | Purpose | Classification | Key stale references | Replacement target | Action |
|---|---|---|---|---|---|
| `docs/README.md` | Documentation index | CURRENT | None major | `docs/README.md` (full rewrite Phase 3 §1) | MIGRATE_CONTENT_THEN_ARCHIVE (replace, save old) |
| `docs/DOCS_STANDARD.md` | Doc writing rules | CURRENT | None | `docs/development/coding_standards.md` or keep as-is at new path | KEEP (relocate optional) |
| `docs/EXECUTION_CONSOLE_PLAN.md` | FastAPI+React rewiring roadmap | PARTIALLY_CURRENT | Status fields like "Phase 1 shipped, Phase 2a-2b in flight" need verification against current React/FastAPI code | `docs/decisions/ADR-0005-react-operator-workspace.md` + `docs/architecture/ui_architecture.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/AS_IS_DESIGN.md` | Architecture snapshot (Rev 2) | PARTIALLY_CURRENT | Pinned to commit `4dfc618` on a different branch; "two-tier layout (canonical src/ + legacy shims)" claim needs verification — root-level `analytics/`, `audit_rank.py` still exist | `docs/architecture/overview.md` + `docs/architecture/target_architecture.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/fundamental_layer.md` | Screener.in fundamentals layer | CURRENT (mostly) | Verify scoring model is still active — code does have `domains/fundamentals/scoring.py` | `docs/domains/fundamentals_domain.md` + `docs/stages/...` if wired in | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/research_backtesting.md` | Backtest data sources and rule engine | CURRENT | None major | `docs/domains/research_domain.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/risk_engine_runbook.md` | Shared trading rule engine runbook | CURRENT | None major | `docs/reference/execution_policy.md` + `docs/runbooks/...` portions | MIGRATE_CONTENT_THEN_ARCHIVE |

---

## docs/architecture/

| Path | Purpose | Classification | Key stale references | Replacement target | Action |
|---|---|---|---|---|---|
| `docs/architecture/system-overview.md` | 5-stage pipeline, storage, UI | PARTIALLY_CURRENT | Stage list says 5; actual pipeline has 11 stages including `candidates`, `fundamentals`, `events`, `insight`, `narrative`, `perf_tracker` | `docs/architecture/overview.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/architecture/pipeline.md` | Stage order, contracts, execution model | PARTIALLY_CURRENT | Likely lists 5-stage order; needs expansion to 11 actual stages | `docs/architecture/operational_data_flow.md` + per-stage `docs/stages/*.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/architecture/module-map.md` | Directory ownership | PARTIALLY_CURRENT | Lists old top-level dirs `run/`, `collectors/`, `features/`, `analytics/`, `execution/`, `publishers/`; canonical code lives under `src/ai_trading_system/domains/` | `docs/architecture/target_architecture.md` + `docs/domains/*.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/architecture/data-model.md` | Storage layout, DuckDB files, tables | PARTIALLY_CURRENT | Lists 4 stores (ohlcv, control_plane, execution, masterdata); truth map confirms ohlcv + control_plane; **no separate `data/execution.duckdb` was confirmed** (execution tables live in control_plane); research stores need adding | `docs/architecture/storage_and_lineage.md` + `docs/reference/database_schema.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/architecture/pattern-scan.md` | Pattern scan sidecar in rank stage | CURRENT | None major; `domains/ranking/patterns/` exists | `docs/reference/breakout_and_patterns.md` + `docs/stages/rank.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/architecture/strategy-optimizer.md` | Optuna rule-pack search | CURRENT | None major; `research/optimization/` exists | `docs/domains/optimization_domain.md` | MIGRATE_CONTENT_THEN_ARCHIVE |

---

## docs/operations/

| Path | Purpose | Classification | Key stale references | Replacement target | Action |
|---|---|---|---|---|---|
| `docs/operations/installation.md` | Setup, venv, masterdata bootstrap | CURRENT | None major | `docs/runbooks/deployment_mac_mini.md` + `docs/reference/configuration.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/operations/configuration.md` | CLI flags, env vars | PARTIALLY_CURRENT | Verify all env var names against truth map (Dhan, Telegram, Google, OPENROUTER_KEY, DATA_DOMAIN, RISK_PROFILE, LLM_BRAIN_CONFIG, ALERT_TELEGRAM_MIN_SEVERITY) | `docs/reference/configuration.md` + `docs/reference/environment_variables.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/operations/runbook.md` | Operator checklist + commands | CURRENT | None major | `docs/runbooks/daily_operations.md` + `docs/runbooks/weekly_operations.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/operations/troubleshooting.md` | Issue-driven recovery | CURRENT | None major | `docs/runbooks/troubleshooting.md` + `docs/runbooks/dq_failure_response.md` + `docs/runbooks/publish_retry.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/operations/market_intel_runner.md` | Always-on market_intel collector | CURRENT | None major (truth map confirms `integrations/market_intel_client.py`) | `docs/domains/catalyst_intelligence_domain.md` + `docs/runbooks/...` | MIGRATE_CONTENT_THEN_ARCHIVE |

---

## docs/interfaces/

| Path | Purpose | Classification | Key stale references | Replacement target | Action |
|---|---|---|---|---|---|
| `docs/interfaces/api.md` | FastAPI operator backend routes | PARTIALLY_CURRENT | Truth map enumerates 14 router modules — verify documented routes cover all (health, pipeline, runs, snapshots, artifacts, stocks, ranking_detail, fundamentals, insight, sectors, tasks, processes, backtest, perf_tracker) | `docs/reference/api_reference.md` + `docs/architecture/ui_architecture.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/interfaces/ui.md` | React V2 console + FastAPI startup | CURRENT | None major; confirms Streamlit removed | `docs/architecture/ui_architecture.md` + `docs/domains/ui_domain.md` | MIGRATE_CONTENT_THEN_ARCHIVE |

---

## docs/reference/

| Path | Purpose | Classification | Key stale references | Replacement target | Action |
|---|---|---|---|---|---|
| `docs/reference/commands.md` | Runnable CLI commands | PARTIALLY_CURRENT | Verify against `pyproject.toml [project.scripts]`: `ai-trading-pipeline`, `ai-trading-daily`, `ai-trading-publish-test`, `ai-trading-execution-api`, `ai-trading-healthcheck`, `ai-trading-bootstrap-data`, `ai-trading-repair-ingest-schema`, `ai-trading-daily-gainers-report`, `ai-trading-research-recipe` | `docs/reference/commands.md` (rewrite) | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/reference/artifacts.md` | Per-stage artifacts | PARTIALLY_CURRENT | Likely doesn't cover all 11 stages; missing candidates, events, insight, narrative, perf_tracker artifacts | `docs/reference/artifacts.md` (rewrite) | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/reference/glossary.md` | 15 key terms | CURRENT | None major | `docs/reference/glossary.md` (keep, expand) | KEEP |

---

## docs/refactor/

| Path | Purpose | Classification | Key stale references | Replacement target | Action |
|---|---|---|---|---|---|
| `docs/refactor/CODEX_READY_REFACTOR_FILE.md` | Refactor execution playbook | LEGACY | Historical session brief; refactor is largely done per truth map | `docs/development/legacy_cleanup_plan.md` (summary only) | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/refactor/final_architecture.md` | Post-refactor end state | PARTIALLY_CURRENT | Useful summary; may overlap with new `target_architecture.md` | `docs/architecture/target_architecture.md` | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/refactor/baseline_inventory.md` | Pre-refactor folder snapshot | LEGACY | Snapshot frozen; mentions `services/` as empty placeholder — truth map does not confirm any `services/` package today | n/a (historical only) | ARCHIVE_AS_IS |
| `docs/refactor/baseline_import_graph.md` | Pre-refactor import traces | LEGACY | Snapshot; stale | n/a | ARCHIVE_AS_IS |
| `docs/refactor/baseline_artifact_contracts.md` | Pre-refactor artifact schemas | LEGACY | Snapshot; superseded by truth map + new `reference/artifacts.md` | `docs/reference/artifacts.md` (selective migration) | MIGRATE_CONTENT_THEN_ARCHIVE |
| `docs/refactor/batch1_legacy_audit.md` | Phase 1a refactor audit | LEGACY | Batch-specific historical context | `docs/development/legacy_cleanup_plan.md` (one-line summary) | ARCHIVE_AS_IS |
| `docs/refactor/batch4_legacy_audit.md` | Phase 4 refactor audit | LEGACY | Batch-specific | `docs/development/legacy_cleanup_plan.md` | ARCHIVE_AS_IS |
| `docs/refactor/collectors_canonical_map.md` | old `collectors/` → `domains/ingest/` map | PARTIALLY_CURRENT | Truth map still notes root-level `analytics/` directory persists; this map may still be load-bearing for the migration page | `docs/development/package_migration.md` | MIGRATE_CONTENT_THEN_ARCHIVE |

---

## docs/archive/

All files in this directory are already marked as superseded. They need to be folded into the new `docs/_legacy/archived_2026-05-16/` structure during Phase 2, with a top-level README in `_legacy/` explaining the relationship.

| Path | Purpose | Classification | Action |
|---|---|---|---|
| `docs/archive/README.md` | Archive index | LEGACY | Replace with `docs/_legacy/README.md` (Phase 2) |
| `docs/archive/architecture.md` | Iceberg-lite feature storage | LEGACY | Move to `docs/_legacy/archived_2026-05-16/architecture_iceberg_lite.md` |
| `docs/archive/data-flow.md` | Historical 5-stage data flow | LEGACY | Move to `_legacy/` |
| `docs/archive/database.md` | Historical DB schema | LEGACY | Move to `_legacy/` |
| `docs/archive/dhan_ohlc_isolation_strategy.md` | Dhan-first ingest design | LEGACY | Move to `_legacy/`; reference from `package_migration.md` |
| `docs/archive/dq_rules.md` | Historical DQ semantics | LEGACY | Move to `_legacy/`; selectively migrate to `architecture/data_trust_and_dq.md` |
| `docs/archive/high_level_operational_data_flow.md` | Historical 5-stage flow | LEGACY | Move to `_legacy/` |
| `docs/archive/ohlcv_reset_reingest_runbook.md` | Historical repair runbook | LEGACY | Selectively migrate to `runbooks/data_repair.md`, then move |
| `docs/archive/ops_runbook.md` | Historical operator procedures | LEGACY | Selectively migrate to `runbooks/daily_operations.md` + `runbooks/troubleshooting.md`, then move |

---

## docs/diagrams/

| Path | Purpose | Classification | Action |
|---|---|---|---|
| `docs/diagrams/data_trust_decision_flow.svg` | Trust decision flow diagram | UNKNOWN | KEEP (verify accuracy in Phase 3) |
| `docs/diagrams/operational_data_flow.svg` | Operational flow diagram | UNKNOWN | KEEP (verify currency in Phase 3 — likely shows 5-stage flow, may need redraw for 11 stages) |

---

## web/

| Path | Purpose | Classification | Action |
|---|---|---|---|
| `web/execution-console-v2/README.md` | Vite+React setup | CURRENT | KEEP (component-local) |
| `web/execution-console-v2/ai-trading-dashboard-starter/README.md` | Component starter README | CURRENT | KEEP |
| `web/execution-console-v2/ai-trading-dashboard-starter/PROXY_ISSUES.md` | Vite proxy debugging | CURRENT | KEEP (component-local; could be linked from `docs/runbooks/troubleshooting.md`) |

These component-local READMEs stay where they are — they live next to the code they describe.

---

## Summary

| Action | Count | Notes |
|---|---|---|
| KEEP | 6 | Component READMEs (3), `glossary.md`, `DOCS_STANDARD.md`, root `README.md` (light rewrite) |
| MIGRATE_CONTENT_THEN_ARCHIVE | 25 | Most current/partially-current docs; useful content folds into new structure first |
| ARCHIVE_AS_IS | 12 | All `docs/archive/*` (9) + clearly-stale `docs/refactor/baseline_*` and `batch*_legacy_audit` (3) |
| DELETE_CANDIDATE | 2 | `AGENTS.md` + `claude.md` duplication — keep one (operator chooses) |
| **Total** | **45** | (Plus 2 SVG diagrams kept) |

---

## Duplicate clusters

1. **Root LLM guidelines** — `AGENTS.md` ≡ `claude.md` (byte-identical).
2. **Pipeline/data flow** — `docs/architecture/pipeline.md` + `docs/architecture/system-overview.md` + `docs/archive/data-flow.md` + `docs/archive/high_level_operational_data_flow.md` all describe the staged flow at different fidelities. New structure consolidates into `docs/architecture/operational_data_flow.md` + per-stage docs.
3. **Module/repo layout** — `docs/architecture/module-map.md` + `docs/AS_IS_DESIGN.md` + `docs/refactor/final_architecture.md` overlap. New `docs/architecture/target_architecture.md` consolidates.
4. **DQ semantics** — `docs/archive/dq_rules.md` + DQ sections inside `pipeline.md`. New `docs/architecture/data_trust_and_dq.md` consolidates.
5. **Database schema** — `docs/architecture/data-model.md` + `docs/archive/database.md`. New `docs/reference/database_schema.md` (factual table list) + `docs/architecture/storage_and_lineage.md` (conceptual) consolidate.
6. **Ops runbook** — `docs/operations/runbook.md` + `docs/operations/troubleshooting.md` + `docs/archive/ops_runbook.md` + `docs/archive/ohlcv_reset_reingest_runbook.md`. New `docs/runbooks/*` set consolidates.

---

## Gaps (no existing doc covers these — new docs must be written from code)

The new structure mandates several docs that have **no current source to migrate from**. These must be written fresh against the code in Phases 4–9:

- `docs/stages/candidates.md` — *not in spec but pipeline has this stage* (truth map line: `PIPELINE_ORDER` includes `candidates`). Spec only lists 5 stage docs — we should flag this to operator.
- `docs/stages/events.md`, `insight.md`, `narrative.md`, `perf_tracker.md` — same problem; spec only covers ingest/features/rank/execute/publish.
- `docs/domains/fundamentals_domain.md`, `catalyst_intelligence_domain.md`, `optimization_domain.md` — spec frames these as "planned" but truth map shows fundamentals + catalysts + optimization code already exists. The new docs need to describe what's *built*, not just what's planned.
- `docs/runbooks/dq_failure_response.md`, `publish_retry.md`, `backup_and_restore.md`, `deployment_mac_mini.md` — no precursor.
- `docs/development/*` series, `docs/decisions/ADR-*` — entirely new.
- `docs/reference/environment_variables.md`, `database_schema.md`, `publish_contracts.md`, `execution_policy.md` — partial precursors in `configuration.md` and `risk_engine_runbook.md`.

## Open questions for the operator (resolve before Phase 1)

1. **Stage count discrepancy.** The spec lists 5 stage docs but the actual `PIPELINE_ORDER` has 11 stages. Add new stage docs (`candidates.md`, `events.md`, `insight.md`, `narrative.md`, `perf_tracker.md`, `fundamentals.md`)? Or fold them into the existing 5?
2. **AGENTS.md vs claude.md.** Which one should survive?
3. **`docs/operations/installation.md`** — keep as-is or fold into `docs/runbooks/deployment_mac_mini.md`?
4. **Root-level `analytics/` directory** — still present in repo. Confirm with operator whether it is legacy-to-be-deleted (informs whether new docs should describe it).
