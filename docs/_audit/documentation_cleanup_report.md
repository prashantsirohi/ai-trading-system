# Documentation Cleanup Report

- **Purpose:** Final report for the 2026-05-16 documentation overhaul. Summarizes what was kept, rewritten, archived, deleted, and what gaps remain.
- **Audience:** Operator, future-you, future agents.
- **Last verified:** 2026-05-16
- **Source of truth:** `docs/_audit/{documentation_inventory,stale_reference_report,current_code_truth_map}.md` + the git history of branch `claude/optimistic-dijkstra-b7054f`.

---

## TL;DR

- Before: 46 markdown docs, fragmented across `docs/`, `docs/archive/`, `docs/refactor/`, `docs/operations/`, `docs/interfaces/`, root. Mixed currency, conflicting claims, references to module paths that no longer exist.
- After: **70 current docs** in a flat, predictable structure (`docs/{architecture,stages,domains,reference,runbooks,development,decisions,_audit}/`); **36 archived** under `docs/_legacy/archived_2026-05-16/` with git history preserved; **1 deleted** (`AGENTS.md`, duplicate); `scripts/check_docs.py` validates every current doc.

```
docs/
├── README.md           — landing page
├── INDEX.md            — complete doc map
├── DOCS_STANDARD.md    — doc writing rules
├── architecture/   (6)
├── stages/         (11)
├── domains/        (11)
├── reference/      (12)
├── runbooks/       (8)
├── development/    (10)
├── decisions/      (5)
├── _audit/         (4)
└── _legacy/        (36 — archived 2026-05-16)
```

`python scripts/check_docs.py` → **OK — validated 70 current docs, no issues.**

## Files

### Kept (in place, content updated)
- `docs/README.md` — full rewrite (now landing page with frontmatter, paths for operator/developer/debug/extension audiences)
- `docs/DOCS_STANDARD.md` — added frontmatter
- `docs/reference/commands.md` — added frontmatter + 2 missing CLI aliases
- `docs/reference/artifacts.md` — added frontmatter + 6 missing stages (candidates, fundamentals, events, insight, narrative, perf_tracker)
- `docs/reference/glossary.md` — added frontmatter
- `web/execution-console-v2/README.md`, `.../ai-trading-dashboard-starter/README.md`, `.../PROXY_ISSUES.md` — component-local READMEs kept where they are
- root `README.md`, `claude.md` — kept

### Rewritten (new content, verified against code)
- 6 architecture docs (`overview`, `operational_data_flow`, `storage_and_lineage`, `data_trust_and_dq`, `ui_architecture`, `target_architecture`)
- 11 stage docs (one per stage in the actual 11-stage `PIPELINE_ORDER`)
- 11 domain docs (one per domain under `src/ai_trading_system/domains/` + UI, platform, research)
- 9 new reference docs (`configuration`, `environment_variables`, `api_reference`, `database_schema`, `data_sources`, `ranking_factors`, `breakout_and_patterns`, `execution_policy`, `publish_contracts`)
- 8 runbooks
- 10 development docs
- 5 ADRs
- `docs/INDEX.md`

### Archived (moved to `docs/_legacy/archived_2026-05-16/`, git mv preserves history)
36 files in 4 waves. See [`docs/_legacy/README.md`](../_legacy/README.md) for the per-file old→new mapping. Highlights:

- Wave 1 (Phase 2 — safe): 9 docs from old `docs/archive/`, 4 frozen refactor baselines.
- Wave 2 (Phase 3–5 — after migration): `AS_IS_DESIGN.md`, `architecture/{pipeline,system-overview,module-map}.md`, `research_backtesting.md`.
- Wave 3 (Phase 6–7 — after migration): all `docs/operations/*` (5), all `docs/interfaces/*` (2), `architecture/{data-model,pattern-scan,strategy-optimizer}.md` (3), `fundamental_layer.md`, `risk_engine_runbook.md`.
- Wave 4 (Phase 11 — final): `EXECUTION_CONSOLE_PLAN.md`, 4 remaining `docs/refactor/*` files.

### Deleted
- `AGENTS.md` (root) — was a slightly truncated older copy of `claude.md`. Kept `claude.md`.

## Stale references resolved

Full list in [`stale_reference_report.md`](stale_reference_report.md). Highlights:

| Stale claim | Reality | Fixed in |
|---|---|---|
| 5-stage pipeline | 11 stages (`pipeline/orchestrator.py:41`) | `docs/architecture/operational_data_flow.md` + 11 stage docs |
| Dhan-first ingest | NSE bhavcopy is source-of-record | `docs/reference/data_sources.md` + `stages/ingest.md` |
| Streamlit operator UI | React V2 + FastAPI; **no Streamlit usage in active code paths** | `docs/architecture/ui_architecture.md` + `domains/ui_domain.md` |
| Old top-level module paths (`collectors/`, `run/`, `core/`, `ui.execution.app`) | Lives under `src/ai_trading_system/domains/` etc. | `docs/development/package_migration.md` |
| `data/execution.duckdb` doesn't exist (Phase 0 truth-map mistake) | It does exist (`execution/store.py:29`) | Corrected mid-cleanup; full storage map in `docs/architecture/storage_and_lineage.md` |
| `data/market_intel.duckdb` not mentioned | It exists (`integrations/market_intel_client.py:32`) | Added to `storage_and_lineage.md` + `target_architecture.md` |
| Artifact names `breakout_signals.csv` / `pattern_signals.csv` | Actual files are `breakout_scan.csv` / `pattern_scan.csv` | `docs/reference/artifacts.md` |
| `services/` package described as empty placeholder | Package no longer exists; the description was a frozen pre-refactor snapshot | `baseline_inventory.md` archived |
| EXECUTION_CONSOLE_PLAN phase-status language | Implementation is complete; phase markers stripped | `decisions/ADR-0005-react-operator-workspace.md` |
| AS_IS_DESIGN commit pin to a different branch | Frozen historical snapshot | Migrated to `architecture/target_architecture.md`, then archived |

## Surprises found during the cleanup

These were uncovered by direct code reading during Phases 4–6:

1. **Live Dhan execution is hard-disabled at the adapter level** — `adapters/dhan.py:62-65` raises `RuntimeError` unless `dry_run=True`. No env-var bypass; the guard must be edited out of source to enable live trading. Reflected in `docs/reference/execution_policy.md` and `stages/execute.md`.
2. **No real trailing stops** — `domains/risk/` has no code that ratchets `stop_price` upward. DMA-exit windows approximate trailing behavior.
3. **Risk profile YAML silently drops unknown keys** (`config.py:82-86`) — typos in profiles fail silently. Footgun documented in `execution_policy.md`.
4. **`yfinance` provider hardcodes `data/masterdata.db`** instead of using `platform/db/paths.py`. Minor inconsistency flagged in `data_sources.md` and `development/package_migration.md`.
5. **`domains/catalysts/collector.py` does not scrape NSE** — only filters frames to pick a catalyst universe. Corporate-action data actually flows in via the read-only `data/market_intel.duckdb`. Documented in `domains/catalyst_intelligence_domain.md`.
6. **Root-level `analytics/` has 8 modules but only `stage_gate_backtest.py` is still imported** (1 test, 1 script). The other 7 modules are orphaned. Plan: delete the 7 after a dynamic-import grep; either migrate the live one or accept it as legacy-forever. Tracked in `development/legacy_cleanup_plan.md`.
7. **`EXECUTION_API_KEY` env var** (read by `routes/_deps.py:31`) was missing from the Phase 0 truth map. Added.

## Doc validation in CI

[`scripts/check_docs.py`](../../scripts/check_docs.py) enforces:

1. No broken relative links in current docs.
2. Every current doc has the 4 required frontmatter fields.
3. No stub-status markers (the literal token `Status` followed by `:` followed by `STUB`) outside `_legacy/`.
4. No forbidden stale terms in current docs (with allowlist for disclaimer paragraphs and for the audit directory itself).
5. Required sections present in every `stages/*.md` and `domains/*.md`.
6. Every `python -m ai_trading_system.<mod>` invocation references a real module.

Current status: **70 docs, 0 issues.**

Recommended next step: wire `python scripts/check_docs.py` into CI as a required check on every docs PR.

## Unresolved gaps

These are flagged in current docs and tracked in `docs/development/legacy_cleanup_plan.md`:

1. **`models/` (root directory)** — purpose not documented. Verify and either describe in `target_architecture.md` or remove.
2. **`src/ai_trading_system/interfaces/api/`** — described as "mostly empty". Verify and either describe in `domains/ui_domain.md` or remove the package.
3. **Live trading production-readiness** — no audit of broker reconciliation, kill switch, position caps against broker state. Documented as a hard disclaimer everywhere `execute` is mentioned.
4. **Stop-loss / trailing-stop implementation** — partial (DMA-exit only). Tracked in `execution_policy.md`.
5. **Catalyst storage table name in DuckDB** — `domains/catalyst_intelligence_domain.md` says "verify exact table when writing `reference/database_schema.md`"; `database_schema.md` lists tables it found but should be re-verified.

## Recommended next steps

1. Wire `python scripts/check_docs.py` into CI (run on every PR touching `docs/` or `scripts/check_docs.py`).
2. Delete the 7 orphaned root `analytics/` modules after a dynamic-import grep.
3. Migrate `analytics/stage_gate_backtest.py` to `src/ai_trading_system/domains/ranking/` (the 2 consumers are easy to update).
4. Confirm `models/` purpose and either document or remove.
5. Audit live-trading guardrails before any thought of enabling live execution; until then, the disclaimer in `execution_policy.md` is load-bearing.
6. Treat the cleanup as v1; expect iteration. The next PR that changes code in `pipeline/`, `domains/execute/`, or any artifact schema should also update the relevant doc and bump its `Last verified` date.

## See also

- [`documentation_inventory.md`](documentation_inventory.md) — Phase 0 inventory
- [`stale_reference_report.md`](stale_reference_report.md) — Phase 0 stale-claim list (with Phase 6 retraction on item D)
- [`current_code_truth_map.md`](current_code_truth_map.md) — Phase 0 truth map (with Phase 6 correction notice on §4)
- [`docs/_legacy/README.md`](../_legacy/README.md) — per-file old→new mapping
- [`docs/development/legacy_cleanup_plan.md`](../development/legacy_cleanup_plan.md) — code-side cleanup follow-ups
