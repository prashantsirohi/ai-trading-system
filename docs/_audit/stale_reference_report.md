# Stale Reference Report

- **Purpose:** Concrete list of stale claims/references found in current docs, cross-checked against the code truth map. Each item names the offending doc, the stale statement, the real code state, and the action needed.
- **Audience:** Phase 1–9 doc authors.
- **Last verified:** 2026-05-16
- **Source of truth:** `current_code_truth_map.md` (this folder) is the ground truth; this file lists where docs diverge from it.
- **Status:** Phase 0 deliverable.

> If you are about to migrate content from a doc listed below into the new structure, fix the stale claim **before** migrating it. Do not propagate.

---


> **Note for `scripts/check_docs.py`:** This file intentionally lists stale terms (the whole point of an audit). Forbidden-term checks below this banner are not enforced — the report itself is the documentation of the stale claims being retired.

## A. Pipeline shape — "5 stages" vs reality (11 stages)

**Where it appears:**
- `docs/architecture/system-overview.md` — calls out a 5-stage pipeline
- `docs/architecture/pipeline.md` — describes ingest→features→rank→execute→publish
- `docs/archive/data-flow.md`, `docs/archive/high_level_operational_data_flow.md` — both legacy 5-stage diagrams
- `docs/diagrams/operational_data_flow.svg` — likely 5-stage (verify by opening)
- `README.md` — references the 5-stage flow
- Task spec itself (in §0 of the user instruction) also assumes 5 stages

**Reality (truth map §2):** `pipeline/orchestrator.py:41` `PIPELINE_ORDER` =
```
ingest → features → rank → fundamentals → candidates → events → execute → insight → narrative → publish → perf_tracker
```
The `ai-trading-daily` legacy wrapper still uses the 5-stage path; the canonical `ai-trading-pipeline` runs all 11.

**Action:** New docs must describe both:
- the **full 11-stage** `ai-trading-pipeline` flow as canonical, and
- the **5-stage** `ai-trading-daily` legacy wrapper as a documented subset.

This also affects which stage docs to create — the spec only lists 5 (`docs/stages/{ingest,features,rank,execute,publish}.md`). Six additional stage docs (`fundamentals.md`, `candidates.md`, `events.md`, `insight.md`, `narrative.md`, `perf_tracker.md`) are needed. Operator decision pending.

---

## B. "Dhan as primary ingest source" — false

**Where it appears:**
- `docs/archive/dhan_ohlc_isolation_strategy.md` — already archived, but content describes Dhan-first
- Possibly older references in `docs/AS_IS_DESIGN.md` and `docs/operations/configuration.md`

**Reality:** `domains/ingest/providers/nse.py` is the source-of-record; Dhan is fallback for OHLC and mandatory for live execution. yfinance is last-resort.

**Action:** Already archived in `docs/archive/`; ensure no surviving doc repeats the claim. New `docs/stages/ingest.md` and `docs/reference/data_sources.md` must state NSE primary explicitly.

---

## C. "Streamlit is the operator UI" — false

**Where it appears:** No surviving doc was found to assert this — `docs/interfaces/ui.md` already documents React V2 + FastAPI. The archive (`docs/archive/`) may contain residual references. Some old refactor docs (`docs/refactor/baseline_*`, `CODEX_READY_REFACTOR_FILE.md`) may reference Streamlit-era assumptions.

**Reality (truth map §6):** No Streamlit usages in active code paths. React (`web/execution-console-v2/`) + FastAPI (`ai-trading-execution-api`) is the operator UI.

**Action:** Confirm by grepping for `streamlit` across `src/` before writing `docs/architecture/ui_architecture.md`. If any active usage remains (e.g., research/admin tools), document it as research-only.

---

## D. "Separate `data/execution.duckdb`" — ~~likely false~~ **CORRECT — Phase 0 was wrong**

**RETRACTED 2026-05-16 (Phase 6):** This item was based on a faulty Phase 0 truth map. `data/execution.duckdb` does exist (`domains/execution/store.py:29` defaults to it). The original `docs/architecture/data-model.md` claim was right. Item is left here for traceability; no doc cleanup is needed because the four-stores claim in `data-model.md` was accurate.

## D-original (kept for traceability)

**Where it appears:**
- `docs/architecture/data-model.md` — names four stores: ohlcv, control_plane, execution, masterdata
- `docs/archive/database.md` — same legacy claim

**Reality (truth map §4):** Only `data/ohlcv.duckdb` and `data/control_plane.duckdb` were confirmed in operational paths. Execution tables (`execution_order`, `execution_fill`) live in `control_plane.duckdb`. Research uses its own DuckDB. No file named `data/execution.duckdb` was found by the truth-map scan.

**Action:** Re-grep code for `execution.duckdb` to confirm before rewriting `docs/architecture/storage_and_lineage.md` and `docs/reference/database_schema.md`. If absent, remove the claim everywhere it appears.

---

## E. Old top-level module paths (`run/`, `collectors/`, `core/`, top-level `analytics/`, `publishers/`, `features/`, `execution/`)

**Where it appears:**
- `docs/architecture/module-map.md` — describes layout with these top-level dirs
- `docs/refactor/collectors_canonical_map.md` — explicitly maps `collectors/` → `domains/ingest/`
- Various refactor docs (`baseline_*`, `batch*_legacy_audit`)

**Reality (truth map §1):**
- Canonical code lives at `src/ai_trading_system/domains/{ingest,features,ranking,...}/`
- Root-level `analytics/` directory **still exists** as legacy
- No root-level `run/`, `collectors/`, `core/`, `publishers/`, top-level `features/`, top-level `execution/` were observed
- Tests may still import root-level `analytics/`

**Action:**
- New `docs/development/package_migration.md` lists the old→new mapping (sourced from `collectors_canonical_map.md`).
- `docs/architecture/module-map.md` content is mostly obsolete — migrate the small portion that still applies into `docs/architecture/target_architecture.md`, then archive.
- Verify whether root-level `analytics/` should be documented as "legacy, to delete" or "still load-bearing for tests".

---

## F. `services/` package — frozen claim

**Where it appears:** `docs/refactor/baseline_inventory.md` describes `services/` as "no Python implementation files — all subdirectory `__pycache__` only".

**Reality:** Truth map did not enumerate any `services/` package under `src/ai_trading_system/`. Status is **unknown** — verify by direct `find`.

**Action:** Re-check the filesystem; if `services/` is gone, archive `baseline_inventory.md` as a frozen historical snapshot with a banner saying so.

---

## G. Commands — verify each before publishing `docs/reference/commands.md`

**Where it appears:** `docs/reference/commands.md`, `docs/operations/runbook.md`, `docs/operations/installation.md` all reference commands.

**Reality (truth map §2):** Authoritative command list is `pyproject.toml [project.scripts]`:
- `ai-trading-pipeline`
- `ai-trading-daily`
- `ai-trading-publish-test`
- `ai-trading-execution-api`
- `ai-trading-healthcheck`
- `ai-trading-bootstrap-data`
- `ai-trading-repair-ingest-schema`
- `ai-trading-daily-gainers-report`
- `ai-trading-research-recipe`

**Action:** For every `python -m ai_trading_system.*` invocation in current docs, verify the module is still importable. The `scripts/check_docs.py` work in Phase 10 should automate this.

---

## H. Env var names — verify against grep

**Where it appears:** `docs/operations/configuration.md`, scattered references in runbooks and `risk_engine_runbook.md`.

**Reality (truth map §9):** Confirmed env vars (cite source modules):
- Dhan: `DHAN_API_KEY`, `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, `DHAN_REFRESH_TOKEN`, `DHAN_PIN`, `DHAN_TOTP`, `DHAN_TOKEN_EXPIRY`
- Telegram: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_CONNECT_TIMEOUT_SECONDS`, `TELEGRAM_READ_TIMEOUT_SECONDS`, `TELEGRAM_WRITE_TIMEOUT_SECONDS`, `TELEGRAM_SEND_ATTEMPTS`
- Google: `GOOGLE_SPREADSHEET_ID`, `GOOGLE_SHEETS_CREDENTIALS` (deprecated), `GOOGLE_TOKEN_PATH`
- Alerts/Risk: `ALERT_TELEGRAM_MIN_SEVERITY`, `RISK_PROFILE`
- LLM: `LLM_BRAIN_CONFIG`, `OPENROUTER_KEY` / `OPENROUTER_API_KEY`
- Platform: `DATA_DOMAIN`, `ENV`, `MPLCONFIGDIR`

**Suspicious / unverified mentioned in old docs (need confirmation):**
- `EXECUTION_MODE` — truth map says **inferred**, not an env var. Old docs may state otherwise.

**Action:** New `docs/reference/environment_variables.md` must be sourced directly from a grep of `os.environ`/`os.getenv`/`getenv`/pydantic `Settings(` in `src/`. Do not copy from old configuration.md without re-verification.

---

## I. AS_IS_DESIGN commit pin

**Where it appears:** `docs/AS_IS_DESIGN.md` is pinned to commit `4dfc618` on branch `claude/xenodochial-payne-53735e`.

**Reality:** Current HEAD is on `claude/optimistic-dijkstra-b7054f` at commit `6a55562`. The pin is stale.

**Action:** Migrate the useful Revision-2 corrections (`src/ai_trading_system/` is canonical) into `docs/architecture/target_architecture.md` with today's verification date. Archive AS_IS_DESIGN.

---

## J. Duplicate root-level files

**`AGENTS.md` ≡ `claude.md`** — byte-identical.

**Action:** Operator decision needed: keep one. Both serve the same audience (LLM behavioral guidelines). Recommendation: keep `AGENTS.md` (becoming a community convention) and delete `claude.md`, **or** vice versa. Either way, after the new docs structure is in place, content should be referenced from a single canonical doc under `docs/development/`.

---

## K. Stage 5-doc spec mismatch

**Where it appears:** Task spec §4 requires exactly 5 stage docs.

**Reality (truth map §3):** 11 stages exist. Six additional stage docs needed (or the existing 5 must clearly cross-reference the additional stages with sub-sections).

**Action:** Resolve in Phase 1 (structure decision). Recommended: add 6 more `docs/stages/*.md`, one per stage. Update the spec's INDEX accordingly.

---

## L. Live trading "production-ready" framing

**Where it appears:** No specific doc *claims* live trading is production-ready, but `risk_engine_runbook.md` and references to the Dhan adapter may imply it.

**Reality (truth map §8):** Live Dhan adapter exists, but order types are hardcoded MARKET+INTRADAY and the kill-switch / sandbox guardrails have not been audited.

**Action:** Per spec STRICT RULES, every new doc that touches execution must state "Current code status: live trading is supported by code path but production-readiness guardrails have not been verified — paper mode is the safe default." until proven otherwise.

---

## M. Status claims with no evidence

Several refactor docs (`EXECUTION_CONSOLE_PLAN.md` Phase 2a/2b, `docs/refactor/CODEX_READY_REFACTOR_FILE.md` phase markers) include "Phase N shipped/in flight" status lines. These rot quickly.

**Action:** Strip phase-status language from migrated content. The new `docs/development/legacy_cleanup_plan.md` should record completion state by *what code exists today*, not by claimed phase numbers.

---

## Summary table

| # | Stale claim | Severity | Affected docs (count) | Resolution |
|---|---|---|---|---|
| A | 5-stage pipeline | High | 5+ | Document 11-stage flow; add 6 stage docs |
| B | Dhan primary ingest | Already-archived | 1 | Ensure no resurrection |
| C | Streamlit operator UI | Already-corrected | 0 active | Confirm by grep |
| D | Separate execution.duckdb | High | 2 | Re-grep; remove claim |
| E | Old top-level module paths | High | 5+ | Migrate to package_migration + target_architecture, archive rest |
| F | services/ empty package | Low | 1 | Verify FS; archive baseline |
| G | Stale commands | Medium | 3 | Source from pyproject.toml |
| H | Env var drift | Medium | 2 | Source from code grep |
| I | AS_IS_DESIGN commit pin | Low | 1 | Migrate corrections, archive |
| J | AGENTS.md / claude.md dup | Trivial | 2 | Operator picks one |
| K | 5-stage doc spec | Medium | n/a (spec) | Add 6 stage docs |
| L | Live trading framing | High | n/a (forward-looking) | Bake disclaimer into all new execution docs |
| M | Phase-status rot | Low | 2 | Strip from migrated content |

---

## Outputs that depend on resolving these before doc writing

- `docs/architecture/operational_data_flow.md` — needs item A resolved.
- `docs/stages/*.md` — needs items A, K resolved.
- `docs/reference/database_schema.md` — needs item D resolved.
- `docs/reference/environment_variables.md` — needs item H resolved.
- `docs/reference/commands.md` — needs item G resolved.
- `docs/architecture/target_architecture.md` — needs items E, I resolved.
- `docs/runbooks/*` — need item L disclaimer template ready.
