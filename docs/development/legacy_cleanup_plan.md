# Legacy Cleanup Plan

- **Purpose:** Track what was archived in the 2026-05-16 docs cleanup, what was replaced, and what remains.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:**
  - [`docs/_audit/documentation_inventory.md`](../_audit/documentation_inventory.md)
  - [`docs/_audit/stale_reference_report.md`](../_audit/stale_reference_report.md)
  - [`docs/_legacy/README.md`](../_legacy/README.md)

---

## Cleanup wave 2026-05-16

A full audit + restructure happened on 2026-05-16 (this branch). The destination structure is:

```
docs/
  architecture/   stages/   domains/   reference/
  runbooks/       development/         decisions/
  _audit/         _legacy/
```

with `INDEX.md` and `README.md` at the top.

### What was archived

See [`docs/_legacy/README.md`](../_legacy/README.md) for the full inventory. Summary:

- **Wave 1 (safe — content already redundant):** 9 files from `docs/archive/` (already-archived) + 4 frozen refactor baselines from `docs/refactor/` (13 total).
- **Wave 2 (after content migrated):** 5 docs — `AS_IS_DESIGN.md`, `architecture/{pipeline,system-overview,module-map}.md`, `research_backtesting.md`.
- **Wave 3 (after content migrated):** 13 docs — all `operations/*` (5), all `interfaces/*` (2), `architecture/{data-model,pattern-scan,strategy-optimizer}.md` (3), `fundamental_layer.md`, `risk_engine_runbook.md`.
- **Wave 4 (final):** `EXECUTION_CONSOLE_PLAN.md` + 4 remaining `docs/refactor/*` files after ADRs landed.

Total archived: ~36 files. Net change: many more docs (~50 new), but each is shorter, focused, and code-verified.

### What was deleted

- `AGENTS.md` — was a slightly older truncated copy of `claude.md`. Kept `claude.md`.

### Stale references resolved

See [`docs/_audit/stale_reference_report.md`](../_audit/stale_reference_report.md) for the full list. Highlights:

| Old claim | Reality | Resolved in |
|---|---|---|
| `5-stage pipeline` (was wrong; legacy claim) | 11 stages (`pipeline/orchestrator.py:41`) | `docs/architecture/operational_data_flow.md` + per-stage docs |
| `Dhan-first` ingest (was wrong; legacy claim) | NSE bhavcopy is source-of-record | `docs/reference/data_sources.md` |
| Streamlit operator UI | React V2 + FastAPI; no Streamlit in active paths | `docs/architecture/ui_architecture.md` |
| Old top-level module paths (`collectors/`, `run/`, `core/`) | Lives under `src/ai_trading_system/domains/` etc. | [`package_migration.md`](package_migration.md) |
| `data/execution.duckdb` doesn't exist (Phase 0 truth-map error) | It does exist (`execution/store.py:29`) | Retracted in `docs/_audit/stale_reference_report.md` item D; corrected in storage docs |
| `data/market_intel.duckdb` not mentioned | It exists (`integrations/market_intel_client.py:32`) | Added to `docs/architecture/storage_and_lineage.md` + `target_architecture.md` |

### Code-level legacy still to clean up

Tracked in [`package_migration.md`](package_migration.md):

1. Root-level `analytics/` — 7 orphaned modules to delete + 1 live legacy (`stage_gate_backtest.py`) to migrate.
2. `yfinance` provider hardcodes `data/masterdata.db` — should use `platform/db/paths.py`.
3. `audit_rank.py` — fold into a CLI or accept as standalone.

## Open documentation gaps

Items the cleanup explicitly did not resolve and where to fix them:

- **`models/` (root directory)** — purpose not documented. Verify and either document in `target_architecture.md` or remove.
- **`interfaces/api/`** — described as "mostly empty"; verify and either describe in `domains/ui_domain.md` or remove the package.
- **Live trading guardrails** — the live Dhan adapter is hard-disabled (`adapters/dhan.py:62-65`); production-ready guardrails (kill switch, broker reconciliation) are not implemented. Tracked in `docs/reference/execution_policy.md`.
- **Stop-loss / trailing-stop implementation** — partial; see findings in `docs/reference/execution_policy.md`.
- **Risk profile YAML parser** — silently drops unknown keys (`config.py:82-86`). Flagged in execution_policy.md, but no code fix yet.
- **Some artifact filenames** historically referenced as `breakout_signals.csv` / `pattern_signals.csv` — actual filenames are `breakout_scan.csv` / `pattern_scan.csv` (per `service.py::TASK_FILE_MAP`). Fixed in `docs/reference/artifacts.md`.

## Recommended next steps

1. Run `python scripts/check_docs.py` (Phase 10 deliverable) and fix any reported issues.
2. Delete the 7 orphaned root `analytics/` modules after a dynamic-import grep.
3. Migrate `stage_gate_backtest.py` to `domains/ranking/` and update the 2 consumer files.
4. Confirm `models/` purpose and either document or remove.
5. Add `EXECUTION_API_KEY` to `.env.example` if one is added.
6. Set up CI to run `scripts/check_docs.py` on every docs PR.

## See also

- [`package_migration.md`](package_migration.md)
- [`docs/_audit/documentation_cleanup_report.md`](../_audit/documentation_cleanup_report.md) (Phase 11 final report)
