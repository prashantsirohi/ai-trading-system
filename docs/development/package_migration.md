# Package Migration

- **Purpose:** Old import paths → new domain layout. Status of the migration from top-level `collectors/`, `analytics/`, `run/`, `core/` etc. to `src/ai_trading_system/`.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** Current `src/ai_trading_system/` layout + `docs/_legacy/archived_2026-05-16/refactor/collectors_canonical_map.md` (the legacy mapping document).

---

## Status (2026-05-16)

The canonical layer is `src/ai_trading_system/`. The migration from top-level legacy modules to `src/ai_trading_system/domains/` is **largely complete**. Most legacy top-level paths (`collectors/`, `run/`, `core/`, `features/`, `execution/`, `publishers/`) have been removed; thin compatibility shims at legacy paths have been retired through a series of refactor batches (see [`legacy_cleanup_plan.md`](legacy_cleanup_plan.md)).

Remaining legacy lives outside `src/`:

- root `analytics/` — partial legacy, see below
- `audit_rank.py` (root) — standalone operator script
- `scripts/`, `tools/` — intentionally outside the package

## Old → new mapping

### Ingest (was `collectors/*`)

| Old | New |
|---|---|
| `collectors.masterdata` | `ai_trading_system.domains.ingest.masterdata` |
| `collectors.nse_collector` | `ai_trading_system.domains.ingest.providers.nse` |
| `collectors.yfinance_collector` | `ai_trading_system.domains.ingest.providers.yfinance` |
| `collectors.dhan_collector` | `ai_trading_system.domains.ingest.providers.dhan` |
| `collectors.delivery_collector` | `ai_trading_system.domains.ingest.delivery` |
| `collectors.ingest_validation` | `ai_trading_system.domains.ingest.validation` |
| `collectors.daily_update_runner` | `ai_trading_system.domains.ingest.daily_update_runner` |
| `collectors.reset_reingest_validate` | `ai_trading_system.domains.ingest.reset_reingest_validate` |
| `collectors.ingest_full` | `ai_trading_system.domains.ingest.ingest_full` |
| `collectors.index_backfill` | `ai_trading_system.domains.ingest.index_backfill` |
| `collectors.stock_backfill` | `ai_trading_system.domains.ingest.stock_backfill` |
| `collectors.token_manager` | `ai_trading_system.domains.ingest.token_manager` |
| `collectors.nse_delivery_scraper` | `ai_trading_system.domains.ingest.nse_delivery_scraper` |
| `collectors.repair_ohlcv_window` | `ai_trading_system.domains.ingest.repair` |
| `collectors.archive_nse_bhavcopy` | `ai_trading_system.domains.ingest.archive_nse_bhavcopy` |
| `collectors.delete_stale` | `ai_trading_system.domains.ingest.delete_stale` |
| `collectors.dhan_ohlc_diagnostics` | `ai_trading_system.domains.ingest.dhan_ohlc_diagnostics` |
| `collectors.test_marketfeed_ohlc` | `ai_trading_system.domains.ingest.test_marketfeed_ohlc` |
| `collectors.zerodha_sector_collector` | `ai_trading_system.domains.ingest.zerodha_sector_collector` |
| `collectors.auth_doctor` | `ai_trading_system.domains.ingest.auth_doctor` |

### Features (was `collectors.compute_features_batch` + scattered)

| Old | New |
|---|---|
| `collectors.compute_features_batch` | `ai_trading_system.domains.features.compute_features_batch` |

### Ranking (was `collectors.run_full_rank` + root `analytics/`)

| Old | New | Status |
|---|---|---|
| `collectors.run_full_rank` | `ai_trading_system.domains.ranking.run_full_rank` | Migrated |
| `analytics.market_stage` (root) | `ai_trading_system.domains.ranking.market_stage` | **Orphaned** — root copy not imported anywhere |
| `analytics.stage_classifier` (root) | `ai_trading_system.domains.ranking.stage_classifier` | **Orphaned** |
| `analytics.sector_health` (root) | (no canonical equivalent — verify) | **Orphaned** |
| `analytics.stage_eligibility` (root) | (likely folded into ranking — verify) | **Orphaned** |
| `analytics.stage_store` (root) | (likely folded — verify) | **Orphaned** |
| `analytics.strategy_router` (root) | `ai_trading_system.domains.ranking.strategy_router` | **Orphaned** |
| `analytics.weekly` (root) | (verify — possibly research) | **Orphaned** |
| `analytics.stage_gate_backtest` (root) | (no canonical equivalent yet) | **Live legacy** — imported by `tests/test_phase5_validation_tools.py:10` and `scripts/run_stage_gate_backtest.py:20` |

### Pipeline (was `run/*`)

| Old | New |
|---|---|
| `run.orchestrator` | `ai_trading_system.pipeline.orchestrator` |
| `run.stages.<name>` | `ai_trading_system.pipeline.stages.<name>` |
| `run.daily_pipeline` | `ai_trading_system.pipeline.daily_pipeline` |
| `core.*` | split across `ai_trading_system.platform.*` and `ai_trading_system.domains.*` |

### UI (was `ui.execution.app`)

| Old | New |
|---|---|
| `ui.execution.app` (legacy path) | `ai_trading_system.ui.execution_api.app` |
| `ui.execution.routes.*` | `ai_trading_system.ui.execution_api.routes.*` |

### Publishing (was `publishers/*`)

Moved under `ai_trading_system.domains.publish.channels.*`.

## Remaining cleanup

1. **Root `analytics/`** — 7 of 8 modules appear orphaned. Recommended next step: confirm via dynamic-import grep, then remove the 7 orphans.
2. **`analytics/stage_gate_backtest.py`** — has 2 live consumers. Either move to `domains/ranking/` and update imports in `tests/test_phase5_validation_tools.py` and `scripts/run_stage_gate_backtest.py`, or accept it as legacy-forever.
3. **`audit_rank.py`** — fold into a CLI command (e.g. `ai-trading-audit-rank`) for discoverability, or leave as standalone script.
4. **`yfinance` provider** hardcodes `data/masterdata.db` instead of `platform/db/paths.py` — minor inconsistency flagged in [`docs/reference/data_sources.md`](../reference/data_sources.md).

## Standalone-package readiness gates

For `ai_trading_system` to be installable as a true standalone package (vs. the current monorepo where `scripts/`, `tools/`, `config/`, `data/`, root `analytics/` are required at runtime), the following must hold:

- [ ] No imports from root-level `analytics/` from any production code path
- [ ] All config paths read via `platform/db/paths.py` / `platform/config/`, not hardcoded
- [ ] All entrypoints in `pyproject.toml [project.scripts]` (no shell scripts required for primary ops)
- [ ] `config/` either bundled into the package or made discoverable via env var

Currently failing: gates 1 and 4.

## See also

- [`legacy_cleanup_plan.md`](legacy_cleanup_plan.md)
- [`docs/architecture/target_architecture.md`](../architecture/target_architecture.md)
