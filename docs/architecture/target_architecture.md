# Target Architecture

- **Purpose:** Describes the *current* canonical package layout (which is already the target — the migration is largely done) and enumerates remaining legacy outside `src/`.
- **Audience:** Developer, future agents.
- **Last verified:** 2026-05-16
- **Source of truth:**
  - `src/ai_trading_system/` filesystem layout
  - `pyproject.toml`
  - Root-level legacy modules (`analytics/`, `audit_rank.py`, `scripts/`, `tools/`)
  - [`docs/_audit/current_code_truth_map.md`](../_audit/current_code_truth_map.md)

---

## Canonical layout

The canonical Python package is `src/ai_trading_system/`. All new code lands here. Top-level subpackages:

| Subpackage | Responsibility | Domain doc |
|---|---|---|
| `domains/` | Domain-driven business logic (10 subdomains, see below) | [domains/*](../domains/) |
| `pipeline/` | Stage orchestration, DQ engine, contracts, SQL migrations | [stages/*](../stages/) |
| `platform/` | Config (Pydantic Settings), `db/paths.py`, logging, utils | [platform_domain](../domains/platform_domain.md) |
| `interfaces/cli/` | Operator CLIs (healthcheck, bootstrap, repair, export) | — |
| `interfaces/api/` | Public API surface (minimal — most API is in `ui/execution_api/`) | — |
| `ui/execution_api/` | FastAPI app + 14 routers powering the React console | [ui_domain](../domains/ui_domain.md) |
| `research/` | Backtesting, optimization, perf tracker — isolated from operational | [research_domain](../domains/research_domain.md) |
| `analytics/` | Cross-cutting: DQ stubs, alpha factors, pattern/indicator registries | — |
| `integrations/` | Outbound integrations (e.g., `market_intel_client.py`) | — |

## Domain ownership

The 10 subdomains under `src/ai_trading_system/domains/`:

| Domain | Key responsibility | Domain doc |
|---|---|---|
| `ingest/` | NSE bhavcopy (source-of-record), Dhan (fallback + live exec), yfinance fallback; trust & validation | [ingest_domain](../domains/ingest_domain.md) |
| `features/` | Indicators, feature store, sector RS, universe index, pattern features | [features_domain](../domains/features_domain.md) |
| `ranking/` | Composite scoring, breakout, regime detection, sector dashboard, stage classifier | [ranking_domain](../domains/ranking_domain.md) |
| `candidates/` | Deterministic filtering from rank outputs | [ranking_domain](../domains/ranking_domain.md) |
| `fundamentals/` | Screener.in importer + scoring + enrich-rank | [fundamentals_domain](../domains/fundamentals_domain.md) |
| `catalysts/` | Corporate action collector | [catalyst_intelligence_domain](../domains/catalyst_intelligence_domain.md) |
| `events/` | Event packet builder, LLM router, noise filter, enrichment | [catalyst_intelligence_domain](../domains/catalyst_intelligence_domain.md) |
| `execution/` | Paper/Dhan adapters, autotrader, portfolio mgr, store, policies | [execution_domain](../domains/execution_domain.md) |
| `publish/` | Multi-channel delivery (Telegram, Google Sheets, QuantStats, PDF, etc.) | [publishing_domain](../domains/publishing_domain.md) |
| `risk/` | Risk profile loader, guardrails | [execution_domain](../domains/execution_domain.md) |
| `strategy/` | Strategy rule packs, bounds, compiler | [execution_domain](../domains/execution_domain.md) |

## Remaining legacy outside `src/`

The repo also contains code outside the canonical package. Status as of 2026-05-16:

### `analytics/` (root) — partial legacy

Eight modules. Only **one** has live consumers:

| Module | Status | Consumers |
|---|---|---|
| `stage_gate_backtest.py` | **Live legacy** | `tests/test_phase5_validation_tools.py:10`, `scripts/run_stage_gate_backtest.py:20` |
| `market_stage.py` | Orphan | none confirmed |
| `sector_health.py` | Orphan | none confirmed |
| `stage_classifier.py` | Orphan | none confirmed |
| `stage_eligibility.py` | Orphan | none confirmed |
| `stage_store.py` | Orphan | none confirmed |
| `strategy_router.py` | Orphan | none confirmed |
| `weekly.py` | Orphan | none confirmed |

**Plan:** Keep `stage_gate_backtest.py` in place until the test and script either migrate to `src/ai_trading_system/` equivalents or are themselves deprecated. The other seven can be removed after confirming no dynamic imports — see [`docs/development/legacy_cleanup_plan.md`](../development/legacy_cleanup_plan.md).

### `audit_rank.py` (root)

Standalone operator script. Reads `data/control_plane.duckdb` and audits rank artifacts. Not imported by anything. Keep as-is or fold into a CLI command later.

### `scripts/` (root)

Operational shell + Python scripts (data repair, weekly stage runner, helpers). Intentionally kept outside the package because they are deployment-time tooling, not library code. The path is conventional for ops scripts.

### `tools/` (root)

Ad-hoc developer tools (e.g., universe index builder). Same rationale as `scripts/`. Not load-bearing for the pipeline.

### `models/` (root)

Verify before documenting — directory exists but its role was not read during truth-mapping.

## Storage layout

See [`docs/architecture/storage_and_lineage.md`](storage_and_lineage.md) for the canonical storage map. Summary:

- `data/ohlcv.duckdb` — operational OHLCV (write owner: ingest)
- `data/control_plane.duckdb` — pipeline governance (`pipeline_run`, `pipeline_stage_run`, `pipeline_artifact`, `dq_rule`, `dq_result`, `model_registry`, pattern cache, watchlist/event tables)
- `data/execution.duckdb` — execution tables (`execution_order`, `execution_fill`, `execution_trade_note`, `execution_position_stop`) — default in [`execution/store.py:29`](../../src/ai_trading_system/domains/execution/store.py)
- `data/research.duckdb` — perf tracker (`rank_cohort_performance`)
- `data/research_ohlcv.duckdb` — research-domain OHLCV isolation (selected by `DATA_DOMAIN=research`)
- `data/market_intel.duckdb` — read-only consumer of the always-on `market_intel` runner ([`integrations/market_intel_client.py:32`](../../src/ai_trading_system/integrations/market_intel_client.py))
- `data/feature_store/` — Parquet feature store
- `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/` — per-run artifacts

## UI surfaces

- FastAPI: `src/ai_trading_system/ui/execution_api/app.py` (CLI: `ai-trading-execution-api`, default port 8090)
- React V2: `web/execution-console-v2/` (Vite + React + TypeScript)
- Streamlit: **none in active code paths**. Any old doc reference is stale.

## Open gaps

Tracked in [`docs/_audit/current_code_truth_map.md`](../_audit/current_code_truth_map.md) §"Things flagged as unknown":

1. `models/` (root) — purpose not yet read.
2. `interfaces/api/` — described as "mostly empty"; verify before docs imply otherwise.
3. Live trading guardrails — what prevents accidental live execution if Dhan creds are set? Current adapter [`execution/adapters/dhan.py:62-65`](../../src/ai_trading_system/domains/execution/adapters/dhan.py) raises `RuntimeError` unless `dry_run=True`, but production-readiness has not been audited.
4. Whether root-level `analytics/` orphans are safely deletable (need dynamic-import grep before removal).
5. Stage wrapper `pipeline/stages/perf_tracker.py` writes to `data/research.duckdb`, but the truth-map agent flagged ambiguity with `data/research_ohlcv.duckdb`. **Resolved by perf_tracker code read** — research.duckdb is correct for the tracker; research_ohlcv.duckdb is for OHLCV isolation only.

## See also

- [`docs/architecture/overview.md`](overview.md)
- [`docs/architecture/storage_and_lineage.md`](storage_and_lineage.md)
- [`docs/development/package_migration.md`](../development/package_migration.md)
- [`docs/development/legacy_cleanup_plan.md`](../development/legacy_cleanup_plan.md)
