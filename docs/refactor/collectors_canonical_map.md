# Collectors Canonical Map (PR-2 tranche)

This document tracks which `collectors/*` modules are already compatibility shims to canonical `src/` implementations, and which remain legacy operational surfaces.

## Shimmed in this tranche (or prior)

| Legacy module | Canonical target |
|---|---|
| `collectors/masterdata.py` | `ai_trading_system.domains.ingest.masterdata` |
| `collectors/nse_collector.py` | `ai_trading_system.domains.ingest.providers.nse` |
| `collectors/yfinance_collector.py` | `ai_trading_system.domains.ingest.providers.yfinance` |
| `collectors/dhan_collector.py` | `ai_trading_system.domains.ingest.providers.dhan` |
| `collectors/delivery_collector.py` | `ai_trading_system.domains.ingest.delivery` |
| `collectors/ingest_validation.py` | `ai_trading_system.domains.ingest.validation` |
| `collectors/daily_update_runner.py` | `ai_trading_system.domains.ingest.daily_update_runner` |
| `collectors/reset_reingest_validate.py` | `ai_trading_system.domains.ingest.reset_reingest_validate` |
| `collectors/ingest_full.py` | `ai_trading_system.domains.ingest.ingest_full` |
| `collectors/index_backfill.py` | `ai_trading_system.domains.ingest.index_backfill` |
| `collectors/stock_backfill.py` | `ai_trading_system.domains.ingest.stock_backfill` |
| `collectors/token_manager.py` | `ai_trading_system.domains.ingest.token_manager` |
| `collectors/nse_delivery_scraper.py` | `ai_trading_system.domains.ingest.nse_delivery_scraper` |

## Deferred legacy modules (explicit PR-2 boundary)

These remain operational in this tranche and are not deep-migrated here:

- `collectors/auth_doctor.py`
- `collectors/archive_nse_bhavcopy.py`
- `collectors/compute_features_batch.py`
- `collectors/delete_stale.py`
- `collectors/dhan_ohlc_diagnostics.py`
- `collectors/run_full_rank.py`
- `collectors/test_marketfeed_ohlc.py`
- `collectors/zerodha_sector_collector.py`

Follow-up PRs should migrate remaining business logic into `src/ai_trading_system/domains/ingest/*` and retain thin import shims at legacy paths for compatibility.
