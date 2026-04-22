from __future__ import annotations


def test_collectors_shims_resolve_to_canonical_modules() -> None:
    import collectors.daily_update_runner as legacy_daily_runner
    import collectors.dhan_collector as legacy_dhan
    import collectors.delivery_collector as legacy_delivery
    import collectors.ingest_full as legacy_ingest_full
    import collectors.ingest_validation as legacy_validation
    import collectors.index_backfill as legacy_index_backfill
    import collectors.masterdata as legacy_masterdata
    import collectors.nse_collector as legacy_nse
    import collectors.nse_delivery_scraper as legacy_nse_delivery_scraper
    import collectors.reset_reingest_validate as legacy_reset_reingest
    import collectors.stock_backfill as legacy_stock_backfill
    import collectors.token_manager as legacy_token_manager
    import collectors.yfinance_collector as legacy_yf

    from ai_trading_system.domains.ingest import daily_update_runner as canonical_daily_runner
    from ai_trading_system.domains.ingest import delivery as canonical_delivery
    from ai_trading_system.domains.ingest import ingest_full as canonical_ingest_full
    from ai_trading_system.domains.ingest import index_backfill as canonical_index_backfill
    from ai_trading_system.domains.ingest import masterdata as canonical_masterdata
    from ai_trading_system.domains.ingest import nse_delivery_scraper as canonical_nse_delivery_scraper
    from ai_trading_system.domains.ingest import reset_reingest_validate as canonical_reset_reingest
    from ai_trading_system.domains.ingest import stock_backfill as canonical_stock_backfill
    from ai_trading_system.domains.ingest import token_manager as canonical_token_manager
    from ai_trading_system.domains.ingest import validation as canonical_validation
    from ai_trading_system.domains.ingest.providers import dhan as canonical_dhan
    from ai_trading_system.domains.ingest.providers import nse as canonical_nse
    from ai_trading_system.domains.ingest.providers import yfinance as canonical_yf

    assert legacy_nse is canonical_nse
    assert legacy_yf is canonical_yf
    assert legacy_dhan is canonical_dhan
    assert legacy_delivery is canonical_delivery
    assert legacy_ingest_full is canonical_ingest_full
    assert legacy_masterdata is canonical_masterdata
    assert legacy_validation is canonical_validation
    assert legacy_daily_runner is canonical_daily_runner
    assert legacy_reset_reingest is canonical_reset_reingest
    assert legacy_index_backfill is canonical_index_backfill
    assert legacy_stock_backfill is canonical_stock_backfill
    assert legacy_nse_delivery_scraper is canonical_nse_delivery_scraper
    assert legacy_token_manager is canonical_token_manager


def test_collectors_shims_keep_expected_public_symbols() -> None:
    from collectors.daily_update_runner import _fetch_nse_bhavcopy_rows, run
    from collectors.dhan_collector import DhanCollector
    from collectors.delivery_collector import DeliveryCollector
    from collectors.ingest_full import get_already_ingested, run_ingestion, write_dfs_to_duckdb
    from collectors.ingest_validation import IngestValidationError, validate_delivery_frame, validate_ohlcv_frame
    from collectors.index_backfill import run_index_backfill
    from collectors.nse_collector import NSECollector
    from collectors.nse_delivery_scraper import NseHistoricalDeliveryScraper
    from collectors.reset_reingest_validate import run_reset_reingest_validate
    from collectors.stock_backfill import fetch_yfinance_ohlc, run_stock_backfill
    from collectors.token_manager import DhanTokenManager
    from collectors.yfinance_collector import YFinanceCollector

    assert NSECollector is not None
    assert YFinanceCollector is not None
    assert DhanCollector is not None
    assert DeliveryCollector is not None
    assert IngestValidationError is not None
    assert callable(validate_ohlcv_frame)
    assert callable(validate_delivery_frame)
    assert callable(_fetch_nse_bhavcopy_rows)
    assert callable(run)
    assert callable(write_dfs_to_duckdb)
    assert callable(get_already_ingested)
    assert callable(run_ingestion)
    assert callable(run_reset_reingest_validate)
    assert callable(run_index_backfill)
    assert callable(fetch_yfinance_ohlc)
    assert callable(run_stock_backfill)
    assert NseHistoricalDeliveryScraper is not None
    assert DhanTokenManager is not None
