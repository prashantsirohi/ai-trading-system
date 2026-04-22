from __future__ import annotations


def test_collectors_shims_resolve_to_canonical_modules() -> None:
    import collectors.dhan_collector as legacy_dhan
    import collectors.delivery_collector as legacy_delivery
    import collectors.ingest_validation as legacy_validation
    import collectors.masterdata as legacy_masterdata
    import collectors.nse_collector as legacy_nse
    import collectors.yfinance_collector as legacy_yf

    from ai_trading_system.domains.ingest import delivery as canonical_delivery
    from ai_trading_system.domains.ingest import masterdata as canonical_masterdata
    from ai_trading_system.domains.ingest import validation as canonical_validation
    from ai_trading_system.domains.ingest.providers import dhan as canonical_dhan
    from ai_trading_system.domains.ingest.providers import nse as canonical_nse
    from ai_trading_system.domains.ingest.providers import yfinance as canonical_yf

    assert legacy_nse is canonical_nse
    assert legacy_yf is canonical_yf
    assert legacy_dhan is canonical_dhan
    assert legacy_delivery is canonical_delivery
    assert legacy_masterdata is canonical_masterdata
    assert legacy_validation is canonical_validation


def test_collectors_shims_keep_expected_public_symbols() -> None:
    from collectors.dhan_collector import DhanCollector
    from collectors.delivery_collector import DeliveryCollector
    from collectors.ingest_validation import IngestValidationError, validate_delivery_frame, validate_ohlcv_frame
    from collectors.nse_collector import NSECollector
    from collectors.yfinance_collector import YFinanceCollector

    assert NSECollector is not None
    assert YFinanceCollector is not None
    assert DhanCollector is not None
    assert DeliveryCollector is not None
    assert IngestValidationError is not None
    assert callable(validate_ohlcv_frame)
    assert callable(validate_delivery_frame)
