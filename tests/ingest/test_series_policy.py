"""Tests for the NSE series whitelist + segment classification."""

from __future__ import annotations

import pandas as pd
import pytest

from ai_trading_system.domains.ingest.series_policy import (
    SUPPORTED_SERIES,
    is_intraday_eligible,
    is_supported,
    normalize_series,
    trading_segment,
)
from ai_trading_system.domains.ingest.daily_update_runner import _normalize_bhavcopy_frame


def test_normalize_series_handles_whitespace_case_and_none():
    assert normalize_series(" eq ") == "EQ"
    assert normalize_series("be") == "BE"
    assert normalize_series(None) == ""


@pytest.mark.parametrize("code,expected", [
    ("EQ", True),
    ("BE", True),
    ("BZ", True),
    (" be ", True),
    ("SM", False),
    ("IT", False),
    ("GS", False),
    ("", False),
])
def test_is_supported_default_whitelist(code, expected):
    assert is_supported(code) is expected


def test_is_supported_with_custom_allowlist():
    assert is_supported("BE", allowed=["EQ"]) is False
    assert is_supported("EQ", allowed=["EQ"]) is True


def test_is_intraday_eligible_excludes_t2t():
    assert is_intraday_eligible("EQ") is True
    assert is_intraday_eligible("BE") is False
    assert is_intraday_eligible("BZ") is False


@pytest.mark.parametrize("code,segment", [
    ("EQ", "regular"),
    ("BE", "t2t"),
    ("BZ", "trade_to_trade_z"),
    ("SM", "unknown"),
])
def test_trading_segment_mapping(code, segment):
    assert trading_segment(code) == segment


def test_supported_series_constant_unchanged():
    # Locks the contract — changing this should be a deliberate review.
    assert SUPPORTED_SERIES == ("EQ", "BE", "BZ")


def _build_bhavcopy_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "SYMBOL": ["AAA", "BBB", "CCC", "DDD", "EEE"],
        "SERIES": ["EQ", "BE", "BZ", "SM", "GS"],
        "OPEN_PRICE": [100.0, 50.0, 25.0, 10.0, 5.0],
        "HIGH_PRICE": [110.0, 55.0, 27.0, 11.0, 6.0],
        "LOW_PRICE":  [ 95.0, 48.0, 24.0,  9.0, 4.5],
        "CLOSE_PRICE":[105.0, 52.0, 26.0, 10.5, 5.5],
        "TTL_TRD_QNTY":[1000, 200, 100, 10, 1],
    })


def test_normalize_bhavcopy_frame_keeps_eq_be_bz_and_drops_others():
    raw = _build_bhavcopy_frame()
    security_map = {
        "AAA": {"symbol_id": "AAA", "security_id": "1", "isin": ""},
        "BBB": {"symbol_id": "BBB", "security_id": "2", "isin": ""},
        "CCC": {"symbol_id": "CCC", "security_id": "3", "isin": ""},
        "DDD": {"symbol_id": "DDD", "security_id": "4", "isin": ""},
        "EEE": {"symbol_id": "EEE", "security_id": "5", "isin": ""},
    }
    out = _normalize_bhavcopy_frame(raw, "2026-05-04", security_map)
    assert set(out["symbol_id"]) == {"AAA", "BBB", "CCC"}
    assert "series" in out.columns
    assert "trading_segment" in out.columns
    by_symbol = out.set_index("symbol_id")
    assert by_symbol.loc["AAA", "series"] == "EQ"
    assert by_symbol.loc["AAA", "trading_segment"] == "regular"
    assert by_symbol.loc["BBB", "series"] == "BE"
    assert by_symbol.loc["BBB", "trading_segment"] == "t2t"
    assert by_symbol.loc["CCC", "series"] == "BZ"
    assert by_symbol.loc["CCC", "trading_segment"] == "trade_to_trade_z"


def test_normalize_bhavcopy_frame_empty_returns_schema_consistent_columns():
    out = _normalize_bhavcopy_frame(pd.DataFrame(), "2026-05-04", {})
    assert "series" in out.columns
    assert "trading_segment" in out.columns
