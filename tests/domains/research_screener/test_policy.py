from datetime import date

import pytest

from ai_trading_system.domains.research_screener.models import Disposition
from ai_trading_system.domains.research_screener.policy import (
    available_period_cagr,
    choose_statement_scope,
    deduplicate_security_rows,
    point_in_time_rows,
    terminal_disposition,
)


def test_isin_dedup_preserves_dual_listings():
    rows = [
        {"isin": "INE1", "exchange": "NSE", "symbol": "ABC"},
        {"isin": "INE1", "exchange": "BSE", "symbol": "ABC", "bse_code": "500001"},
    ]
    result = deduplicate_security_rows(rows)
    assert len(result) == 1
    assert {x["exchange"] for x in result[0]["listings"]} == {"NSE", "BSE"}


def test_dedup_refuses_missing_isin():
    with pytest.raises(ValueError):
        deduplicate_security_rows([{"symbol": "ABC"}])


def test_scope_prefers_usable_consolidated():
    selected = choose_statement_scope([{"scope": "standalone", "completeness": 1.0}, {"scope": "consolidated", "completeness": 0.8}])
    assert selected["scope"] == "consolidated"


def test_scope_rejects_nearly_empty_consolidated_and_uses_standalone():
    selected = choose_statement_scope([{"scope": "standalone", "completeness": 0.8}, {"scope": "consolidated", "completeness": 0.125}])
    assert selected == {"scope": "standalone", "completeness": 0.8, "reason": "standalone_fallback_no_splicing"}


def test_scope_does_not_splice_unusable_records():
    selected = choose_statement_scope([{"scope": "standalone", "completeness": 0.0}, {"scope": "consolidated", "completeness": 0.1}])
    assert selected["scope"] == "SCOPE_UNRESOLVED"


def test_short_history_is_not_zero_cagr():
    result = available_period_cagr([(date(2022, 3, 31), 100), (date(2023, 3, 31), None)])
    assert result["value"] is None
    assert result["state"] == "INSUFFICIENT_HISTORY"


def test_available_history_cagr_discloses_period_count():
    result = available_period_cagr([(date(2021, 3, 31), 100), (date(2022, 3, 31), 110), (date(2023, 3, 31), 121)])
    assert result["period_count"] == 2
    assert result["value"] == pytest.approx(0.10, abs=0.001)


def test_point_in_time_excludes_later_publication():
    rows = [{"id": 1, "available_at": "2026-08-08"}, {"id": 2, "available_at": "2026-08-10"}, {"id": 3, "available_at": None}]
    assert [x["id"] for x in point_in_time_rows(rows, date(2026, 8, 9))] == [1]


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"identity_status": "IDENTITY_CONFLICT", "board_status": "MAIN", "market_cap_cr": 5000, "fundamental_completeness": 1.0}, Disposition.DATA_REPAIR_REQUIRED),
        ({"identity_status": "RESOLVED", "board_status": "BOARD_UNKNOWN", "market_cap_cr": 5000, "fundamental_completeness": 1.0}, Disposition.ELIGIBILITY_UNKNOWN),
        ({"identity_status": "RESOLVED", "board_status": "SME", "market_cap_cr": 5000, "fundamental_completeness": 1.0}, Disposition.INELIGIBLE_BOARD_OR_INSTRUMENT),
        ({"identity_status": "RESOLVED", "board_status": "MAIN", "market_cap_cr": None, "fundamental_completeness": 1.0}, Disposition.ELIGIBILITY_UNKNOWN),
        ({"identity_status": "RESOLVED", "board_status": "MAIN", "market_cap_cr": 999, "fundamental_completeness": 1.0}, Disposition.INELIGIBLE_MARKET_CAP),
        ({"identity_status": "RESOLVED", "board_status": "MAIN", "market_cap_cr": 100001, "fundamental_completeness": 1.0}, Disposition.INELIGIBLE_MARKET_CAP),
        ({"identity_status": "RESOLVED", "board_status": "MAIN", "market_cap_cr": 5000, "fundamental_completeness": 0.69}, Disposition.DATA_REPAIR_REQUIRED),
        ({"identity_status": "RESOLVED", "board_status": "MAIN", "market_cap_cr": 5000, "fundamental_completeness": 0.70}, Disposition.BOUNDARY_REVIEW),
    ],
)
def test_terminal_dispositions(kwargs, expected):
    assert terminal_disposition(min_market_cap_cr=1000, max_market_cap_cr=100000, **kwargs) == expected
