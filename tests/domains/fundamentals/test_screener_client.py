from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import pandas as pd

from ai_trading_system.domains.fundamentals.screener_client import (
    ScreenerClient,
    ScreenerHTTPError,
    ScreenerRateLimitError,
    _company_url,
    _detect_rendered_basis,
    _has_rendered_financial_periods,
    _section_dates,
    _validate_company_response,
    _values_by_date,
)


class _Locator:
    def __init__(self, count: int):
        self._count = count

    def count(self) -> int:
        return self._count


class _Page:
    def __init__(self, *, standalone_toggle: int, consolidated_toggle: int, financial_periods: int = 0):
        self._counts = {
            "View Standalone": standalone_toggle,
            "View Consolidated": consolidated_toggle,
        }
        self._financial_periods = financial_periods

    def locator(self, _selector: str, *, has_text: str | None = None) -> _Locator:
        return _Locator(self._counts[has_text] if has_text is not None else self._financial_periods)


def test_sparse_report_date_columns_keep_values_aligned() -> None:
    frame = pd.DataFrame(
        [
            ["Report Date", None, None, "2025-03-31", "2026-03-31"],
            ["Net profit", None, None, 49.23, 43.96],
        ]
    )

    dates = _section_dates(frame, 0)
    values = _values_by_date(dates, frame.iloc[1])

    assert values == {"2025-03-31": 49.23, "2026-03-31": 43.96}


def test_basis_specific_urls_and_export_paths(tmp_path: Path) -> None:
    client = ScreenerClient(data_dir=tmp_path, exports_dir=tmp_path / "exports")

    assert _company_url("reliance", "standalone") == "https://www.screener.in/company/RELIANCE/"
    assert _company_url("reliance", "consolidated") == "https://www.screener.in/company/RELIANCE/consolidated/"
    assert client.excel_path("reliance", statement_basis="standalone").name == "RELIANCE_screener.xlsx"
    assert client.excel_path("reliance", statement_basis="consolidated").name == "RELIANCE_consolidated_screener.xlsx"


@pytest.mark.parametrize(
    ("standalone_toggle", "consolidated_toggle", "expected"),
    [(1, 0, "consolidated"), (0, 1, "standalone")],
)
def test_detect_rendered_basis_from_inverse_toggle(
    standalone_toggle: int,
    consolidated_toggle: int,
    expected: str,
) -> None:
    assert _detect_rendered_basis(
        _Page(standalone_toggle=standalone_toggle, consolidated_toggle=consolidated_toggle)
    ) == expected


@pytest.mark.parametrize(("standalone_toggle", "consolidated_toggle"), [(0, 0), (1, 1)])
def test_detect_rendered_basis_rejects_missing_or_ambiguous_toggle(
    standalone_toggle: int,
    consolidated_toggle: int,
) -> None:
    with pytest.raises(RuntimeError, match="expected exactly one"):
        _detect_rendered_basis(
            _Page(standalone_toggle=standalone_toggle, consolidated_toggle=consolidated_toggle)
        )


def test_financial_period_detection_requires_rendered_statement_headers() -> None:
    assert _has_rendered_financial_periods(
        _Page(standalone_toggle=0, consolidated_toggle=0, financial_periods=2)
    )
    assert not _has_rendered_financial_periods(
        _Page(standalone_toggle=0, consolidated_toggle=0, financial_periods=0)
    )


def test_company_response_accepts_only_http_200() -> None:
    _validate_company_response(SimpleNamespace(status=200, headers={}), "https://example.test/company/AAA/")

    with pytest.raises(ScreenerHTTPError) as exc_info:
        _validate_company_response(SimpleNamespace(status=404, headers={}), "https://example.test/company/AAA/")
    assert exc_info.value.status == 404


def test_company_response_marks_429_retryable_and_preserves_retry_after() -> None:
    with pytest.raises(ScreenerRateLimitError) as exc_info:
        _validate_company_response(
            SimpleNamespace(status=429, headers={"retry-after": "17"}),
            "https://example.test/company/AAA/consolidated/",
        )

    assert exc_info.value.status == 429
    assert exc_info.value.retry_after == 17.0


def test_company_response_rejects_missing_http_response() -> None:
    with pytest.raises(RuntimeError, match="no HTTP response"):
        _validate_company_response(None, "https://example.test/company/AAA/")
