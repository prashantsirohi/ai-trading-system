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
    _extract_screen_query,
    _extract_screen_symbols,
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
    ("master_symbol", "screener_symbol"),
    [("AMIRCHAND", "AEROPLANE"), ("MIRCELECTR", "ONIDA"), ("SASTASUNDR", "HEALTHX")],
)
def test_company_url_maps_renamed_screener_tickers(master_symbol: str, screener_symbol: str) -> None:
    assert _company_url(master_symbol, "standalone") == f"https://www.screener.in/company/{screener_symbol}/"
    assert _company_url(master_symbol, "consolidated") == (
        f"https://www.screener.in/company/{screener_symbol}/consolidated/"
    )


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


def test_detect_rendered_basis_accepts_populated_explicit_standalone_page_without_toggle() -> None:
    assert (
        _detect_rendered_basis(
            _Page(standalone_toggle=0, consolidated_toggle=0, financial_periods=2),
            explicit_basis="standalone",
        )
        == "standalone"
    )


def test_detect_rendered_basis_rejects_empty_explicit_standalone_page_without_toggle() -> None:
    with pytest.raises(RuntimeError, match="expected exactly one"):
        _detect_rendered_basis(
            _Page(standalone_toggle=0, consolidated_toggle=0, financial_periods=0),
            explicit_basis="standalone",
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


class _ScreenLocator:
    def __init__(self, values: list[str]):
        self.values = values

    @property
    def first(self):
        return self

    def count(self) -> int:
        return len(self.values)

    def input_value(self) -> str:
        return self.values[0]

    def nth(self, index: int):
        return _ScreenLink(self.values[index])


class _ScreenLink:
    def __init__(self, href: str):
        self.href = href

    def get_attribute(self, name: str) -> str | None:
        return self.href if name == "href" else None


class _ScreenPage:
    def locator(self, selector: str):
        if selector == "#query-builder textarea":
            return _ScreenLocator(["Net block + Capital work in progress > preceding year"])
        if selector == "a[href*='/company/']":
            return _ScreenLocator([
                "/company/FCL/", "/company/E2E/consolidated/", "/company/FCL/",
            ])
        return _ScreenLocator([])


def test_screen_query_and_symbols_are_frozen_from_rendered_page() -> None:
    page = _ScreenPage()
    assert _extract_screen_query(page) == "Net block + Capital work in progress > preceding year"
    assert _extract_screen_symbols(page) == ("E2E", "FCL")


def test_screen_download_rejects_url_outside_governed_screen() -> None:
    client = ScreenerClient(username="configured", password="configured")

    with pytest.raises(ValueError, match="requested screen_id"):
        client.download_screen_export(
            317873,
            destination="unused.xlsx",
            screen_url="https://www.screener.in/screens/999/other/",
        )

    with pytest.raises(ValueError, match="HTTPS www.screener.in"):
        client.download_screen_export(
            317873,
            destination="unused.xlsx",
            screen_url="https://example.test/screens/317873/companies-with-capex/",
        )


def test_bse_download_uses_numeric_url_and_master_filename(tmp_path, monkeypatch):
    import sys
    from contextlib import nullcontext
    import types
    from ai_trading_system.domains.fundamentals import screener_client as module

    urls = []

    class Page:
        def goto(self, url, **kwargs):
            urls.append(url)
            return SimpleNamespace(status=200)

        def title(self):
            return "Vipul Organics"

        def wait_for_selector(self, *args, **kwargs):
            pass

        def click(self, *args):
            pass

        def expect_download(self, **kwargs):
            download = SimpleNamespace(
                save_as=lambda path: Path(path).write_bytes(b"export")
            )
            return nullcontext(SimpleNamespace(value=download))

    api = types.ModuleType("playwright.sync_api")
    api.sync_playwright = lambda: nullcontext(None)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", api)
    client = ScreenerClient(username="test", password="test", data_dir=tmp_path)
    client.company_identifiers = {"VIPULORG": "530627"}
    monkeypatch.setattr(
        client,
        "_authenticated_page",
        lambda p: (SimpleNamespace(close=lambda: None), None, Page()),
    )
    monkeypatch.setattr(
        module, "_detect_rendered_basis", lambda *a, **kw: "consolidated"
    )
    result = client.download_excel("VIPULORG", statement_basis="consolidated")
    assert urls == ["https://www.screener.in/company/530627/consolidated/"]
    assert result.path.name == "VIPULORG_consolidated_screener.xlsx"
