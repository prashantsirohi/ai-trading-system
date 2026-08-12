"""Screener.in Excel download and parser."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.domains.fundamentals.contracts import normalize_statement_basis

logger = logging.getLogger(__name__)
DEFAULT_DOWNLOAD_TIMEOUT_MS = 10_000


@dataclass(frozen=True)
class ScreenerDownloadResult:
    path: Path
    requested_basis: str
    detected_basis: str


@dataclass(frozen=True)
class ScreenerFetchResult:
    data: dict[str, Any]
    export_path: Path
    requested_basis: str
    detected_basis: str


class ScreenerHTTPError(RuntimeError):
    """Raised before page parsing when Screener returns a non-success response."""

    def __init__(self, status: int, url: str, *, retry_after: float | None = None):
        super().__init__(f"Screener request failed with HTTP {status}: {url}")
        self.status = int(status)
        self.url = str(url)
        self.retry_after = retry_after


class ScreenerRateLimitError(ScreenerHTTPError):
    """Retryable HTTP 429 response."""


class ScreenerClient:
    """Client for downloaded Screener Excel workbooks."""

    def __init__(
        self,
        *,
        username: str | None = None,
        password: str | None = None,
        data_dir: str | Path | None = None,
        exports_dir: str | Path | None = None,
        storage_state_path: str | Path | None = None,
    ):
        paths = get_domain_paths()
        self.username = username or os.getenv("SCREENER_USERNAME")
        self.password = password or os.getenv("SCREENER_PASSWORD")
        self.data_dir = Path(data_dir) if data_dir is not None else paths.fundamentals_dir
        self.exports_dir = Path(exports_dir) if exports_dir is not None else self.data_dir / "exports"
        self.exports_dir.mkdir(parents=True, exist_ok=True)
        self.storage_state_path = Path(storage_state_path) if storage_state_path else self.data_dir / "cache" / "screener_auth_state.json"
        self.storage_state_path.parent.mkdir(parents=True, exist_ok=True)

    def excel_path(self, ticker: str, *, statement_basis: str) -> Path:
        ticker = ticker.upper().strip()
        basis = normalize_statement_basis(statement_basis)
        suffix = "_consolidated" if basis == "consolidated" else ""
        return self.exports_dir / f"{ticker}{suffix}_screener.xlsx"

    def fetch_company_data(
        self,
        ticker: str,
        *,
        statement_basis: str,
        force_download: bool = False,
        allow_download: bool = False,
    ) -> ScreenerFetchResult:
        requested_basis = normalize_statement_basis(statement_basis)
        download = (
            self.download_excel(ticker, statement_basis=requested_basis, force_download=force_download)
            if allow_download
            else ScreenerDownloadResult(
                path=self.excel_path(ticker, statement_basis=requested_basis),
                requested_basis=requested_basis,
                detected_basis=requested_basis,
            )
        )
        path = download.path
        if not path.exists():
            raise FileNotFoundError(f"Screener export not found for {ticker}: {path}")
        return ScreenerFetchResult(
            data=self.parse_excel(path),
            export_path=path,
            requested_basis=download.requested_basis,
            detected_basis=download.detected_basis,
        )

    def download_excel(
        self,
        ticker: str,
        *,
        statement_basis: str,
        force_download: bool = False,
    ) -> ScreenerDownloadResult:
        ticker = ticker.upper().strip()
        requested_basis = normalize_statement_basis(statement_basis)
        output_path = self.excel_path(ticker, statement_basis=requested_basis)
        if output_path.exists() and not force_download:
            return ScreenerDownloadResult(output_path, requested_basis, requested_basis)
        if not self.username or not self.password:
            raise RuntimeError("SCREENER_USERNAME and SCREENER_PASSWORD are required for live downloads")
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError("playwright is required for live Screener downloads") from exc

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = (
                browser.new_context(storage_state=str(self.storage_state_path))
                if self.storage_state_path.exists()
                else browser.new_context()
            )
            page = context.new_page()
            page.goto("https://www.screener.in/dash/")
            if page.url.rstrip("/") != "https://www.screener.in/dash":
                page.goto("https://www.screener.in/login/?next=/dash/")
                page.fill("input[name='username']", self.username)
                page.fill("input[name='password']", self.password)
                page.click("button[type='submit']")
                page.wait_for_url("https://www.screener.in/dash/")
                context.storage_state(path=str(self.storage_state_path))
            company_url = _company_url(ticker, requested_basis)
            response = page.goto(company_url, wait_until="domcontentloaded")
            _validate_company_response(response, company_url)
            if "Page not found" in page.title() or "404" in page.title():
                raise ValueError(f"Company ticker '{ticker}' not found on Screener.in")
            try:
                detected_basis = _detect_rendered_basis(page)
            except RuntimeError:
                if requested_basis != "consolidated" or _has_rendered_financial_periods(page):
                    raise
                standalone_url = _company_url(ticker, "standalone")
                response = page.goto(standalone_url, wait_until="domcontentloaded")
                _validate_company_response(response, standalone_url)
                if "Page not found" in page.title() or "404" in page.title():
                    raise ValueError(f"Company ticker '{ticker}' not found on Screener.in")
                try:
                    detected_basis = _detect_rendered_basis(page)
                except RuntimeError:
                    if not _has_rendered_financial_periods(page):
                        raise RuntimeError(
                            "Unable to detect Screener statement basis: consolidated page has no toggle or "
                            "financial periods, and the canonical standalone page is also empty"
                        ) from None
                    detected_basis = "standalone"
            output_path = self.excel_path(ticker, statement_basis=detected_basis)
            button_selector = (
                "button:has-text('EXPORT TO EXCEL'), "
                "button:has-text('Export to Excel'), "
                "button:has-text('Export to excel')"
            )
            page.wait_for_selector(button_selector, timeout=10000)
            with page.expect_download(timeout=DEFAULT_DOWNLOAD_TIMEOUT_MS) as download_info:
                page.click(button_selector)
            download_info.value.save_as(str(output_path))
            browser.close()
        return ScreenerDownloadResult(output_path, requested_basis, detected_basis)

    def parse_excel(self, file_path: str | Path) -> dict[str, Any]:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        try:
            df = pd.read_excel(path, sheet_name="Data Sheet", header=None)
        except ImportError as exc:
            raise RuntimeError("openpyxl is required to parse Screener Excel exports") from exc

        result: dict[str, Any] = {
            "metadata": {},
            "profit_loss": {},
            "quarters": {},
            "balance_sheet": {},
            "cash_flow": {},
            "derived": {},
        }
        current_section: str | None = None
        section_dates: list[tuple[int, str]] = []
        seen_assets = False

        def clean(value: Any) -> str:
            return "" if pd.isna(value) else str(value).strip()

        i = 0
        while i < len(df):
            row = df.iloc[i]
            col0 = clean(row[0])
            if not col0:
                i += 1
                continue
            if col0 == "COMPANY NAME":
                result["metadata"]["company_name"] = clean(row[1])
                i += 1
                continue
            if col0 in {"LATEST VERSION", "CURRENT VERSION"}:
                result["metadata"][col0.lower().replace(" ", "_")] = clean(row[1])
                i += 1
                continue
            if col0 == "Face Value":
                result["metadata"]["face_value"] = row[1]
                i += 1
                continue
            if col0 == "Current Price":
                result["metadata"]["current_price"] = row[1]
                i += 1
                continue
            if col0 == "Market Capitalization":
                result["metadata"]["market_cap_cr"] = row[1]
                i += 1
                continue
            if col0 == "PROFIT & LOSS":
                current_section, section_dates, i = "profit_loss", _section_dates(df, i + 1), i + 2
                continue
            if col0 == "Quarters":
                current_section, section_dates, i = "quarters", _section_dates(df, i + 1), i + 2
                continue
            if col0 == "BALANCE SHEET":
                current_section, section_dates, i = "balance_sheet", _section_dates(df, i + 1), i + 2
                continue
            if col0 == "CASH FLOW:":
                current_section, section_dates, i = "cash_flow", _section_dates(df, i + 1), i + 2
                continue
            if col0 == "DERIVED:":
                current_section = "derived"
                i += 1
                continue
            if col0 == "PRICE:":
                result["derived"]["prices"] = _values_by_date(section_dates, row)
                i += 1
                continue
            if current_section and current_section != "metadata":
                label = col0
                if current_section == "balance_sheet":
                    if col0 in {"Net Block", "Capital Work in Progress", "Investments", "Other Assets"}:
                        seen_assets = True
                    if col0 == "Total":
                        label = "Total Assets" if seen_assets else "Total Liabilities"
                result[current_section][label] = _values_by_date(section_dates, row)
            i += 1
        return result


def _section_dates(df: pd.DataFrame, row_index: int) -> list[tuple[int, str]]:
    if row_index >= len(df):
        return []
    row = df.iloc[row_index]
    if str(row[0]).strip() != "Report Date":
        return []
    return [(idx, str(value).split()[0]) for idx, value in row.iloc[1:].items() if pd.notnull(value)]


def _values_by_date(section_dates: list[tuple[int, str]], row: pd.Series) -> dict[str, Any]:
    return {
        date: row.iloc[idx] if idx < len(row) and pd.notnull(row.iloc[idx]) else None
        for idx, date in section_dates
    }


def _detect_rendered_basis(page: Any) -> str:
    standalone_toggle = page.locator("a", has_text="View Standalone").count() > 0
    consolidated_toggle = page.locator("a", has_text="View Consolidated").count() > 0
    if standalone_toggle == consolidated_toggle:
        raise RuntimeError(
            "Unable to detect Screener statement basis: expected exactly one of "
            "'View Standalone' or 'View Consolidated'"
        )
    return "consolidated" if standalone_toggle else "standalone"


def _has_rendered_financial_periods(page: Any) -> bool:
    selectors = (
        "#quarters table thead th:not(:first-child), "
        "section#quarters table thead th:not(:first-child), "
        "#profit-loss table thead th:not(:first-child), "
        "section#profit-loss table thead th:not(:first-child)"
    )
    return page.locator(selectors).count() > 0


def _company_url(ticker: str, statement_basis: str) -> str:
    basis = normalize_statement_basis(statement_basis)
    suffix = "consolidated/" if basis == "consolidated" else ""
    return f"https://www.screener.in/company/{ticker.upper().strip()}/{suffix}"


def _validate_company_response(response: Any, url: str) -> None:
    if response is None:
        raise RuntimeError(f"Screener navigation returned no HTTP response: {url}")
    status = int(response.status)
    if status == 200:
        return
    retry_after = _retry_after_seconds(response.headers.get("retry-after"))
    error_type = ScreenerRateLimitError if status == 429 else ScreenerHTTPError
    raise error_type(status, url, retry_after=retry_after)


def _retry_after_seconds(value: object) -> float | None:
    try:
        seconds = float(str(value).strip())
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(str(value).strip())
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
    else:
        return max(0.0, seconds)


__all__ = [
    "ScreenerClient",
    "ScreenerDownloadResult",
    "ScreenerFetchResult",
    "ScreenerHTTPError",
    "ScreenerRateLimitError",
]
