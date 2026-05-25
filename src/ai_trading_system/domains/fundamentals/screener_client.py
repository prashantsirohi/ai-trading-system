"""Screener.in Excel download and parser."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths

logger = logging.getLogger(__name__)


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

    def excel_path(self, ticker: str) -> Path:
        return self.exports_dir / f"{ticker.upper().strip()}_screener.xlsx"

    def fetch_company_data(self, ticker: str, *, force_download: bool = False, allow_download: bool = False) -> dict[str, Any]:
        path = self.download_excel(ticker, force_download=force_download) if allow_download else self.excel_path(ticker)
        if not path.exists():
            raise FileNotFoundError(f"Screener export not found for {ticker}: {path}")
        return self.parse_excel(path)

    def download_excel(self, ticker: str, *, force_download: bool = False) -> Path:
        ticker = ticker.upper().strip()
        output_path = self.excel_path(ticker)
        if output_path.exists() and not force_download:
            return output_path
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
            page.goto(f"https://www.screener.in/company/{ticker}/")
            if "Page not found" in page.title() or "404" in page.title():
                raise ValueError(f"Company ticker '{ticker}' not found on Screener.in")
            button_selector = (
                "button:has-text('EXPORT TO EXCEL'), "
                "button:has-text('Export to Excel'), "
                "button:has-text('Export to excel')"
            )
            page.wait_for_selector(button_selector, timeout=10000)
            with page.expect_download() as download_info:
                page.click(button_selector)
            download_info.value.save_as(str(output_path))
            browser.close()
        return output_path

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
        section_dates: list[str] = []
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


def _section_dates(df: pd.DataFrame, row_index: int) -> list[str]:
    if row_index >= len(df):
        return []
    row = df.iloc[row_index]
    if str(row[0]).strip() != "Report Date":
        return []
    return [str(value).split()[0] for value in row[1:] if pd.notnull(value)]


def _values_by_date(section_dates: list[str], row: pd.Series) -> dict[str, Any]:
    values = [value if pd.notnull(value) else None for value in row[1:]]
    return {date: values[idx] for idx, date in enumerate(section_dates) if idx < len(values)}


__all__ = ["ScreenerClient"]
