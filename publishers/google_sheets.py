"""Google Sheets delivery adapters and sheet publishing helpers."""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from channel.google_sheets_manager import (
    GoogleSheetsManager,
    PortfolioSheets,
    SectorReportSheets,
)
from utils.logger import logger


def _require_spreadsheet_id() -> Optional[str]:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.warning("GOOGLE_SPREADSHEET_ID not set, skipping Google Sheets publish")
        return None
    return spreadsheet_id


def publish_stock_scan(stocks: pd.DataFrame) -> bool:
    """Publish stock scan results to the Stock Scan worksheet."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        return False

    manager = GoogleSheetsManager()
    stocks_with_index = stocks.reset_index()
    stocks_with_index.rename(columns={"index": "Symbol"}, inplace=True)
    stocks_with_index["report_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    sheet = manager.get_or_create_sheet("Stock Scan")
    if not sheet:
        return False

    worksheet = manager.get_worksheet("Stock Scan")
    worksheet.clear()
    manager.append_rows(stocks_with_index, "Stock Scan", include_header=True)
    logger.info("Stock scan updated in Google Sheets (%s stocks)", len(stocks))
    return True


def publish_sector_dashboard(dashboard: pd.DataFrame) -> bool:
    """Append sector dashboard rows to the Sector Dashboard worksheet."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        return False

    manager = GoogleSheetsManager()
    dashboard_with_index = dashboard.reset_index()
    dashboard_with_index.rename(columns={"index": "Sector"}, inplace=True)

    sheet = manager.get_or_create_sheet("Sector Dashboard")
    if not sheet:
        return False

    existing = sheet.get_all_values()
    is_empty = not existing or existing == [[]]
    manager.append_rows(
        dashboard_with_index,
        "Sector Dashboard",
        include_header=is_empty,
    )
    logger.info("Dashboard appended to Google Sheets (%s sectors)", len(dashboard))
    return True


__all__ = [
    "GoogleSheetsManager",
    "PortfolioSheets",
    "SectorReportSheets",
    "publish_sector_dashboard",
    "publish_stock_scan",
]
