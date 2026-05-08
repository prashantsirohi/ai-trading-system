"""Google Sheets delivery adapters and sheet publishing helpers."""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from ai_trading_system.domains.publish.channels.google_sheets_manager import (
    GoogleSheetsManager,
    PortfolioSheets,
    SectorReportSheets,
)
from ai_trading_system.domains.publish.decision_bundle import PublishDecisionBundle
from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.domains.publish.publish_payloads import format_rows_for_channel


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
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")
    stocks_rows = format_rows_for_channel(stocks.to_dict(orient="records"), "sheets")["rows"]
    stocks_with_index = pd.DataFrame(stocks_rows).reset_index()
    stocks_with_index.rename(columns={"index": "Symbol"}, inplace=True)
    stocks_with_index["report_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    sheet = manager.get_or_create_sheet("Stock Scan")
    if not sheet:
        raise RuntimeError(f"Could not get/create 'Stock Scan' sheet: {manager.last_error or 'unknown error'}")

    worksheet = manager.get_worksheet("Stock Scan")
    if worksheet is None:
        raise RuntimeError(f"Could not open 'Stock Scan' worksheet: {manager.last_error or 'unknown error'}")
    worksheet.clear()
    if not manager.append_rows(stocks_with_index, "Stock Scan", include_header=True):
        raise RuntimeError(f"Failed writing stock scan rows: {manager.last_error or 'unknown error'}")
    logger.info("Stock scan updated in Google Sheets (%s stocks)", len(stocks))
    return True


def publish_sector_dashboard(dashboard: pd.DataFrame) -> bool:
    """Append sector dashboard rows to the Sector Dashboard worksheet."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")
    dashboard_rows = format_rows_for_channel(dashboard.to_dict(orient="records"), "sheets")["rows"]
    dashboard_with_index = pd.DataFrame(dashboard_rows).reset_index()
    dashboard_with_index.rename(columns={"index": "Sector"}, inplace=True)

    sheet = manager.get_or_create_sheet("Sector Dashboard")
    if not sheet:
        raise RuntimeError(f"Could not get/create 'Sector Dashboard' sheet: {manager.last_error or 'unknown error'}")

    existing = sheet.get_all_values()
    is_empty = not existing or existing == [[]]
    ok = manager.append_rows(
        dashboard_with_index,
        "Sector Dashboard",
        include_header=is_empty,
    )
    if not ok:
        raise RuntimeError(f"Failed writing sector dashboard rows: {manager.last_error or 'unknown error'}")
    logger.info("Dashboard appended to Google Sheets (%s sectors)", len(dashboard))
    return True


def publish_watchlist_candidates(watchlist: pd.DataFrame, *, decision_bundle: PublishDecisionBundle | None = None) -> bool:
    """Publish watchlist candidates to a dedicated worksheet."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")
    if decision_bundle is not None:
        frame = decision_bundle.watchlist_candidates.copy()
    else:
        rows = format_rows_for_channel(watchlist.to_dict(orient="records"), "sheets")["rows"]
        frame = pd.DataFrame(rows).head(15)
        frame["report_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
    frame = frame.fillna("")

    sheet_name = "Watchlist Current"
    sheet = manager.get_or_create_sheet(sheet_name)
    if not sheet:
        raise RuntimeError(f"Could not get/create '{sheet_name}' sheet: {manager.last_error or 'unknown error'}")
    worksheet = manager.get_worksheet(sheet_name)
    if worksheet is None:
        raise RuntimeError(f"Could not open '{sheet_name}' worksheet: {manager.last_error or 'unknown error'}")
    worksheet.clear()
    if not manager.append_rows(frame, sheet_name, include_header=True):
        raise RuntimeError(f"Failed writing watchlist rows: {manager.last_error or 'unknown error'}")
    logger.info("Watchlist candidates updated in Google Sheets (%s rows)", len(frame))
    return True


def publish_event_log_sheet(decision_bundle: PublishDecisionBundle, *, sheet_name: str = "Event_Log") -> bool:
    """Publish raw event rows to a non-user-facing event log worksheet."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")
    sheet = manager.get_or_create_sheet(sheet_name, rows=5000, cols=12)
    if not sheet:
        raise RuntimeError(f"Could not get/create '{sheet_name}' sheet: {manager.last_error or 'unknown error'}")
    frame = decision_bundle.event_log.fillna("")
    if not manager.write_dataframe(frame, sheet_name, include_header=True, clear_sheet=True):
        raise RuntimeError(f"Failed writing event log rows: {manager.last_error or 'unknown error'}")
    logger.info("Event log updated in Google Sheets (%s rows)", len(frame))
    return True


def publish_log_sheet(decision_bundle: PublishDecisionBundle, *, sheet_name: str = "Publish_Log") -> bool:
    """Publish internal publish diagnostics to a non-user-facing log worksheet."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")
    sheet = manager.get_or_create_sheet(sheet_name, rows=1000, cols=8)
    if not sheet:
        raise RuntimeError(f"Could not get/create '{sheet_name}' sheet: {manager.last_error or 'unknown error'}")
    frame = decision_bundle.publish_log.fillna("")
    if not manager.write_dataframe(frame, sheet_name, include_header=True, clear_sheet=True):
        raise RuntimeError(f"Failed writing publish log rows: {manager.last_error or 'unknown error'}")
    logger.info("Publish log updated in Google Sheets (%s rows)", len(frame))
    return True


__all__ = [
    "GoogleSheetsManager",
    "PortfolioSheets",
    "SectorReportSheets",
    "publish_sector_dashboard",
    "publish_stock_scan",
    "publish_watchlist_candidates",
    "publish_event_log_sheet",
    "publish_log_sheet",
]
