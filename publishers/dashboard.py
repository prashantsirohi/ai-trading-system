"""Dashboard payload delivery adapters."""

from __future__ import annotations

from typing import Any, Dict, Iterable

import pandas as pd

from publishers.google_sheets import GoogleSheetsManager
from utils.logger import logger


def _frame(records: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    rows = list(records)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def publish_dashboard_payload(payload: Dict[str, Any]) -> bool:
    """Write dashboard payload sections into dedicated Google Sheets tabs."""
    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        logger.warning("Dashboard publish skipped: spreadsheet unavailable")
        return False

    summary = pd.DataFrame([payload.get("summary", {})])
    ranked = _frame(payload.get("ranked_signals", []))
    breakouts = _frame(payload.get("breakout_scan", []))
    stock_scan = _frame(payload.get("stock_scan", []))
    sectors = _frame(payload.get("sector_dashboard", []))
    warnings = _frame({"warning": warning} for warning in payload.get("warnings", []))

    writes = [
        manager.write_dataframe(summary, "Dashboard Summary", clear_sheet=True),
        manager.write_dataframe(ranked, "Dashboard Ranked", clear_sheet=True),
        manager.write_dataframe(breakouts, "Dashboard Breakouts", clear_sheet=True),
        manager.write_dataframe(stock_scan, "Dashboard Stock Scan", clear_sheet=True),
        manager.write_dataframe(sectors, "Dashboard Sectors", clear_sheet=True),
        manager.write_dataframe(warnings, "Dashboard Warnings", clear_sheet=True),
    ]
    return all(writes)


__all__ = ["publish_dashboard_payload"]
