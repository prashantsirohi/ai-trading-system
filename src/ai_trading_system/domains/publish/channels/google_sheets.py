"""Google Sheets delivery adapters and sheet publishing helpers."""

from __future__ import annotations

import json
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


# Per-sheet column → number-format spec. Applied after every successful
# write so the sheet renders numerics/dates correctly even if the sheet was
# recreated. Columns not present in the actual data are skipped silently.
_STOCK_SCAN_FORMATS: dict[str, dict[str, str]] = {
    "report_date": GoogleSheetsManager.FORMAT_DATE,
    "close": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "composite_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "rel_strength": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "rel_strength_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "momentum_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
}
_SECTOR_DASHBOARD_FORMATS: dict[str, dict[str, str]] = {
    "RS": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "rel_strength": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "Momentum": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "momentum": GoogleSheetsManager.FORMAT_DECIMAL_2,
}
_WATCHLIST_FORMATS: dict[str, dict[str, str]] = {
    "report_date": GoogleSheetsManager.FORMAT_DATE,
    "score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "composite_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "rank": GoogleSheetsManager.FORMAT_INT,
    "rank_change": GoogleSheetsManager.FORMAT_INT,
}
_FUNDAMENTAL_FORMATS: dict[str, dict[str, str]] = {
    "report_date": GoogleSheetsManager.FORMAT_DATE,
    "date": GoogleSheetsManager.FORMAT_DATE,
    "sales_yoy_growth": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "profit_yoy_growth": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "profit_qoq_growth": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "opm_yoy_change": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "great_result_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "turnaround_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "compounder_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "sector_fundamental_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pe_ttm": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pe_200dma": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pe_percentile_5y": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pe_zscore_5y": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "index_level_equal_weight": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "index_level_mcap_weight": GoogleSheetsManager.FORMAT_DECIMAL_2,
}


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
    manager.apply_number_formats("Stock Scan", _STOCK_SCAN_FORMATS)
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
    manager.apply_number_formats("Sector Dashboard", _SECTOR_DASHBOARD_FORMATS)
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
    manager.apply_number_formats(sheet_name, _WATCHLIST_FORMATS)
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


def publish_fundamental_dashboard(datasets: dict[str, object]) -> bool:
    """Publish the compact fundamental insight dashboard worksheet group."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")

    frames = _fundamental_dashboard_frames(datasets)
    for sheet_name, frame in frames.items():
        sheet = manager.get_or_create_sheet(sheet_name, rows=max(1000, len(frame) + 20), cols=max(12, len(frame.columns) + 2))
        if not sheet:
            raise RuntimeError(f"Could not get/create '{sheet_name}' sheet: {manager.last_error or 'unknown error'}")
        if not manager.write_dataframe(frame.fillna(""), sheet_name, include_header=True, clear_sheet=True):
            raise RuntimeError(f"Failed writing {sheet_name}: {manager.last_error or 'unknown error'}")
        manager.apply_number_formats(sheet_name, _FUNDAMENTAL_FORMATS)
    logger.info("Fundamental dashboard updated in Google Sheets (%s tabs)", len(frames))
    return True


def _fundamental_dashboard_frames(datasets: dict[str, object]) -> dict[str, pd.DataFrame]:
    payload = datasets.get("fundamental_dashboard_payload")
    payload = payload if isinstance(payload, dict) else {}
    universe = payload.get("universe") if isinstance(payload.get("universe"), dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    summary_rows = [
        {"metric": "Run Date", "value": payload.get("run_date")},
        {"metric": "Universe PE", "value": universe.get("pe_ttm")},
        {"metric": "PE 200DMA", "value": universe.get("pe_200dma")},
        {"metric": "PE 5Y Percentile", "value": universe.get("pe_percentile_5y")},
        {"metric": "Valuation Zone", "value": universe.get("valuation_zone")},
        {"metric": "Top Earnings Sector", "value": summary.get("top_earnings_sector")},
        {"metric": "Great Result Count", "value": summary.get("great_results_count")},
        {"metric": "Turnaround Count", "value": summary.get("turnaround_count")},
        {"metric": "Compounder Count", "value": summary.get("compounder_count")},
    ]
    return {
        "FUNDAMENTAL_SUMMARY": pd.DataFrame(summary_rows),
        "GREAT_RESULTS": _select_columns(_sheet_stock_frame(_first_frame(datasets, "great_results_latest", "great_results"), limit=100), [
            "symbol", "company_name", "sector", "report_date", "sales_yoy_growth", "profit_yoy_growth",
            "profit_qoq_growth", "opm_yoy_change", "great_result_score", "insight_score", "evidence", "evidence_json",
        ]),
        "TURNAROUNDS": _select_columns(_sheet_stock_frame(_first_frame(datasets, "turnaround_candidates_latest", "turnaround_candidates"), limit=100), [
            "symbol", "sector", "report_date", "sales_yoy_growth", "profit_yoy_growth", "loss_to_profit",
            "opm_yoy_change", "turnaround_score", "insight_score", "turnaround_stage", "insight_type", "evidence", "evidence_json",
        ]),
        "COMPOUNDERS": _select_columns(_sheet_stock_frame(_first_frame(datasets, "compounder_candidates_latest", "compounder_candidates"), limit=100), [
            "symbol", "sector", "report_date", "sales_8q_consistency", "profit_8q_consistency",
            "sales_8q_cagr", "profit_8q_cagr", "margin_stability", "compounder_score", "insight_score",
            "valuation_zone", "insight_type", "evidence_json",
        ]),
        "SECTOR_EARNINGS": _select_columns(_latest_by_date(_first_frame(datasets, "sector_earnings_latest", "sector_earnings_leadership"), "report_date"), [
            "sector_name", "sector", "report_date", "sector_sales_yoy_growth", "sector_profit_yoy_growth",
            "sales_positive_pct", "profit_positive_pct", "margin_expansion_pct", "great_result_count",
            "turnaround_count", "compounder_count", "sector_earnings_score", "sector_fundamental_score",
        ]),
        "SECTOR_VALUATION": _latest_by_date(_first_frame(datasets, "sector_valuation_latest", "sector_valuation_daily"), "date"),
        "UNIVERSE_VALUATION": _recent_by_date(_first_frame(datasets, "universe_valuation_latest", "universe_valuation_daily"), "date", limit=500),
        "VALUATION_CYCLE": _select_columns(_recent_by_date(_first_frame(datasets, "valuation_cycle_latest", "valuation_cycle_features"), "date", limit=500), [
            "date", "entity_id", "universe_id", "pe_ttm", "pe_200dma", "pe_percentile_5y",
            "pe_zscore_5y", "valuation_zone", "index_level", "index_200dma", "pe_distance_from_200dma", "cycle_signal",
        ]),
    }


def _as_frame(value: object) -> pd.DataFrame:
    return value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _first_frame(datasets: dict[str, object], *names: str) -> pd.DataFrame:
    for name in names:
        frame = _as_frame(datasets.get(name))
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _sheet_stock_frame(frame: pd.DataFrame, *, limit: int) -> pd.DataFrame:
    if frame.empty:
        return frame
    output = frame.copy()
    if "report_date" in output.columns:
        output = _latest_by_date(output, "report_date")
    if "evidence" not in output.columns and "evidence_json" in output.columns:
        output.loc[:, "evidence"] = output["evidence_json"].map(_evidence_text)
    sort_cols = [column for column in ["insight_score", "symbol"] if column in output.columns]
    if sort_cols:
        output = output.sort_values(
            sort_cols,
            ascending=[False if column == "insight_score" else True for column in sort_cols],
            na_position="last",
        )
    if "symbol" in output.columns:
        output = output.drop_duplicates("symbol", keep="first")
    return output.head(limit).reset_index(drop=True)


def _latest_by_date(frame: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if frame.empty or date_col not in frame.columns:
        return frame
    output = frame.copy()
    output.loc[:, date_col] = pd.to_datetime(output[date_col], errors="coerce")
    latest = output[date_col].max()
    if pd.isna(latest):
        return output
    return output[output[date_col].eq(latest)].reset_index(drop=True)


def _recent_by_date(frame: pd.DataFrame, date_col: str, *, limit: int) -> pd.DataFrame:
    if frame.empty or date_col not in frame.columns:
        return frame
    output = frame.copy()
    output.loc[:, date_col] = pd.to_datetime(output[date_col], errors="coerce")
    return output.sort_values(date_col).tail(limit).reset_index(drop=True)


def _evidence_text(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return value
    if not isinstance(parsed, dict):
        return ""
    return str(parsed.get("note") or "")


def _select_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    selected = [column for column in columns if column in frame.columns]
    extras = [column for column in frame.columns if column not in selected]
    return frame[selected + extras[: max(0, 12 - len(selected))]].copy()


__all__ = [
    "GoogleSheetsManager",
    "PortfolioSheets",
    "SectorReportSheets",
    "publish_sector_dashboard",
    "publish_stock_scan",
    "publish_watchlist_candidates",
    "publish_event_log_sheet",
    "publish_log_sheet",
    "publish_fundamental_dashboard",
]
