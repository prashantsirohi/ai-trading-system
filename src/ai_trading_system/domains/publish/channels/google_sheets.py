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
from ai_trading_system.domains.fundamentals.presentation_payloads import (
    DEFAULT_PUBLISH_UNIVERSE_ID,
    build_fundamental_sheet_payload,
)
from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.domains.publish.publish_payloads import format_rows_for_channel

WATCHLIST_CURRENT_SHEET = "02_Watchlist_Current"
PORTFOLIO_SHEET = "03_Portfolio"
RUN_LOG_SHEET = "_RAW_RUN_LOG"
RAW_VALUATION_SHEET = "_RAW_VALUATION_DASHBOARD"
RAW_FUNDAMENTAL_WATCHLIST_SHEET = "_RAW_Fundamental_Watchlist"
VISIBLE_SHEET_MAX_ROWS = 60
VISIBLE_SHEET_MAX_COLS = 14
FINAL_3Q_GATE_SHEET = "Final 3Q Gate"
OPERATOR_TAB_ORDER = [
    "01_Daily_Report",
    PORTFOLIO_SHEET,
    "04_Sector_Leadership",
]


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
    "Tracking Health": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "Technical Health": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "Fundamental Health": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "Return Since First Seen": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "Drawdown From High": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "Result Delta": GoogleSheetsManager.FORMAT_DECIMAL_2,
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
_FUNDAMENTAL_WATCHLIST_FORMATS: dict[str, dict[str, str]] = {
    "report_date": GoogleSheetsManager.FORMAT_DATE,
    "available_at": GoogleSheetsManager.FORMAT_DATE,
    "final_watchlist_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "composite_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "quarterly_result_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "valuation_history_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "fundamental_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "sector_strength": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "breakout_pattern_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pe_ttm": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "ps_ttm": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pb": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pe_median_5y": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "ps_median_5y": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pb_median_5y": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pe_pctile_5y": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "ps_pctile_5y": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "pb_pctile_5y": GoogleSheetsManager.FORMAT_DECIMAL_2,
}
_INVESTIGATOR_FORMATS: dict[str, dict[str, str]] = {
    "trade_date": GoogleSheetsManager.FORMAT_DATE,
    "first_seen_date": GoogleSheetsManager.FORMAT_DATE,
    "last_seen_date": GoogleSheetsManager.FORMAT_DATE,
    "archived_at": GoogleSheetsManager.FORMAT_DATE,
    "daily_return_pct": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "volume_ratio_20": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "delivery_pct": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "composite_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "final_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "repeat_score": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "price_progression_pct": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "price_vs_first_trigger_pct": GoogleSheetsManager.FORMAT_DECIMAL_2,
    "rank_change_20d": GoogleSheetsManager.FORMAT_DECIMAL_2,
}
_FUNDAMENTAL_TRACKING_PUBLISH_BUCKETS = frozenset(
    {
        "F4_ACTION_CANDIDATE",
        "F3_FUND_VALUE_TECH_READY",
        "F2_RESULT_VALUE_ACCUMULATION",
        "F1_FUNDAMENTAL_WATCH",
    }
)


def _cap_visible_frame(frame: pd.DataFrame) -> pd.DataFrame:
    safe = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    return safe.iloc[: VISIBLE_SHEET_MAX_ROWS - 1, :VISIBLE_SHEET_MAX_COLS].copy()


def _sort_final_3q_gate_for_publish(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    out = frame.copy()
    verdict_priority = {
        "HIGH_CONVICTION": 0,
        "MEDIUM_CONVICTION": 1,
    }
    verdict = out.get("verdict", pd.Series("", index=out.index)).fillna("").astype(str).str.upper()
    out.loc[:, "_verdict_priority"] = verdict.map(verdict_priority).fillna(99)
    out.loc[:, "_final_score_sort"] = pd.to_numeric(out.get("final_score", pd.Series(pd.NA, index=out.index)), errors="coerce")
    sort_columns = ["_verdict_priority", "_final_score_sort"]
    ascending = [True, False]
    if "symbol_id" in out.columns:
        sort_columns.append("symbol_id")
        ascending.append(True)
    out = out.sort_values(sort_columns, ascending=ascending, na_position="last", kind="stable")
    return out.drop(columns=["_verdict_priority", "_final_score_sort"], errors="ignore").reset_index(drop=True)


def _require_spreadsheet_id() -> Optional[str]:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.warning("GOOGLE_SPREADSHEET_ID not set, skipping Google Sheets publish")
        return None
    return spreadsheet_id


def publish_stock_scan(stocks: pd.DataFrame) -> bool:
    """Deprecated: stock scan artifacts are no longer published as raw tabs."""
    logger.info("Skipping legacy Stock Scan sheet publish; rank artifacts are the source of truth (%s rows)", len(stocks))
    return True


def publish_sector_dashboard(dashboard: pd.DataFrame) -> bool:
    """Deprecated: sector dashboard artifacts are no longer published as raw tabs."""
    logger.info("Skipping legacy Sector Dashboard sheet publish; rank artifacts are the source of truth (%s rows)", len(dashboard))
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
    frame = _cap_visible_frame(frame).fillna("")

    sheet_name = WATCHLIST_CURRENT_SHEET
    sheet = manager.get_or_create_sheet(sheet_name, rows=VISIBLE_SHEET_MAX_ROWS, cols=VISIBLE_SHEET_MAX_COLS)
    if not sheet:
        raise RuntimeError(f"Could not get/create '{sheet_name}' sheet: {manager.last_error or 'unknown error'}")
    grid = [frame.columns.tolist()] + frame.values.tolist()
    grid = [row[:VISIBLE_SHEET_MAX_COLS] + [""] * max(0, VISIBLE_SHEET_MAX_COLS - len(row)) for row in grid[:VISIBLE_SHEET_MAX_ROWS]]
    while len(grid) < VISIBLE_SHEET_MAX_ROWS:
        grid.append([""] * VISIBLE_SHEET_MAX_COLS)
    if hasattr(manager, "update_worksheet_values"):
        manager.update_worksheet_values(sheet, grid, range_name="A1")
    else:
        fallback = pd.DataFrame(grid[1:], columns=grid[0])
        if not manager.write_dataframe(fallback, sheet_name=sheet_name, include_header=True, clear_sheet=True):
            raise RuntimeError(f"Failed writing watchlist rows: {manager.last_error or 'unknown error'}")
    manager.apply_number_formats(sheet_name, _WATCHLIST_FORMATS)
    logger.info("Watchlist candidates updated in Google Sheets (%s rows)", len(frame))
    return True


def publish_fundamental_watchlist(watchlist: pd.DataFrame, *, sheet_name: str = RAW_FUNDAMENTAL_WATCHLIST_SHEET) -> bool:
    """Publish the raw fundamental-tracking watchlist to an archive worksheet."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")

    frame = _fundamental_watchlist_frame(watchlist)
    sheet = manager.get_or_create_sheet(sheet_name, rows=max(1000, len(frame) + 20), cols=max(24, len(frame.columns) + 2))
    if not sheet:
        raise RuntimeError(f"Could not get/create '{sheet_name}' sheet: {manager.last_error or 'unknown error'}")
    if hasattr(manager, "write_hidden_data_sheet"):
        wrote = manager.write_hidden_data_sheet(sheet_name, frame.fillna(""), max_rows=100, max_cols=max(1, min(24, len(frame.columns))))
    else:
        wrote = manager.write_dataframe(frame.fillna(""), sheet_name, include_header=True, clear_sheet=True)
    if not wrote:
        raise RuntimeError(f"Failed writing {sheet_name}: {manager.last_error or 'unknown error'}")
    manager.apply_number_formats(sheet_name, _FUNDAMENTAL_WATCHLIST_FORMATS)
    logger.info("Fundamental watchlist updated in Google Sheets (%s rows)", len(frame))
    return True


def publish_investigator(sheets: dict[str, pd.DataFrame]) -> bool:
    """Publish investigator outputs to dedicated worksheets."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")
    for sheet_name, frame in sheets.items():
        safe = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        if sheet_name == FINAL_3Q_GATE_SHEET:
            safe = _sort_final_3q_gate_for_publish(safe)
        safe = safe.fillna("")
        rows = max(1000, len(safe) + 20)
        cols = max(12, len(safe.columns) + 2)
        sheet = manager.get_or_create_sheet(sheet_name, rows=rows, cols=cols)
        if not sheet:
            raise RuntimeError(f"Could not get/create '{sheet_name}' sheet: {manager.last_error or 'unknown error'}")
        if not manager.write_dataframe(safe, sheet_name, include_header=True, clear_sheet=True):
            raise RuntimeError(f"Failed writing {sheet_name}: {manager.last_error or 'unknown error'}")
        manager.apply_number_formats(sheet_name, _INVESTIGATOR_FORMATS)
    logger.info("Investigator sheets updated (%s tabs)", len(sheets))
    return True


def _fundamental_watchlist_frame(watchlist: pd.DataFrame) -> pd.DataFrame:
    if watchlist is None or watchlist.empty:
        return pd.DataFrame(columns=_FUNDAMENTAL_WATCHLIST_COLUMNS)
    frame = watchlist.copy()
    if "watchlist_bucket" in frame.columns:
        bucket = frame["watchlist_bucket"].fillna("").astype(str).str.upper()
        frame = frame.loc[bucket.isin(_FUNDAMENTAL_TRACKING_PUBLISH_BUCKETS)].copy()
    else:
        frame = frame.iloc[0:0].copy()
    bucket_priority = {
        "F4_ACTION_CANDIDATE": 0,
        "F3_FUND_VALUE_TECH_READY": 1,
        "F2_RESULT_VALUE_ACCUMULATION": 2,
        "F1_FUNDAMENTAL_WATCH": 3,
    }
    if "watchlist_bucket" in frame.columns:
        frame.loc[:, "_bucket_priority"] = frame["watchlist_bucket"].astype(str).map(bucket_priority).fillna(99)
    else:
        frame.loc[:, "_bucket_priority"] = 99
    for column in ("final_watchlist_score", "composite_score"):
        if column not in frame.columns:
            frame.loc[:, column] = pd.NA
        frame.loc[:, column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_values(
        ["_bucket_priority", "final_watchlist_score", "composite_score"],
        ascending=[True, False, False],
        na_position="last",
        kind="stable",
    )
    for column in _FUNDAMENTAL_WATCHLIST_COLUMNS:
        if column not in frame.columns:
            frame.loc[:, column] = pd.NA
    return frame[_FUNDAMENTAL_WATCHLIST_COLUMNS].head(100).reset_index(drop=True)


_FUNDAMENTAL_WATCHLIST_COLUMNS = [
    "symbol",
    "name",
    "industry_group",
    "watchlist_bucket",
    "final_watchlist_score",
    "quarterly_result_bucket",
    "quarterly_result_score",
    "valuation_history_bucket",
    "valuation_history_score",
    "valuation_reason",
    "composite_score",
    "sector_strength",
    "breakout_pattern_score",
    "candidate_tier",
    "qualified",
    "fundamental_score",
    "fundamental_tier",
    "pe_ttm",
    "ps_ttm",
    "pb",
    "pe_median_5y",
    "ps_median_5y",
    "pb_median_5y",
    "pe_pctile_5y",
    "ps_pctile_5y",
    "pb_pctile_5y",
    "sales_yoy_pct",
    "operating_profit_yoy_pct",
    "opm_yoy_change_bps",
    "watchlist_reason",
    "next_action",
    "report_date",
    "available_at",
]


def publish_log_sheet(decision_bundle: PublishDecisionBundle, *, sheet_name: str = RUN_LOG_SHEET) -> bool:
    """Deprecated: per-run/channel status now appends to 99_Run_Log."""
    _ = decision_bundle, sheet_name
    logger.info("Skipping legacy Publish_Log sheet publish; use %s", RUN_LOG_SHEET)
    return True


def publish_run_log_sheet(rows: list[dict[str, object]], *, sheet_name: str = RUN_LOG_SHEET) -> bool:
    """Append one row per run/channel delivery to the hidden raw run log."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")
    if not rows:
        return True

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")
    sheet = manager.get_or_create_sheet(sheet_name, rows=max(1000, len(rows) + 20), cols=14)
    if not sheet:
        raise RuntimeError(f"Could not get/create '{sheet_name}' sheet: {manager.last_error or 'unknown error'}")

    frame = pd.DataFrame(rows).fillna("")
    if hasattr(sheet, "get_all_values") and hasattr(manager, "_execute_with_backoff"):
        existing = manager._execute_with_backoff(lambda: sheet.get_all_values())
    else:
        existing = []
    include_header = not existing or existing == [[]]
    if not manager.append_rows(frame, sheet_name, include_header=include_header):
        raise RuntimeError(f"Failed appending run log rows: {manager.last_error or 'unknown error'}")
    if hasattr(manager, "hide_worksheet"):
        manager.hide_worksheet(sheet_name)
    if hasattr(manager, "reorder_worksheets"):
        manager.reorder_worksheets(OPERATOR_TAB_ORDER)
    logger.info("Run log appended in Google Sheets (%s rows)", len(frame))
    return True


def publish_fundamental_dashboard(datasets: dict[str, object]) -> bool:
    """Publish the single-tab fundamental valuation dashboard."""
    universe_id = str(datasets.get("fundamental_publish_universe_id") or DEFAULT_PUBLISH_UNIVERSE_ID)
    payload = datasets.get("fundamental_sheet_payload")
    if not isinstance(payload, dict):
        payload = build_fundamental_sheet_payload(
            universe_valuation=_first_frame(datasets, "universe_valuation_latest", "universe_valuation_daily"),
            valuation_cycle=_first_frame(datasets, "valuation_cycle_latest", "valuation_cycle_features"),
            sector_dashboard=_first_frame(datasets, "sector_dashboard"),
            sector_valuation=_first_frame(datasets, "sector_valuation_latest", "sector_valuation_daily"),
            universe_id=universe_id,
            years=int(datasets.get("fundamental_publish_years") or 5),
        )
    return publish_fundamental_valuation_dashboard(payload=payload)


def publish_fundamental_valuation_dashboard(
    *,
    payload: dict[str, object],
    sheet_name: str = RAW_VALUATION_SHEET,
) -> bool:
    """Publish valuation details to a raw/archive worksheet."""
    spreadsheet_id = _require_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID not set")

    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        raise RuntimeError(f"Google Sheets authentication failed: {manager.last_error or 'unable to open spreadsheet'}")

    frame = _valuation_dashboard_frame(payload)
    sheet = manager.get_or_create_sheet(sheet_name, rows=max(1000, len(frame) + 20), cols=max(12, len(frame.columns) + 2))
    if not sheet:
        raise RuntimeError(f"Could not get/create '{sheet_name}' sheet: {manager.last_error or 'unknown error'}")
    if hasattr(manager, "write_hidden_data_sheet"):
        wrote = manager.write_hidden_data_sheet(sheet_name, frame.fillna(""), max_rows=max(1, min(500, len(frame))), max_cols=max(1, min(14, len(frame.columns))))
    else:
        wrote = manager.write_dataframe(frame.fillna(""), sheet_name, include_header=False, clear_sheet=True)
    if not wrote:
        raise RuntimeError(f"Failed writing {sheet_name}: {manager.last_error or 'unknown error'}")
    manager.apply_number_formats(sheet_name, _FUNDAMENTAL_FORMATS, header_row=_chart_header_row(payload))
    _write_valuation_charts(manager, sheet_name, payload)
    logger.info("Fundamental valuation dashboard updated in Google Sheets (%s rows)", len(frame))
    return True


def _valuation_dashboard_frame(payload: dict[str, object]) -> pd.DataFrame:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    chart_rows = payload.get("chart_rows") if isinstance(payload.get("chart_rows"), list) else []
    sector_rows = payload.get("sector_context_rows") if isinstance(payload.get("sector_context_rows"), list) else []
    rows: list[list[object]] = [["VALUATION DASHBOARD", ""]]
    rows.append(["Metric", "Value"])
    for key, value in summary.items():
        rows.append([key, value])
    rows.extend([[], ["Chart Data"]])
    chart = pd.DataFrame(chart_rows)
    if chart.empty:
        chart = pd.DataFrame(columns=["date", "index_level", "index_200dma", "pe_ttm", "pe_200dma", "pe_5y_median", "pe_percentile_5y"])
    rows.append(chart.columns.tolist())
    rows.extend(chart.where(chart.notna(), "").values.tolist())
    rows.extend([[], ["SECTOR CONTEXT - Leading/Improving only; Rank = absolute RS rank across all sectors"]])
    sector = pd.DataFrame(sector_rows)
    if sector.empty:
        sector = pd.DataFrame(columns=["Rank", "Sector", "RS", "Momentum", "Quadrant", "Valuation vs 5Y Avg PE"])
    rows.append(sector.columns.tolist())
    rows.extend(sector.where(sector.notna(), "").values.tolist())
    width = max((len(row) for row in rows), default=1)
    normalized = [row + [""] * (width - len(row)) for row in rows]
    return pd.DataFrame(normalized)


def _chart_header_row(payload: dict[str, object]) -> int:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return len(summary) + 5


def _write_valuation_charts(manager: GoogleSheetsManager, sheet_name: str, payload: dict[str, object]) -> None:
    chart_rows = payload.get("chart_rows") if isinstance(payload.get("chart_rows"), list) else []
    if not chart_rows or not hasattr(manager, "replace_line_charts"):
        return
    chart = pd.DataFrame(chart_rows)
    if chart.empty or "date" not in chart.columns:
        return
    header_row_zero = _chart_header_row(payload) - 1
    start_row = header_row_zero
    end_row = header_row_zero + len(chart) + 1
    columns = list(chart.columns)

    def idx(name: str) -> int | None:
        return columns.index(name) if name in columns else None

    specs = []
    date_col = idx("date")
    if date_col is None:
        return
    for title, names, anchor_row in [
        ("Index Level vs Index 200DMA", ["index_level", "index_200dma"], 1),
        ("PE TTM vs PE 200DMA / PE 5Y Median", ["pe_ttm", "pe_200dma", "pe_5y_median"], 17),
        ("PE Percentile 5Y", ["pe_percentile_5y"], 33),
    ]:
        y_cols = [idx(name) for name in names if idx(name) is not None]
        if y_cols:
            specs.append(
                {
                    "title": title,
                    "start_row": start_row,
                    "end_row": end_row,
                    "x_col": date_col,
                    "y_cols": y_cols,
                    "anchor_row": anchor_row,
                    "anchor_col": 8,
                }
            )
    if specs:
        manager.replace_line_charts(sheet_name, chart_specs=specs)


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
    "publish_fundamental_watchlist",
    "publish_investigator",
    "publish_log_sheet",
    "publish_fundamental_dashboard",
    "publish_fundamental_valuation_dashboard",
]
