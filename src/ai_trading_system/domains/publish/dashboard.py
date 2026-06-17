"""Dashboard payload delivery adapters."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable

import pandas as pd

from ai_trading_system.domains.publish.channels.google_sheets import GoogleSheetsManager
from ai_trading_system.domains.publish.decision_bundle import (
    PublishDecisionBundle,
    build_publish_decision_bundle,
)
from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.domains.publish.publish_payloads import format_rows_for_channel
from ai_trading_system.domains.publish.channels.weekly_pdf import metrics as weekly_metrics
from ai_trading_system.ui.execution_api.services.readmodels.market_breadth import (
    load_operational_breadth_frame,
)

DAILY_REPORT_SHEET = "01_Daily_Report"
SECTOR_LEADERSHIP_SHEET = "04_Sector_Leadership"
MARKET_BREADTH_SHEET = "05_Market_Breadth"
INVESTIGATOR_SHEET = "06_Investigator"
DATA_BREADTH_SHEET = "_DATA_BREADTH"
DATA_SECTOR_HISTORY_SHEET = "_DATA_SECTOR_HISTORY"
DATA_INVESTIGATOR_SHEET = "_DATA_INVESTIGATOR"
VISIBLE_SHEET_MAX_ROWS = 60
DAILY_REPORT_MAX_ROWS = 140
VISIBLE_SHEET_MAX_COLS = 14
DATA_BREADTH_MAX_ROWS = 250
DATA_SECTOR_HISTORY_MAX_ROWS = 500
DATA_INVESTIGATOR_MAX_ROWS = 300
OPERATOR_TAB_ORDER = [
    DAILY_REPORT_SHEET,
    "03_Portfolio",
    SECTOR_LEADERSHIP_SHEET,
]
LEGACY_OPERATOR_TABS = [
    "DATA",
    "FILTER",
    "DAILY_GAINER",
    "SECTOR",
    "Stock Scan",
    "Sector Dashboard",
    "Portfolio Analysis",
    "Fundamental Watchlist",
    "Watchlist Current",
    "02_Watchlist_Current",
    MARKET_BREADTH_SHEET,
    "Publish_Log",
    "VALUATION_DASHBOARD",
    INVESTIGATOR_SHEET,
    "99_Run_Log",
]


def _frame(records: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    rows = list(records)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _resolve_sheet_name(payload: Dict[str, Any], run_date: str | None) -> str:
    candidate = run_date or str(payload.get("summary", {}).get("run_date") or "").strip()
    if not candidate:
        candidate = datetime.now().strftime("%Y-%m-%d")
    safe = "".join(ch for ch in candidate if ch.isalnum() or ch in ("-", "_", " ")).strip()
    return safe[:95] or datetime.now().strftime("%Y-%m-%d")


def _resolve_unique_sheet_name(manager: GoogleSheetsManager, base_name: str) -> str:
    if manager.get_worksheet(base_name) is None:
        manager.last_error = None
        return base_name
    for attempt in range(2, 100):
        candidate = f"{base_name} attempt {attempt}"
        if manager.get_worksheet(candidate) is None:
            manager.last_error = None
            return candidate[:95]
    return f"{base_name} {datetime.now().strftime('%H%M%S')}"[:95]


def _compact_summary_frame(summary: pd.DataFrame) -> pd.DataFrame:
    if summary is None or summary.empty:
        return pd.DataFrame(columns=["Metric", "Value"])
    row = summary.iloc[0].to_dict()
    metrics = [
        ("Run Date", row.get("Daily Market Insight")),
        ("Trust", row.get("Trust")),
        ("Breadth > 200DMA", row.get("Breadth > 200DMA")),
        ("Market State", row.get("Market State")),
        ("Direction Bias", row.get("Direction Bias")),
        ("Allowed Exposure", row.get("Allowed Exposure")),
        ("Regime Phase", row.get("Regime Phase")),
        ("Qualified Breakouts", row.get("Qualified breakouts")),
        ("Pattern Setups", row.get("Pattern setups")),
        ("Watchlist Candidates", row.get("Watchlist candidates")),
    ]
    return pd.DataFrame(
        [
            {"Metric": metric, "Value": "" if pd.isna(value) else value}
            for metric, value in metrics
            if value is not None and not (isinstance(value, str) and not value.strip())
        ]
    )


def _ranking_feedback_frame(feedback: dict[str, Any] | None) -> pd.DataFrame:
    columns = ["Signal", "Subject", "Evidence", "Action"]
    if not isinstance(feedback, dict) or feedback.get("status") in {None, "missing", "empty"}:
        return pd.DataFrame(
            [{"Signal": "tracker", "Subject": "ranking_feedback", "Evidence": "No mature trusted tracker data available", "Action": "insufficient_sample"}],
            columns=columns,
        )

    rows: list[dict[str, object]] = []
    rank_rows = [
        row for row in list(feedback.get("rank_bucket_rows") or [])
        if row.get("horizon") == "20d"
    ]
    by_bucket = {str(row.get("rank_bucket")): row for row in rank_rows}
    top10 = by_bucket.get("top-10", {}).get("avg_return")
    lower = by_bucket.get("rank-51-plus", {}).get("avg_return")
    if top10 is not None and lower is not None:
        edge = round(float(top10) - float(lower), 3)
        rows.append({
            "Signal": "rank_edge",
            "Subject": "top-10 vs rank-51-plus",
            "Evidence": f"20d avg edge {edge} pp",
            "Action": "backtest_required" if edge > 0 else "reduce_candidate",
        })

    factor_rows = [
        row for row in list(feedback.get("factor_ic_rows") or [])
        if row.get("horizon") == "20d" and row.get("ic") is not None
    ]
    factor_rows = sorted(factor_rows, key=lambda row: float(row.get("ic") or 0), reverse=True)
    for factor_row in factor_rows[:2]:
        rows.append({
            "Signal": "best_factor_ic",
            "Subject": factor_row.get("factor"),
            "Evidence": f"20d IC {factor_row.get('ic')} over {factor_row.get('rows')} rows",
            "Action": "backtest_required",
        })
    for factor_row in factor_rows[-2:]:
        if factor_row.get("signal") == "negative":
            rows.append({
                "Signal": "weak_factor_ic",
                "Subject": factor_row.get("factor"),
                "Evidence": f"20d IC {factor_row.get('ic')} over {factor_row.get('rows')} rows",
                "Action": "reduce_candidate",
            })

    for bucket_row in list(feedback.get("bucket_rows") or []):
        if bucket_row.get("horizon") == "20d" and bucket_row.get("interpretation") == "weak":
            rows.append({
                "Signal": "weak_bucket",
                "Subject": bucket_row.get("bucket"),
                "Evidence": f"20d avg {bucket_row.get('avg_return')}; win {bucket_row.get('win_rate_pct')}%",
                "Action": "gate_candidate",
            })

    for drift_row in list(feedback.get("drift_rows") or []):
        if drift_row.get("status") in {"warning", "critical"}:
            rows.append({
                "Signal": f"drift_{drift_row.get('status')}",
                "Subject": drift_row.get("factor"),
                "Evidence": f"recent IC {drift_row.get('recent_ic')} vs baseline {drift_row.get('baseline_ic')}",
                "Action": "reduce_candidate",
            })

    for recommendation in list(feedback.get("recommendations") or []):
        if len(rows) >= 8:
            break
        rows.append({
            "Signal": recommendation.get("category"),
            "Subject": recommendation.get("subject"),
            "Evidence": recommendation.get("evidence"),
            "Action": recommendation.get("decision"),
        })

    if not rows:
        rows.append({
            "Signal": "tracker",
            "Subject": "ranking_feedback",
            "Evidence": "No mature trusted tracker data available",
            "Action": "insufficient_sample",
        })
    rows.append({
        "Signal": "guardrail",
        "Subject": "production weights",
        "Evidence": "Observational only",
        "Action": "backtest before changing weights",
    })
    return pd.DataFrame(rows[:10], columns=columns)


def _to_numeric(df: pd.DataFrame, columns: list[str], places: int) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out.loc[:, col] = pd.to_numeric(out[col], errors="coerce").round(places)
    return out


def _sheet_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if pd.isna(value):
        return ""
    if isinstance(value, (int, float)):
        return value
    return str(value)


def _cap_visible_frame(frame: pd.DataFrame, *, rows: int | None = None) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    row_limit = rows if rows is not None else VISIBLE_SHEET_MAX_ROWS
    return frame.iloc[:row_limit, :VISIBLE_SHEET_MAX_COLS].copy()


def _normalise_grid(rows: list[list[Any]], *, max_rows: int = VISIBLE_SHEET_MAX_ROWS, max_cols: int = VISIBLE_SHEET_MAX_COLS) -> list[list[Any]]:
    clipped = [row[:max_cols] for row in rows[:max_rows]]
    if not clipped:
        clipped = [[]]
    normalized = [[_sheet_cell(value) for value in row] + [""] * max(0, max_cols - len(row)) for row in clipped]
    while len(normalized) < max_rows:
        normalized.append([""] * max_cols)
    return normalized


def _sections_to_grid(
    sections: list[tuple[str, pd.DataFrame]],
    *,
    max_rows: int = VISIBLE_SHEET_MAX_ROWS,
    max_cols: int = VISIBLE_SHEET_MAX_COLS,
) -> tuple[list[list[Any]], list[dict[str, int | str | None]]]:
    rows: list[list[Any]] = []
    layouts: list[dict[str, int | str | None]] = []
    for title, frame in sections:
        if len(rows) >= max_rows:
            break
        title_row = len(rows) + 1
        rows.append([title])
        safe = _cap_visible_frame(frame, rows=max(0, max_rows - len(rows) - 1))
        if safe.empty:
            rows.append(["No data available"])
            layouts.append({"title": title, "title_row": title_row, "header_row": None, "row_count": 0, "col_count": 1})
        else:
            header_row = len(rows) + 1
            rows.append(list(safe.columns))
            for row in safe.itertuples(index=False, name=None):
                if len(rows) >= max_rows:
                    break
                rows.append(list(row))
            layouts.append(
                {
                    "title_row": title_row,
                    "title": title,
                    "header_row": header_row,
                    "row_count": int(min(len(safe), max(0, len(rows) - header_row))),
                    "col_count": int(min(len(safe.columns), max_cols)),
                }
            )
        if len(rows) < max_rows:
            rows.append([])
    return _normalise_grid(rows, max_rows=max_rows, max_cols=max_cols), layouts


def _write_visible_grid_sheet(
    *,
    manager: GoogleSheetsManager,
    sheet_name: str,
    sections: list[tuple[str, pd.DataFrame]],
    extra_requests: list[dict[str, Any]] | None = None,
    extra_request_builder: Callable[[Any, list[dict[str, int | str | None]]], list[dict[str, Any]]] | None = None,
    max_rows: int = VISIBLE_SHEET_MAX_ROWS,
    max_cols: int = VISIBLE_SHEET_MAX_COLS,
) -> tuple[Any, list[dict[str, int | str | None]], int]:
    worksheet = manager.get_or_create_sheet(sheet_name, rows=max_rows, cols=max_cols)
    if worksheet is None:
        raise RuntimeError(f"Dashboard publish failed creating sheet '{sheet_name}': {manager.last_error or 'unknown error'}")
    grid, layouts = _sections_to_grid(sections, max_rows=max_rows, max_cols=max_cols)
    if hasattr(manager, "update_worksheet_values"):
        manager.update_worksheet_values(worksheet, grid, range_name="A1")
    else:
        raise RuntimeError("GoogleSheetsManager.update_worksheet_values is required")
    requests = _grid_layout_requests(worksheet, layouts, max_rows=max_rows, max_cols=max_cols)
    requests.extend(extra_requests or [])
    if extra_request_builder is not None:
        requests.extend(extra_request_builder(worksheet, layouts))
    if requests and hasattr(manager, "batch_update"):
        manager.batch_update({"requests": requests})
    non_empty_rows = sum(1 for row in grid if any(str(value).strip() for value in row))
    return worksheet, layouts, non_empty_rows


def _grid_layout_requests(
    worksheet: Any,
    section_layouts: list[dict[str, int | str | None]],
    *,
    max_rows: int = VISIBLE_SHEET_MAX_ROWS,
    max_cols: int = VISIBLE_SHEET_MAX_COLS,
) -> list[dict[str, Any]]:
    sheet_id = int(worksheet.id)
    requests: list[dict[str, Any]] = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "rowCount": max_rows,
                        "columnCount": max_cols,
                        "frozenRowCount": 1,
                    },
                },
                "fields": "gridProperties(rowCount,columnCount,frozenRowCount)",
            }
        },
    ]
    widths = [190, 130, 110, 110, 120, 160, 220, 180, 130, 130, 130, 130, 130, 130]
    for idx, width in enumerate(widths[:max_cols]):
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": idx, "endIndex": idx + 1},
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            }
        )
    for layout in section_layouts:
        title_idx = int(layout["title_row"] or 1) - 1
        requests.append(
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": title_idx, "endRowIndex": title_idx + 1, "startColumnIndex": 0, "endColumnIndex": max_cols},
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True, "fontSize": 11, "foregroundColor": {"red": 0.11, "green": 0.16, "blue": 0.26}},
                            "backgroundColor": {"red": 0.91, "green": 0.95, "blue": 0.99},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            }
        )
        if layout.get("header_row"):
            header_idx = int(layout["header_row"]) - 1
            requests.append(
                {
                    "repeatCell": {
                        "range": {"sheetId": sheet_id, "startRowIndex": header_idx, "endRowIndex": header_idx + 1, "startColumnIndex": 0, "endColumnIndex": int(layout.get("col_count") or VISIBLE_SHEET_MAX_COLS)},
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {"bold": True},
                                "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                }
            )
    return requests


def _write_hidden_data_sheet(
    manager: GoogleSheetsManager,
    sheet_name: str,
    frame: pd.DataFrame,
    *,
    max_rows: int,
    max_cols: int,
) -> Any | None:
    safe = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    safe = safe.iloc[:max_rows, :max_cols]
    if hasattr(manager, "write_hidden_data_sheet"):
        if not manager.write_hidden_data_sheet(sheet_name, safe, max_rows=max_rows, max_cols=max_cols):
            return None
        return manager.get_worksheet(sheet_name)
    worksheet = manager.get_or_create_sheet(sheet_name, rows=max_rows + 5, cols=max_cols)
    if worksheet is None:
        return None
    manager.write_dataframe(safe.fillna(""), sheet_name, include_header=True, clear_sheet=True)
    return worksheet


def _combine_frames(frames: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    combined: list[pd.DataFrame] = []
    for section, frame in frames:
        safe = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        if safe.empty:
            safe = pd.DataFrame([{"note": "No data available"}])
        safe.insert(0, "section", section)
        combined.append(safe)
    return pd.concat(combined, ignore_index=True, sort=False) if combined else pd.DataFrame()


def _last_5y(frame: pd.DataFrame, date_col: str = "Date") -> pd.DataFrame:
    if frame.empty or date_col not in frame.columns:
        return frame.copy()
    out = frame.copy()
    out.loc[:, date_col] = pd.to_datetime(out[date_col], errors="coerce")
    latest = out[date_col].max()
    if pd.isna(latest):
        return out
    cutoff = latest - pd.DateOffset(years=5)
    return out.loc[out[date_col].ge(cutoff)].reset_index(drop=True)


def _market_breadth_snapshot_frame(breadth: pd.DataFrame) -> pd.DataFrame:
    columns = ["Metric", "Min", "Current", "Max", "Percentile", "Low", "Mid", "High", "Marker"]
    if breadth is None or breadth.empty:
        return pd.DataFrame(columns=columns)
    recent = _last_5y(breadth)
    specs = [
        ("% Above SMA200", ["PctAbove200"]),
        ("PE 5Y Percentile", ["PEPctile5YSMA20", "PEPctile5Y"]),
        ("New High / Low", ["HighLowRatioSMA10", "HighLowRatio", "NetNewHighs", "NetNewHighsPct"]),
    ]
    rows: list[dict[str, Any]] = []
    for metric, candidates in specs:
        column = next((name for name in candidates if name in recent.columns), None)
        if column is None:
            continue
        series = pd.to_numeric(recent[column], errors="coerce").dropna()
        if series.empty:
            continue
        current = float(series.iloc[-1])
        min_value = float(series.min())
        max_value = float(series.max())
        span = max_value - min_value
        pct = 50.0 if span == 0 else max(0.0, min(100.0, ((current - min_value) / span) * 100.0))
        marker = "Low" if pct < 33 else "Mid" if pct < 67 else "High"
        rows.append(
            {
                "Metric": metric,
                "Min": round(min_value, 2),
                "Current": round(current, 2),
                "Max": round(max_value, 2),
                "Percentile": round(pct, 1),
                "Low": "*" if marker == "Low" else "",
                "Mid": "*" if marker == "Mid" else "",
                "High": "*" if marker == "High" else "",
                "Marker": marker,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _breadth_snapshot_format_requests(worksheet: Any, section_layouts: list[dict[str, int | str | None]]) -> list[dict[str, Any]]:
    layout = next((item for item in section_layouts if item.get("title") == "MARKET BREADTH SNAPSHOT"), None)
    if not layout or not layout.get("header_row"):
        return []
    sheet_id = int(worksheet.id)
    header_row = int(layout["header_row"])
    row_count = int(layout.get("row_count") or 0)
    start = header_row
    end = header_row + row_count
    marker_col = 5
    requests = []
    colors = [
        {"red": 0.95, "green": 0.55, "blue": 0.52},
        {"red": 0.98, "green": 0.86, "blue": 0.40},
        {"red": 0.42, "green": 0.76, "blue": 0.50},
    ]
    for idx, color in enumerate(colors):
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start,
                        "endRowIndex": end,
                        "startColumnIndex": marker_col + idx,
                        "endColumnIndex": marker_col + idx + 1,
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }
        )
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start,
                    "endRowIndex": end,
                    "startColumnIndex": marker_col,
                    "endColumnIndex": marker_col + 3,
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "textFormat": {"bold": True},
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,textFormat)",
            }
        }
    )
    return requests


def _sector_rotation_frame(frame: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "industry", "rs_ratio", "rs_momentum", "quadrant", "alpha_20d"]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame(
        {
            "date": frame.get("date", ""),
            "industry": frame.get("industry", frame.get("Sector", frame.get("sector", ""))),
            "rs_ratio": pd.to_numeric(frame.get("rs_ratio", frame.get("RS", "")), errors="coerce"),
            "rs_momentum": pd.to_numeric(frame.get("rs_momentum", frame.get("Momentum", "")), errors="coerce"),
            "quadrant": frame.get("quadrant", frame.get("Quadrant", "")),
            "alpha_20d": pd.to_numeric(frame.get("alpha_20d", pd.Series(pd.NA, index=frame.index)), errors="coerce"),
        }
    )
    out = out.dropna(subset=["rs_ratio", "rs_momentum"]).copy()
    if "date" in out.columns:
        out.loc[:, "date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        out = out.sort_values(["date", "industry"], ascending=[False, True], na_position="last", kind="stable")
    return out[columns].reset_index(drop=True)


def _sector_rotation_latest(frame: pd.DataFrame) -> pd.DataFrame:
    columns = ["Industry", "RS Ratio", "RS Momentum", "Quadrant", "Alpha20D"]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    source = frame.copy()
    latest_date = source["date"].dropna().max() if "date" in source.columns else None
    if latest_date:
        source = source.loc[source["date"].eq(latest_date)].copy()
    out = pd.DataFrame(
        {
            "Industry": source.get("industry", ""),
            "RS Ratio": pd.to_numeric(source.get("rs_ratio", ""), errors="coerce").round(2),
            "RS Momentum": pd.to_numeric(source.get("rs_momentum", ""), errors="coerce").round(2),
            "Quadrant": source.get("quadrant", ""),
            "Alpha20D": pd.to_numeric(source.get("alpha_20d", ""), errors="coerce").round(3),
        }
    )
    return out.sort_values(["Quadrant", "RS Ratio"], ascending=[True, False], na_position="last", kind="stable").head(25).reset_index(drop=True)


def _sector_quadrant_guide_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Quadrant": "Leading", "Relative Strength": "Above 100", "Momentum": "Above 100", "Operator Read": "Leadership with positive momentum"},
            {"Quadrant": "Weakening", "Relative Strength": "Above 100", "Momentum": "Below 100", "Operator Read": "Leadership losing momentum"},
            {"Quadrant": "Improving", "Relative Strength": "Below 100", "Momentum": "Above 100", "Operator Read": "Early rotation candidate"},
            {"Quadrant": "Lagging", "Relative Strength": "Below 100", "Momentum": "Below 100", "Operator Read": "Avoid until strength improves"},
        ]
    )


def _sector_rotation_chart_requests(
    *,
    manager: GoogleSheetsManager,
    visible_worksheet: Any,
    data_worksheet: Any | None,
    rotation_frame: pd.DataFrame,
) -> list[dict[str, Any]]:
    if data_worksheet is None or rotation_frame.empty:
        return []
    latest = _sector_rotation_latest(rotation_frame)
    if latest.empty:
        return []
    visible_sheet_id = int(visible_worksheet.id)
    data_sheet_id = int(data_worksheet.id)
    latest_date = rotation_frame["date"].dropna().max() if "date" in rotation_frame.columns else None
    latest_rows = rotation_frame.loc[rotation_frame["date"].eq(latest_date)].copy() if latest_date else rotation_frame.copy()
    latest_rows = latest_rows.head(60)
    start_idx = 0
    end_idx = min(len(latest_rows) + 1, DATA_SECTOR_HISTORY_MAX_ROWS + 1)
    return _existing_chart_delete_requests(manager, visible_sheet_id) + [
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Sector Rotation: Relative Strength vs Momentum",
                        "subtitle": f"Latest sector points{f' as of {latest_date}' if latest_date else ''}",
                        "basicChart": {
                            "chartType": "SCATTER",
                            "legendPosition": "NO_LEGEND",
                            "headerCount": 1,
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "Relative Strength / RS Ratio"},
                                {"position": "LEFT_AXIS", "title": "Momentum / RS Momentum"},
                            ],
                            "domains": [{"domain": _chart_range(data_sheet_id, start_idx, end_idx, 2)}],
                            "series": [_chart_series(data_sheet_id, start_idx, end_idx, 3)],
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": visible_sheet_id, "rowIndex": 8, "columnIndex": 0},
                            "offsetXPixels": 8,
                            "offsetYPixels": 8,
                            "widthPixels": 900,
                            "heightPixels": 430,
                        }
                    },
                }
            }
        }
    ]


def _minimal_sector_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Sector", "Rank", "RS", "Momentum", "Quadrant"])
    out = pd.DataFrame(
        {
            "Sector": df.get("Sector", df.get("sector", "")),
            "Rank": df.get("RS_rank", df.get("rank", "")),
            "RS": df.get("RS", df.get("rs", "")),
            "Momentum": df.get("Momentum", df.get("momentum", "")),
            "Quadrant": df.get("Quadrant", df.get("quadrant", df.get("state", ""))),
        }
    )
    out = _to_numeric(out, ["Rank"], 0)
    out = _to_numeric(out, ["RS", "Momentum"], 2)
    out = out.sort_values(["Rank", "RS"], ascending=[True, False], na_position="last")
    return out.reset_index(drop=True).copy()


def _minimal_rank_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Symbol", "Score", "RS", "Close", "TradingView"])
    out = pd.DataFrame(
        {
            "Symbol": df.get("symbol_id", ""),
            "Score": df.get("composite_score", ""),
            "RS": df.get("rel_strength_score", ""),
            "Close": df.get("close", ""),
        }
    )
    out = _to_numeric(out, ["Score", "RS", "Close"], 2)
    out = out.sort_values(["Score", "RS"], ascending=[False, False], na_position="last")
    out = out.head(25).reset_index(drop=True).copy()
    out.loc[:, "TradingView"] = out["Symbol"].astype(str).map(
        lambda symbol: f"https://www.tradingview.com/chart/?symbol=NSE:{symbol}"
        if symbol and symbol.lower() != "nan"
        else ""
    )
    return out


def _weekly_move_frame(df: pd.DataFrame) -> pd.DataFrame:
    moves = weekly_metrics.volume_delivery_movers(df, n=25)
    if moves.empty:
        return pd.DataFrame(columns=["Symbol", "Sector", "Ret5", "Ret20", "Delivery", "VolZ", "Score", "Stage"])
    out = pd.DataFrame(
        {
            "Symbol": moves.get("symbol_id", ""),
            "Sector": moves.get("sector_name", ""),
            "Ret5": moves.get("return_5", ""),
            "Ret20": moves.get("return_20", ""),
            "Delivery": moves.get("delivery_pct", ""),
            "VolZ": moves.get("volume_zscore_20", ""),
            "Score": moves.get("composite_score", ""),
            "Stage": moves.get("stage2_label", ""),
        }
    )
    return _to_numeric(out, ["Ret5", "Ret20", "Delivery", "VolZ", "Score"], 2)


def _volume_shocker_frame(df: pd.DataFrame) -> pd.DataFrame:
    shockers = weekly_metrics.unusual_volume_shockers(df, n=25)
    if shockers.empty:
        return pd.DataFrame(columns=["Symbol", "Sector", "VolZ", "Delivery", "Ret5", "Ret20", "Score"])
    out = pd.DataFrame(
        {
            "Symbol": shockers.get("symbol_id", ""),
            "Sector": shockers.get("sector_name", ""),
            "VolZ": shockers.get("volume_zscore_20", ""),
            "Delivery": shockers.get("delivery_pct", ""),
            "Ret5": shockers.get("return_5", ""),
            "Ret20": shockers.get("return_20", ""),
            "Score": shockers.get("composite_score", ""),
        }
    )
    return _to_numeric(out, ["VolZ", "Delivery", "Ret5", "Ret20", "Score"], 2)


def _rank_mover_frame(current: pd.DataFrame, prior: pd.DataFrame | None = None) -> pd.DataFrame:
    improvers, decliners = weekly_metrics.compute_rank_movers(current, prior if isinstance(prior, pd.DataFrame) else pd.DataFrame(), top_n=15)
    frames = []
    for label, frame in (("Improver", improvers), ("Decliner", decliners)):
        if frame.empty:
            continue
        out = pd.DataFrame(
            {
                "Move": label,
                "Symbol": frame.get("symbol_id", ""),
                "Sector": frame.get("sector_name", ""),
                "RankDelta": frame.get("rank_change", ""),
                "ScoreDelta": frame.get("score_change", ""),
                "Ret5": frame.get("return_5", ""),
                "Score": frame.get("composite_score", ""),
            }
        )
        frames.append(out)
    if not frames:
        return pd.DataFrame(columns=["Move", "Symbol", "Sector", "RankDelta", "ScoreDelta", "Ret5", "Score"])
    return _to_numeric(pd.concat(frames, ignore_index=True), ["RankDelta", "ScoreDelta", "Ret5", "Score"], 2)


def _failed_breakout_frame(failed: pd.DataFrame | None) -> pd.DataFrame:
    if failed is None or failed.empty:
        return pd.DataFrame(columns=["Symbol", "Sector", "TriggerRun", "Trigger", "Close", "DropPct", "Tier"])
    out = pd.DataFrame(
        {
            "Symbol": failed.get("symbol_id", ""),
            "Sector": failed.get("sector_name", ""),
            "TriggerRun": failed.get("trigger_run_id", ""),
            "Trigger": failed.get("trigger_level", ""),
            "Close": failed.get("current_close", ""),
            "DropPct": failed.get("drop_pct", ""),
            "Tier": failed.get("trigger_tier", ""),
        }
    )
    return _to_numeric(out, ["Trigger", "Close", "DropPct"], 2)


def _investigator_frame(scores: pd.DataFrame | None) -> pd.DataFrame:
    if scores is None or scores.empty:
        return pd.DataFrame(columns=["Symbol", "Verdict", "Score", "Status", "Move", "Delivery", "VolRatio", "Rank"])
    out = pd.DataFrame(
        {
            "Symbol": scores.get("symbol_id", ""),
            "Verdict": scores.get("verdict", ""),
            "Score": scores.get("final_score", ""),
            "Status": scores.get("status", ""),
            "Move": scores.get("move_tag", ""),
            "Delivery": scores.get("delivery_pct", ""),
            "VolRatio": scores.get("volume_ratio_20", ""),
            "Rank": scores.get("rank_position", ""),
        }
    )
    out = _to_numeric(out, ["Score", "Delivery", "VolRatio", "Rank"], 2)
    return out.sort_values(["Score", "Symbol"], ascending=[False, True], na_position="last").head(25).reset_index(drop=True)


def _investigator_repeat_frame(repeat: pd.DataFrame | None) -> pd.DataFrame:
    if repeat is None or repeat.empty:
        return pd.DataFrame(columns=["Symbol", "Appear20D", "RepeatScore", "PriceProgress", "RankChange", "VolumeEscalation", "Priority"])
    out = pd.DataFrame(
        {
            "Symbol": repeat.get("symbol_id", ""),
            "Appear20D": repeat.get("appearance_count_20d", ""),
            "RepeatScore": repeat.get("repeat_score", ""),
            "PriceProgress": repeat.get("price_progression_pct", ""),
            "RankChange": repeat.get("rank_change_20d", ""),
            "VolumeEscalation": repeat.get("volume_escalation", ""),
            "Priority": repeat.get("high_priority_repeat", ""),
        }
    )
    out = _to_numeric(out, ["Appear20D", "RepeatScore", "PriceProgress", "RankChange"], 2)
    priority = out["Priority"].fillna("").astype(str).str.lower().isin({"true", "1", "yes"}).astype(int)
    out = out.assign(_PrioritySort=priority)
    return (
        out.sort_values(["_PrioritySort", "RepeatScore", "Symbol"], ascending=[False, False, True], na_position="last")
        .drop(columns=["_PrioritySort"])
        .head(25)
        .reset_index(drop=True)
    )


def _investigator_active_frame(active: pd.DataFrame | None) -> pd.DataFrame:
    if active is None or active.empty:
        return pd.DataFrame(columns=["Symbol", "Status", "Verdict", "Current", "Peak", "Appear20D", "DaysStale", "PriceVsFirst"])
    out = pd.DataFrame(
        {
            "Symbol": active.get("symbol_id", ""),
            "Status": active.get("status", ""),
            "Verdict": active.get("verdict", ""),
            "Current": active.get("score_current", ""),
            "Peak": active.get("score_peak", ""),
            "Appear20D": active.get("appearance_count_20d", ""),
            "DaysStale": active.get("days_since_last_seen", ""),
            "PriceVsFirst": active.get("price_vs_first_trigger_pct", ""),
        }
    )
    out = _to_numeric(out, ["Current", "Peak", "Appear20D", "DaysStale", "PriceVsFirst"], 2)
    verdict_order = {"HIGH_CONVICTION": 0, "MEDIUM_CONVICTION": 1, "WATCH_ONLY": 2, "NOISE_TRAP": 3}
    out = out.assign(_VerdictSort=out["Verdict"].fillna("").astype(str).str.upper().map(verdict_order).fillna(99))
    return (
        out.sort_values(["_VerdictSort", "Current", "Peak", "Appear20D", "Symbol"], ascending=[True, False, False, False, True], na_position="last")
        .drop(columns=["_VerdictSort"])
        .head(25)
        .reset_index(drop=True)
    )


def _investigator_trap_frame(traps: pd.DataFrame | None) -> pd.DataFrame:
    if traps is None or traps.empty:
        return pd.DataFrame(columns=["Symbol", "Verdict", "Score", "Trap", "Delivery", "Rank"])
    out = pd.DataFrame(
        {
            "Symbol": traps.get("symbol_id", ""),
            "Verdict": traps.get("verdict", ""),
            "Score": traps.get("final_score", ""),
            "Trap": traps.get("drop_reason", traps.get("move_tag", "")),
            "Delivery": traps.get("delivery_pct", ""),
            "Rank": traps.get("rank_position", ""),
        }
    )
    return _to_numeric(out, ["Score", "Delivery", "Rank"], 2).head(25).reset_index(drop=True)


def _pattern_frame(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Symbol", "Pattern", "State", "Tier", "Score", "Trigger", "VolRatio", "Stage"])
    out = pd.DataFrame(
        {
            "Symbol": df.get("symbol_id", ""),
            "Pattern": df.get("pattern_family", ""),
            "State": df.get("pattern_state", ""),
            "Tier": df.get("pattern_operational_tier", ""),
            "Score": df.get("pattern_score", ""),
            "Trigger": df.get("breakout_level", ""),
            "VolRatio": df.get("volume_ratio_20", ""),
            "Stage": df.get("stage2_label", ""),
        }
    )
    out = _to_numeric(out, ["Score", "Trigger", "VolRatio"], 2)
    if "Score" in out.columns:
        out = out.sort_values("Score", ascending=False, na_position="last")
    return out.head(25).reset_index(drop=True)


def _minimal_breakout_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Symbol", "Setup", "State", "Tier", "Score", "TradingView"])
    out = pd.DataFrame(
        {
            "Symbol": df.get("symbol_id", ""),
            "Setup": df.get("taxonomy_family", df.get("setup_family", df.get("execution_label", ""))),
            "State": df.get("breakout_state", "watchlist"),
            "Tier": df.get("candidate_tier", ""),
            "Score": df.get("breakout_score", df.get("setup_quality", "")),
        }
    )
    out = _to_numeric(out, ["Score"], 2)
    # Explicitly keep all rows; no breakout-state filtering.
    sort_cols = [c for c in ["Score", "Symbol"] if c in out.columns]
    out = out.sort_values(sort_cols, ascending=[False, True], na_position="last")
    out = out.reset_index(drop=True).copy()
    out.loc[:, "TradingView"] = out["Symbol"].astype(str).map(
        lambda symbol: f"https://www.tradingview.com/chart/?symbol=NSE:{symbol}"
        if symbol and symbol.lower() != "nan"
        else ""
    )
    return out


def _load_operational_breadth(project_root: Path) -> pd.DataFrame:
    breadth_df = load_operational_breadth_frame(project_root)
    if breadth_df.empty:
        return pd.DataFrame(
            columns=[
                "Date",
                "PctAbove200",
                "New52WHighs",
                "New52WLows",
                "HighLowRatio",
                "HighLowRatioSMA10",
                "Advancers",
                "Decliners",
                "Unchanged",
                "ADNet",
                "ADPct",
                "ADPctSMA10",
                "ADPctSMA20",
                "ADPctSum63",
                "ADZ252",
                "ADLine",
                "NetNewHighs",
                "NetNewHighsPct",
                "IndexLevel",
                "PEPctile5Y",
                "PEPctile5YSMA20",
            ]
        )
    return breadth_df.rename(
        columns={
            "trade_date": "Date",
            "pct_above_sma200": "PctAbove200",
            "new_52w_highs": "New52WHighs",
            "new_52w_lows": "New52WLows",
            "high_low_ratio": "HighLowRatio",
            "high_low_ratio_sma10": "HighLowRatioSMA10",
            "advancers": "Advancers",
            "decliners": "Decliners",
            "unchanged": "Unchanged",
            "ad_net": "ADNet",
            "ad_pct": "ADPct",
            "ad_pct_sma10": "ADPctSMA10",
            "ad_pct_sma20": "ADPctSMA20",
            "ad_pct_sum63": "ADPctSum63",
            "ad_z252": "ADZ252",
            "ad_line": "ADLine",
            "net_new_highs": "NetNewHighs",
            "net_new_highs_pct": "NetNewHighsPct",
            "index_level": "IndexLevel",
            "pe_pctile_5y": "PEPctile5Y",
            "pe_pctile_5y_sma20": "PEPctile5YSMA20",
        }
    )[
        [
            "Date",
            "PctAbove200",
            "New52WHighs",
            "New52WLows",
            "HighLowRatio",
            "HighLowRatioSMA10",
            "Advancers",
            "Decliners",
            "Unchanged",
            "ADNet",
            "ADPct",
            "ADPctSMA10",
            "ADPctSMA20",
            "ADPctSum63",
            "ADZ252",
            "ADLine",
            "NetNewHighs",
            "NetNewHighsPct",
            "IndexLevel",
            "PEPctile5Y",
            "PEPctile5YSMA20",
        ]
    ].reset_index(drop=True)


def _chart_range(sheet_id: int, start_idx: int, end_idx: int, column: int) -> dict[str, Any]:
    return {
        "sourceRange": {
            "sources": [
                {
                    "sheetId": sheet_id,
                    "startRowIndex": start_idx,
                    "endRowIndex": end_idx,
                    "startColumnIndex": column,
                    "endColumnIndex": column + 1,
                }
            ]
        }
    }


def _chart_series(sheet_id: int, start_idx: int, end_idx: int, column: int, axis: str = "LEFT_AXIS") -> dict[str, Any]:
    return {
        "series": _chart_range(sheet_id, start_idx, end_idx, column),
        "targetAxis": axis,
    }


def _existing_chart_delete_requests(manager: GoogleSheetsManager, sheet_id: int) -> list[dict[str, Any]]:
    if manager.spreadsheet is None:
        return []
    try:
        metadata = manager.fetch_sheet_metadata() if hasattr(manager, "fetch_sheet_metadata") else {}
    except Exception:
        return []
    requests: list[dict[str, Any]] = []
    for sheet in metadata.get("sheets", []):
        properties = sheet.get("properties", {})
        if int(properties.get("sheetId", -1)) != int(sheet_id):
            continue
        for chart in sheet.get("charts", []):
            chart_id = chart.get("chartId")
            if chart_id is not None:
                requests.append({"deleteEmbeddedObject": {"objectId": chart_id}})
    return requests


def _breadth_chart_requests(
    *,
    manager: GoogleSheetsManager,
    visible_worksheet: Any,
    data_worksheet: Any | None,
    data_rows: int,
) -> list[dict[str, Any]]:
    if data_worksheet is None or data_rows < 2:
        return []
    visible_sheet_id = int(visible_worksheet.id)
    data_sheet_id = int(data_worksheet.id)
    start_idx = 0
    end_idx = min(data_rows + 1, DATA_BREADTH_MAX_ROWS + 1)
    domain = [{"domain": _chart_range(data_sheet_id, start_idx, end_idx, 0)}]

    def request(
        *,
        title: str,
        chart_type: str,
        series: list[dict[str, Any]],
        anchor_row: int,
        left_title: str,
        right_title: str | None = None,
    ) -> dict[str, Any]:
        axes = [
            {"position": "BOTTOM_AXIS", "title": "Date"},
            {"position": "LEFT_AXIS", "title": left_title},
        ]
        if right_title:
            axes.append({"position": "RIGHT_AXIS", "title": right_title})
        return {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": title,
                        "basicChart": {
                            "chartType": chart_type,
                            "legendPosition": "BOTTOM_LEGEND",
                            "headerCount": 1,
                            "axis": axes,
                            "domains": domain,
                            "series": series,
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": visible_sheet_id, "rowIndex": anchor_row, "columnIndex": 7},
                            "offsetXPixels": 16,
                            "offsetYPixels": 8,
                            "widthPixels": 760,
                            "heightPixels": 260,
                        }
                    },
                }
            }
        }

    return _existing_chart_delete_requests(manager, visible_sheet_id) + [
        request(
            title="Operational Long-Term Breadth (% Above SMA200 and PE 5Y Percentile SMA20)",
            chart_type="LINE",
            series=[
                _chart_series(data_sheet_id, start_idx, end_idx, 1),
                _chart_series(data_sheet_id, start_idx, end_idx, 11, "RIGHT_AXIS"),
            ],
            anchor_row=2,
            left_title="% Above SMA200",
            right_title="PE 5Y percentile SMA20",
        ),
        request(
            title="New 52W Highs / Lows and Ratio SMA10",
            chart_type="COLUMN",
            series=[
                _chart_series(data_sheet_id, start_idx, end_idx, 2),
                _chart_series(data_sheet_id, start_idx, end_idx, 3),
                {**_chart_series(data_sheet_id, start_idx, end_idx, 5, "RIGHT_AXIS"), "type": "LINE"},
            ],
            anchor_row=20,
            left_title="New highs / lows",
            right_title="High / low ratio SMA10",
        ),
    ]


def _cleanup_operator_workbook(manager: GoogleSheetsManager) -> dict[str, Any]:
    cleanup: dict[str, Any] = {}
    if hasattr(manager, "delete_worksheets"):
        cleanup["legacy_tabs"] = manager.delete_worksheets(LEGACY_OPERATOR_TABS)
    if hasattr(manager, "prune_date_named_worksheets"):
        cleanup["date_tabs"] = manager.prune_date_named_worksheets(keep=0)
    if hasattr(manager, "reorder_worksheets"):
        cleanup["reordered"] = manager.reorder_worksheets(OPERATOR_TAB_ORDER)
    return cleanup


def publish_dashboard_payload(
    payload: Dict[str, Any],
    *,
    project_root: str | Path | None = None,
    run_date: str | None = None,
    ranked_df: pd.DataFrame | None = None,
    breakout_df: pd.DataFrame | None = None,
    sector_df: pd.DataFrame | None = None,
    prior_ranked_df: pd.DataFrame | None = None,
    failed_breakouts_df: pd.DataFrame | None = None,
    pattern_df: pd.DataFrame | None = None,
    watchlist_df: pd.DataFrame | None = None,
    candidate_tracker_df: pd.DataFrame | None = None,
    investigator_scores_df: pd.DataFrame | None = None,
    investigator_repeat_df: pd.DataFrame | None = None,
    investigator_active_df: pd.DataFrame | None = None,
    investigator_trap_df: pd.DataFrame | None = None,
    sector_rotation_df: pd.DataFrame | None = None,
    decision_bundle: PublishDecisionBundle | None = None,
    ranking_feedback: dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Write compact operator workbook tabs without using Sheets as storage."""
    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        message = manager.last_error or "spreadsheet unavailable"
        raise RuntimeError(f"Dashboard publish failed: {message}")

    base_sheet_name = _resolve_sheet_name(payload, run_date=run_date)
    sheet_name = DAILY_REPORT_SHEET

    source_ranked = ranked_df if isinstance(ranked_df, pd.DataFrame) and not ranked_df.empty else _frame(payload.get("ranked_signals", []))
    source_ranked = pd.DataFrame(
        format_rows_for_channel(source_ranked.to_dict(orient="records") if isinstance(source_ranked, pd.DataFrame) else [], "dashboard")["rows"]
    )
    source_breakout = breakout_df if isinstance(breakout_df, pd.DataFrame) and not breakout_df.empty else _frame(payload.get("breakout_scan", []))
    source_sector = sector_df if isinstance(sector_df, pd.DataFrame) and not sector_df.empty else _frame(payload.get("sector_dashboard", []))
    source_sector_rotation = sector_rotation_df if isinstance(sector_rotation_df, pd.DataFrame) and not sector_rotation_df.empty else pd.DataFrame()
    source_watchlist = watchlist_df if isinstance(watchlist_df, pd.DataFrame) and not watchlist_df.empty else _frame(payload.get("watchlist", []))

    rank_min = _minimal_rank_frame(source_ranked)
    breakout_min = _minimal_breakout_frame(source_breakout)
    pattern_min = _pattern_frame(pattern_df)
    weekly_moves = _weekly_move_frame(source_ranked)
    failed_breakouts = _failed_breakout_frame(failed_breakouts_df)
    investigator_today = _investigator_frame(investigator_scores_df)
    investigator_repeat = _investigator_repeat_frame(investigator_repeat_df)
    investigator_active = _investigator_active_frame(investigator_active_df)
    investigator_traps = _investigator_trap_frame(investigator_trap_df)
    events_index = _frame(payload.get("events_index", []))
    breadth = _load_operational_breadth(Path(project_root) if project_root else Path(__file__).resolve().parents[1])
    bundle = decision_bundle or build_publish_decision_bundle(
        run_date=run_date or payload.get("summary", {}).get("run_date") or base_sheet_name,
        ranked_signals=source_ranked,
        breakout_scan=source_breakout,
        pattern_scan=pattern_df,
        sector_dashboard=source_sector,
        event_frame=events_index,
        breadth_frame=breadth,
        watchlist_frame=source_watchlist,
        candidate_tracker_frame=candidate_tracker_df,
        trust_status=str(payload.get("summary", {}).get("data_trust_status") or payload.get("data_trust", {}).get("status") or "unknown"),
        failed_breakouts=failed_breakouts_df,
        market_regime_phase=payload.get("market_regime_phase", {}),
    )

    summary = bundle.run_summary
    breadth_snapshot = _market_breadth_snapshot_frame(breadth)
    daily_sections = [
        ("RUN SUMMARY", summary),
        ("DAILY SUMMARY", _compact_summary_frame(summary)),
        ("TODAY'S DECISION SHORTLIST", bundle.watchlist_candidates),
        ("MARKET BREADTH SNAPSHOT", breadth_snapshot),
        ("PATTERN SETUPS", bundle.pattern_setups if not bundle.pattern_setups.empty else pattern_min),
        ("ACTIVE INVESTIGATOR LIST", investigator_active),
        ("TOP RANKED", bundle.top_ranked if not bundle.top_ranked.empty else rank_min),
        ("RANKING FEEDBACK", _ranking_feedback_frame(ranking_feedback)),
        ("BREAKOUTS (all, unfiltered)", breakout_min),
        ("MARKET MOVES SNAPSHOT", bundle.market_moves if not bundle.market_moves.empty else weekly_moves),
        ("FAILED BREAKOUTS", bundle.failed_breakouts if not bundle.failed_breakouts.empty else failed_breakouts),
    ]
    _daily_worksheet, _daily_layouts, daily_rows = _write_visible_grid_sheet(
        manager=manager,
        sheet_name=sheet_name,
        sections=daily_sections,
        extra_request_builder=_breadth_snapshot_format_requests,
        max_rows=DAILY_REPORT_MAX_ROWS,
    )

    sector_rotation = _sector_rotation_frame(source_sector_rotation)
    sector_hidden_frame = sector_rotation if not sector_rotation.empty else source_sector
    sector_data_worksheet = _write_hidden_data_sheet(
        manager=manager,
        sheet_name=DATA_SECTOR_HISTORY_SHEET,
        frame=sector_hidden_frame,
        max_rows=DATA_SECTOR_HISTORY_MAX_ROWS,
        max_cols=VISIBLE_SHEET_MAX_COLS,
    )
    sector_min = _minimal_sector_frame(source_sector)
    sector_sections = (
        [
            ("SECTOR ROTATION", _sector_rotation_latest(sector_rotation)),
            ("QUADRANT GUIDE", _sector_quadrant_guide_frame()),
        ]
        if not sector_rotation.empty
        else [
            ("SECTOR LEADERSHIP", bundle.sector_leaders),
            ("SECTOR CONTEXT", sector_min),
        ]
    )
    sector_chart_requests = _sector_rotation_chart_requests(
        manager=manager,
        visible_worksheet=manager.get_or_create_sheet(SECTOR_LEADERSHIP_SHEET, rows=VISIBLE_SHEET_MAX_ROWS, cols=VISIBLE_SHEET_MAX_COLS),
        data_worksheet=sector_data_worksheet,
        rotation_frame=sector_rotation,
    ) if not sector_rotation.empty else []
    _sector_worksheet, _sector_layouts, _sector_rows = _write_visible_grid_sheet(
        manager=manager,
        sheet_name=SECTOR_LEADERSHIP_SHEET,
        sections=sector_sections,
        extra_requests=sector_chart_requests,
    )

    investigator_detail = _combine_frames(
        [
            ("STOCK INVESTIGATOR", investigator_today),
            ("INVESTIGATOR REPEAT ACCUMULATION", investigator_repeat),
            ("ACTIVE INVESTIGATOR LIST", investigator_active),
            ("INVESTIGATOR TRAP LIST", investigator_traps),
        ]
    )
    _write_hidden_data_sheet(
        manager=manager,
        sheet_name=DATA_INVESTIGATOR_SHEET,
        frame=investigator_detail,
        max_rows=DATA_INVESTIGATOR_MAX_ROWS,
        max_cols=VISIBLE_SHEET_MAX_COLS,
    )

    breadth_cols = [
        "Date",
        "PctAbove200",
        "New52WHighs",
        "New52WLows",
        "HighLowRatio",
        "HighLowRatioSMA10",
        "Advancers",
        "Decliners",
        "ADLine",
        "IndexLevel",
        "PEPctile5Y",
        "PEPctile5YSMA20",
    ]
    breadth_summary = breadth[[col for col in breadth_cols if col in breadth.columns]].tail(DATA_BREADTH_MAX_ROWS).reset_index(drop=True)
    breadth_data_worksheet = _write_hidden_data_sheet(
        manager=manager,
        sheet_name=DATA_BREADTH_SHEET,
        frame=breadth_summary,
        max_rows=DATA_BREADTH_MAX_ROWS,
        max_cols=VISIBLE_SHEET_MAX_COLS,
    )
    _ = breadth_data_worksheet

    cleanup = _cleanup_operator_workbook(manager)

    logger.info("Dashboard payload published to operator workbook tab '%s'", sheet_name)
    quota_meta = manager.quota_metadata() if hasattr(manager, "quota_metadata") else {}
    return {
        "sheet_name": sheet_name,
        "base_sheet_name": base_sheet_name,
        "rows_written": int(daily_rows),
        "sector_sheet_name": SECTOR_LEADERSHIP_SHEET,
        "breadth_sheet_name": DAILY_REPORT_SHEET,
        "investigator_sheet_name": DATA_INVESTIGATOR_SHEET,
        "hidden_data_sheets": [DATA_BREADTH_SHEET, DATA_SECTOR_HISTORY_SHEET, DATA_INVESTIGATOR_SHEET],
        "cleanup": cleanup,
        **quota_meta,
    }


__all__ = ["publish_dashboard_payload"]
