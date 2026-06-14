"""Dashboard payload delivery adapters."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable

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


def _write_section(
    *,
    manager: GoogleSheetsManager,
    worksheet: Any,
    sheet_name: str,
    start_row: int,
    title: str,
    frame: pd.DataFrame,
) -> tuple[int, int | None, int]:
    worksheet.update([[title]], range_name=f"A{start_row}")
    header_row = start_row + 1
    if frame.empty:
        worksheet.update([["No data available"]], range_name=f"A{header_row}")
        return header_row + 2, None, 0
    if not manager.write_dataframe(frame, sheet_name, start_cell=f"A{header_row}", include_header=True, clear_sheet=False):
        raise RuntimeError(f"Failed writing section '{title}': {manager.last_error or 'write error'}")
    return header_row + len(frame) + 2, header_row, len(frame)


def _style_section(
    *,
    worksheet: Any,
    title_row: int,
    header_row: int | None,
    row_count: int,
    col_count: int,
) -> None:
    if not hasattr(worksheet, "format"):
        return
    worksheet.format(
        f"A{title_row}:F{title_row}",
        {
            "textFormat": {"bold": True, "fontSize": 11, "foregroundColor": {"red": 0.11, "green": 0.16, "blue": 0.26}},
            "backgroundColor": {"red": 0.91, "green": 0.95, "blue": 0.99},
        },
    )
    if header_row is None or row_count <= 0:
        return
    last_col = chr(ord("A") + max(col_count - 1, 0))
    worksheet.format(
        f"A{header_row}:{last_col}{header_row}",
        {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
        },
    )


def _apply_sheet_readability_layout(
    *,
    manager: GoogleSheetsManager,
    worksheet: Any,
    section_layouts: list[dict[str, int | None]],
) -> None:
    if manager.spreadsheet is None:
        return

    sheet_id = int(worksheet.id)
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": 1,
                    },
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
    ]
    widths = [180, 110, 90, 110, 150, 420]
    for idx, width in enumerate(widths):
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": idx,
                        "endIndex": idx + 1,
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            }
        )
    manager.spreadsheet.batch_update({"requests": requests})

    if hasattr(worksheet, "format"):
        worksheet.format(
            "A1:F1",
            {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.88, "green": 0.93, "blue": 0.98},
            },
        )
    for layout in section_layouts:
        _style_section(
            worksheet=worksheet,
            title_row=int(layout["title_row"]),
            header_row=int(layout["header_row"]) if layout.get("header_row") else None,
            row_count=int(layout["row_count"] or 0),
            col_count=int(layout["col_count"] or 1),
        )


def _delete_existing_charts(manager: GoogleSheetsManager, sheet_id: int) -> None:
    if manager.spreadsheet is None:
        return
    metadata = manager.spreadsheet.fetch_sheet_metadata()
    requests = []
    for sheet in metadata.get("sheets", []):
        properties = sheet.get("properties", {})
        if int(properties.get("sheetId", -1)) != int(sheet_id):
            continue
        for chart in sheet.get("charts", []):
            chart_id = chart.get("chartId")
            if chart_id is not None:
                requests.append({"deleteEmbeddedObject": {"objectId": chart_id}})
    if requests:
        manager.spreadsheet.batch_update({"requests": requests})


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


def _add_breadth_charts(
    *,
    manager: GoogleSheetsManager,
    worksheet: Any,
    header_row: int,
    data_rows: int,
) -> None:
    if data_rows < 2 or manager.spreadsheet is None:
        return
    sheet_id = int(worksheet.id)
    start_idx = header_row - 1
    end_idx = header_row + data_rows
    _delete_existing_charts(manager, sheet_id)
    domain = [{"domain": _chart_range(sheet_id, start_idx, end_idx, 0)}]

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
                            "anchorCell": {"sheetId": sheet_id, "rowIndex": anchor_row, "columnIndex": 12},
                            "offsetXPixels": 16,
                            "offsetYPixels": 8,
                            "widthPixels": 860,
                            "heightPixels": 300,
                        }
                    },
                }
            }
        }

    requests = [
        request(
            title="Operational Long-Term Breadth (% Above SMA200 and PE 5Y Percentile SMA20)",
            chart_type="LINE",
            series=[
                _chart_series(sheet_id, start_idx, end_idx, 1),
                _chart_series(sheet_id, start_idx, end_idx, 11, "RIGHT_AXIS"),
            ],
            anchor_row=start_idx,
            left_title="% Above SMA200",
            right_title="PE 5Y percentile SMA20",
        ),
        request(
            title="New 52W Highs / Lows and Ratio SMA10",
            chart_type="COLUMN",
            series=[
                _chart_series(sheet_id, start_idx, end_idx, 2),
                _chart_series(sheet_id, start_idx, end_idx, 3),
                {**_chart_series(sheet_id, start_idx, end_idx, 5, "RIGHT_AXIS"), "type": "LINE"},
            ],
            anchor_row=start_idx + 16,
            left_title="New highs / lows",
            right_title="High / low ratio SMA10",
        ),
        request(
            title="A/D Divergence and TOP1000 Index",
            chart_type="LINE",
            series=[
                _chart_series(sheet_id, start_idx, end_idx, 8),
                _chart_series(sheet_id, start_idx, end_idx, 9, "RIGHT_AXIS"),
            ],
            anchor_row=start_idx + 32,
            left_title="A/D line",
            right_title="Index level",
        ),
    ]
    manager.spreadsheet.batch_update({"requests": requests})


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
    decision_bundle: PublishDecisionBundle | None = None,
    ranking_feedback: dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Write a single compact daily sheet with sector/rank/breakout and breadth chart."""
    manager = GoogleSheetsManager()
    if not manager.open_spreadsheet():
        message = manager.last_error or "spreadsheet unavailable"
        raise RuntimeError(f"Dashboard publish failed: {message}")

    base_sheet_name = _resolve_sheet_name(payload, run_date=run_date)
    sheet_name = base_sheet_name
    worksheet = manager.get_or_create_sheet(sheet_name, rows=5000, cols=20)
    if worksheet is None:
        raise RuntimeError(f"Dashboard publish failed creating sheet '{sheet_name}': {manager.last_error or 'unknown error'}")

    source_ranked = ranked_df if isinstance(ranked_df, pd.DataFrame) and not ranked_df.empty else _frame(payload.get("ranked_signals", []))
    source_ranked = pd.DataFrame(
        format_rows_for_channel(source_ranked.to_dict(orient="records") if isinstance(source_ranked, pd.DataFrame) else [], "dashboard")["rows"]
    )
    source_breakout = breakout_df if isinstance(breakout_df, pd.DataFrame) and not breakout_df.empty else _frame(payload.get("breakout_scan", []))
    source_sector = sector_df if isinstance(sector_df, pd.DataFrame) and not sector_df.empty else _frame(payload.get("sector_dashboard", []))
    source_watchlist = watchlist_df if isinstance(watchlist_df, pd.DataFrame) and not watchlist_df.empty else _frame(payload.get("watchlist", []))

    rank_min = _minimal_rank_frame(source_ranked)
    breakout_min = _minimal_breakout_frame(source_breakout)
    weekly_moves = _weekly_move_frame(source_ranked)
    failed_breakouts = _failed_breakout_frame(failed_breakouts_df)
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
    if not manager.write_dataframe(summary, sheet_name, clear_sheet=True, start_cell="A1", include_header=True):
        raise RuntimeError(f"Dashboard publish failed writing summary: {manager.last_error or 'unknown error'}")

    row = 4
    section_layouts: list[dict[str, int | None]] = []
    breadth_header_row: int | None = None
    breadth_rows = 0
    sections = [
        ("DAILY SUMMARY", _compact_summary_frame(summary)),
        ("RANKING FEEDBACK", _ranking_feedback_frame(ranking_feedback)),
        ("TODAY'S DECISION SHORTLIST", bundle.watchlist_candidates),
        ("SECTOR CONTEXT — Leading/Improving only; Rank = absolute RS rank across all sectors", bundle.sector_leaders),
        ("PATTERN SETUPS", bundle.pattern_setups),
        ("MARKET MOVES SNAPSHOT", bundle.market_moves if not bundle.market_moves.empty else weekly_moves),
        ("FAILED BREAKOUTS", bundle.failed_breakouts if not bundle.failed_breakouts.empty else failed_breakouts),
        ("TOP RANKED", bundle.top_ranked if not bundle.top_ranked.empty else rank_min),
        ("BREAKOUTS (all, unfiltered)", breakout_min),
        ("LONG-TERM BREADTH (operational, since 2020)", breadth),
    ]
    for title, frame in sections:
        section_title_row = row
        row, header_row, row_count = _write_section(
            manager=manager,
            worksheet=worksheet,
            sheet_name=sheet_name,
            start_row=row,
            title=title,
            frame=frame,
        )
        if title.startswith("LONG-TERM BREADTH"):
            breadth_header_row = int(header_row) if header_row is not None else None
            breadth_rows = int(row_count)
        section_layouts.append(
            {
                "title_row": section_title_row,
                "header_row": int(header_row) if header_row is not None else None,
                "row_count": int(row_count),
                "col_count": int(len(frame.columns)),
            }
        )

    try:
        _apply_sheet_readability_layout(
            manager=manager,
            worksheet=worksheet,
            section_layouts=section_layouts,
        )
    except Exception as exc:
        logger.warning("Sheet readability styling failed on '%s': %s", sheet_name, exc)

    if breadth_header_row is not None and breadth_rows >= 2:
        try:
            _add_breadth_charts(
                manager=manager,
                worksheet=worksheet,
                header_row=breadth_header_row,
                data_rows=breadth_rows,
            )
        except Exception as exc:
            logger.warning("Breadth chart creation failed on sheet %s: %s", sheet_name, exc)

    logger.info("Dashboard payload published to single dated sheet '%s'", sheet_name)
    return {"sheet_name": sheet_name, "base_sheet_name": base_sheet_name, "rows_written": int(row)}


__all__ = ["publish_dashboard_payload"]
