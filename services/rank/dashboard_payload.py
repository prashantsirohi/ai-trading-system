"""Dashboard payload builders for the rank stage."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from run.stages.base import StageContext


def summarize_task_statuses(task_status: Dict[str, Any]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for payload in (task_status or {}).values():
        status = str((payload or {}).get("status", "unknown"))
        counts[status] = int(counts.get(status, 0)) + 1
    return counts


def build_dashboard_payload(
    *,
    context: StageContext,
    ranked_df: pd.DataFrame,
    breakout_df: pd.DataFrame,
    pattern_df: pd.DataFrame,
    stock_scan_df: pd.DataFrame,
    sector_dashboard_df: pd.DataFrame,
    warnings: list[str],
    trust_summary: Optional[Dict[str, Any]] = None,
    task_status: Optional[Dict[str, Any]] = None,
) -> Dict[str, object]:
    """Assemble a unified operator payload from the rank-stage artifacts."""

    def _records(df: pd.DataFrame, limit: int = 10) -> list[dict]:
        if df is None or df.empty:
            return []
        return df.head(limit).to_dict(orient="records")

    top_sector = None
    if not sector_dashboard_df.empty:
        sector_col = "Sector" if "Sector" in sector_dashboard_df.columns else sector_dashboard_df.columns[0]
        top_sector = sector_dashboard_df.iloc[0].get(sector_col)
    breakout_state_counts: Dict[str, int] = {}
    if breakout_df is not None and not breakout_df.empty and "breakout_state" in breakout_df.columns:
        breakout_state_counts = breakout_df["breakout_state"].astype(str).value_counts().to_dict()
    candidate_tier_counts: Dict[str, int] = {}
    if breakout_df is not None and not breakout_df.empty and "candidate_tier" in breakout_df.columns:
        candidate_tier_counts = breakout_df["candidate_tier"].astype(str).value_counts().to_dict()
    pattern_state_counts: Dict[str, int] = {}
    pattern_family_counts: Dict[str, int] = {}
    if pattern_df is not None and not pattern_df.empty:
        if "pattern_state" in pattern_df.columns:
            pattern_state_counts = pattern_df["pattern_state"].astype(str).value_counts().to_dict()
        if "pattern_family" in pattern_df.columns:
            pattern_family_counts = pattern_df["pattern_family"].astype(str).value_counts().to_dict()

    return {
        "summary": {
            "run_id": context.run_id,
            "run_date": context.run_date,
            "ranked_count": int(len(ranked_df)),
            "breakout_count": int(len(breakout_df)),
            "pattern_count": int(len(pattern_df)),
            "stock_scan_count": int(len(stock_scan_df)),
            "sector_count": int(len(sector_dashboard_df)),
            "top_symbol": (
                ranked_df.iloc[0]["symbol_id"]
                if not ranked_df.empty and "symbol_id" in ranked_df.columns
                else None
            ),
            "top_sector": top_sector,
            "breakout_engine": str(context.params.get("breakout_engine", "v2")),
            "breakout_qualified_count": int(breakout_state_counts.get("qualified", 0)),
            "breakout_watchlist_count": int(breakout_state_counts.get("watchlist", 0)),
            "breakout_filtered_count": int(
                breakout_state_counts.get("filtered_by_regime", 0)
                + breakout_state_counts.get("filtered_by_symbol_trend", 0)
            ),
            "breakout_state_counts": breakout_state_counts,
            "breakout_tier_counts": candidate_tier_counts,
            "pattern_confirmed_count": int(pattern_state_counts.get("confirmed", 0)),
            "pattern_watchlist_count": int(pattern_state_counts.get("watchlist", 0)),
            "pattern_state_counts": pattern_state_counts,
            "pattern_family_counts": pattern_family_counts,
            "data_trust_status": (trust_summary or {}).get("status", "unknown"),
            "latest_trade_date": (trust_summary or {}).get("latest_trade_date"),
            "latest_validated_date": (trust_summary or {}).get("latest_validated_date"),
            "task_status_counts": summarize_task_statuses(task_status or {}),
        },
        "ranked_signals": _records(ranked_df, limit=10),
        "breakout_scan": _records(breakout_df, limit=10),
        "pattern_scan": _records(pattern_df, limit=10),
        "stock_scan": _records(stock_scan_df, limit=10),
        "sector_dashboard": _records(sector_dashboard_df, limit=10),
        "task_status": task_status or {},
        "data_trust": trust_summary or {},
        "warnings": warnings,
    }


def augment_dashboard_payload_with_ml(
    dashboard_payload: Optional[Dict[str, object]],
    *,
    ml_status: str,
    ml_mode: str,
    ml_overlay_df: pd.DataFrame,
) -> Optional[Dict[str, object]]:
    if dashboard_payload is None:
        return None

    payload = dict(dashboard_payload)
    summary = dict(payload.get("summary", {}))
    summary["ml_mode"] = ml_mode
    summary["ml_status"] = ml_status
    summary["ml_overlay_count"] = int(len(ml_overlay_df)) if ml_overlay_df is not None else 0
    payload["summary"] = summary
    payload["ml_overlay"] = (
        ml_overlay_df.head(10).to_dict(orient="records")
        if ml_overlay_df is not None and not ml_overlay_df.empty
        else []
    )
    return payload
