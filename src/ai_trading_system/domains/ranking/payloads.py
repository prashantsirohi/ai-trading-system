"""Dashboard payload builders for the rank stage."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from ai_trading_system.pipeline.contracts import StageContext


def summarize_task_statuses(task_status: Dict[str, Any]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for payload in (task_status or {}).values():
        status = str((payload or {}).get("status", "unknown"))
        counts[status] = int(counts.get(status, 0)) + 1
    return counts


def build_score_breakdown(row: dict) -> dict:
    keys = [
        "relative_strength",
        "volume_intensity",
        "trend_persistence",
        "proximity_to_highs",
        "delivery_pct",
        "sector_strength",
        "penalty_score",
    ]
    return {key: row.get(key) for key in keys if key in row}


def build_top_factors(row: dict) -> list[str]:
    score_map = {
        "relative_strength": row.get("rel_strength_score"),
        "volume_intensity": row.get("vol_intensity_score"),
        "trend_persistence": row.get("trend_score_score"),
        "proximity_to_highs": row.get("prox_high_score"),
        "delivery_pct": row.get("delivery_pct_score"),
        "sector_strength": row.get("sector_strength_score"),
    }
    normalized = []
    for name, value in score_map.items():
        try:
            normalized.append((name, float(value)))
        except (TypeError, ValueError):
            continue
    normalized.sort(key=lambda item: item[1], reverse=True)
    return [name for name, _ in normalized[:3]]


def build_rejection_reasons(row: dict) -> list[str]:
    reasons = []
    if row.get("eligible_rank") is False:
        reasons.append("failed_eligibility")
    if isinstance(row.get("rejection_reasons"), list):
        for reason in row.get("rejection_reasons"):
            if reason not in reasons:
                reasons.append(str(reason))
    return reasons


def _discovery_visibility_summary(
    *,
    ranked_df: pd.DataFrame,
    stock_scan_df: pd.DataFrame,
    pattern_discoveries: list[dict],
    breakout_candidates: list[dict],
) -> dict[str, object]:
    ranked_symbols = (
        set(ranked_df["symbol_id"].astype(str))
        if ranked_df is not None and not ranked_df.empty and "symbol_id" in ranked_df.columns
        else set()
    )
    stock_symbols = (
        set(stock_scan_df["symbol_id"].astype(str))
        if stock_scan_df is not None and not stock_scan_df.empty and "symbol_id" in stock_scan_df.columns
        else set()
    )
    ranked_covers_stock_scan = bool(stock_symbols) and stock_symbols.issubset(ranked_symbols)

    reason = None
    note = None
    if ranked_covers_stock_scan and not pattern_discoveries and not breakout_candidates:
        reason = "ranked_universe_covers_stock_scan"
        note = (
            "No non-ranked pattern discoveries or breakout candidates are shown because "
            "the ranked universe already covers the full stock-scan symbol set for this run."
        )

    coverage_pct = 0.0
    if stock_symbols:
        coverage_pct = round((len(ranked_symbols & stock_symbols) / len(stock_symbols)) * 100.0, 2)

    return {
        "ranked_universe_covers_stock_scan": ranked_covers_stock_scan,
        "ranked_universe_stock_scan_coverage_pct": coverage_pct,
        "discovery_visibility_reason": reason,
        "discovery_visibility_note": note,
    }


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
        records = df.head(limit).to_dict(orient="records")
        if df is ranked_df:
            enriched = []
            for row in records:
                row = dict(row)
                row["score_breakdown"] = build_score_breakdown(row)
                row["top_factors"] = build_top_factors(row)
                row["rejection_reasons"] = build_rejection_reasons(row)
                enriched.append(row)
            return enriched
        return records

    def _stock_scan_records(predicate, *, limit: int = 10) -> list[dict]:
        if stock_scan_df is None or stock_scan_df.empty:
            return []
        return _records(stock_scan_df.loc[predicate(stock_scan_df)], limit=limit)

    def _stage2_leader_records(*, limit: int = 50) -> list[dict]:
        if stock_scan_df is None or stock_scan_df.empty:
            return []
        stage2_labels = stock_scan_df.get("stage2_label", pd.Series("", index=stock_scan_df.index)).astype(str)
        stage2_mask = stage2_labels.isin({"strong_stage2", "stage2"})
        focused = stock_scan_df.loc[stage2_mask].copy()
        if focused.empty:
            return []
        focused.loc[:, "_rank_sort"] = pd.to_numeric(focused.get("rank"), errors="coerce")
        focused.loc[:, "_composite_sort"] = pd.to_numeric(focused.get("composite_score"), errors="coerce")
        focused = focused.sort_values(
            ["_rank_sort", "_composite_sort", "symbol_id"],
            ascending=[True, False, True],
            na_position="last",
            kind="stable",
        ).drop(columns=["_rank_sort", "_composite_sort"], errors="ignore")
        return _records(focused, limit=limit)

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
    ranked_leaders = _stock_scan_records(lambda df: pd.to_numeric(df.get("rank"), errors="coerce").notna())
    pattern_discoveries = _stock_scan_records(
        lambda df: (
            df.get("discovered_by_pattern_scan", pd.Series(False, index=df.index))
            .fillna(False)
            .astype(bool)
        )
    )
    breakout_candidates = _stock_scan_records(
        lambda df: (
            pd.to_numeric(df.get("rank"), errors="coerce").isna()
            & df.get("breakout_positive", pd.Series(False, index=df.index)).fillna(False).astype(bool)
        )
    )
    stage2_leaders = _stage2_leader_records(limit=50)
    stage2_total_count = 0
    stage2_label_counts: Dict[str, int] = {}
    if stock_scan_df is not None and not stock_scan_df.empty and "stage2_label" in stock_scan_df.columns:
        stage2_mask = stock_scan_df["stage2_label"].astype(str).isin({"strong_stage2", "stage2"})
        stage2_total_count = int(stage2_mask.sum())
        if stage2_total_count:
            stage2_label_counts = (
                stock_scan_df.loc[stage2_mask, "stage2_label"].astype(str).value_counts().to_dict()
            )
    discovery_visibility = _discovery_visibility_summary(
        ranked_df=ranked_df,
        stock_scan_df=stock_scan_df,
        pattern_discoveries=pattern_discoveries,
        breakout_candidates=breakout_candidates,
    )

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
            "ranked_leader_count": len(ranked_leaders),
            "pattern_discovery_count": len(pattern_discoveries),
            "breakout_candidate_count": len(breakout_candidates),
            "stage2_leader_count": len(stage2_leaders),
            "stage2_total_count": stage2_total_count,
            "stage2_label_counts": stage2_label_counts,
            **discovery_visibility,
            "data_trust_status": (trust_summary or {}).get("status", "unknown"),
            "latest_trade_date": (trust_summary or {}).get("latest_trade_date"),
            "latest_validated_date": (trust_summary or {}).get("latest_validated_date"),
            "task_status_counts": summarize_task_statuses(task_status or {}),
        },
        "ranked_signals": _records(ranked_df, limit=10),
        "breakout_scan": _records(breakout_df, limit=10),
        "pattern_scan": _records(pattern_df, limit=10),
        "stock_scan": _records(stock_scan_df, limit=10),
        "ranked_leaders": ranked_leaders,
        "stage2_leaders": stage2_leaders,
        "pattern_discoveries": pattern_discoveries,
        "breakout_candidates": breakout_candidates,
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
