"""Read models for rank-backed operator and execution API views."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ai_trading_system.ui.execution_api.services.readmodels.latest_operational_snapshot import (
    LatestOperationalSnapshot,
    load_latest_operational_snapshot,
)
from ai_trading_system.ui.execution_api.services.readmodels.pipeline_status import (
    get_execution_data_trust_snapshot,
    get_execution_health,
    get_execution_ops_health_snapshot,
)


def _records(frame: pd.DataFrame, *, limit: Optional[int] = None) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    display = frame.copy()
    if limit is not None:
        display = display.head(int(limit))
    display = display.where(pd.notnull(display), None)
    for column in display.columns:
        if pd.api.types.is_datetime64_any_dtype(display[column]):
            display = display.astype({column: "object"})
            display.loc[:, column] = pd.to_datetime(display[column], errors="coerce").astype(str)
    return display.to_dict(orient="records")


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _as_bool_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").fillna(0).astype(float) > 0
    text = series.astype(str).str.strip().str.lower()
    return text.isin({"1", "true", "t", "yes", "y", "uptrend"})


def _stage2_summary(ranked: pd.DataFrame, *, top_symbols: int = 8) -> dict[str, Any]:
    if ranked is None or ranked.empty:
        return {
            "available": False,
            "columns": {"stage2_score": False, "is_stage2_uptrend": False, "stage2_label": False},
            "uptrend_count": 0,
            "uptrend_ratio_pct": 0.0,
            "counts_by_label": {},
            "score": {"min": None, "max": None, "avg": None},
            "top_symbols": [],
        }

    has_score = "stage2_score" in ranked.columns
    has_uptrend = "is_stage2_uptrend" in ranked.columns
    has_label = "stage2_label" in ranked.columns
    labels: dict[str, int] = {}
    if has_label:
        label_series = (
            ranked["stage2_label"]
            .fillna("unknown")
            .astype(str)
            .str.strip()
            .replace("", "unknown")
            .str.lower()
        )
        labels = {str(key): int(value) for key, value in label_series.value_counts().items()}

    uptrend_count = 0
    if has_uptrend:
        uptrend_count = int(_as_bool_series(ranked["is_stage2_uptrend"]).sum())

    min_score = max_score = avg_score = None
    if has_score:
        score_series = pd.to_numeric(ranked["stage2_score"], errors="coerce").dropna()
        if not score_series.empty:
            min_score = float(score_series.min())
            max_score = float(score_series.max())
            avg_score = float(score_series.mean())

    top_symbols_list: list[dict[str, Any]] = []
    if {"symbol_id", "composite_score"}.issubset(ranked.columns):
        display = ranked.copy()
        display.loc[:, "composite_score"] = pd.to_numeric(display["composite_score"], errors="coerce")
        display = display.dropna(subset=["composite_score"]).sort_values("composite_score", ascending=False).head(max(int(top_symbols), 1))
        for _, row in display.iterrows():
            item: dict[str, Any] = {"symbol_id": row.get("symbol_id"), "composite_score": _safe_float(row.get("composite_score"))}
            if has_score:
                item["stage2_score"] = _safe_float(row.get("stage2_score"))
            if has_uptrend:
                item["is_stage2_uptrend"] = bool(_as_bool_series(pd.Series([row.get("is_stage2_uptrend")])).iloc[0])
            if has_label:
                item["stage2_label"] = str(row.get("stage2_label") or "").strip() or None
            top_symbols_list.append(item)

    total = int(len(ranked.index))
    ratio = (float(uptrend_count) / float(total) * 100.0) if total else 0.0
    return {
        "available": bool(has_score or has_uptrend or has_label),
        "columns": {"stage2_score": has_score, "is_stage2_uptrend": has_uptrend, "stage2_label": has_label},
        "uptrend_count": uptrend_count,
        "uptrend_ratio_pct": ratio,
        "counts_by_label": labels,
        "score": {"min": min_score, "max": max_score, "avg": avg_score},
        "top_symbols": top_symbols_list,
    }


def _apply_stage2_filter(
    ranked: pd.DataFrame,
    *,
    stage2_only: bool = False,
    stage2_min_score: float | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if ranked is None or ranked.empty:
        return pd.DataFrame(), {
            "requested": bool(stage2_only or stage2_min_score is not None),
            "stage2_only": bool(stage2_only),
            "min_score": _safe_float(stage2_min_score),
            "available": False,
            "gate_unavailable": bool(stage2_only or stage2_min_score is not None),
            "before_count": 0,
            "after_count": 0,
            "dropped_count": 0,
        }

    filtered = ranked.copy()
    before_count = int(len(filtered.index))
    has_uptrend = "is_stage2_uptrend" in filtered.columns
    has_score = "stage2_score" in filtered.columns
    gate_unavailable = False

    if stage2_only:
        if has_uptrend:
            filtered = filtered.loc[_as_bool_series(filtered["is_stage2_uptrend"])]
        else:
            gate_unavailable = True

    min_score = _safe_float(stage2_min_score)
    if min_score is not None:
        if has_score:
            score_series = pd.to_numeric(filtered["stage2_score"], errors="coerce")
            filtered = filtered.loc[score_series >= float(min_score)]
        else:
            gate_unavailable = True

    after_count = int(len(filtered.index))
    diagnostics = {
        "requested": bool(stage2_only or min_score is not None),
        "stage2_only": bool(stage2_only),
        "min_score": min_score,
        "available": bool(has_uptrend or has_score),
        "gate_unavailable": gate_unavailable,
        "before_count": before_count,
        "after_count": after_count,
        "dropped_count": max(before_count - after_count, 0),
    }
    return filtered, diagnostics


def get_ranking_snapshot_read_model(
    project_root: str | Path,
    *,
    limit: int = 25,
    stage2_only: bool = False,
    stage2_min_score: float | None = None,
    snapshot: Optional[LatestOperationalSnapshot] = None,
) -> dict[str, Any]:
    current_snapshot = snapshot or load_latest_operational_snapshot(project_root)
    ranked = current_snapshot.frames.get("ranked_signals", pd.DataFrame())
    stage2_summary = _stage2_summary(ranked)
    filtered_ranked, stage2_filter = _apply_stage2_filter(
        ranked,
        stage2_only=stage2_only,
        stage2_min_score=stage2_min_score,
    )
    return {
        "top_ranked": _records(filtered_ranked, limit=limit),
        "chart": _records(filtered_ranked[["symbol_id", "composite_score"]], limit=min(limit, 10))
        if not filtered_ranked.empty and {"symbol_id", "composite_score"}.issubset(filtered_ranked.columns)
        else [],
        "artifact_count": int(len(ranked.index)) if ranked is not None else 0,
        "visible_count": int(len(filtered_ranked.index)) if filtered_ranked is not None else 0,
        "stage2_summary": stage2_summary,
        "stage2_filter": stage2_filter,
    }


def get_market_snapshot_read_model(
    project_root: str | Path,
    *,
    limit: int = 25,
    snapshot: Optional[LatestOperationalSnapshot] = None,
) -> dict[str, Any]:
    current_snapshot = snapshot or load_latest_operational_snapshot(project_root)
    health = get_execution_health(project_root, snapshot=current_snapshot)
    return {
        "breakouts": _records(current_snapshot.frames.get("breakout_scan", pd.DataFrame()), limit=limit),
        "sectors": _records(current_snapshot.frames.get("sector_dashboard", pd.DataFrame()), limit=limit),
        "health": health,
        "summary": current_snapshot.payload.get("summary", {}),
    }


def get_pipeline_workspace_snapshot_read_model(
    project_root: str | Path,
    *,
    limit: int = 20,
    stage2_only: bool = False,
    stage2_min_score: float | None = None,
    snapshot: Optional[LatestOperationalSnapshot] = None,
) -> dict[str, Any]:
    current_snapshot = snapshot or load_latest_operational_snapshot(project_root)
    health = get_execution_health(project_root, snapshot=current_snapshot)
    ops_health = get_execution_ops_health_snapshot(project_root)
    data_trust = get_execution_data_trust_snapshot(project_root)

    ranked = current_snapshot.frames.get("ranked_signals", pd.DataFrame())
    stage2_summary = _stage2_summary(ranked)
    filtered_ranked, stage2_filter = _apply_stage2_filter(
        ranked,
        stage2_only=stage2_only,
        stage2_min_score=stage2_min_score,
    )
    breakouts = current_snapshot.frames.get("breakout_scan", pd.DataFrame())
    patterns = current_snapshot.frames.get("pattern_scan", pd.DataFrame())
    sectors = current_snapshot.frames.get("sector_dashboard", pd.DataFrame())
    stock_scan = current_snapshot.frames.get("stock_scan", pd.DataFrame())
    payload_ranked_leaders = current_snapshot.payload.get("ranked_leaders")
    payload_pattern_discoveries = current_snapshot.payload.get("pattern_discoveries")
    payload_breakout_candidates = current_snapshot.payload.get("breakout_candidates")

    # ``DataFrame.get("rank")`` returns ``None`` when the column is absent,
    # and ``pd.to_numeric(None)`` collapses to a numpy scalar that lacks
    # ``.notna()`` — we have to provide a NaN-filled Series aligned with
    # the DataFrame's index so the boolean mask survives a missing column.
    ranked_leaders = (
        payload_ranked_leaders
        if isinstance(payload_ranked_leaders, list)
        else _records(
            stock_scan.loc[
                pd.to_numeric(
                    stock_scan.get("rank", pd.Series(pd.NA, index=stock_scan.index)),
                    errors="coerce",
                ).notna()
            ]
            if stock_scan is not None and not stock_scan.empty
            else pd.DataFrame(),
            limit=limit,
        )
    )
    pattern_discoveries = (
        payload_pattern_discoveries
        if isinstance(payload_pattern_discoveries, list)
        else _records(
            stock_scan.loc[
                stock_scan.get("discovered_by_pattern_scan", pd.Series(False, index=stock_scan.index))
                .fillna(False)
                .astype(bool)
            ]
            if stock_scan is not None and not stock_scan.empty
            else pd.DataFrame(),
            limit=limit,
        )
    )
    breakout_candidates = (
        payload_breakout_candidates
        if isinstance(payload_breakout_candidates, list)
        else _records(
            stock_scan.loc[
                pd.to_numeric(
                    stock_scan.get("rank", pd.Series(pd.NA, index=stock_scan.index)),
                    errors="coerce",
                ).isna()
                & stock_scan.get("breakout_positive", pd.Series(False, index=stock_scan.index)).fillna(False).astype(bool)
            ]
            if stock_scan is not None and not stock_scan.empty
            else pd.DataFrame(),
            limit=limit,
        )
    )

    return {
        "artifact_path": current_snapshot.payload.get("_artifact_path"),
        "summary": current_snapshot.payload.get("summary", {}),
        "warnings": current_snapshot.payload.get("warnings", []),
        "health": health,
        "ops_health": ops_health,
        "data_trust": data_trust,
        "top_ranked": _records(filtered_ranked, limit=limit),
        "breakouts": _records(breakouts, limit=limit),
        "patterns": _records(patterns, limit=limit),
        "sectors": _records(sectors, limit=limit),
        "stock_scan": _records(stock_scan, limit=limit),
        "ranked_leaders": ranked_leaders,
        "pattern_discoveries": pattern_discoveries,
        "breakout_candidates": breakout_candidates,
        "stage2_summary": stage2_summary,
        "stage2_filter": stage2_filter,
        "counts": {
            "ranked": int(len(ranked.index)) if ranked is not None else 0,
            "breakouts": int(len(breakouts.index)) if breakouts is not None else 0,
            "patterns": int(len(patterns.index)) if patterns is not None else 0,
            "sectors": int(len(sectors.index)) if sectors is not None else 0,
            "stock_scan": int(len(stock_scan.index)) if stock_scan is not None else 0,
        },
        "visible_counts": {
            "ranked": int(len(filtered_ranked.index)) if filtered_ranked is not None else 0,
        },
    }
