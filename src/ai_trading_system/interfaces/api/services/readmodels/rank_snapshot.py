"""Read models for rank-backed operator and execution API views."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ai_trading_system.interfaces.api.services.readmodels.latest_operational_snapshot import (
    LatestOperationalSnapshot,
    load_latest_operational_snapshot,
)
from ai_trading_system.interfaces.api.services.readmodels.pipeline_status import (
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
            display[column] = pd.to_datetime(display[column], errors="coerce").astype(str)
    return display.to_dict(orient="records")


def get_ranking_snapshot_read_model(
    project_root: str | Path,
    *,
    limit: int = 25,
    snapshot: Optional[LatestOperationalSnapshot] = None,
) -> dict[str, Any]:
    current_snapshot = snapshot or load_latest_operational_snapshot(project_root)
    ranked = current_snapshot.frames.get("ranked_signals", pd.DataFrame())
    return {
        "top_ranked": _records(ranked, limit=limit),
        "chart": _records(ranked[["symbol_id", "composite_score"]], limit=min(limit, 10))
        if not ranked.empty and {"symbol_id", "composite_score"}.issubset(ranked.columns)
        else [],
        "artifact_count": int(len(ranked.index)) if ranked is not None else 0,
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
    snapshot: Optional[LatestOperationalSnapshot] = None,
) -> dict[str, Any]:
    current_snapshot = snapshot or load_latest_operational_snapshot(project_root)
    health = get_execution_health(project_root, snapshot=current_snapshot)
    ops_health = get_execution_ops_health_snapshot(project_root)
    data_trust = get_execution_data_trust_snapshot(project_root)

    ranked = current_snapshot.frames.get("ranked_signals", pd.DataFrame())
    breakouts = current_snapshot.frames.get("breakout_scan", pd.DataFrame())
    patterns = current_snapshot.frames.get("pattern_scan", pd.DataFrame())
    sectors = current_snapshot.frames.get("sector_dashboard", pd.DataFrame())
    stock_scan = current_snapshot.frames.get("stock_scan", pd.DataFrame())

    return {
        "artifact_path": current_snapshot.payload.get("_artifact_path"),
        "summary": current_snapshot.payload.get("summary", {}),
        "warnings": current_snapshot.payload.get("warnings", []),
        "health": health,
        "ops_health": ops_health,
        "data_trust": data_trust,
        "top_ranked": _records(ranked, limit=limit),
        "breakouts": _records(breakouts, limit=limit),
        "patterns": _records(patterns, limit=limit),
        "sectors": _records(sectors, limit=limit),
        "stock_scan": _records(stock_scan, limit=limit),
        "counts": {
            "ranked": int(len(ranked.index)) if ranked is not None else 0,
            "breakouts": int(len(breakouts.index)) if breakouts is not None else 0,
            "patterns": int(len(patterns.index)) if patterns is not None else 0,
            "sectors": int(len(sectors.index)) if sectors is not None else 0,
            "stock_scan": int(len(stock_scan.index)) if stock_scan is not None else 0,
        },
    }
