"""Read-model snapshot endpoints (ranking, market, workspace, shadow)."""

from __future__ import annotations

from typing import Any

import pandas as pd
from fastapi import APIRouter, Query

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.execution_operator import (
    get_market_snapshot,
    get_pipeline_workspace_snapshot,
    get_ranking_snapshot,
    get_shadow_snapshot,
)
from ai_trading_system.ui.execution_api.services.readmodels.ranking_detail import (
    get_workspace_snapshot_compact,
)
from ai_trading_system.ui.execution_api.services.readmodels.latest_operational_snapshot import (
    load_latest_operational_snapshot,
)
from ai_trading_system.ui.execution_api.services.readmodels.market_breadth import (
    get_market_breadth_history,
)


router = APIRouter(prefix="/api/execution", tags=["snapshots"])


def _records(frame, limit: int | None = None) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    safe = frame.head(limit).copy() if limit else frame.copy()
    safe = safe.where(safe.notna(), None)
    return safe.to_dict(orient="records")


@router.get("/ranking")
def execution_ranking(
    limit: int = Query(default=25, ge=1, le=2500),
    stage2_only: bool = Query(default=False),
    stage2_min_score: float | None = Query(default=None, ge=0.0, le=100.0),
) -> dict[str, Any]:
    return get_ranking_snapshot(
        project_root(),
        limit=limit,
        stage2_only=stage2_only,
        stage2_min_score=stage2_min_score,
    )


@router.get("/market")
def execution_market(
    limit: int = Query(default=25, ge=1, le=200),
) -> dict[str, Any]:
    return get_market_snapshot(project_root(), limit=limit)


@router.get("/market/breadth")
def execution_market_breadth(
    limit: int = Query(
        default=0,
        ge=0,
        le=10000,
        description="Most recent rows to return. Use 0 for all operational history.",
    ),
) -> dict[str, Any]:
    return get_market_breadth_history(project_root(), limit=limit)


@router.get("/workspace/pipeline")
def execution_workspace_pipeline(
    limit: int = Query(default=20, ge=1, le=200),
    stage2_only: bool = Query(default=False),
    stage2_min_score: float | None = Query(default=None, ge=0.0, le=100.0),
) -> dict[str, Any]:
    return get_pipeline_workspace_snapshot(
        project_root(),
        limit=limit,
        stage2_only=stage2_only,
        stage2_min_score=stage2_min_score,
    )


@router.get("/shadow")
def execution_shadow() -> dict[str, Any]:
    return get_shadow_snapshot(project_root())


@router.get("/workspace/snapshot")
def execution_workspace_snapshot(
    top_n: int = Query(
        default=3,
        ge=1,
        le=10,
        description="How many top actions / sector leaders to surface.",
    ),
) -> dict[str, Any]:
    """Slim Control Tower payload — top-N actions + summary cards + leaders.

    Use ``/workspace/pipeline`` for the heavier tabbed workspace view; this
    endpoint exists to keep the landing page responsive without round-tripping
    the full ranked / breakout / pattern / sector tables.
    """

    return get_workspace_snapshot_compact(project_root(), top_n=top_n)


@router.get("/workspace/sector-rotation")
def execution_workspace_sector_rotation(
    group_type: str = Query(default="industry", pattern="^(sector|industry)$"),
    lookback: int = Query(default=20, ge=5, le=120),
    date: str | None = Query(default=None),
    sector: str | None = Query(default=None),
    show_stocks: bool = Query(default=True),
) -> dict[str, Any]:
    snapshot = load_latest_operational_snapshot(project_root())
    payload_summary = snapshot.payload.get("summary", {}) if isinstance(snapshot.payload, dict) else {}
    rotation_payload = snapshot.payload.get("sector_rotation", {}) if isinstance(snapshot.payload, dict) else {}
    run_id = payload_summary.get("run_id")
    if not run_id and snapshot.rank_attempt_dir is not None:
        try:
            run_id = snapshot.rank_attempt_dir.parents[1].name
        except IndexError:
            run_id = None
    run_date = payload_summary.get("run_date") or snapshot.payload.get("run_date")
    frames = snapshot.frames
    latest_key = "sector_rotation" if group_type == "sector" else "industry_rotation"
    history_key = "sector_rotation_history" if group_type == "sector" else "industry_rotation_history"
    latest_frame = _normalize_rotation_frame(frames.get(latest_key), group_type=group_type)
    history_frame = _normalize_rotation_frame(frames.get(history_key), group_type=group_type)
    if history_frame.empty and group_type == "sector":
        history_frame = _normalize_rotation_frame(frames.get("sector_rotation"), group_type="sector")
    if latest_frame.empty and group_type == "industry":
        latest_frame = _normalize_rotation_frame(frames.get("sector_rotation"), group_type="industry")
    if history_frame.empty and group_type == "industry":
        history_frame = latest_frame.copy()

    available_dates = _available_dates(history_frame if not history_frame.empty else latest_frame)
    selected_date = date if date in available_dates else (available_dates[-1] if available_dates else run_date)
    history_window = _rotation_history_window(history_frame, selected_date=selected_date, lookback=lookback)
    groups = _rotation_groups_for_date(latest_frame, history_window, selected_date=selected_date)
    if sector and group_type == "industry":
        groups = groups.loc[groups.get("parent_sector", "").astype(str) == sector] if not groups.empty else groups
        history_window = history_window.loc[history_window.get("parent_sector", "").astype(str) == sector] if not history_window.empty else history_window

    accumulation = frames.get("accumulation_distribution")
    if accumulation is None or accumulation.empty:
        accumulation_rows = []
        distribution_rows = []
    else:
        signal = accumulation.get("delivery_signal")
        accumulation_rows = _records(accumulation.loc[signal == "Accumulation"] if signal is not None else accumulation.iloc[0:0])
        distribution_rows = _records(accumulation.loc[signal == "Distribution"] if signal is not None else accumulation.iloc[0:0])
    stocks = frames.get("stock_rotation")
    if not show_stocks:
        stocks = None
    elif stocks is not None and not stocks.empty and sector:
        if "sector" in stocks.columns:
            stocks = stocks.loc[stocks["sector"].astype(str) == sector]
    return {
        "run_id": run_id,
        "run_date": run_date,
        "group_type": group_type,
        "benchmark_name": rotation_payload.get("benchmark_name"),
        "selected_date": selected_date,
        "available_dates": available_dates,
        "groups": _records(groups),
        "history": _records(history_window),
        "sectors": _records(frames.get("sector_rotation")),
        "stocks": _records(stocks),
        "accumulation": accumulation_rows,
        "distribution": distribution_rows,
        "custom_indices": _records(frames.get("sector_custom_indices"), limit=500),
    }


def _normalize_rotation_frame(frame, *, group_type: str):
    if frame is None or frame.empty:
        return frame
    output = frame.copy()
    if "rotation_group_type" not in output.columns:
        output.loc[:, "rotation_group_type"] = group_type
    if "rotation_group_name" not in output.columns:
        if "industry" in output.columns:
            output.loc[:, "rotation_group_name"] = output["industry"]
        elif "sector" in output.columns:
            output.loc[:, "rotation_group_name"] = output["sector"]
    if "parent_sector" not in output.columns:
        if "sector" in output.columns:
            output.loc[:, "parent_sector"] = output["sector"]
        elif "industry" in output.columns:
            output.loc[:, "parent_sector"] = output["industry"]
        else:
            output.loc[:, "parent_sector"] = output.get("rotation_group_name")
    return output


def _available_dates(frame) -> list[str]:
    if frame is None or frame.empty or "date" not in frame.columns:
        return []
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna().dt.date.astype(str).sort_values(kind="stable").unique().tolist()
    return [str(value) for value in dates]


def _rotation_history_window(frame, *, selected_date: str | None, lookback: int):
    if frame is None or frame.empty or "date" not in frame.columns or not selected_date:
        return frame.iloc[0:0] if frame is not None else frame
    output = frame.copy()
    output.loc[:, "_date"] = pd.to_datetime(output["date"], errors="coerce").dt.date.astype(str)
    output = output.loc[output["_date"] <= selected_date].copy()
    if output.empty:
        return output.drop(columns=["_date"])
    if "rotation_group_name" not in output.columns:
        dates = [value for value in _available_dates(output) if value <= selected_date]
        tail_dates = set(dates[-lookback:])
        output = output.loc[output["_date"].isin(tail_dates)]
        return output.drop(columns=["_date"]).sort_values("date", kind="stable")
    output = output.sort_values(
        ["rotation_group_name", "date"],
        ascending=[True, False],
        kind="stable",
    ).copy()
    pieces = [group.head(lookback) for _, group in output.groupby("rotation_group_name", sort=False)]
    output = pd.concat(pieces, ignore_index=True) if pieces else output.iloc[0:0]
    return output.drop(columns=["_date"]).sort_values(["rotation_group_name", "date"], kind="stable")


def _rotation_groups_for_date(latest_frame, history_window, *, selected_date: str | None):
    if history_window is not None and not history_window.empty and selected_date:
        selected = history_window.loc[history_window["date"].astype(str) <= selected_date].copy()
        if not selected.empty and "rotation_group_name" in selected.columns:
            return (
                selected.sort_values(["rotation_group_name", "date"], kind="stable")
                .drop_duplicates(subset=["rotation_group_name"], keep="last")
                .sort_values(["quadrant", "rs_ratio", "rotation_group_name"], ascending=[True, False, True], kind="stable")
            )
    return latest_frame if latest_frame is not None else history_window
