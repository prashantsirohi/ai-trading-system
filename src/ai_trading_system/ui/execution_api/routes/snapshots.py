"""Read-model snapshot endpoints (ranking, market, workspace, shadow)."""

from __future__ import annotations

from typing import Any

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
def execution_workspace_sector_rotation() -> dict[str, Any]:
    snapshot = load_latest_operational_snapshot(project_root())
    payload_summary = snapshot.payload.get("summary", {}) if isinstance(snapshot.payload, dict) else {}
    run_id = payload_summary.get("run_id")
    if not run_id and snapshot.rank_attempt_dir is not None:
        try:
            run_id = snapshot.rank_attempt_dir.parents[1].name
        except IndexError:
            run_id = None
    run_date = payload_summary.get("run_date") or snapshot.payload.get("run_date")
    frames = snapshot.frames
    accumulation = frames.get("accumulation_distribution")
    if accumulation is None or accumulation.empty:
        accumulation_rows = []
        distribution_rows = []
    else:
        signal = accumulation.get("delivery_signal")
        accumulation_rows = _records(accumulation.loc[signal == "Accumulation"] if signal is not None else accumulation.iloc[0:0])
        distribution_rows = _records(accumulation.loc[signal == "Distribution"] if signal is not None else accumulation.iloc[0:0])
    return {
        "run_id": run_id,
        "run_date": run_date,
        "sectors": _records(frames.get("sector_rotation")),
        "stocks": _records(frames.get("stock_rotation")),
        "accumulation": accumulation_rows,
        "distribution": distribution_rows,
        "custom_indices": _records(frames.get("sector_custom_indices"), limit=500),
    }
