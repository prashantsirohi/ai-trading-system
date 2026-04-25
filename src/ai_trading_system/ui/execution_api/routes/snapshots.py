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


router = APIRouter(prefix="/api/execution", tags=["snapshots"])


@router.get("/ranking")
def execution_ranking(
    limit: int = Query(default=25, ge=1, le=200),
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
