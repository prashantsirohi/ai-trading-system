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
