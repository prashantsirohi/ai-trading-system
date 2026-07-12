"""Stock investigator API route."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.investigator import (
    get_investigator_snapshot,
)
from ai_trading_system.ui.execution_api.services.readmodels.stage1_operator import (
    get_stage1_current, get_stage1_detail, get_stage1_exits, get_stage1_summary,
    get_stage1_transitions,
)


router = APIRouter(prefix="/api/execution", tags=["investigator"])


@router.get("/investigator")
def execution_investigator() -> dict[str, Any]:
    return get_investigator_snapshot(project_root())


@router.get("/investigator/stage1/summary")
def execution_investigator_stage1_summary() -> dict[str, Any]:
    return get_stage1_summary(project_root())


@router.get("/investigator/stage1/current")
def execution_investigator_stage1_current(
    lifecycle_state: str | None = None, operator_status: str | None = None,
    operator_priority: str | None = None, sector: str | None = None,
    golden_cross_status: str | None = None, pattern_promotion_state: str | None = None,
    promotion_eligibility: bool | None = None, search: str | None = None,
    include_blocked: bool = False, limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0), sort_by: str | None = None,
    sort_direction: str = Query("asc", pattern="^(asc|desc)$"),
) -> dict[str, Any]:
    return get_stage1_current(project_root(), lifecycle_state=lifecycle_state, operator_status=operator_status,
        operator_priority=operator_priority, sector=sector, golden_cross_status=golden_cross_status,
        pattern_promotion_state=pattern_promotion_state, promotion_eligibility=promotion_eligibility,
        search=search, include_blocked=include_blocked, limit=limit, offset=offset,
        sort_by=sort_by, sort_direction=sort_direction)


@router.get("/investigator/stage1/transitions")
def execution_investigator_stage1_transitions(trade_date: str | None = None, limit: int = Query(200, ge=1, le=1000)) -> dict[str, Any]:
    return get_stage1_transitions(project_root(), trade_date=trade_date, limit=limit)


@router.get("/investigator/stage1/exits")
def execution_investigator_stage1_exits(trade_date: str | None = None, limit: int = Query(200, ge=1, le=1000)) -> dict[str, Any]:
    return get_stage1_exits(project_root(), trade_date=trade_date, limit=limit)


@router.get("/investigator/stage1/{symbol_id}")
def execution_investigator_stage1_history(symbol_id: str, lookback_days: int = Query(180, ge=1, le=730)) -> dict[str, Any]:
    return get_stage1_detail(symbol_id, lookback_days, project_root())


__all__ = ["router"]
