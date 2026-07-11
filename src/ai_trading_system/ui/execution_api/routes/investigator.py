"""Stock investigator API route."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.investigator import (
    get_stage1_lifecycle_history,
    get_investigator_snapshot,
)


router = APIRouter(prefix="/api/execution", tags=["investigator"])


@router.get("/investigator")
def execution_investigator() -> dict[str, Any]:
    return get_investigator_snapshot(project_root())


@router.get("/investigator/stage1/{symbol_id}")
def execution_investigator_stage1_history(symbol_id: str, lookback_days: int = 180) -> dict[str, Any]:
    return get_stage1_lifecycle_history(symbol_id, lookback_days, project_root=project_root())


__all__ = ["router"]
