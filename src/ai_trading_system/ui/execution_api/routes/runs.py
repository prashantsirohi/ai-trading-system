"""Run-history endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.control_center import (
    get_recent_runs,
    get_run_details,
)


router = APIRouter(prefix="/api/execution/runs", tags=["runs"])


@router.get("")
def execution_runs(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, Any]:
    return {"runs": get_recent_runs(project_root(), limit=limit)}


@router.get("/{run_id}")
def execution_run_details(run_id: str) -> dict[str, Any]:
    try:
        return get_run_details(project_root(), run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
