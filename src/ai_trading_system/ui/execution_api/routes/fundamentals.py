"""Fundamentals artifact endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.fundamentals import get_latest_fundamentals


router = APIRouter(prefix="/api/execution/fundamentals", tags=["fundamentals"])


@router.get("/latest")
def latest_fundamentals(limit: int = Query(default=25, ge=1, le=100)) -> dict[str, Any]:
    return get_latest_fundamentals(project_root(), limit=limit)


__all__ = ["router"]

