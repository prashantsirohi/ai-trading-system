"""Health and summary endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.execution_operator import (
    get_execution_summary,
)


router = APIRouter(prefix="/api/execution", tags=["health"])


@router.get("/summary")
def execution_summary() -> dict[str, Any]:
    return get_execution_summary(project_root())


@router.get("/health")
def execution_health() -> dict[str, Any]:
    return get_execution_summary(project_root())["health"]
