"""Latest event-aware insight endpoint."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.latest_insight import get_latest_insight


router = APIRouter(prefix="/api/execution", tags=["insight"])


@router.get("/insight/latest")
def execution_latest_insight() -> dict[str, Any]:
    return get_latest_insight(project_root())


__all__ = ["router"]
