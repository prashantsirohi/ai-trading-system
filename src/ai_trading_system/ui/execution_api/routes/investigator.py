"""Stock investigator API route."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.investigator import (
    get_investigator_snapshot,
)


router = APIRouter(prefix="/api/execution", tags=["investigator"])


@router.get("/investigator")
def execution_investigator() -> dict[str, Any]:
    return get_investigator_snapshot(project_root())


__all__ = ["router"]
