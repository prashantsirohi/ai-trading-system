"""Process snapshot + termination endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.execution_operator import (
    get_process_snapshot,
    terminate_process_action,
)


router = APIRouter(prefix="/api/execution/processes", tags=["processes"])


@router.get("")
def execution_processes() -> dict[str, Any]:
    return get_process_snapshot(project_root())


@router.post("/{pid}/terminate")
def execution_terminate_process(pid: int) -> dict[str, Any]:
    return terminate_process_action(project_root(), pid)
