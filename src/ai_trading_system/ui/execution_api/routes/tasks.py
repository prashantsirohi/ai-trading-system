"""Task-detail endpoints (list, detail, logs, SSE events, terminate)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.execution_operator import (
    get_task_detail,
    get_task_snapshot,
    list_task_details,
    terminate_task_action,
)


router = APIRouter(prefix="/api/execution/tasks", tags=["tasks"])


_TERMINAL_STATUSES = frozenset(
    {
        "completed",
        "completed_with_publish_errors",
        "failed",
        "terminated",
    }
)


@router.get("")
def execution_tasks(limit: int = Query(default=50, ge=1, le=300)) -> dict[str, Any]:
    return {"tasks": list_task_details(project_root(), limit=limit)}


@router.get("/{task_id}")
def execution_task(task_id: str) -> dict[str, Any]:
    try:
        return {"task": get_task_detail(project_root(), task_id)}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{task_id}/logs")
def execution_task_logs(
    task_id: str,
    after: int = Query(default=0, ge=0),
    limit: int = Query(default=300, ge=1, le=1000),
) -> dict[str, Any]:
    try:
        return get_task_snapshot(
            project_root(), task_id, after=after, log_limit=limit
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{task_id}/events")
async def execution_task_events(
    task_id: str, cursor: int = Query(default=0, ge=0)
) -> StreamingResponse:
    root = project_root()

    async def _stream() -> Any:
        next_cursor = int(cursor)
        while True:
            try:
                payload = get_task_snapshot(
                    root, task_id, after=next_cursor, log_limit=100
                )
            except KeyError as exc:
                event = {"error": str(exc)}
                yield f"data: {json.dumps(event)}\n\n"
                return
            logs = payload.get("logs", [])
            if logs:
                next_cursor = max(
                    int(row.get("log_cursor", next_cursor)) for row in logs
                )
            event = {
                "task": payload["task"],
                "logs": logs,
                "cursor": next_cursor,
            }
            yield f"data: {json.dumps(event, default=str)}\n\n"
            if payload["task"].get("status") in _TERMINAL_STATUSES:
                return
            await asyncio.sleep(1.0)

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.post("/{task_id}/terminate")
def execution_terminate_task(task_id: str) -> dict[str, Any]:
    try:
        return terminate_task_action(project_root(), task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
