"""FastAPI backend for the React execution console."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ai_trading_system.ui.execution_api.services.execution_operator import (
    get_execution_summary,
    get_market_snapshot,
    get_pipeline_workspace_snapshot,
    get_process_snapshot,
    get_ranking_snapshot,
    get_shadow_snapshot,
    get_task_detail,
    list_task_details,
    get_task_snapshot,
    launch_research_action,
    retry_publish_action,
    run_pipeline_action,
    run_shadow_action,
    terminate_task_action,
    terminate_process_action,
)
from ai_trading_system.ui.execution_api.services.control_center import get_recent_runs, get_run_details


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[4]
API_KEY_HEADER = "x-api-key"


class PipelineRunRequest(BaseModel):
    label: str = "Execution API pipeline run"
    stages: list[str] = Field(
        default_factory=lambda: ["ingest", "features", "rank", "publish"]
    )
    params: dict[str, Any] = Field(default_factory=dict)
    run_id: str | None = None
    run_date: str | None = None


class PublishRetryRequest(BaseModel):
    local_publish: bool = False
    run_id: str | None = None


class ShadowRunRequest(BaseModel):
    label: str = "Shadow refresh"
    backfill_days: int = 0
    prediction_date: str | None = None


class ResearchLaunchRequest(BaseModel):
    port: int = 8501


def _project_root() -> Path:
    return Path(os.getenv("AI_TRADING_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)).resolve()


def _configured_api_key() -> str | None:
    value = os.getenv("EXECUTION_API_KEY")
    if value is None:
        return None
    value = value.strip()
    return value or None


def create_app() -> FastAPI:
    app = FastAPI(title="AI Trading Execution API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*", API_KEY_HEADER],
    )

    @app.middleware("http")
    async def api_key_auth(request: Request, call_next):
        # Allow CORS preflight requests (OPTIONS without API key)
        if request.method == "OPTIONS":
            return await call_next(request)
        if request.url.path.startswith("/api"):
            api_key = _configured_api_key()
            if api_key is None:
                return JSONResponse(
                    status_code=500,
                    content={"detail": "Execution API key is not configured"},
                )
            key = (request.headers.get(API_KEY_HEADER) or "").strip()
            if key != api_key:
                return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
        return await call_next(request)

    @app.get("/api/execution/summary")
    def execution_summary() -> dict[str, Any]:
        return get_execution_summary(_project_root())

    @app.get("/api/execution/health")
    def execution_health() -> dict[str, Any]:
        return get_execution_summary(_project_root())["health"]

    @app.get("/api/execution/ranking")
    def execution_ranking(
        limit: int = Query(default=25, ge=1, le=200),
        stage2_only: bool = Query(default=False),
        stage2_min_score: float | None = Query(default=None, ge=0.0, le=100.0),
    ) -> dict[str, Any]:
        return get_ranking_snapshot(
            _project_root(),
            limit=limit,
            stage2_only=stage2_only,
            stage2_min_score=stage2_min_score,
        )

    @app.get("/api/execution/market")
    def execution_market(
        limit: int = Query(default=25, ge=1, le=200),
    ) -> dict[str, Any]:
        return get_market_snapshot(_project_root(), limit=limit)

    @app.get("/api/execution/workspace/pipeline")
    def execution_workspace_pipeline(
        limit: int = Query(default=20, ge=1, le=200),
        stage2_only: bool = Query(default=False),
        stage2_min_score: float | None = Query(default=None, ge=0.0, le=100.0),
    ) -> dict[str, Any]:
        return get_pipeline_workspace_snapshot(
            _project_root(),
            limit=limit,
            stage2_only=stage2_only,
            stage2_min_score=stage2_min_score,
        )

    @app.get("/api/execution/shadow")
    def execution_shadow() -> dict[str, Any]:
        return get_shadow_snapshot(_project_root())

    @app.get("/api/execution/runs")
    def execution_runs(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, Any]:
        return {"runs": get_recent_runs(_project_root(), limit=limit)}

    @app.get("/api/execution/runs/{run_id}")
    def execution_run_details(run_id: str) -> dict[str, Any]:
        try:
            return get_run_details(_project_root(), run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/execution/tasks")
    def execution_tasks(limit: int = Query(default=50, ge=1, le=300)) -> dict[str, Any]:
        return {"tasks": list_task_details(_project_root(), limit=limit)}

    @app.get("/api/execution/tasks/{task_id}")
    def execution_task(task_id: str) -> dict[str, Any]:
        try:
            return {"task": get_task_detail(_project_root(), task_id)}
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/execution/tasks/{task_id}/logs")
    def execution_task_logs(
        task_id: str,
        after: int = Query(default=0, ge=0),
        limit: int = Query(default=300, ge=1, le=1000),
    ) -> dict[str, Any]:
        try:
            return get_task_snapshot(
                _project_root(), task_id, after=after, log_limit=limit
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/execution/tasks/{task_id}/events")
    async def execution_task_events(
        task_id: str, cursor: int = Query(default=0, ge=0)
    ) -> StreamingResponse:
        project_root = _project_root()

        async def _stream() -> Any:
            next_cursor = int(cursor)
            while True:
                try:
                    payload = get_task_snapshot(
                        project_root, task_id, after=next_cursor, log_limit=100
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
                if payload["task"].get("status") in {
                    "completed",
                    "completed_with_publish_errors",
                    "failed",
                    "terminated",
                }:
                    return
                await asyncio.sleep(1.0)

        return StreamingResponse(_stream(), media_type="text/event-stream")

    @app.get("/api/execution/processes")
    def execution_processes() -> dict[str, Any]:
        return get_process_snapshot(_project_root())

    @app.post("/api/execution/pipeline/run")
    def execution_pipeline_run(request: PipelineRunRequest) -> dict[str, Any]:
        task = run_pipeline_action(
            _project_root(),
            label=request.label,
            stages=request.stages,
            params=request.params,
            run_id=request.run_id,
            run_date=request.run_date,
        )
        return {"task": task}

    @app.post("/api/execution/pipeline/publish-retry")
    def execution_publish_retry(request: PublishRetryRequest) -> dict[str, Any]:
        try:
            task = retry_publish_action(
                _project_root(),
                local_publish=request.local_publish,
                run_id=request.run_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"task": task}

    @app.post("/api/execution/shadow/run")
    def execution_shadow_run(request: ShadowRunRequest) -> dict[str, Any]:
        task = run_shadow_action(
            _project_root(),
            label=request.label,
            backfill_days=request.backfill_days,
            prediction_date=request.prediction_date,
        )
        return {"task": task}

    @app.post("/api/execution/research/launch")
    def execution_launch_research(request: ResearchLaunchRequest) -> dict[str, Any]:
        task = launch_research_action(_project_root(), port=request.port)
        return {"task": task}

    @app.post("/api/execution/processes/{pid}/terminate")
    def execution_terminate_process(pid: int) -> dict[str, Any]:
        return terminate_process_action(_project_root(), pid)

    @app.post("/api/execution/tasks/{task_id}/terminate")
    def execution_terminate_task(task_id: str) -> dict[str, Any]:
        try:
            return terminate_task_action(_project_root(), task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return app


app = create_app()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the FastAPI execution backend")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    uvicorn.run(
        "ui.execution_api.app:app", host=args.host, port=args.port, reload=False
    )


if __name__ == "__main__":  # pragma: no cover
    main()
