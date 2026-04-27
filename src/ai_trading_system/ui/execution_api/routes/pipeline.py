"""Pipeline / shadow / research action endpoints (POST verbs)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.schemas.requests import (
    PipelineRunRequest,
    PublishRetryRequest,
    ShadowRunRequest,
)
from ai_trading_system.ui.execution_api.services.execution_operator import (
    retry_publish_action,
    run_pipeline_action,
    run_shadow_action,
)


router = APIRouter(prefix="/api/execution", tags=["actions"])


@router.post("/pipeline/run")
def execution_pipeline_run(request: PipelineRunRequest) -> dict[str, Any]:
    task = run_pipeline_action(
        project_root(),
        label=request.label,
        stages=request.stages,
        params=request.params,
        run_id=request.run_id,
        run_date=request.run_date,
    )
    return {"task": task}


@router.post("/pipeline/publish-retry")
def execution_publish_retry(request: PublishRetryRequest) -> dict[str, Any]:
    try:
        task = retry_publish_action(
            project_root(),
            local_publish=request.local_publish,
            run_id=request.run_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"task": task}


@router.post("/shadow/run")
def execution_shadow_run(request: ShadowRunRequest) -> dict[str, Any]:
    task = run_shadow_action(
        project_root(),
        label=request.label,
        backfill_days=request.backfill_days,
        prediction_date=request.prediction_date,
    )
    return {"task": task}
