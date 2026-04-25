"""Run-history endpoints."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.control_center import (
    get_recent_runs,
    get_run_details,
)
from ai_trading_system.ui.execution_api.services.readmodels.runs_introspection import (
    get_artifacts_for_run,
    get_dq_results_for_run,
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


@router.get("/{run_id}/dq")
def execution_run_dq(
    run_id: str,
    severity: Optional[str] = Query(
        default=None,
        description="Filter to a single severity tier (e.g. 'warn', 'error').",
    ),
    stage: Optional[str] = Query(
        default=None,
        description="Filter to a single pipeline stage (e.g. 'ingest', 'features').",
    ),
) -> dict[str, Any]:
    """Return DQ rule results for ``run_id`` plus per-severity aggregates."""

    return get_dq_results_for_run(
        project_root(),
        run_id,
        severity=severity,
        stage=stage,
    )


@router.get("/{run_id}/artifacts")
def execution_run_artifacts(run_id: str) -> dict[str, Any]:
    """Return the artifact registry for ``run_id``, grouped by stage."""

    return get_artifacts_for_run(project_root(), run_id)
