"""Optimization read endpoints.

Surfaces ``strategy_optimization_run``, ``strategy_iteration_result``, and
``strategy_rule_pack`` (all in ``data/control_plane.duckdb``) to the React
console. All queries delegate to
``services/readmodels/optimization_runs.py`` so the route layer stays thin
and the AST layer-boundary lint is satisfied.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.optimization_runs import (
    get_leaderboard,
    get_report,
    get_run_detail,
    get_trials,
    list_runs,
)


router = APIRouter(prefix="/api/execution/optimization", tags=["optimization"])


@router.get("/runs")
def optimization_runs(
    recipe: Optional[str] = Query(default=None, description="Filter by recipe_name (exact match)."),
    status: Optional[str] = Query(
        default=None,
        description="Filter by run status: pending | running | completed | failed | cancelled.",
    ),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    return list_runs(project_root(), recipe=recipe, status=status, limit=limit)


@router.get("/runs/{optimization_run_id}")
def optimization_run_detail(optimization_run_id: str) -> dict[str, Any]:
    detail = get_run_detail(project_root(), optimization_run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"unknown optimization_run_id: {optimization_run_id}")
    return detail


@router.get("/runs/{optimization_run_id}/trials")
def optimization_run_trials(
    optimization_run_id: str,
    limit: int = Query(default=200, ge=1, le=2000),
    sort: str = Query(
        default="iteration",
        description="Sort column: iteration|fitness|cagr|sharpe|max_drawdown_pct|win_rate|trade_count|total_return_pct (unknown values fall back to iteration).",
    ),
) -> dict[str, Any]:
    return get_trials(
        project_root(), optimization_run_id, limit=limit, sort=sort
    )


@router.get("/leaderboard")
def optimization_leaderboard(
    metric: str = Query(
        default="sharpe",
        description="Metric to rank champions by: fitness|cagr|sharpe|win_rate|total_return_pct|trade_count.",
    ),
    top: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    return get_leaderboard(project_root(), metric=metric, top=top)


@router.get("/runs/{optimization_run_id}/report")
def optimization_run_report(optimization_run_id: str) -> dict[str, Any]:
    """Return the auto-written markdown report for a run, if present.

    The runner writes the report to
    ``reports/optimization/<recipe>/<run_id>.md`` after a successful run
    (see ``research/optimization/runner.py::_write_run_report``). If the
    report is missing (run failed, ``--no-report`` was passed, or the file
    was deleted) this returns 404.
    """
    payload = get_report(project_root(), optimization_run_id)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"no report for optimization_run_id: {optimization_run_id}",
        )
    return payload
