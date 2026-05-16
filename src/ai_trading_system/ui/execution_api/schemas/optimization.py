"""Pydantic response models for the optimization router.

Lives next to ``schemas/requests.py``. Route handlers under
``routes/optimization.py`` return ``dict`` payloads built by the readmodel
(``services/readmodels/optimization_runs.py``); these models document and
type-check the response shape for OpenAPI codegen and the React client.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class OptimizationRunListItem(BaseModel):
    """One row in ``GET /api/execution/optimization/runs``."""

    optimization_run_id: str
    recipe_name: str
    strategy_id: str
    status: str = Field(description="pending | running | completed | failed | cancelled")
    from_date: date
    to_date: date
    seed: int
    max_trials: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    champion_rule_pack_id: Optional[str] = None
    trial_count: int = Field(description="Distinct trials persisted (excludes baseline marker iteration=-1).")
    error: Optional[str] = None


class OptimizationRunListResponse(BaseModel):
    runs: list[OptimizationRunListItem]


class FoldMetrics(BaseModel):
    """Per-fold metrics row from ``strategy_iteration_result`` (fold_index >= 0)."""

    fold_index: int
    fitness: Optional[float] = None
    cagr: Optional[float] = None
    sharpe: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    win_rate: Optional[float] = None
    trade_count: Optional[int] = None
    total_return_pct: Optional[float] = None
    benchmark_return_pct: Optional[float] = None


class OptimizationRunDetail(BaseModel):
    """Run header + champion + baseline per-fold + report path."""

    optimization_run_id: str
    recipe_name: str
    strategy_id: str
    status: str
    from_date: date
    to_date: date
    seed: int
    max_trials: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    baseline_rule_pack_id: str
    baseline_folds: list[FoldMetrics] = Field(default_factory=list)
    champion_rule_pack_id: Optional[str] = None
    champion_folds: list[FoldMetrics] = Field(default_factory=list)
    champion_lifecycle_status: Optional[str] = None
    trial_count: int
    report_path: Optional[str] = Field(
        default=None,
        description="Filesystem path to the auto-written report, if present.",
    )
    report_exists: bool = False


class OptimizationTrial(BaseModel):
    """One row in ``GET /api/execution/optimization/runs/{run_id}/trials``.

    Sourced from the aggregate row (``fold_index = -1``) for each trial
    (``iteration >= 0``). Baseline (iteration = -1) is excluded.
    """

    iteration: int
    rule_pack_id: str
    fitness: Optional[float] = None
    cagr: Optional[float] = None
    sharpe: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    win_rate: Optional[float] = None
    trade_count: Optional[int] = None
    total_return_pct: Optional[float] = None
    accepted: Optional[bool] = None
    rejection_reason: Optional[str] = None
    created_at: Optional[datetime] = None


class OptimizationTrialsResponse(BaseModel):
    optimization_run_id: str
    trials: list[OptimizationTrial]


class LeaderboardRow(BaseModel):
    """One row in ``GET /api/execution/optimization/leaderboard``.

    The best champion across recipes by the selected metric.
    """

    recipe_name: str
    strategy_id: str
    optimization_run_id: str
    champion_rule_pack_id: str
    champion_lifecycle_status: str
    fitness: Optional[float] = None
    cagr: Optional[float] = None
    sharpe: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    win_rate: Optional[float] = None
    trade_count: Optional[int] = None
    total_return_pct: Optional[float] = None
    completed_at: Optional[datetime] = None


class LeaderboardResponse(BaseModel):
    metric: str
    rows: list[LeaderboardRow]


class ReportContentResponse(BaseModel):
    optimization_run_id: str
    recipe_name: str
    report_path: str
    content: str


__all__ = [
    "OptimizationRunListItem",
    "OptimizationRunListResponse",
    "FoldMetrics",
    "OptimizationRunDetail",
    "OptimizationTrial",
    "OptimizationTrialsResponse",
    "LeaderboardRow",
    "LeaderboardResponse",
    "ReportContentResponse",
]
