"""FastAPI routers for the execution console API."""

from ai_trading_system.ui.execution_api.routes import (
    artifacts,
    health,
    insight,
    pipeline,
    processes,
    ranking_detail,
    runs,
    sectors,
    snapshots,
    stocks,
    tasks,
)

ALL_ROUTERS = (
    health.router,
    insight.router,
    snapshots.router,
    runs.router,
    artifacts.router,
    stocks.router,
    ranking_detail.router,
    tasks.router,
    processes.router,
    pipeline.router,
    sectors.router,
)


__all__ = [
    "ALL_ROUTERS",
    "artifacts",
    "health",
    "insight",
    "pipeline",
    "processes",
    "ranking_detail",
    "runs",
    "sectors",
    "snapshots",
    "stocks",
    "tasks",
]
