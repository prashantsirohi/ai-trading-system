"""FastAPI routers for the execution console API."""

from ai_trading_system.ui.execution_api.routes import (
    artifacts,
    health,
    pipeline,
    processes,
    runs,
    snapshots,
    stocks,
    tasks,
)

ALL_ROUTERS = (
    health.router,
    snapshots.router,
    runs.router,
    artifacts.router,
    stocks.router,
    tasks.router,
    processes.router,
    pipeline.router,
)


__all__ = [
    "ALL_ROUTERS",
    "artifacts",
    "health",
    "pipeline",
    "processes",
    "runs",
    "snapshots",
    "stocks",
    "tasks",
]
