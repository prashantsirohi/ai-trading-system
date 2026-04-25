"""FastAPI routers for the execution console API."""

from ai_trading_system.ui.execution_api.routes import (
    health,
    pipeline,
    processes,
    runs,
    snapshots,
    tasks,
)

ALL_ROUTERS = (
    health.router,
    snapshots.router,
    runs.router,
    tasks.router,
    processes.router,
    pipeline.router,
)


__all__ = [
    "ALL_ROUTERS",
    "health",
    "pipeline",
    "processes",
    "runs",
    "snapshots",
    "tasks",
]
