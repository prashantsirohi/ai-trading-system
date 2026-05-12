"""FastAPI routers for the execution console API."""

from ai_trading_system.ui.execution_api.routes import (
    artifacts,
    backtest,
    fundamentals,
    health,
    insight,
    perf_tracker,
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
    fundamentals.router,
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
    backtest.router,
    perf_tracker.router,
)


__all__ = [
    "ALL_ROUTERS",
    "artifacts",
    "backtest",
    "fundamentals",
    "health",
    "insight",
    "perf_tracker",
    "pipeline",
    "processes",
    "ranking_detail",
    "runs",
    "sectors",
    "snapshots",
    "stocks",
    "tasks",
]
