"""Engine-driven backtesting.

Built on top of ``ai_trading_system.domains.risk.TradingRuleEngine`` so that the
research backtest and the paper-trade execution path produce identical
``EntryDecision`` / ``ExitDecision`` outputs for the same inputs.
"""

from ai_trading_system.research.backtesting.engine_runner import (
    BacktestResult,
    BacktestTrade,
    EngineBacktestRunner,
)
from ai_trading_system.research.backtesting.pipeline_loader import (
    discover_runs,
    load_ranked_by_date,
)
from ai_trading_system.research.backtesting.research_loader import (
    RANKING_METHOD_VERSION,
    load_research_ranked_by_date,
    validate_research_dynamic_data,
)
from ai_trading_system.research.backtesting.winner_capture import (
    WinnerCaptureConfig,
    run_winner_capture_analysis,
)

__all__ = [
    "BacktestResult",
    "BacktestTrade",
    "EngineBacktestRunner",
    "RANKING_METHOD_VERSION",
    "discover_runs",
    "load_ranked_by_date",
    "load_research_ranked_by_date",
    "WinnerCaptureConfig",
    "WinnerValidationConfig",
    "run_winner_capture_analysis",
    "run_winner_validation_report",
    "validate_research_dynamic_data",
]


def __getattr__(name: str):
    if name in {"WinnerValidationConfig", "run_winner_validation_report"}:
        from ai_trading_system.research.backtesting import winner_validation

        return getattr(winner_validation, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
