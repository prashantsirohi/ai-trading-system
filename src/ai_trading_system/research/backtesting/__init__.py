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
from ai_trading_system.research.backtesting.research_loader import load_research_ranked_by_date

__all__ = [
    "BacktestResult",
    "BacktestTrade",
    "EngineBacktestRunner",
    "discover_runs",
    "load_ranked_by_date",
    "load_research_ranked_by_date",
]
