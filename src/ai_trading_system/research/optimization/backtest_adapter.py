"""Wraps research_loader + engine_runner. Takes a RulePack, returns a result."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.domains.strategy import (
    StrategyRulePack,
    to_ranking_weights,
    to_risk_policy_config,
)
from ai_trading_system.research.backtesting import EngineBacktestRunner
from ai_trading_system.research.backtesting.engine_runner import BacktestResult
from ai_trading_system.research.backtesting.research_loader import (
    load_research_ranked_by_date,
)


def run_backtest(
    pack: StrategyRulePack,
    *,
    project_root: Path | str,
    from_date: date,
    to_date: date,
    exchange: str = "NSE",
    benchmark_symbol: str = "NIFTY50",
    starting_equity: float = 1_000_000.0,
    commission_bps: float = 10.0,
    slippage_bps: float = 20.0,
) -> BacktestResult:
    """Run the engine-driven research backtest under the given rule pack."""
    ranked_by_date = load_research_ranked_by_date(
        project_root,
        from_date=from_date,
        to_date=to_date,
        exchange=exchange,
        benchmark_symbol=benchmark_symbol,
        weights_override=to_ranking_weights(pack),
    )
    runner = EngineBacktestRunner(
        risk_config=to_risk_policy_config(pack),
        starting_equity=starting_equity,
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
    )
    return runner.run(ranked_by_date)
