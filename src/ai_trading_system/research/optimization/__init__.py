"""Research-only strategy optimizer.

Wraps the existing research backtester to consume a ``StrategyRulePack`` and
emit a ``BacktestResult``. Never touches the live ``rank``/``execute`` stages.
"""

from ai_trading_system.research.optimization.acceptance import (
    AcceptanceThresholds,
    AcceptanceVerdict,
    FoldResult,
    aggregate_fitness,
    is_accepted,
)
from ai_trading_system.research.optimization.backtest_adapter import run_backtest
from ai_trading_system.research.optimization.baselines import (
    BenchmarkReturn,
    benchmark_buyhold_return,
)
from ai_trading_system.research.optimization.evaluator import (
    FitnessWeights,
    Metrics,
    compute_metrics,
    fitness,
)
from ai_trading_system.research.optimization.walkforward import (
    WalkForwardFold,
    build_folds,
)

__all__ = [
    "AcceptanceThresholds",
    "AcceptanceVerdict",
    "BenchmarkReturn",
    "FitnessWeights",
    "FoldResult",
    "Metrics",
    "WalkForwardFold",
    "aggregate_fitness",
    "benchmark_buyhold_return",
    "build_folds",
    "compute_metrics",
    "fitness",
    "is_accepted",
    "run_backtest",
]
