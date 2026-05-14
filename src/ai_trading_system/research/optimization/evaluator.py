"""Backtest metrics + fitness composite.

Metrics are computed from a ``BacktestResult`` (trades + equity_curve). Fitness
is a single scalar Optuna maximises; weights are surfaced in OptimizationRecipe
and pinned for a study — do not retune based on outcomes (meta-overfitting).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date

import numpy as np

from ai_trading_system.research.backtesting.engine_runner import BacktestResult


TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True)
class Metrics:
    trade_count: int
    final_equity: float
    starting_equity: float
    total_return_pct: float
    cagr: float
    sharpe: float
    sortino: float
    max_drawdown_pct: float
    win_rate: float
    profit_factor: float
    avg_holding_days: float
    turnover_per_year: float
    bars: int

    @property
    def trades_per_year(self) -> float:
        if self.bars <= 0:
            return 0.0
        years = self.bars / TRADING_DAYS_PER_YEAR
        return self.trade_count / years if years > 0 else 0.0


@dataclass(frozen=True)
class FitnessWeights:
    """Default weights from the plan. Pinned per study."""

    cagr: float = 0.25
    sharpe: float = 0.20
    sortino: float = 0.10
    win_rate: float = 0.05
    drawdown_penalty: float = 0.30
    turnover_penalty: float = 0.10


def compute_metrics(result: BacktestResult, *, starting_equity: float) -> Metrics:
    trades = result.trades
    equity_rows = result.equity_curve

    if not equity_rows:
        return Metrics(
            trade_count=0,
            final_equity=starting_equity,
            starting_equity=starting_equity,
            total_return_pct=0.0,
            cagr=0.0,
            sharpe=0.0,
            sortino=0.0,
            max_drawdown_pct=0.0,
            win_rate=0.0,
            profit_factor=0.0,
            avg_holding_days=0.0,
            turnover_per_year=0.0,
            bars=0,
        )

    equity = np.asarray([row["equity"] for row in equity_rows], dtype=float)
    final_equity = float(equity[-1])
    total_return = final_equity / starting_equity - 1.0

    # Per-bar returns (treat each row as one trading day).
    rets = np.diff(equity) / equity[:-1] if len(equity) > 1 else np.array([])
    if rets.size > 1 and rets.std(ddof=0) > 0:
        sharpe = float(rets.mean() / rets.std(ddof=0) * math.sqrt(TRADING_DAYS_PER_YEAR))
    else:
        sharpe = 0.0

    downside = rets[rets < 0]
    if downside.size > 1 and downside.std(ddof=0) > 0:
        sortino = float(rets.mean() / downside.std(ddof=0) * math.sqrt(TRADING_DAYS_PER_YEAR))
    else:
        sortino = 0.0

    running_max = np.maximum.accumulate(equity)
    drawdown = (equity - running_max) / running_max
    max_dd_pct = float(drawdown.min() * 100.0) if drawdown.size else 0.0

    bars = len(equity)
    years = bars / TRADING_DAYS_PER_YEAR
    cagr = float((final_equity / starting_equity) ** (1.0 / years) - 1.0) if years > 0 and final_equity > 0 else 0.0

    wins = [t for t in trades if (t.pnl or 0.0) > 0]
    losses = [t for t in trades if (t.pnl or 0.0) <= 0]
    win_rate = (len(wins) / len(trades)) if trades else 0.0
    sum_win = sum((t.pnl or 0.0) for t in wins)
    sum_loss = abs(sum((t.pnl or 0.0) for t in losses))
    profit_factor = (sum_win / sum_loss) if sum_loss > 0 else (math.inf if sum_win > 0 else 0.0)

    avg_holding = (
        float(np.mean([t.bars_held for t in trades])) if trades else 0.0
    )
    turnover_per_year = (len(trades) / years) if years > 0 else 0.0

    return Metrics(
        trade_count=len(trades),
        final_equity=final_equity,
        starting_equity=starting_equity,
        total_return_pct=total_return * 100.0,
        cagr=cagr,
        sharpe=sharpe,
        sortino=sortino,
        max_drawdown_pct=max_dd_pct,
        win_rate=win_rate,
        profit_factor=profit_factor,
        avg_holding_days=avg_holding,
        turnover_per_year=turnover_per_year,
        bars=bars,
    )


def fitness(metrics: Metrics, weights: FitnessWeights = FitnessWeights()) -> float:
    """Single scalar score. Higher is better. MDD enters as a penalty (it's
    negative, so we add ``drawdown_penalty * mdd_pct/100``)."""
    # Clamp profit_factor to keep one bad fold from dominating.
    sharpe = max(min(metrics.sharpe, 5.0), -5.0)
    sortino = max(min(metrics.sortino, 5.0), -5.0)
    # mdd is negative (e.g. -15.0 means -15%). Penalty: weight * |mdd_pct|/100.
    dd_penalty = weights.drawdown_penalty * abs(metrics.max_drawdown_pct) / 100.0
    # Turnover penalty: scaled so that 20 trades/yr ≈ 0, 100 trades/yr ≈ 0.1.
    turnover_excess = max(0.0, metrics.turnover_per_year - 20.0) / 80.0
    turnover_penalty = weights.turnover_penalty * turnover_excess

    return (
        weights.cagr * metrics.cagr
        + weights.sharpe * sharpe / 5.0  # normalised so sharpe=5 contributes weight
        + weights.sortino * sortino / 5.0
        + weights.win_rate * metrics.win_rate
        - dd_penalty
        - turnover_penalty
    )
