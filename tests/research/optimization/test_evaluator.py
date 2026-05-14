"""Metrics + fitness."""

from __future__ import annotations

from datetime import date, timedelta

from ai_trading_system.research.backtesting.engine_runner import BacktestResult, BacktestTrade
from ai_trading_system.research.optimization.evaluator import (
    FitnessWeights,
    compute_metrics,
    fitness,
)


def _result_with_equity(equity_path: list[float], trades: list[BacktestTrade]) -> BacktestResult:
    rows = [{"date": date(2024, 1, 1) + timedelta(days=i), "equity": eq, "open_positions": 0} for i, eq in enumerate(equity_path)]
    return BacktestResult(trades=trades, equity_curve=rows)


def _trade(pnl: float, bars_held: int = 10) -> BacktestTrade:
    return BacktestTrade(
        symbol_id="X",
        exchange="NSE",
        entry_date=date(2024, 1, 1),
        entry_price=100.0,
        entry_reason="entry_confirmed",
        stop_price=None,
        stop_method=None,
        rank_at_entry=1,
        score_at_entry=80.0,
        sector="TECH",
        shares=100,
        bars_held=bars_held,
        pnl=pnl,
        pnl_pct=pnl / 10_000.0,
    )


def test_zero_trades_returns_neutral_metrics():
    res = BacktestResult(trades=[], equity_curve=[])
    m = compute_metrics(res, starting_equity=1_000_000.0)
    assert m.trade_count == 0
    assert m.total_return_pct == 0.0
    assert m.sharpe == 0.0


def test_monotonic_equity_yields_positive_cagr_and_sharpe():
    path = [1_000_000.0 + i * 1000 for i in range(252)]
    res = _result_with_equity(path, trades=[_trade(1000.0) for _ in range(50)])
    m = compute_metrics(res, starting_equity=1_000_000.0)
    assert m.cagr > 0
    assert m.sharpe > 0
    assert m.max_drawdown_pct == 0.0
    assert m.win_rate == 1.0


def test_drawdown_detected():
    path = [1_000_000.0, 1_100_000.0, 1_200_000.0, 900_000.0, 1_050_000.0]
    res = _result_with_equity(path, trades=[_trade(50_000.0), _trade(-300_000.0)])
    m = compute_metrics(res, starting_equity=1_000_000.0)
    # MDD = (900 - 1200) / 1200 = -25%
    assert -25.001 < m.max_drawdown_pct < -24.999


def test_win_rate_and_profit_factor():
    res = _result_with_equity(
        [1_000_000.0, 1_005_000.0],
        trades=[_trade(2000.0), _trade(1000.0), _trade(-1500.0)],
    )
    m = compute_metrics(res, starting_equity=1_000_000.0)
    assert abs(m.win_rate - 2 / 3) < 1e-9
    assert abs(m.profit_factor - 3000 / 1500) < 1e-9


def test_fitness_penalises_drawdown():
    # Two paths with same CAGR but different MDD — bigger MDD has lower fitness.
    flat = _result_with_equity([1_000_000.0, 1_010_000.0], trades=[_trade(10_000.0)])
    drawn = _result_with_equity([1_000_000.0, 1_500_000.0, 500_000.0, 1_010_000.0], trades=[_trade(10_000.0)])
    f_flat = fitness(compute_metrics(flat, starting_equity=1_000_000.0))
    f_drawn = fitness(compute_metrics(drawn, starting_equity=1_000_000.0))
    assert f_flat > f_drawn


def test_fitness_weights_are_dataclass_default():
    w = FitnessWeights()
    assert w.drawdown_penalty == 0.30
    assert w.cagr == 0.25
