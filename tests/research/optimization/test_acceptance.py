"""Acceptance gates — worst-fold guards must reject early."""

from __future__ import annotations

from ai_trading_system.research.optimization.acceptance import (
    AcceptanceThresholds,
    FoldResult,
    is_accepted,
)
from ai_trading_system.research.optimization.evaluator import Metrics


def _metrics(
    *,
    total_return_pct: float = 10.0,
    mdd: float = -10.0,
    trade_count: int = 50,
    bars: int = 252,
) -> Metrics:
    return Metrics(
        trade_count=trade_count,
        final_equity=1_100_000.0,
        starting_equity=1_000_000.0,
        total_return_pct=total_return_pct,
        cagr=0.10,
        sharpe=1.0,
        sortino=1.2,
        max_drawdown_pct=mdd,
        win_rate=0.55,
        profit_factor=1.5,
        avg_holding_days=20.0,
        turnover_per_year=trade_count * (252 / bars),
        bars=bars,
    )


def _fold(idx: int, fitness: float, **overrides) -> FoldResult:
    return FoldResult(
        fold_index=idx,
        fitness=fitness,
        metrics=_metrics(**overrides),
        nifty_return_pct=overrides.pop("nifty_return_pct", 5.0) if "nifty_return_pct" in overrides else 5.0,
    )


def test_zero_trade_fold_rejected():
    cand = [_fold(0, 1.0, trade_count=0)]
    base = [_fold(0, 0.5)]
    verdict = is_accepted(cand, champion_folds=None, baseline_folds=base)
    assert not verdict.accepted
    assert verdict.reason == "zero_trade_fold"


def test_worst_fold_underperforms_nifty_rejected():
    cand = [
        FoldResult(0, 1.0, _metrics(total_return_pct=2.0), nifty_return_pct=8.0),  # underperforms
        FoldResult(1, 1.2, _metrics(total_return_pct=15.0), nifty_return_pct=5.0),
    ]
    base = [
        FoldResult(0, 0.5, _metrics(total_return_pct=4.0), nifty_return_pct=8.0),
        FoldResult(1, 0.6, _metrics(total_return_pct=6.0), nifty_return_pct=5.0),
    ]
    verdict = is_accepted(cand, champion_folds=None, baseline_folds=base)
    assert not verdict.accepted
    assert verdict.reason == "worst_fold_underperforms_nifty"


def test_worst_fold_mdd_too_deep_rejected():
    cand = [
        FoldResult(0, 1.0, _metrics(mdd=-30.0), nifty_return_pct=5.0),
        FoldResult(1, 1.2, _metrics(mdd=-5.0), nifty_return_pct=5.0),
    ]
    base = [
        FoldResult(0, 0.5, _metrics(mdd=-15.0), nifty_return_pct=5.0),  # baseline worst-fold MDD 15%
        FoldResult(1, 0.6, _metrics(mdd=-5.0), nifty_return_pct=5.0),
    ]
    verdict = is_accepted(cand, champion_folds=None, baseline_folds=base)
    assert not verdict.accepted
    assert verdict.reason == "worst_fold_mdd_too_deep"


def test_too_few_trades_rejected():
    cand = [_fold(0, 1.0, trade_count=10)]
    base = [_fold(0, 0.5)]
    verdict = is_accepted(cand, champion_folds=None, baseline_folds=base)
    assert not verdict.accepted
    assert verdict.reason == "too_few_trades"


def test_fold_improvement_rate_too_low_rejected():
    cand = [_fold(i, 0.4) for i in range(5)]  # all worse than baseline
    base = [_fold(i, 0.5) for i in range(5)]
    verdict = is_accepted(cand, champion_folds=None, baseline_folds=base)
    assert not verdict.accepted
    assert verdict.reason == "fold_improvement_rate_too_low"


def test_accepts_when_all_gates_pass():
    cand = [_fold(i, 0.8) for i in range(5)]
    base = [_fold(i, 0.5) for i in range(5)]
    verdict = is_accepted(cand, champion_folds=None, baseline_folds=base)
    assert verdict.accepted, verdict.reason


def test_insufficient_improvement_vs_champion_rejected():
    cand = [_fold(i, 0.80) for i in range(5)]
    champ = [_fold(i, 0.79) for i in range(5)]  # only 0.01 worse — below 0.01 + epsilon
    base = [_fold(i, 0.50) for i in range(5)]
    thresholds = AcceptanceThresholds(min_fitness_improvement=0.05)
    verdict = is_accepted(
        cand, champion_folds=champ, baseline_folds=base, thresholds=thresholds
    )
    assert not verdict.accepted
    assert verdict.reason == "insufficient_fitness_improvement"
