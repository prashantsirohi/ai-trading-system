"""Champion guards: pinning, worst-fold-vs-NIFTY, zero-trade."""

from __future__ import annotations

from ai_trading_system.domains.strategy import StrategyRulePack
from ai_trading_system.research.optimization.acceptance import FoldResult
from ai_trading_system.research.optimization.evaluator import Metrics
from ai_trading_system.research.optimization.guards import champion_guards


def _metrics(trade_count: int = 50, total_return: float = 10.0, mdd: float = -10.0) -> Metrics:
    return Metrics(
        trade_count=trade_count,
        final_equity=1_100_000.0,
        starting_equity=1_000_000.0,
        total_return_pct=total_return,
        cagr=0.10,
        sharpe=1.0,
        sortino=1.2,
        max_drawdown_pct=mdd,
        win_rate=0.55,
        profit_factor=1.5,
        avg_holding_days=20.0,
        turnover_per_year=50.0,
        bars=252,
    )


def test_promotes_clean_champion():
    pack = StrategyRulePack(strategy_id="t")
    folds = [
        FoldResult(0, 0.8, _metrics(), nifty_return_pct=4.0),
        FoldResult(1, 0.9, _metrics(), nifty_return_pct=5.0),
    ]
    v = champion_guards(pack, folds)
    assert v.promote, v.reason


def test_blocks_on_zero_trade_fold():
    pack = StrategyRulePack(strategy_id="t")
    folds = [
        FoldResult(0, 0.8, _metrics(trade_count=0), nifty_return_pct=4.0),
        FoldResult(1, 0.9, _metrics(), nifty_return_pct=5.0),
    ]
    v = champion_guards(pack, folds)
    assert not v.promote
    assert v.reason == "zero_trade_fold"


def test_blocks_on_worst_fold_below_nifty():
    pack = StrategyRulePack(strategy_id="t")
    folds = [
        FoldResult(0, 0.2, _metrics(total_return=2.0), nifty_return_pct=8.0),  # bad fold
        FoldResult(1, 0.9, _metrics(total_return=15.0), nifty_return_pct=5.0),
    ]
    v = champion_guards(pack, folds)
    assert not v.promote
    assert v.reason == "worst_fold_below_nifty"


def test_blocks_when_too_many_weights_pinned():
    # 5 of 7 factors pinned at 0.0 → ratio 5/7 > 0.5
    pinned_weights = {
        "relative_strength": 0.6,
        "trend_persistence": 0.4,
        "volume_intensity": 0.0,
        "momentum_acceleration": 0.0,
        "proximity_highs": 0.0,
        "delivery_pct": 0.0,
        "sector_strength": 0.0,
    }
    pack = StrategyRulePack(strategy_id="t", ranking={"weights": pinned_weights})
    folds = [FoldResult(0, 0.8, _metrics(), nifty_return_pct=4.0)]
    v = champion_guards(pack, folds)
    assert not v.promote
    assert v.reason == "too_many_pinned_weights"
