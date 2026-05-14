"""Final champion guards applied at champion-record time.

These supplement ``acceptance.is_accepted`` (which runs per-trial inside the
Optuna objective). The acceptance gate watches each candidate as it streams
through; these guards inspect the final champion holistically — e.g. parameter
pinning, fold-level NIFTY dominance — and decide whether to *promote* the
champion to ``walkforward_passed`` or hold it back at ``backtested``.
"""

from __future__ import annotations

from dataclasses import dataclass

from ai_trading_system.domains.strategy.rule_pack import FACTOR_KEYS, StrategyRulePack
from ai_trading_system.research.optimization.acceptance import FoldResult


@dataclass(frozen=True)
class ChampionGuardVerdict:
    promote: bool
    reason: str
    detail: dict


def _weight_pinned(value: float, lo: float, hi: float, tol: float = 1e-3) -> bool:
    return value <= lo + tol or value >= hi - tol


def champion_guards(
    champion_pack: StrategyRulePack,
    champion_folds: list[FoldResult],
    *,
    weight_lo: float = 0.0,
    weight_hi: float = 1.0,
    max_pinned_factor_ratio: float = 0.5,
) -> ChampionGuardVerdict:
    """Holistic checks before promoting champion to walkforward_passed."""
    if not champion_folds:
        return ChampionGuardVerdict(False, "no_folds", {})

    # 1) Zero-trade fold.
    zero_folds = [f.fold_index for f in champion_folds if f.metrics.trade_count == 0]
    if zero_folds:
        return ChampionGuardVerdict(
            False, "zero_trade_fold", {"folds": zero_folds}
        )

    # 2) Worst-fold fitness vs NIFTY (when NIFTY available).
    worst = min(champion_folds, key=lambda f: f.fitness)
    if (
        worst.nifty_return_pct is not None
        and worst.metrics.total_return_pct < worst.nifty_return_pct
    ):
        return ChampionGuardVerdict(
            False,
            "worst_fold_below_nifty",
            {
                "fold_index": worst.fold_index,
                "return_pct": worst.metrics.total_return_pct,
                "nifty_pct": worst.nifty_return_pct,
            },
        )

    # 3) Weight pinning — flag if > N% of factors are at their search-space bound.
    weights = champion_pack.ranking.weights
    pinned = [k for k in FACTOR_KEYS if _weight_pinned(weights.get(k, 0.0), weight_lo, weight_hi)]
    if len(pinned) / len(FACTOR_KEYS) > max_pinned_factor_ratio:
        return ChampionGuardVerdict(
            False, "too_many_pinned_weights", {"pinned": pinned}
        )

    return ChampionGuardVerdict(
        True,
        "ok",
        {
            "worst_fold_index": worst.fold_index,
            "worst_fold_fitness": worst.fitness,
            "pinned_factors": pinned,
        },
    )
