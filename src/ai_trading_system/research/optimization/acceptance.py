"""Acceptance gate. Applied per-trial inside the Optuna objective AND before
a champion is recorded. Worst-fold guards live here, not in reports.
"""

from __future__ import annotations

from dataclasses import dataclass

from ai_trading_system.research.optimization.evaluator import Metrics


@dataclass(frozen=True)
class AcceptanceThresholds:
    """All gates configurable via OptimizationRecipe."""

    min_fitness_improvement: float = 0.01
    max_mdd_ratio_vs_champion: float = 1.10
    min_trades_per_year: float = 40.0
    min_fold_improvement_rate: float = 0.60  # ≥ 60% folds must beat baseline
    # Worst-fold hard rejects.
    worst_fold_min_return_vs_benchmark: bool = True
    worst_fold_max_mdd_ratio_vs_baseline: float = 1.10
    require_no_zero_trade_fold: bool = True

    # ------ Backwards-compat aliases ------
    # The legacy field name still parses from YAML and constructor calls.
    # Resolved to the canonical field in ``__post_init__``-style helper below.
    @classmethod
    def from_legacy(cls, **kwargs):
        legacy = kwargs.pop("worst_fold_min_return_vs_nifty", None)
        if legacy is not None and "worst_fold_min_return_vs_benchmark" not in kwargs:
            kwargs["worst_fold_min_return_vs_benchmark"] = bool(legacy)
        return cls(**kwargs)


@dataclass(frozen=True)
class FoldResult:
    fold_index: int
    fitness: float
    metrics: Metrics
    benchmark_return_pct: float | None = None
    benchmark_symbol: str | None = None

    # Backwards-compat alias for one release. Code paths should use
    # ``benchmark_return_pct`` going forward.
    @property
    def nifty_return_pct(self) -> float | None:
        return self.benchmark_return_pct


@dataclass(frozen=True)
class AcceptanceVerdict:
    accepted: bool
    reason: str
    detail: dict


def aggregate_fitness(folds: list[FoldResult]) -> float:
    if not folds:
        return float("-inf")
    return sum(f.fitness for f in folds) / len(folds)


def is_accepted(
    candidate_folds: list[FoldResult],
    *,
    champion_folds: list[FoldResult] | None,
    baseline_folds: list[FoldResult],
    thresholds: AcceptanceThresholds = AcceptanceThresholds(),
) -> AcceptanceVerdict:
    """Return ``AcceptanceVerdict(accepted, reason, detail)``.

    ``candidate_folds`` and ``baseline_folds`` must be aligned by fold index.
    ``champion_folds`` is the current champion (None ⇒ candidate is a new
    champion candidate vs only the baseline).
    """
    if not candidate_folds:
        return AcceptanceVerdict(False, "no_folds", {})

    # Worst-fold guards — hard rejects first.
    if thresholds.require_no_zero_trade_fold:
        zero = [f.fold_index for f in candidate_folds if f.metrics.trade_count == 0]
        if zero:
            return AcceptanceVerdict(False, "zero_trade_fold", {"folds": zero})

    worst = min(candidate_folds, key=lambda f: f.fitness)
    if (
        thresholds.worst_fold_min_return_vs_benchmark
        and worst.benchmark_return_pct is not None
        and worst.metrics.total_return_pct < worst.benchmark_return_pct
    ):
        return AcceptanceVerdict(
            False,
            "worst_fold_underperforms_benchmark",
            {
                "fold_index": worst.fold_index,
                "benchmark_symbol": worst.benchmark_symbol,
                "return_pct": worst.metrics.total_return_pct,
                "benchmark_pct": worst.benchmark_return_pct,
            },
        )

    # Worst-fold MDD vs baseline worst fold.
    baseline_by_idx = {f.fold_index: f for f in baseline_folds}
    baseline_for_worst = baseline_by_idx.get(worst.fold_index)
    if baseline_for_worst is not None:
        baseline_worst_mdd = abs(baseline_for_worst.metrics.max_drawdown_pct)
        cand_worst_mdd = abs(worst.metrics.max_drawdown_pct)
        if (
            baseline_worst_mdd > 0
            and cand_worst_mdd
            > baseline_worst_mdd * thresholds.worst_fold_max_mdd_ratio_vs_baseline
        ):
            return AcceptanceVerdict(
                False,
                "worst_fold_mdd_too_deep",
                {
                    "fold_index": worst.fold_index,
                    "cand_mdd_pct": cand_worst_mdd,
                    "baseline_mdd_pct": baseline_worst_mdd,
                },
            )

    # Trades-per-year (averaged across folds).
    avg_tpy = sum(f.metrics.trades_per_year for f in candidate_folds) / len(candidate_folds)
    if avg_tpy < thresholds.min_trades_per_year:
        return AcceptanceVerdict(
            False, "too_few_trades", {"avg_trades_per_year": avg_tpy}
        )

    # Must beat baseline on ≥ 60% of folds.
    beat = 0
    for cand in candidate_folds:
        base = baseline_by_idx.get(cand.fold_index)
        if base is not None and cand.fitness > base.fitness:
            beat += 1
    fold_rate = beat / len(candidate_folds)
    if fold_rate < thresholds.min_fold_improvement_rate:
        return AcceptanceVerdict(
            False, "fold_improvement_rate_too_low", {"rate": fold_rate}
        )

    # Mean fitness must beat the incumbent champion (if any).
    cand_mean = aggregate_fitness(candidate_folds)
    if champion_folds:
        champ_mean = aggregate_fitness(champion_folds)
        if cand_mean < champ_mean + thresholds.min_fitness_improvement:
            return AcceptanceVerdict(
                False,
                "insufficient_fitness_improvement",
                {"candidate": cand_mean, "champion": champ_mean},
            )
        # Mean MDD constraint (vs champion).
        cand_mdd = sum(abs(f.metrics.max_drawdown_pct) for f in candidate_folds) / len(
            candidate_folds
        )
        champ_mdd = sum(abs(f.metrics.max_drawdown_pct) for f in champion_folds) / len(
            champion_folds
        )
        if champ_mdd > 0 and cand_mdd > champ_mdd * thresholds.max_mdd_ratio_vs_champion:
            return AcceptanceVerdict(
                False, "mdd_worsened", {"cand_mdd": cand_mdd, "champ_mdd": champ_mdd}
            )

    return AcceptanceVerdict(
        True,
        "accepted",
        {"mean_fitness": cand_mean, "fold_improvement_rate": fold_rate, "avg_tpy": avg_tpy},
    )
