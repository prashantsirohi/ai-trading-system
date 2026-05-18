"""Expanding-window walk-forward driver for v2 (production-factor) optimiser."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import numpy as np
import optuna
import pandas as pd

from ai_trading_system.research.ranking_optimisation.data_v2 import (
    LiveFactorPanel,
    PRODUCTION_FACTOR_COLUMNS,
    SCORE_TO_WEIGHT_KEY,
    WEIGHT_KEYS,
    load_live_factor_panel,
    quarterly_anchors,
)
from ai_trading_system.research.ranking_optimisation.fitness_v2 import (
    V2FoldScore,
    combined_objective,
    normalise_weights_v2,
    score_weights_v2,
    single_metric_objective,
)

OBJECTIVE_MODES = ("ic_only", "lift_only", "hit_only", "combined")


@dataclass(frozen=True)
class V2FoldOutcome:
    test_year: int
    train_years: tuple[int, ...]
    train_panel_count: int
    test_panel_count: int
    active_factors: tuple[str, ...]
    best_weights: dict[str, float]
    train_objective_breakdown: dict[str, float]
    train_mean_ic: float
    oos_mean_ic: float
    oos_mean_lift: float
    oos_mean_hit: float
    oos_ic_per_panel: tuple[float, ...]


@dataclass
class WalkForwardResultV2:
    folds: list[V2FoldOutcome] = field(default_factory=list)
    panels: dict[pd.Timestamp, LiveFactorPanel] = field(default_factory=dict)
    objective_mode: str = "combined"
    horizon_days: int = 20
    top_n: int = 100

    def to_dataframe(self) -> pd.DataFrame:
        rows = []
        for f in self.folds:
            row = {
                "test_year":          f.test_year,
                "train_years":        ",".join(str(y) for y in f.train_years),
                "train_panels":       f.train_panel_count,
                "test_panels":        f.test_panel_count,
                "active_factors":     ",".join(f.active_factors),
                "train_mean_ic":      f.train_mean_ic,
                "oos_mean_ic":        f.oos_mean_ic,
                "oos_mean_lift":      f.oos_mean_lift,
                "oos_mean_hit":       f.oos_mean_hit,
            }
            row.update({f"w_{k}": v for k, v in f.best_weights.items()})
            rows.append(row)
        return pd.DataFrame(rows)


def _intersection_active(panels: list[LiveFactorPanel]) -> tuple[str, ...]:
    """Factors active on EVERY panel in the pool."""
    if not panels:
        return ()
    common = set(PRODUCTION_FACTOR_COLUMNS)
    for p in panels:
        common &= set(p.active_factors)
    return tuple(c for c in PRODUCTION_FACTOR_COLUMNS if c in common)


def _suggest_weights(trial: optuna.Trial, active_factors: tuple[str, ...]) -> dict[str, float]:
    raw: dict[str, float] = {}
    for key in WEIGHT_KEYS:
        score_col = next(c for c, k in SCORE_TO_WEIGHT_KEY.items() if k == key)
        if score_col not in active_factors:
            raw[key] = 0.0
        else:
            raw[key] = trial.suggest_float(f"w_{key}", 0.0, 1.0)
    return raw


def _objective_value(
    panels: list[LiveFactorPanel],
    weights: Mapping[str, float],
    *,
    mode: str,
    top_n: int,
    active_factors: tuple[str, ...],
) -> float:
    if mode == "combined":
        return combined_objective(
            panels, weights, top_n=top_n, active_factors=active_factors
        )["combined"]
    return single_metric_objective(
        panels, weights, mode=mode, top_n=top_n, active_factors=active_factors
    )


def run_walkforward_v2(
    years: list[int],
    *,
    horizon_days: int = 20,
    top_n: int = 100,
    n_trials: int = 200,
    min_train_years: int = 3,
    project_root: Path | str = Path.cwd(),
    rebalance_freq: str = "quarterly",
    objective_mode: str = "combined",
    exchange: str = "NSE",
    degenerate_var_floor: float = 1.0,
    seed: int = 42,
    log: bool = True,
) -> WalkForwardResultV2:
    """Run expanding-window walk-forward weight search using live factor scores."""
    if rebalance_freq != "quarterly":
        raise ValueError(f"rebalance_freq={rebalance_freq!r} not supported in v2 (only 'quarterly')")
    if objective_mode not in OBJECTIVE_MODES:
        raise ValueError(f"objective_mode={objective_mode!r} not in {OBJECTIVE_MODES}")
    if len(years) <= min_train_years:
        raise ValueError(f"need at least {min_train_years + 1} years; got {len(years)}")

    anchors = quarterly_anchors(years)
    if log:
        print(f"v2 walk-forward: {len(anchors)} quarterly anchors over years {years}")

    panels: dict[pd.Timestamp, LiveFactorPanel] = {}
    for anchor in anchors:
        if log:
            print(f"  loading panel {anchor.date()}...", flush=True)
        panel = load_live_factor_panel(
            anchor,
            horizon_days=horizon_days,
            project_root=project_root,
            exchange=exchange,
            degenerate_var_floor=degenerate_var_floor,
        )
        panels[panel.as_of] = panel
        if log:
            degenerate = ", ".join(panel.degenerate_factors) or "none"
            print(f"    n={panel.n}, degenerate=[{degenerate}]")

    by_year: dict[int, list[LiveFactorPanel]] = {}
    for p in panels.values():
        if p.n == 0:
            continue
        by_year.setdefault(p.as_of.year, []).append(p)

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sorted_years = sorted(by_year)
    result = WalkForwardResultV2(
        panels=panels, objective_mode=objective_mode, horizon_days=horizon_days, top_n=top_n,
    )

    for i, test_year in enumerate(sorted_years):
        if i < min_train_years:
            continue
        train_years = tuple(sorted_years[:i])
        train_pool: list[LiveFactorPanel] = []
        for y in train_years:
            train_pool.extend(by_year.get(y, []))
        test_pool = by_year.get(test_year, [])
        if not train_pool or not test_pool:
            if log:
                print(f"  fold {test_year}: skipped (empty pool)")
            continue

        train_active = _intersection_active(train_pool)
        test_active = _intersection_active(test_pool)
        active = tuple(c for c in train_active if c in test_active)
        if not active:
            if log:
                print(f"  fold {test_year}: skipped (no factor active on both train and test)")
            continue

        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(seed=seed),
        )

        def _trial(trial: optuna.Trial) -> float:
            raw = _suggest_weights(trial, active)
            return _objective_value(
                train_pool, raw, mode=objective_mode, top_n=top_n, active_factors=active,
            )

        study.optimize(_trial, n_trials=n_trials, show_progress_bar=False)

        # study.best_params has w_<key> prefix; strip before normalising.
        unprefixed = {
            k[len("w_"):] if k.startswith("w_") else k: v for k, v in study.best_params.items()
        }
        best_weights = normalise_weights_v2(unprefixed)
        breakdown = combined_objective(
            train_pool, best_weights, top_n=top_n, active_factors=active
        )
        train_mean_ic = breakdown["mean_ic"]

        oos_scores: list[V2FoldScore] = [
            score_weights_v2(p, best_weights, top_n=top_n, active_factors=active)
            for p in test_pool
        ]
        oos_ic_per_panel = tuple(float(s.ic) for s in oos_scores)
        finite = lambda xs: float(np.nanmean(xs)) if any(np.isfinite(xs)) else float("nan")
        oos_mean_ic   = finite([s.ic for s in oos_scores])
        oos_mean_lift = finite([s.top_decile_lift for s in oos_scores])
        oos_mean_hit  = finite([s.hit_rate for s in oos_scores])

        outcome = V2FoldOutcome(
            test_year=test_year,
            train_years=train_years,
            train_panel_count=len(train_pool),
            test_panel_count=len(test_pool),
            active_factors=active,
            best_weights=best_weights,
            train_objective_breakdown=breakdown,
            train_mean_ic=float(train_mean_ic),
            oos_mean_ic=oos_mean_ic,
            oos_mean_lift=oos_mean_lift,
            oos_mean_hit=oos_mean_hit,
            oos_ic_per_panel=oos_ic_per_panel,
        )
        result.folds.append(outcome)

        if log:
            top3 = sorted(best_weights.items(), key=lambda kv: -kv[1])[:3]
            top_str = ", ".join(f"{k}={v:.2f}" for k, v in top3)
            print(
                f"  fold {test_year}: train_ic={train_mean_ic:+.3f} "
                f"oos_ic={oos_mean_ic:+.3f} oos_lift={oos_mean_lift:+.1%} "
                f"oos_hit={oos_mean_hit:.0%}  top: {top_str}"
            )
    return result
