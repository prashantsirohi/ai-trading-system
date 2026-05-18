"""Expanding-window walk-forward driver for ranking-weight optimisation.

For each test year Y in ``test_years``:
  1. Train panels = panels at as-of dates from years [first_year .. Y-1].
  2. Optuna searches weight vectors that maximise mean(IC) over train panels.
  3. Best train weights are applied to year Y → reports OOS IC / hit / lift.

The first ``min_train_years`` years are always training-only (no OOS fold
produced for them) so the first test fold has at least that many training
samples to fit on. With years [2020..2024] and ``min_train_years=3`` this
yields 2 OOS folds: 2023 and 2024.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import optuna
import pandas as pd

from ai_trading_system.research.ranking_optimisation.data import (
    DEFAULT_DB_PATH,
    FACTOR_NAMES,
    FactorPanel,
    load_factor_panel,
)
from ai_trading_system.research.ranking_optimisation.fitness import (
    FoldScore,
    mean_ic_over_panels,
    normalise_weights,
    score_weights,
)


@dataclass(frozen=True)
class FoldOutcome:
    """One walk-forward fold: weights learned on train years, evaluated OOS."""

    test_year: int
    train_years: tuple[int, ...]
    best_weights: dict[str, float]
    train_mean_ic: float
    oos_score: FoldScore


@dataclass
class WalkForwardResult:
    folds: list[FoldOutcome] = field(default_factory=list)
    panels: dict[int, FactorPanel] = field(default_factory=dict)

    def to_dataframe(self) -> pd.DataFrame:
        rows = []
        for f in self.folds:
            row = {
                "test_year": f.test_year,
                "train_years": ",".join(str(y) for y in f.train_years),
                "train_ic": f.train_mean_ic,
                "oos_ic": f.oos_score.ic,
                "oos_hit": f.oos_score.hit_rate,
                "oos_lift": f.oos_score.top_decile_lift,
                "oos_n": f.oos_score.n,
            }
            row.update({f"w_{k}": v for k, v in f.best_weights.items()})
            rows.append(row)
        return pd.DataFrame(rows)


def _objective(trial: optuna.Trial, panels: list[FactorPanel], top_n: int) -> float:
    raw = np.array([trial.suggest_float(f"w_{name}", 0.0, 1.0) for name in FACTOR_NAMES])
    score = mean_ic_over_panels(panels, raw, top_n=top_n)
    # Optuna minimises by default; we maximise IC.
    return -score if np.isfinite(score) else 1.0


def _suggest_best_weights(study: optuna.Study) -> dict[str, float]:
    raw = np.array([study.best_params[f"w_{name}"] for name in FACTOR_NAMES])
    norm = normalise_weights(raw)
    return {name: float(w) for name, w in zip(FACTOR_NAMES, norm)}


def _anchor_date_for_year(year: int) -> pd.Timestamp:
    """First trading-day-ish anchor for a calendar year.

    Use Jan 2 as a deterministic anchor — the panel loader's coverage filter
    handles weekends/holidays by demanding ``horizon_min`` forward days.
    """
    return pd.Timestamp(year=year, month=1, day=2)


def run_walkforward(
    years: list[int],
    *,
    horizon_days: int = 252,
    top_n: int = 100,
    n_trials: int = 100,
    min_train_years: int = 3,
    db_path: str | Path = DEFAULT_DB_PATH,
    min_turnover_crores: float = 1.0,
    seed: int = 42,
    log: bool = True,
) -> WalkForwardResult:
    """Run expanding-window walk-forward weight search."""
    if len(years) <= min_train_years:
        raise ValueError(
            f"need at least {min_train_years + 1} years; got {len(years)}"
        )

    panels: dict[int, FactorPanel] = {}
    for y in years:
        if log:
            print(f"  loading panel for {y}...", flush=True)
        panels[y] = load_factor_panel(
            _anchor_date_for_year(y),
            horizon_days=horizon_days,
            db_path=db_path,
            min_turnover_crores=min_turnover_crores,
        )
        if log:
            print(f"    {panels[y].n} symbols")

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sorted_years = sorted(panels.keys())
    result = WalkForwardResult(panels=panels)

    for i, test_year in enumerate(sorted_years):
        if i < min_train_years:
            continue
        train_years = tuple(sorted_years[:i])
        train_panels = [panels[y] for y in train_years if panels[y].n > 0]
        test_panel = panels[test_year]
        if not train_panels or test_panel.n == 0:
            if log:
                print(f"  fold {test_year}: skipped (empty panel)")
            continue

        study = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(seed=seed),
        )
        study.optimize(
            lambda trial: _objective(trial, train_panels, top_n),
            n_trials=n_trials,
            show_progress_bar=False,
        )
        best_weights = _suggest_best_weights(study)
        train_mean_ic = -study.best_value
        oos = score_weights(test_panel, best_weights, top_n=top_n)
        outcome = FoldOutcome(
            test_year=test_year,
            train_years=train_years,
            best_weights=best_weights,
            train_mean_ic=float(train_mean_ic),
            oos_score=oos,
        )
        result.folds.append(outcome)
        if log:
            top = sorted(best_weights.items(), key=lambda kv: -kv[1])[:3]
            top_str = ", ".join(f"{k}={v:.2f}" for k, v in top)
            print(
                f"  fold {test_year}: train_ic={train_mean_ic:+.3f} "
                f"oos_ic={oos.ic:+.3f} oos_hit={oos.hit_rate:.0%} "
                f"oos_lift={oos.top_decile_lift:+.1%}  top-weights: {top_str}"
            )
    return result
