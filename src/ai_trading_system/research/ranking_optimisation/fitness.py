"""Fitness scoring for ranking-weight optimisation.

Given a panel (one as-of date) and a weight vector over ``FACTOR_NAMES``,
compute the composite score for each symbol and report:
  - Spearman IC between composite and realised forward return
  - top-N hit rate: |top-N(composite) ∩ top-N(realised)| / N
  - top-decile lift: mean forward return of top decile by composite,
                     minus universe mean

The optimiser maximises a single objective (default IC). Hit rate and lift
are reported for diagnostics.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from ai_trading_system.research.ranking_optimisation.data import (
    FACTOR_NAMES,
    FactorPanel,
)


@dataclass(frozen=True)
class FoldScore:
    ic: float
    hit_rate: float
    top_decile_lift: float
    n: int


def normalise_weights(weights: dict[str, float] | np.ndarray) -> np.ndarray:
    """Take any non-negative weight vector and normalise to sum=1.

    Negative entries are clipped to 0. An all-zero input returns a uniform
    weight vector (treating "no opinion" as "equal weight").
    """
    if isinstance(weights, dict):
        vec = np.array([float(weights.get(name, 0.0)) for name in FACTOR_NAMES])
    else:
        vec = np.asarray(weights, dtype=float)
    vec = np.clip(vec, 0.0, None)
    total = vec.sum()
    if total <= 0:
        return np.full(len(FACTOR_NAMES), 1.0 / len(FACTOR_NAMES))
    return vec / total


def compute_composite(panel: FactorPanel, weights: np.ndarray) -> pd.Series:
    """Weighted sum of per-factor percentile ranks. Higher is "better".

    Missing factor values become the neutral 0.5 percentile so they neither
    pull a symbol toward the top nor the bottom.
    """
    df = panel.df
    if df.empty:
        return pd.Series(dtype=float)
    pct_ranks = df[list(FACTOR_NAMES)].rank(pct=True).fillna(0.5)
    return pd.Series(pct_ranks.values @ weights, index=df.index)


def score_weights(
    panel: FactorPanel,
    weights: dict[str, float] | np.ndarray,
    *,
    top_n: int = 100,
) -> FoldScore:
    """Score a weight vector on one panel."""
    if panel.df.empty:
        return FoldScore(float("nan"), float("nan"), float("nan"), 0)
    w = normalise_weights(weights)
    df = panel.df.copy()
    df["composite"] = compute_composite(panel, w)
    sub = df[["composite", "forward_return"]].dropna()
    if sub.empty or len(sub) < top_n:
        return FoldScore(float("nan"), float("nan"), float("nan"), len(sub))

    ic = sub["composite"].corr(sub["forward_return"], method="spearman")

    top_by_composite = sub.nlargest(top_n, "composite").index
    top_by_return = sub["forward_return"].nlargest(top_n).index
    hit_rate = len(set(top_by_composite) & set(top_by_return)) / top_n

    deciles = pd.qcut(sub["composite"].rank(method="first"), 10, labels=False)
    top_decile_mean = sub.loc[deciles == 9, "forward_return"].mean()
    overall_mean = sub["forward_return"].mean()
    lift = float(top_decile_mean - overall_mean)
    return FoldScore(float(ic), float(hit_rate), float(lift), len(sub))


def mean_ic_over_panels(
    panels: list[FactorPanel],
    weights: dict[str, float] | np.ndarray,
    *,
    top_n: int = 100,
) -> float:
    """Mean Spearman IC across panels. NaN-safe; returns -inf if all NaN."""
    ics = [score_weights(p, weights, top_n=top_n).ic for p in panels]
    valid = [x for x in ics if not np.isnan(x)]
    if not valid:
        return float("-inf")
    return float(np.mean(valid))
