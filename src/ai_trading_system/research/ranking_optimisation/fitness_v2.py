"""v2 fitness — IC / hit / lift / combined objective over production factor scores.

Weight vectors here are keyed by ``WEIGHT_KEYS`` (``relative_strength``,
``volume_intensity``, ...), matching ``DEFAULT_FACTOR_WEIGHTS`` so the result
can be written to ``rank_factor_weights.json`` directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import numpy as np
import pandas as pd

from ai_trading_system.research.ranking_optimisation.data_v2 import (
    LiveFactorPanel,
    PRODUCTION_FACTOR_COLUMNS,
    SCORE_TO_WEIGHT_KEY,
    WEIGHT_KEYS,
    WEIGHT_KEY_TO_SCORE,
)


@dataclass(frozen=True)
class V2FoldScore:
    ic: float
    hit_rate: float
    top_decile_lift: float
    n: int


def normalise_weights_v2(weights: Mapping[str, float]) -> dict[str, float]:
    """Clip negatives, sum-normalise to 1.0. Missing keys default to 0.

    Returns a dict keyed by ``WEIGHT_KEYS``. If the input sums to 0 after
    clipping, returns a uniform distribution across all 8 keys.
    """
    raw = np.array([max(0.0, float(weights.get(k, 0.0))) for k in WEIGHT_KEYS])
    total = raw.sum()
    if total <= 0:
        norm = np.full(len(WEIGHT_KEYS), 1.0 / len(WEIGHT_KEYS))
    else:
        norm = raw / total
    return {k: float(v) for k, v in zip(WEIGHT_KEYS, norm)}


def _composite(
    panel: LiveFactorPanel,
    weights: Mapping[str, float],
    active_factors: tuple[str, ...] | None,
) -> pd.Series | None:
    if panel.df.empty:
        return None
    cols = active_factors if active_factors is not None else panel.active_factors
    if not cols:
        return None
    # Map back to weight keys; weights for inactive factors are zeroed.
    w_vec = np.array(
        [float(weights.get(SCORE_TO_WEIGHT_KEY[c], 0.0)) for c in cols], dtype=float
    )
    if w_vec.sum() <= 0:
        # Uniform across active subset if all-zero weights — caller can still
        # measure how a degenerate-weights pick scores.
        w_vec = np.full(len(cols), 1.0 / len(cols))
    scores = panel.df[list(cols)].astype(float).fillna(0.0).to_numpy()
    return pd.Series(scores @ w_vec, index=panel.df.index)


def score_weights_v2(
    panel: LiveFactorPanel,
    weights: Mapping[str, float],
    *,
    top_n: int = 100,
    active_factors: tuple[str, ...] | None = None,
) -> V2FoldScore:
    """Score one weight vector on one panel."""
    composite = _composite(panel, weights, active_factors)
    if composite is None or panel.df.empty:
        return V2FoldScore(float("nan"), float("nan"), float("nan"), 0)
    sub = pd.DataFrame(
        {"composite": composite, "forward_return": panel.df["forward_return"]}
    ).dropna()
    if len(sub) < top_n:
        return V2FoldScore(float("nan"), float("nan"), float("nan"), len(sub))

    ic = sub["composite"].corr(sub["forward_return"], method="spearman")
    top_by_composite = sub.nlargest(top_n, "composite").index
    top_by_return = sub["forward_return"].nlargest(top_n).index
    hit_rate = len(set(top_by_composite) & set(top_by_return)) / top_n
    deciles = pd.qcut(sub["composite"].rank(method="first"), 10, labels=False)
    top_decile_mean = sub.loc[deciles == 9, "forward_return"].mean()
    overall_mean = sub["forward_return"].mean()
    return V2FoldScore(float(ic), float(hit_rate), float(top_decile_mean - overall_mean), len(sub))


def _per_panel_scores(
    panels: list[LiveFactorPanel],
    weights: Mapping[str, float],
    *,
    top_n: int,
    active_factors: tuple[str, ...] | None,
) -> list[V2FoldScore]:
    return [score_weights_v2(p, weights, top_n=top_n, active_factors=active_factors) for p in panels]


def _finite_mean(values: list[float]) -> float:
    arr = np.array(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return float("nan")
    return float(arr.mean())


def _finite_std(values: list[float]) -> float:
    arr = np.array(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < 2:
        return 0.0
    return float(arr.std(ddof=0))


def _herfindahl(weights: Mapping[str, float], active_factors: tuple[str, ...] | None) -> float:
    cols = active_factors if active_factors is not None else PRODUCTION_FACTOR_COLUMNS
    norm = normalise_weights_v2(weights)
    active_keys = [SCORE_TO_WEIGHT_KEY[c] for c in cols]
    masked = np.array([norm[k] for k in active_keys], dtype=float)
    masked_sum = masked.sum()
    if masked_sum <= 0:
        return 1.0 / max(len(active_keys), 1)
    masked = masked / masked_sum
    return float((masked ** 2).sum())


def combined_objective(
    panels: list[LiveFactorPanel],
    weights: Mapping[str, float],
    *,
    top_n: int = 100,
    ic_w: float = 0.55,
    lift_w: float = 0.25,
    hit_w: float = 0.20,
    concentration_lambda: float = 0.3,
    instability_lambda: float = 0.5,
    active_factors: tuple[str, ...] | None = None,
) -> dict[str, float]:
    """Compute the combined objective + per-component breakdown.

    Returns dict with keys: combined, mean_ic, mean_lift, mean_hit, ic_std,
    concentration_penalty, instability_penalty.
    """
    scores = _per_panel_scores(panels, weights, top_n=top_n, active_factors=active_factors)
    ics  = [s.ic for s in scores]
    lifts = [s.top_decile_lift for s in scores]
    hits = [s.hit_rate for s in scores]

    mean_ic   = _finite_mean(ics)
    mean_lift = _finite_mean(lifts)
    mean_hit  = _finite_mean(hits)
    ic_std    = _finite_std(ics)

    cols = active_factors if active_factors is not None else PRODUCTION_FACTOR_COLUMNS
    n_active = max(len(cols), 1)
    hhi = _herfindahl(weights, active_factors=cols)
    hhi_floor = 1.0 / n_active
    concentration_penalty = concentration_lambda * max(0.0, hhi - hhi_floor)
    instability_penalty = instability_lambda * (ic_std if np.isfinite(ic_std) else 0.0)

    components = (
        ic_w * (mean_ic if np.isfinite(mean_ic) else 0.0)
        + lift_w * (mean_lift if np.isfinite(mean_lift) else 0.0)
        + hit_w * (mean_hit if np.isfinite(mean_hit) else 0.0)
    )
    combined = components - concentration_penalty - instability_penalty
    return {
        "combined": float(combined),
        "mean_ic": float(mean_ic),
        "mean_lift": float(mean_lift),
        "mean_hit": float(mean_hit),
        "ic_std": float(ic_std),
        "concentration_penalty": float(concentration_penalty),
        "instability_penalty": float(instability_penalty),
    }


def single_metric_objective(
    panels: list[LiveFactorPanel],
    weights: Mapping[str, float],
    *,
    mode: str,
    top_n: int = 100,
    active_factors: tuple[str, ...] | None = None,
) -> float:
    """Single-metric objective for ic_only / lift_only / hit_only modes."""
    scores = _per_panel_scores(panels, weights, top_n=top_n, active_factors=active_factors)
    if mode == "ic_only":
        return _finite_mean([s.ic for s in scores])
    if mode == "lift_only":
        return _finite_mean([s.top_decile_lift for s in scores])
    if mode == "hit_only":
        return _finite_mean([s.hit_rate for s in scores])
    raise ValueError(f"unknown objective mode: {mode}")
