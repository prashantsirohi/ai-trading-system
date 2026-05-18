"""Hermetic tests for v2 fitness — synthetic panels, no DB dependency."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.research.ranking_optimisation.data_v2 import (
    LiveFactorPanel,
    PRODUCTION_FACTOR_COLUMNS,
    SCORE_TO_WEIGHT_KEY,
    WEIGHT_KEYS,
)
from ai_trading_system.research.ranking_optimisation.fitness_v2 import (
    combined_objective,
    normalise_weights_v2,
    score_weights_v2,
    single_metric_objective,
)


def _make_live_panel(
    n: int,
    *,
    signal_score_col: str,
    noise_std: float = 0.0,
    degenerate_cols: tuple[str, ...] = (),
) -> LiveFactorPanel:
    """Synthetic panel where ``signal_score_col`` perfectly orders forward return."""
    rng = np.random.default_rng(42)
    df = pd.DataFrame({"symbol_id": [f"SYM{i:04d}" for i in range(n)]})
    signal = np.linspace(-1.0, 1.0, n)
    df["anchor_close"] = 100.0
    df["forward_return"] = signal + rng.normal(0, noise_std, n)
    for col in PRODUCTION_FACTOR_COLUMNS:
        if col == signal_score_col:
            df[col] = signal * 100.0  # production scores are 0-100
        elif col in degenerate_cols:
            df[col] = 50.0  # flat → variance 0 → degenerate
        else:
            df[col] = rng.normal(50, 20, n)
    panel = LiveFactorPanel(
        as_of=pd.Timestamp("2023-03-31"),
        horizon_days=20,
        df=df,
        degenerate_factors=tuple(degenerate_cols),
    )
    return panel


# ---------------- normalise_weights_v2 ---------------------------------------


def test_normalise_weights_v2_clips_negative_and_sums_to_one():
    raw = {"relative_strength": -0.5, "trend_persistence": 2.0, "above_200dma": 1.0}
    out = normalise_weights_v2(raw)
    assert sum(out.values()) == pytest.approx(1.0)
    assert out["relative_strength"] == 0.0
    assert out["trend_persistence"] > out["above_200dma"]


def test_normalise_weights_v2_missing_keys_default_to_zero():
    raw = {"relative_strength": 1.0}
    out = normalise_weights_v2(raw)
    assert set(out.keys()) == set(WEIGHT_KEYS)
    assert out["relative_strength"] == pytest.approx(1.0)
    for k in WEIGHT_KEYS:
        if k != "relative_strength":
            assert out[k] == 0.0


def test_normalise_weights_v2_all_zero_returns_uniform():
    out = normalise_weights_v2({k: 0.0 for k in WEIGHT_KEYS})
    assert sum(out.values()) == pytest.approx(1.0)
    uniform = 1.0 / len(WEIGHT_KEYS)
    for v in out.values():
        assert v == pytest.approx(uniform)


# ---------------- score_weights_v2 -------------------------------------------


def test_score_weights_v2_recovers_perfect_signal_concentrated_weight():
    panel = _make_live_panel(n=300, signal_score_col="rel_strength_score")
    weights = {k: 0.0 for k in WEIGHT_KEYS}
    weights[SCORE_TO_WEIGHT_KEY["rel_strength_score"]] = 1.0
    score = score_weights_v2(panel, weights, top_n=30)
    assert score.ic == pytest.approx(1.0)
    assert score.hit_rate == pytest.approx(1.0)


def test_score_weights_v2_active_factors_subset_ignores_others():
    # Signal lives in rel_strength_score but caller marks ONLY trend_score_score
    # as active — composite should not see the signal.
    panel = _make_live_panel(n=300, signal_score_col="rel_strength_score")
    weights = {k: 0.0 for k in WEIGHT_KEYS}
    weights[SCORE_TO_WEIGHT_KEY["rel_strength_score"]] = 1.0  # but ignored
    weights[SCORE_TO_WEIGHT_KEY["trend_score_score"]] = 0.0
    score = score_weights_v2(panel, weights, top_n=30, active_factors=("trend_score_score",))
    # With only trend active and noise data, IC is near zero (not 1.0).
    assert abs(score.ic) < 0.3


def test_score_weights_v2_empty_panel_returns_nan():
    empty = LiveFactorPanel(pd.Timestamp("2023-03-31"), 20, pd.DataFrame(), ())
    score = score_weights_v2(empty, {k: 1.0 for k in WEIGHT_KEYS}, top_n=30)
    assert np.isnan(score.ic)
    assert score.n == 0


# ---------------- combined_objective -----------------------------------------


def test_combined_objective_breakdown_components_for_uniform_weights():
    # Two identical panels with perfect signal. Uniform weights → HHI == 1/8.
    panel = _make_live_panel(n=300, signal_score_col="rel_strength_score")
    panels = [panel, panel]
    uniform = {k: 1.0 / len(WEIGHT_KEYS) for k in WEIGHT_KEYS}
    out = combined_objective(panels, uniform, top_n=30)
    # 1/8 weight on the perfect-signal factor + 7/8 noise → some signal remains
    # but IC is well below 1.0. Just verify the structural relationship.
    expected = 0.55 * out["mean_ic"] + 0.25 * out["mean_lift"] + 0.20 * out["mean_hit"]
    assert out["concentration_penalty"] == pytest.approx(0.0)
    assert out["instability_penalty"] == pytest.approx(0.0)  # ic_std=0 for identical panels
    assert out["combined"] == pytest.approx(expected)


def test_combined_objective_concentration_penalty_fires_for_single_factor():
    panel = _make_live_panel(n=300, signal_score_col="rel_strength_score")
    weights = {k: 0.0 for k in WEIGHT_KEYS}
    weights["relative_strength"] = 1.0
    out = combined_objective([panel, panel], weights, top_n=30)
    # HHI=1.0 (all in one), floor=1/8 → penalty = 0.3 * (1 - 1/8) = 0.2625
    assert out["concentration_penalty"] == pytest.approx(0.3 * (1.0 - 1.0 / 8))


def test_combined_objective_instability_penalty_uses_per_panel_std():
    panel_strong = _make_live_panel(n=300, signal_score_col="rel_strength_score")
    # Noisy panel: signal flipped + heavy noise → very different IC.
    panel_noisy = _make_live_panel(n=300, signal_score_col="vol_intensity_score", noise_std=1.5)
    weights = {k: 0.0 for k in WEIGHT_KEYS}
    weights["relative_strength"] = 1.0
    out = combined_objective([panel_strong, panel_noisy], weights, top_n=30)
    assert out["ic_std"] > 0
    assert out["instability_penalty"] == pytest.approx(0.5 * out["ic_std"])


def test_combined_objective_with_active_factors_subset_uses_subset_for_hhi_floor():
    # active=2 → uniform HHI floor is 0.5; concentrated weight HHI=1.0 → penalty
    # = 0.3 * (1.0 - 0.5) = 0.15.
    panel = _make_live_panel(n=300, signal_score_col="rel_strength_score")
    weights = {k: 0.0 for k in WEIGHT_KEYS}
    weights["relative_strength"] = 1.0
    active = ("rel_strength_score", "trend_score_score")
    out = combined_objective([panel, panel], weights, top_n=30, active_factors=active)
    assert out["concentration_penalty"] == pytest.approx(0.3 * (1.0 - 0.5))


# ---------------- single_metric_objective ------------------------------------


def test_single_metric_objective_ic_only_matches_combined_breakdown():
    panel = _make_live_panel(n=300, signal_score_col="rel_strength_score")
    weights = {k: 0.0 for k in WEIGHT_KEYS}
    weights["relative_strength"] = 1.0
    ic = single_metric_objective([panel, panel], weights, mode="ic_only", top_n=30)
    breakdown = combined_objective([panel, panel], weights, top_n=30)
    assert ic == pytest.approx(breakdown["mean_ic"])


def test_single_metric_objective_unknown_mode_raises():
    panel = _make_live_panel(n=100, signal_score_col="rel_strength_score")
    with pytest.raises(ValueError, match="unknown objective mode"):
        single_metric_objective([panel], {k: 1.0 for k in WEIGHT_KEYS}, mode="bogus")


# ---------------- missing factor handling ------------------------------------


def test_score_weights_v2_handles_missing_factor_via_active_factors():
    """When a factor is degenerate on the panel, the runner passes a restricted
    active_factors list. score_weights_v2 must zero its weight contribution."""
    panel = _make_live_panel(
        n=300,
        signal_score_col="rel_strength_score",
        degenerate_cols=("delivery_pct_score", "vol_intensity_score"),
    )
    weights = {k: 1.0 / len(WEIGHT_KEYS) for k in WEIGHT_KEYS}
    active = tuple(c for c in PRODUCTION_FACTOR_COLUMNS if c not in panel.degenerate_factors)
    score = score_weights_v2(panel, weights, top_n=30, active_factors=active)
    # No NaN/error and result is sensible.
    assert score.n > 0
    assert np.isfinite(score.ic)
