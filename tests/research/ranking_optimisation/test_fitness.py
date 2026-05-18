"""Tests for ranking-weight fitness helpers.

Uses synthetic panels so the test is hermetic — no DB dependency. The
synthetic data has a known relationship between one factor and forward
return so we can assert the fitness vector recovers it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.research.ranking_optimisation.data import (
    FACTOR_NAMES,
    FactorPanel,
)
from ai_trading_system.research.ranking_optimisation.fitness import (
    compute_composite,
    mean_ic_over_panels,
    normalise_weights,
    score_weights,
)


def _make_panel(n: int, signal_factor: str, noise_std: float = 0.0) -> FactorPanel:
    """Build a synthetic panel where ``signal_factor`` perfectly orders symbols.

    Forward returns are a strict monotonic function of ``signal_factor`` + noise.
    Other factors are random.
    """
    rng = np.random.default_rng(0)
    signal = np.linspace(-1.0, 1.0, n)
    df = pd.DataFrame({"symbol_id": [f"SYM{i:04d}" for i in range(n)]})
    df["anchor_close"] = 100.0
    df["forward_return"] = signal + rng.normal(0, noise_std, n)
    for name in FACTOR_NAMES:
        if name == signal_factor:
            df[name] = signal
        else:
            df[name] = rng.normal(0, 1.0, n)
    return FactorPanel(pd.Timestamp("2023-01-02"), 252, df)


def test_normalise_weights_dict_sums_to_one():
    w = normalise_weights({"rs_12_1": 2.0, "rs_6m": 1.0, "rs_3m": 1.0})
    assert w.sum() == pytest.approx(1.0)
    # rs_12_1 is twice the next two combined → weight 0.5
    idx = FACTOR_NAMES.index("rs_12_1")
    assert w[idx] == pytest.approx(0.5)


def test_normalise_weights_zero_input_returns_uniform():
    w = normalise_weights({name: 0.0 for name in FACTOR_NAMES})
    assert w.sum() == pytest.approx(1.0)
    assert np.allclose(w, 1.0 / len(FACTOR_NAMES))


def test_normalise_weights_negative_clipped():
    w = normalise_weights({"rs_12_1": -1.0, "rs_6m": 1.0})
    idx_rs12 = FACTOR_NAMES.index("rs_12_1")
    idx_rs6 = FACTOR_NAMES.index("rs_6m")
    assert w[idx_rs12] == 0.0
    assert w[idx_rs6] == pytest.approx(1.0)


def test_score_weights_recovers_perfect_signal():
    panel = _make_panel(n=300, signal_factor="rs_12_1", noise_std=0.0)
    weights = {name: (1.0 if name == "rs_12_1" else 0.0) for name in FACTOR_NAMES}
    score = score_weights(panel, weights, top_n=30)
    assert score.ic == pytest.approx(1.0)
    assert score.hit_rate == pytest.approx(1.0)
    assert score.top_decile_lift > 0


def test_score_weights_uniform_weights_no_better_than_noise():
    panel = _make_panel(n=300, signal_factor="rs_12_1", noise_std=0.0)
    score = score_weights(panel, {name: 1.0 for name in FACTOR_NAMES}, top_n=30)
    # 1/8 weight on the signal factor + 7/8 on noise → some IC remains, but
    # nowhere near perfect.
    assert 0.0 < score.ic < 0.7


def test_compute_composite_weighted_sum_of_percentile_ranks():
    panel = _make_panel(n=100, signal_factor="rs_12_1")
    w = normalise_weights({"rs_12_1": 1.0})
    composite = compute_composite(panel, w)
    # When weight is concentrated on rs_12_1, the composite ranking should
    # exactly match the rs_12_1 ranking.
    assert composite.argsort().tolist() == panel.df["rs_12_1"].argsort().tolist()


def test_mean_ic_over_panels_averages_finite_only():
    panel_a = _make_panel(n=200, signal_factor="rs_12_1")
    panel_b = _make_panel(n=200, signal_factor="rs_12_1")
    weights = {name: (1.0 if name == "rs_12_1" else 0.0) for name in FACTOR_NAMES}
    ic = mean_ic_over_panels([panel_a, panel_b], weights, top_n=30)
    assert ic == pytest.approx(1.0)


def test_score_weights_empty_panel_returns_nan():
    empty = FactorPanel(pd.Timestamp("2023-01-02"), 252, pd.DataFrame())
    score = score_weights(empty, {name: 1.0 for name in FACTOR_NAMES})
    assert np.isnan(score.ic)
    assert score.n == 0
