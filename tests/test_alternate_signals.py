"""Tests for the alternate-signal investigation module."""

from __future__ import annotations

import pandas as pd

from ai_trading_system.research.backtesting.alternate_signals import (
    _binary_ordering,
    _quintile_bucket,
    _signal_ordering_pass,
)


# ── quintile bucketing ────────────────────────────────────────────────────


def test_quintile_bucket_splits_into_five_named_buckets() -> None:
    s = pd.Series(range(100))
    buckets = _quintile_bucket(s)
    counts = buckets.value_counts()
    assert set(counts.index) == {"Q1_lowest", "Q2", "Q3", "Q4", "Q5_highest"}
    # ~20 each
    assert all(15 <= n <= 25 for n in counts)


def test_quintile_bucket_handles_low_variance() -> None:
    """When inputs are all equal, qcut can't form 5 buckets — return NA."""
    s = pd.Series([1.0] * 10)
    buckets = _quintile_bucket(s)
    assert buckets.isna().all()


def test_quintile_bucket_drops_nan_inputs_cleanly() -> None:
    s = pd.Series([1.0, 2.0, pd.NA, 4.0, 5.0])
    buckets = _quintile_bucket(s)
    # Index alignment preserved; NaN input stays NaN
    assert buckets.iloc[2] is pd.NA or pd.isna(buckets.iloc[2])


# ── ordering verdict ─────────────────────────────────────────────────────


def test_signal_ordering_pass_detects_monotone_quintiles() -> None:
    daily = pd.DataFrame(
        {
            "signal_q": (
                ["Q1_lowest"] * 5
                + ["Q2"] * 5
                + ["Q3"] * 5
                + ["Q4"] * 5
                + ["Q5_highest"] * 5
            ),
            # Monotone increasing means by quintile
            "fwd_5_return": (
                [0.1, 0.2, 0.0, 0.3, -0.1]
                + [0.5, 0.4, 0.6, 0.3, 0.5]
                + [1.0, 1.1, 0.9, 1.0, 1.1]
                + [1.5, 1.4, 1.6, 1.3, 1.5]
                + [2.0, 2.1, 1.9, 2.0, 2.1]
            ),
        }
    )
    verdict = _signal_ordering_pass(daily, signal_col="signal_q", horizons=(5,))
    assert verdict["5d"]["monotone_non_decreasing"] is True
    assert verdict["5d"]["q5_minus_q1_pct"] > 0
    # Q5 mean should be ~2.0
    q5 = next(b for b in verdict["5d"]["by_bucket"] if b["bucket"] == "Q5_highest")
    assert abs(q5["mean_return_pct"] - 2.02) < 0.05


def test_signal_ordering_pass_flags_non_monotone() -> None:
    """Q3 > Q4 in mean — ordering must fail."""
    daily = pd.DataFrame(
        {
            "signal_q": (
                ["Q1_lowest"] * 3
                + ["Q2"] * 3
                + ["Q3"] * 3
                + ["Q4"] * 3
                + ["Q5_highest"] * 3
            ),
            "fwd_5_return": [0.0, 0.1, 0.0]  # Q1
            + [0.3, 0.3, 0.3]                # Q2
            + [1.5, 1.5, 1.5]                # Q3 (spike)
            + [0.8, 0.9, 0.7]                # Q4 (dip below Q3)
            + [1.6, 1.7, 1.8],               # Q5
        }
    )
    verdict = _signal_ordering_pass(daily, signal_col="signal_q", horizons=(5,))
    assert verdict["5d"]["monotone_non_decreasing"] is False


# ── binary ordering ─────────────────────────────────────────────────────


def test_binary_ordering_compares_true_false_groups() -> None:
    daily = pd.DataFrame(
        {
            "flag": [True, True, True, False, False, False],
            "fwd_5_return": [2.0, 3.0, 1.0, -1.0, 0.0, 0.5],
        }
    )
    out = _binary_ordering(
        daily,
        signal_col="flag",
        true_label="on",
        false_label="off",
        horizons=(5,),
    )
    payload = out["5d"]
    # on mean = 2.0; off mean = -0.167 ≈ -0.17
    assert abs(payload["on"]["mean_return_pct"] - 2.0) < 1e-6
    assert payload["on"]["sample_size"] == 3
    assert payload["off"]["sample_size"] == 3
    assert payload["on"]["win_rate_pct"] == 100.0
    assert payload["off"]["win_rate_pct"] < 50.0
    # Δ should be positive (on > off)
    assert payload["true_minus_false_pct"] > 0
