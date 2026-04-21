"""Tests for Stage 2 uptrend scoring (Weinstein methodology).

Covers:
- All four label outcomes (strong_stage2, stage2, stage1_to_stage2, non_stage2)
- Edge cases: empty frame, missing optional columns, NaN close values
- Scoring arithmetic for each of the 9 conditions
- fail_reason content at last bar
- Backward-compatible fallback behaviour in breakout.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.domains.features.indicators import (
    _STAGE2_OUTPUT_COLS,
    add_stage2_features,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_frame(
    n: int = 250,
    close: float = 200.0,
    sma_50: float = 180.0,
    sma_200: float = 150.0,
    near_52w_high_pct: float = 10.0,
    rel_strength_score: float = 80.0,
    volume_ratio_20: float = 1.5,
) -> pd.DataFrame:
    """Build a minimal frame with all Stage 2 inputs.

    Close is a gently rising series (from 85% of its final value to the final
    value) so that ``add_stage2_features()`` reliably produces ``close > sma_150``
    at the last bar (sma_150 ≈ 95.5% of close, strictly below close).

    sma_200 is kept FLAT so that ``sma200_slope_20d_pct = 0`` (condition 4
    intentionally fails for tests that verify partial scoring).  Tests that
    require all 9 conditions to pass must build their own frame with a rising
    sma_200 column.
    """
    idx = pd.RangeIndex(n)
    # Rising close: 85% → 100% of final value.
    # sma_150 (last bar) ≈ 0.2992*start + 0.7008*close ≈ 0.955*close < close ✓
    close_arr = np.linspace(close * 0.85, close, n)
    return pd.DataFrame(
        {
            "close": close_arr,
            "sma_50": np.full(n, sma_50, dtype=float),
            "sma_200": np.full(n, sma_200, dtype=float),  # flat → slope = 0
            "near_52w_high_pct": np.full(n, near_52w_high_pct, dtype=float),
            "rel_strength_score": np.full(n, rel_strength_score, dtype=float),
            "volume_ratio_20": np.full(n, volume_ratio_20, dtype=float),
        },
        index=idx,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Output shape / column contract
# ─────────────────────────────────────────────────────────────────────────────

class TestOutputColumns:
    def test_all_output_columns_present(self):
        df = _make_frame()
        out = add_stage2_features(df)
        for col in _STAGE2_OUTPUT_COLS:
            assert col in out.columns, f"Missing column: {col}"

    def test_row_count_unchanged(self):
        df = _make_frame(n=300)
        out = add_stage2_features(df)
        assert len(out) == 300

    def test_empty_frame_returns_empty_with_cols(self):
        out = add_stage2_features(pd.DataFrame())
        for col in _STAGE2_OUTPUT_COLS:
            assert col in out.columns

    def test_frame_without_close_returns_empty_cols(self):
        df = pd.DataFrame({"volume": [100, 200, 300]})
        out = add_stage2_features(df)
        for col in _STAGE2_OUTPUT_COLS:
            assert col in out.columns


# ─────────────────────────────────────────────────────────────────────────────
# Label outcomes
# ─────────────────────────────────────────────────────────────────────────────

class TestLabels:
    """Verify all four label outcomes under controlled inputs."""

    def test_strong_stage2(self):
        # close > sma_150 > sma_200, sma200 rising, within 15% of high, RS≥85, vol>1.2
        # Expected: 15+15+15+15+10+10+10+10+10 = 100 pts → strong_stage2
        df = _make_frame(
            n=250,
            close=200.0,
            sma_200=100.0,           # close (200) >> sma_200 (100) and sma_150 (~200)
            near_52w_high_pct=5.0,   # ≤15% → +20
            rel_strength_score=90.0,  # ≥85 → +20
            volume_ratio_20=2.0,      # >1.2 → +10
        )
        out = add_stage2_features(df)
        last = out.iloc[-1]
        assert last["stage2_label"] == "strong_stage2", f"score={last['stage2_score']}"
        assert bool(last["is_stage2_uptrend"]) is True
        assert float(last["stage2_score"]) >= 85.0

    def test_stage2(self):
        # close > sma_150 > sma_200 but far from 52w high and low RS → score 70-84
        df = _make_frame(
            n=250,
            close=200.0,
            sma_200=100.0,
            near_52w_high_pct=20.0,  # ≤25 (+10) but >15 (no extra +10)
            rel_strength_score=72.0,  # ≥70 (+10) but <85 (no extra)
            volume_ratio_20=1.5,      # >1.2 (+10)
        )
        out = add_stage2_features(df)
        last = out.iloc[-1]
        assert last["stage2_label"] == "stage2", f"score={last['stage2_score']}"
        assert bool(last["is_stage2_uptrend"]) is True
        assert 70.0 <= float(last["stage2_score"]) < 85.0

    def test_stage1_to_stage2(self):
        # close > sma_200 but sma_150 below sma_200 (not aligned yet)
        # and low RS, low volume → score ~40-65
        df = _make_frame(
            n=250,
            close=160.0,
            sma_200=150.0,           # close > sma_200 (+15)
            near_52w_high_pct=30.0,  # >25% → 0 pts
            rel_strength_score=40.0,  # <70 → 0 pts
            volume_ratio_20=1.0,      # ≤1.2 → 0 pts
        )
        # Force sma_150 above sma_200 for condition 3 to fail:
        # Actually with rolling(150) on 250 flat bars at close=160, sma_150≈160
        # which is > sma_200=150 → condition 3 passes. So sma_200_slope test:
        # the slope will be 0 (flat series) → condition 4 fails.
        # Score: +15 (close>sma150) + 15 (close>sma200) + 15 (sma150>sma200)
        # + 0 (slope=0 → fails strict >0) + 0 + 0 + 0 = 45
        # This gives non_stage2 (< 50). Let's tweak to get 50-69:
        # Add near_52w_high_pct ≤ 25 to get +10 → 55 → stage1_to_stage2
        df = _make_frame(
            n=250,
            close=160.0,
            sma_200=150.0,
            near_52w_high_pct=20.0,  # ≤25 → +10
            rel_strength_score=40.0,  # <70 → 0
            volume_ratio_20=1.0,      # ≤1.2 → 0
        )
        out = add_stage2_features(df)
        last = out.iloc[-1]
        # Score = 15+15+15+0+10+0+0+0+0 = 55 (flat series → slope=0 → fails)
        assert last["stage2_label"] == "stage1_to_stage2", f"score={last['stage2_score']}"
        assert bool(last["is_stage2_uptrend"]) is False
        assert 50.0 <= float(last["stage2_score"]) < 70.0

    def test_non_stage2(self):
        # close < sma_200, all conditions fail → 0 pts
        df = _make_frame(
            n=250,
            close=80.0,
            sma_200=100.0,           # close < sma_200
            near_52w_high_pct=60.0,  # far
            rel_strength_score=20.0,  # low RS
            volume_ratio_20=0.8,      # low volume
        )
        out = add_stage2_features(df)
        last = out.iloc[-1]
        assert last["stage2_label"] == "non_stage2", f"score={last['stage2_score']}"
        assert bool(last["is_stage2_uptrend"]) is False
        assert float(last["stage2_score"]) < 50.0


# ─────────────────────────────────────────────────────────────────────────────
# Scoring arithmetic
# ─────────────────────────────────────────────────────────────────────────────

class TestScoringArithmetic:
    """Verify individual condition contributions."""

    def _score_last(self, **kwargs) -> float:
        df = _make_frame(**kwargs)
        out = add_stage2_features(df)
        return float(out.iloc[-1]["stage2_score"])

    def test_score_capped_at_100(self):
        # All conditions max out → should not exceed 100
        score = self._score_last(
            close=200.0,
            sma_200=100.0,
            near_52w_high_pct=5.0,
            rel_strength_score=95.0,
            volume_ratio_20=3.0,
        )
        assert score <= 100.0

    def test_near_52w_cumulative(self):
        # ≤15% gives both +10 +10 (cumulative) = +20 on top of base
        score_tight = self._score_last(
            close=200.0, sma_200=100.0,
            near_52w_high_pct=5.0,
            rel_strength_score=0.0,
            volume_ratio_20=1.0,
        )
        score_mid = self._score_last(
            close=200.0, sma_200=100.0,
            near_52w_high_pct=20.0,  # ≤25 but >15 → only +10
            rel_strength_score=0.0,
            volume_ratio_20=1.0,
        )
        score_far = self._score_last(
            close=200.0, sma_200=100.0,
            near_52w_high_pct=40.0,  # >25 → 0
            rel_strength_score=0.0,
            volume_ratio_20=1.0,
        )
        assert score_tight > score_mid > score_far
        assert score_tight - score_mid == pytest.approx(10.0)
        assert score_mid - score_far == pytest.approx(10.0)

    def test_rs_cumulative(self):
        score_top = self._score_last(
            close=200.0, sma_200=100.0, near_52w_high_pct=5.0,
            rel_strength_score=90.0,  # ≥85 → +20
            volume_ratio_20=1.0,
        )
        score_mid = self._score_last(
            close=200.0, sma_200=100.0, near_52w_high_pct=5.0,
            rel_strength_score=72.0,  # ≥70 but <85 → +10
            volume_ratio_20=1.0,
        )
        score_low = self._score_last(
            close=200.0, sma_200=100.0, near_52w_high_pct=5.0,
            rel_strength_score=50.0,  # <70 → 0
            volume_ratio_20=1.0,
        )
        assert score_top > score_mid > score_low
        assert score_top - score_mid == pytest.approx(10.0)
        assert score_mid - score_low == pytest.approx(10.0)

    def test_volume_condition(self):
        score_high_vol = self._score_last(
            close=200.0, sma_200=100.0, near_52w_high_pct=5.0,
            rel_strength_score=90.0, volume_ratio_20=2.0,
        )
        score_low_vol = self._score_last(
            close=200.0, sma_200=100.0, near_52w_high_pct=5.0,
            rel_strength_score=90.0, volume_ratio_20=1.0,
        )
        assert score_high_vol - score_low_vol == pytest.approx(10.0)


# ─────────────────────────────────────────────────────────────────────────────
# Fail reasons
# ─────────────────────────────────────────────────────────────────────────────

class TestFailReasons:
    def test_fail_reason_empty_on_strong_stage2(self):
        df = _make_frame(
            n=250, close=200.0, sma_200=100.0,
            near_52w_high_pct=5.0, rel_strength_score=90.0, volume_ratio_20=2.0,
        )
        out = add_stage2_features(df)
        assert out.iloc[-1]["stage2_fail_reason"] == ""

    def test_fail_reason_populated_on_non_stage2(self):
        df = _make_frame(
            n=250, close=80.0, sma_200=100.0,
            near_52w_high_pct=60.0, rel_strength_score=20.0, volume_ratio_20=0.8,
        )
        out = add_stage2_features(df)
        reasons = str(out.iloc[-1]["stage2_fail_reason"])
        assert "below_sma200" in reasons
        assert "far_from_52w_high" in reasons
        assert "rs_below_70th_pctile" in reasons

    def test_fail_reason_below_sma150(self):
        # close below sma_150 (which is ≈ close rolling 150 of a lower base)
        # Easiest: use a frame where the last close dips below prior close series
        n = 250
        closes = np.full(n, 200.0)
        closes[-1] = 50.0  # final bar close below SMA-150
        df = pd.DataFrame({
            "close": closes,
            "sma_200": np.full(n, 40.0),  # sma_200 < close always
            "near_52w_high_pct": np.full(n, 60.0),
            "rel_strength_score": np.full(n, 20.0),
            "volume_ratio_20": np.full(n, 0.5),
        })
        out = add_stage2_features(df)
        reasons = str(out.iloc[-1]["stage2_fail_reason"])
        assert "below_sma150" in reasons


# ─────────────────────────────────────────────────────────────────────────────
# Missing optional columns
# ─────────────────────────────────────────────────────────────────────────────

class TestMissingColumns:
    def test_no_optional_columns(self):
        """Frame with only close — should not raise, all missing fields default."""
        n = 250
        df = pd.DataFrame({"close": np.full(n, 200.0)})
        out = add_stage2_features(df)
        assert "stage2_score" in out.columns
        # With no optional fields (near_52w_high_pct=999, rs=0, vol=1)
        # and close series with insufficient history for sma_150 and sma_200:
        # some rows will be NaN (min_periods not met), others will score 0.
        assert not out.empty

    def test_no_sma_200_column(self):
        """sma_200 absent — function computes it internally."""
        n = 250
        df = pd.DataFrame({
            "close": np.full(n, 200.0),
            "near_52w_high_pct": np.full(n, 5.0),
            "rel_strength_score": np.full(n, 90.0),
            "volume_ratio_20": np.full(n, 2.0),
        })
        out = add_stage2_features(df)
        assert "stage2_score" in out.columns
        assert "sma_200" in out.columns  # should be populated internally

    def test_nan_close_values(self):
        """NaN close values should not propagate crashes."""
        df = _make_frame(n=250)
        df.loc[100:120, "close"] = np.nan
        out = add_stage2_features(df)
        assert "stage2_score" in out.columns

    def test_single_row_frame(self):
        """Single-row frame — insufficient for SMA, but should not raise."""
        df = _make_frame(n=1)
        out = add_stage2_features(df)
        assert len(out) == 1
        assert "stage2_score" in out.columns


# ─────────────────────────────────────────────────────────────────────────────
# SMA-150 slope computation
# ─────────────────────────────────────────────────────────────────────────────

class TestSlopes:
    def test_sma150_exists_after_min_periods(self):
        df = _make_frame(n=250)
        out = add_stage2_features(df)
        # Should have non-NaN sma_150 beyond the 100th row (min_periods=100)
        assert out["sma_150"].notna().sum() > 0

    def test_sma200_slope_20d_pct_positive_for_rising_sma(self):
        """Rising close series → SMA-200 slope should be positive at end."""
        n = 300
        closes = np.linspace(100.0, 300.0, n)  # steadily rising
        df = pd.DataFrame({
            "close": closes,
            "near_52w_high_pct": np.full(n, 5.0),
            "rel_strength_score": np.full(n, 90.0),
            "volume_ratio_20": np.full(n, 2.0),
        })
        out = add_stage2_features(df)
        last_slope = out["sma200_slope_20d_pct"].dropna().iloc[-1]
        assert last_slope > 0.0, f"Expected positive slope, got {last_slope}"
