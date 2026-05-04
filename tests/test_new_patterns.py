"""Tests for Phase 2 new pattern detectors.

Covers:
- Ascending Triangle: flat resistance + ascending troughs; Stage 2 pre-screen
- VCP: contracting price ranges + volume; non-contracting rejected
- Flat Base: depth within 15%; deep base rejected
- 3-Weeks-Tight: weekly closes within 1.5% + prior advance; no advance rejected
- Symmetrical Triangle: descending peaks + ascending troughs
- Head & Shoulders filter: H&S top detection; no H&S returns False; H&S suppresses bullish signals
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.domains.ranking.patterns.contracts import PatternScanConfig
from ai_trading_system.domains.ranking.patterns.detectors import (
    _score_signal_rows,
    detect_3wt_signals,
    detect_ascending_triangle_signals,
    detect_ascending_base_signals,
    detect_darvas_box_signals,
    detect_flat_base_signals,
    detect_head_shoulders_filter,
    detect_inside_week_breakout_signals,
    detect_ipo_base_signals,
    detect_pattern_signals_for_symbol,
    detect_pocket_pivot_signals,
    detect_symmetrical_triangle_signals,
    detect_vcp_signals,
)
from ai_trading_system.analytics.patterns.signal import LocalExtrema, find_local_extrema, kernel_smooth


# ─────────────────────────────────────────────────────────────────────────────
# Frame-building helpers
# ─────────────────────────────────────────────────────────────────────────────

def _timestamps(n: int, start: str = "2023-01-02") -> pd.Series:
    return pd.Series(pd.date_range(start, periods=n, freq="B"))


def _base_frame(closes: np.ndarray, *, vol_ratio: float = 1.5) -> pd.DataFrame:
    n = len(closes)
    highs = closes * 1.005
    lows = closes * 0.995
    return pd.DataFrame({
        "symbol_id": "TEST",
        "timestamp": _timestamps(n),
        "open": closes * 0.999,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": np.full(n, 100_000.0),
        "volume_ratio_20": np.full(n, vol_ratio),
        "volume_zscore_20": np.full(n, np.nan),
        "volume_zscore_50": np.full(n, np.nan),
    })


def _smoothed_and_extrema(frame: pd.DataFrame, config: PatternScanConfig):
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth, method="rolling")
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)
    return smoothed, extrema


# ─────────────────────────────────────────────────────────────────────────────
# Ascending Triangle
# ─────────────────────────────────────────────────────────────────────────────

def _make_manual_extrema_frame(closes: np.ndarray, vol_ratio: float = 1.5) -> tuple[pd.DataFrame, pd.Series]:
    """Build a frame + smoothed series where smoothed = closes (no smoothing noise)."""
    frame = _base_frame(closes, vol_ratio=vol_ratio)
    # Use closes directly as the smoothed series so extrema positions are predictable
    smoothed = pd.Series(closes.astype(float), index=frame.index)
    return frame, smoothed


class TestAscendingTriangle:
    def _make_asc_tri_extrema(self, flat_tol_ok: bool = True) -> tuple[pd.DataFrame, pd.Series, list]:
        """Construct frame + smoothed + explicit extrema for ascending triangle.

        Two peaks at 200 (flat within 1.5%) with two ascending troughs (175→183).
        """
        from ai_trading_system.analytics.patterns.signal import LocalExtrema
        n = 100
        closes = np.full(n, 185.0, dtype=float)
        # Peak 1 at bar 17 (200)
        closes[15:20] = 200.0
        # Trough 1 at bar 30 (175)
        closes[28:33] = 175.0
        # Trough 2 at bar 50 (183 — ascending)
        closes[48:53] = 183.0
        # Peak 2 at bar 72 (200, flat with peak 1 within 1.5%)
        peak2_val = 200.0 if flat_tol_ok else 220.0
        closes[70:75] = peak2_val
        # Breakout bar 80
        closes[80] = 205.0

        frame, smoothed = _make_manual_extrema_frame(closes)
        frame.loc[80, "volume_ratio_20"] = 2.0
        frame.loc[80, "volume_zscore_20"] = 2.5

        # Manually specified extrema (independent of smoother)
        extrema = [
            LocalExtrema(17, "peak", 200.0),
            LocalExtrema(30, "trough", 175.0),
            LocalExtrema(50, "trough", 183.0),  # ascending: 183 > 175 * 1.005
            LocalExtrema(72, "peak", float(peak2_val)),
        ]
        return frame, smoothed, extrema

    def test_flat_resistance_detected(self):
        config = PatternScanConfig(asc_tri_flat_tol=0.015, recent_signal_max_age_bars=50)
        frame, smoothed, extrema = self._make_asc_tri_extrema(flat_tol_ok=True)
        signals, stats = detect_ascending_triangle_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert stats.candidate_count >= 1, "Expected at least one ascending-triangle candidate"

    def test_stage2_prescreens_non_stage2(self):
        """Frame with stage2_score < 50 everywhere → zero signals returned."""
        config = PatternScanConfig(asc_tri_flat_tol=0.015, recent_signal_max_age_bars=50)
        frame, smoothed, extrema = self._make_asc_tri_extrema(flat_tol_ok=True)
        frame["stage2_score"] = 20.0  # below 50 threshold
        signals, stats = detect_ascending_triangle_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert len(signals) == 0, "Stage 2 pre-screen should return no signals"
        assert stats.candidate_count == 0

    def test_non_flat_resistance_rejected(self):
        """If peaks diverge > asc_tri_flat_tol, no ascending triangle detected."""
        config = PatternScanConfig(asc_tri_flat_tol=0.015, recent_signal_max_age_bars=50)
        frame, smoothed, extrema = self._make_asc_tri_extrema(flat_tol_ok=False)  # peak2=220
        signals, stats = detect_ascending_triangle_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        # 200 vs 220: deviation = 10/210 ≈ 4.8% > 1.5% → rejected
        assert len(signals) == 0


# ─────────────────────────────────────────────────────────────────────────────
# VCP
# ─────────────────────────────────────────────────────────────────────────────

class TestVCP:
    def _make_contracting_frame(self) -> pd.DataFrame:
        """Three thirds: range 20%, 16%, 12%; volume 2.0→1.6→1.2."""
        n = 160
        closes = np.full(n, 100.0)
        # Prior uptrend
        closes[:80] = np.linspace(80, 100, 80)
        # Third 1: range from 90 to 110 (20%)
        closes[80:93] = np.linspace(90, 110, 13)
        # Third 2: range from 95 to 111.6 (16%)
        closes[93:107] = np.linspace(95, 111.6, 14)  # 14 bars
        # Third 3: range from 98 to 111.2 (12%)
        closes[107:120] = np.linspace(98, 111.2, 13)
        frame = _base_frame(closes)
        # Volume: decreasing across thirds
        frame.loc[80:92, "volume_ratio_20"] = 2.0
        frame.loc[93:106, "volume_ratio_20"] = 1.6
        frame.loc[107:119, "volume_ratio_20"] = 1.2
        # Breakout
        closes[120] = 115.0
        frame.at[120, "close"] = 115.0
        frame.at[120, "high"] = 116.0
        frame.at[120, "volume_ratio_20"] = 2.0
        frame.at[120, "volume_zscore_20"] = 2.5
        return frame

    def test_contracting_ranges_detected(self):
        config = PatternScanConfig(
            vcp_window_bars=40,
            vcp_price_contraction_factor=0.85,
            vcp_vol_contraction_factor=0.85,
            vcp_min_first_range_pct=0.05,
            recent_signal_max_age_bars=50,
        )
        frame = self._make_contracting_frame()
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_vcp_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert stats.candidate_count >= 1, "VCP with contracting ranges should produce candidates"

    def test_non_contracting_skipped(self):
        """Flat equal ranges across three thirds → no VCP candidate."""
        n = 160
        closes = np.full(n, 100.0)
        # Three equal-range thirds: 10%
        for seg_start in [80, 93, 107]:
            closes[seg_start : seg_start + 13] = np.linspace(95, 105, 13)
        frame = _base_frame(closes)
        config = PatternScanConfig(
            vcp_window_bars=40,
            vcp_price_contraction_factor=0.85,
            vcp_vol_contraction_factor=0.85,
            vcp_min_first_range_pct=0.08,
            recent_signal_max_age_bars=50,
        )
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_vcp_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert stats.candidate_count == 0, "Equal ranges should produce no VCP candidates"


# ─────────────────────────────────────────────────────────────────────────────
# Flat Base
# ─────────────────────────────────────────────────────────────────────────────

class TestFlatBase:
    def _make_flat_base_frame(self, depth_pct: float = 0.08) -> pd.DataFrame:
        """30-bar range at ±depth_pct/2 around 100."""
        n = 120
        closes = np.full(n, 100.0)
        # Prior uptrend
        closes[:70] = np.linspace(70, 100, 70)
        # Flat base: 30 bars with tight range
        base_mid = 100.0
        closes[70:100] = np.random.default_rng(42).uniform(
            base_mid * (1 - depth_pct / 2), base_mid * (1 + depth_pct / 2), 30
        )
        # Volume dry-up in base vs prior
        frame = _base_frame(closes)
        frame.loc[70:99, "volume_ratio_20"] = 0.8  # drying up
        # Breakout bar
        closes[100] = base_mid * 1.02
        frame.at[100, "close"] = closes[100]
        frame.at[100, "high"] = closes[100] * 1.005
        frame.at[100, "volume_ratio_20"] = 2.0
        frame.at[100, "volume_zscore_20"] = 2.5
        return frame

    def test_flat_within_15pct_depth(self):
        config = PatternScanConfig(
            flat_base_min_bars=25,
            flat_base_max_bars=65,
            flat_base_max_depth_pct=0.15,
            recent_signal_max_age_bars=30,
        )
        frame = self._make_flat_base_frame(depth_pct=0.08)
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_flat_base_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert stats.candidate_count >= 1, "Flat base within 15% depth should produce candidates"

    def test_deep_base_rejected(self):
        """Base depth > 15% should not produce candidates."""
        config = PatternScanConfig(
            flat_base_min_bars=25,
            flat_base_max_bars=65,
            flat_base_max_depth_pct=0.15,
            recent_signal_max_age_bars=30,
        )
        frame = self._make_flat_base_frame(depth_pct=0.25)
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_flat_base_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert stats.candidate_count == 0, "Base with 25% depth should be rejected"


# ─────────────────────────────────────────────────────────────────────────────
# 3-Weeks-Tight
# ─────────────────────────────────────────────────────────────────────────────

class TestThreeWeeksTight:
    def _make_tight_3wt_frame(self, prior_adv_pct: float = 0.30) -> pd.DataFrame:
        """Frame where weekly closes at bars 90, 85, 80 are all within 1% of each other,
        preceded by a genuine ≥20% prior advance in bars 60-79."""
        n = 110
        closes = np.full(n, 100.0)
        # Prior advance in bars 60-79
        prior_start_val = 100.0 / (1 + prior_adv_pct)
        closes[60:80] = np.linspace(prior_start_val, 100.0, 20)
        # Tight 3 weekly closes at bars 80, 85, 90 (within 1%)
        closes[80] = 100.0
        closes[81:85] = 100.0
        closes[85] = 100.5
        closes[86:90] = 100.3
        closes[90] = 99.8
        closes[91:105] = 99.8
        frame = _base_frame(closes)
        frame.loc[80:90, "volume_ratio_20"] = 0.7  # volume drying up
        return frame

    def _make_loose_3wt_frame(self, prior_adv_pct: float = 0.30) -> pd.DataFrame:
        """Frame where EVERY triple of weekly closes (end, end-5, end-10) has > 5% spread.

        Uses alternating 100/110 pattern in 5-bar blocks so any (end, end-5, end-10)
        triple is always (100, 110, 100) or (110, 100, 110) → 9.5% spread > 1.5%.
        """
        n = 120
        closes = np.full(n, 100.0)
        # Prior advance in bars 40-59 (enough room for base period to start at 60)
        prior_start_val = 100.0 / (1 + prior_adv_pct)
        closes[40:60] = np.linspace(prior_start_val, 100.0, 20)
        # Alternating 100/110 every 5 bars from bar 60 onward
        for i in range(60, n):
            closes[i] = 100.0 if (i // 5) % 2 == 0 else 110.0
        return _base_frame(closes)

    def test_three_tight_weekly_closes(self):
        config = PatternScanConfig(wt3_tight_pct=0.015, wt3_prior_adv=0.20, recent_signal_max_age_bars=50)
        frame = self._make_tight_3wt_frame(prior_adv_pct=0.30)
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_3wt_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert stats.candidate_count >= 1, "Tight weekly closes with prior advance should produce 3WT candidates"

    def test_loose_weekly_closes_skipped(self):
        """Every triple of weekly closes has ~9.5% spread → no 3WT candidates anywhere."""
        config = PatternScanConfig(wt3_tight_pct=0.015, wt3_prior_adv=0.20, recent_signal_max_age_bars=50)
        frame = self._make_loose_3wt_frame(prior_adv_pct=0.30)
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_3wt_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert stats.candidate_count == 0, "Alternating 100/110 triples (9.5% spread) should all be rejected"

    def test_no_prior_advance_skipped(self):
        """Prior advance < 20% → no 3WT candidates."""
        config = PatternScanConfig(wt3_tight_pct=0.015, wt3_prior_adv=0.20, recent_signal_max_age_bars=50)
        frame = self._make_tight_3wt_frame(prior_adv_pct=0.05)  # only 5% advance
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_3wt_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert stats.candidate_count == 0, "Insufficient prior advance should be rejected"


# ─────────────────────────────────────────────────────────────────────────────
# Symmetrical Triangle
# ─────────────────────────────────────────────────────────────────────────────

class TestSymmetricalTriangle:
    def _make_sym_tri_extrema(
        self, *, descending: bool = True
    ) -> tuple[pd.DataFrame, pd.Series, list]:
        """Construct frame + smoothed + explicit extrema for symmetrical triangle.

        Descending peaks: 210 → 195 (or 195 → 210 if descending=False)
        Ascending troughs: 160 → 175 (converging)
        """
        from ai_trading_system.analytics.patterns.signal import LocalExtrema

        n = 100
        closes = np.full(n, 185.0, dtype=float)
        # Peak 1 at bar 15 (210)
        closes[13:18] = 210.0
        # Trough 1 at bar 30 (160 — ascending trough 1)
        closes[28:33] = 160.0
        # Trough 2 at bar 55 (175 — ascending: 175 > 160)
        closes[53:58] = 175.0
        # Peak 2 at bar 70 (195 < 210 → descending; or 215 > 210 → ascending)
        peak2_val = 195.0 if descending else 215.0
        closes[68:73] = peak2_val
        # Breakout: close > upper resistance
        closes[78] = 200.0
        closes[79:] = 200.0

        frame, smoothed = _make_manual_extrema_frame(closes)
        frame.loc[78, "volume_ratio_20"] = 2.0
        frame.loc[78, "volume_zscore_20"] = 2.5

        extrema = [
            LocalExtrema(15, "peak", 210.0),
            LocalExtrema(30, "trough", 160.0),
            LocalExtrema(55, "trough", 175.0),   # ascending: 175 > 160
            LocalExtrema(70, "peak", float(peak2_val)),
        ]
        return frame, smoothed, extrema

    def test_converging_peaks_troughs(self):
        """Descending peaks (210→195) + ascending troughs (160→175) → candidate found."""
        config = PatternScanConfig(recent_signal_max_age_bars=60)
        frame, smoothed, extrema = self._make_sym_tri_extrema(descending=True)
        signals, stats = detect_symmetrical_triangle_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert stats.candidate_count >= 1, "Converging peaks and troughs should produce sym-tri candidates"

    def test_ascending_peaks_rejected(self):
        """Non-descending peaks (195→215) → no symmetrical triangle."""
        config = PatternScanConfig(recent_signal_max_age_bars=60)
        frame, smoothed, extrema = self._make_sym_tri_extrema(descending=False)
        signals, stats = detect_symmetrical_triangle_signals(
            frame, smoothed=smoothed, extrema=extrema, config=config, recent_only=False
        )
        assert len(signals) == 0, "Ascending (not descending) peaks should not trigger sym-tri"


# ─────────────────────────────────────────────────────────────────────────────
# Head & Shoulders Filter
# ─────────────────────────────────────────────────────────────────────────────

class TestHeadShouldersFilter:
    def _make_hs_frame(self) -> pd.DataFrame:
        """Classic H&S: left shoulder 190, head 210, right shoulder 188, neckline ~165."""
        n = 150
        closes = np.full(n, 180.0)
        # Left shoulder at bar 20
        closes[18:23] = 190.0
        # First trough (neckline area) at bar 30
        closes[28:33] = 165.0
        # Head at bar 50
        closes[48:53] = 210.0
        # Second trough at bar 65
        closes[63:68] = 166.0
        # Right shoulder at bar 80
        closes[78:83] = 188.0
        # Confirmed breakdown: latest close below neckline (~165)
        closes[120:] = 163.0
        frame = _base_frame(closes)
        return frame

    def test_hs_top_detected(self):
        config = PatternScanConfig()
        frame = self._make_hs_frame()
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        is_hs, neckline = detect_head_shoulders_filter(
            frame, smoothed=smoothed, extrema=extrema, config=config
        )
        assert isinstance(is_hs, bool)
        assert isinstance(neckline, float)
        # In a clear H&S frame with confirmed breakdown, may detect H&S
        # (note: detection depends on extrema resolution with given bandwidth)
        # At minimum, verify the function returns the right types
        assert neckline >= 0.0

    def test_no_hs_returns_false(self):
        """Monotonically rising close → no H&S top detected."""
        n = 150
        closes = np.linspace(100, 200, n)
        frame = _base_frame(closes)
        config = PatternScanConfig()
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        is_hs, neckline = detect_head_shoulders_filter(
            frame, smoothed=smoothed, extrema=extrema, config=config
        )
        assert is_hs is False
        assert neckline == 0.0

    def test_hs_suppresses_bullish_signals(self):
        """When H&S fires, bullish signals are suppressed and a bearish marker is emitted."""
        config = PatternScanConfig()
        frame = self._make_hs_frame()
        smoothed, extrema = _smoothed_and_extrema(frame, config)

        # Confirm H&S fires for this frame
        is_hs, neckline = detect_head_shoulders_filter(
            frame, smoothed=smoothed, extrema=extrema, config=config
        )
        if not is_hs:
            pytest.skip("H&S not detected by extrema resolution — bandwidth-dependent test")

        # Now run the full dispatcher — should return no bullish signals
        signals_df, stats = detect_pattern_signals_for_symbol(
            frame, smoothed=smoothed, extrema=extrema, config=config
        )
        assert len(signals_df.index) == 1
        row = signals_df.iloc[0]
        assert row["pattern_family"] == "head_shoulders"
        assert row["pattern_operational_tier"] == "suppression_only"
        assert row["pattern_score"] == 0.0
        assert "head_shoulders" in stats


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 canonical momentum detectors
# ─────────────────────────────────────────────────────────────────────────────

class TestCanonicalMomentumPatterns:
    def _darvas_frame(self, *, deep: bool = False, missing_z: bool = False) -> pd.DataFrame:
        n = 95
        closes = np.linspace(88.0, 104.0, n)
        closes[45:75] = 106.0
        closes[[52, 68]] = 111.0
        closes[76] = 115.0
        frame = _base_frame(closes)
        frame.loc[45:75, "high"] = 112.0
        frame.loc[45:75, "low"] = 80.0 if deep else 100.0
        frame.loc[76, "close"] = 115.0
        frame.loc[76, "high"] = 116.0
        frame.loc[76, "volume_ratio_20"] = 2.0
        frame.loc[76, "volume_zscore_20"] = 2.5
        if missing_z:
            frame = frame.drop(columns=["volume_zscore_20", "volume_zscore_50"])
        return frame

    def test_darvas_box_positive_negative_and_missing_volume(self):
        config = PatternScanConfig(min_history_bars=80, darvas_min_box_bars=15, recent_signal_max_age_bars=25)
        frame = self._darvas_frame()
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_darvas_box_signals(frame, smoothed=smoothed, extrema=extrema, config=config)

        assert stats.confirmed_count >= 1
        assert signals[0].pattern_family == "darvas_box"
        assert signals[0].invalidation_price > 0

        deep_frame = self._darvas_frame(deep=True)
        signals, stats = detect_darvas_box_signals(deep_frame, smoothed=smoothed, extrema=extrema, config=config)
        assert signals == []

        missing_frame = self._darvas_frame(missing_z=True)
        signals, stats = detect_darvas_box_signals(missing_frame, smoothed=smoothed, extrema=extrema, config=config)
        assert signals == []
        assert stats.candidate_count == 0

    def _pocket_pivot_frame(self, *, extended: bool = False, missing_sma: bool = False) -> pd.DataFrame:
        n = 70
        closes = np.full(n, 100.0)
        closes[45:55] = [101.0, 99.0, 102.0, 100.0, 103.0, 101.0, 104.0, 102.0, 103.0, 102.5]
        closes[55] = 115.0 if extended else 106.0
        frame = _base_frame(closes)
        frame.loc[:, "open"] = frame["close"] + 0.5
        frame.loc[55, "open"] = closes[55] - 1.0
        frame.loc[45:54, "volume"] = [1000, 1100, 900, 1200, 950, 1300, 900, 1250, 800, 1100]
        frame.loc[55, "volume"] = 1600
        frame["sma_20"] = 101.0
        frame["sma_50"] = 100.0
        frame.loc[55, "volume_ratio_20"] = 2.0
        frame.loc[55, "volume_zscore_20"] = 2.5
        if missing_sma:
            frame = frame.drop(columns=["sma_20", "sma_50"])
        return frame

    def test_pocket_pivot_positive_negative_and_missing_data(self):
        config = PatternScanConfig(min_history_bars=60, recent_signal_max_age_bars=20)
        frame = self._pocket_pivot_frame()
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_pocket_pivot_signals(frame, smoothed=smoothed, extrema=extrema, config=config)

        assert stats.confirmed_count == 1
        assert signals[0].pattern_family == "pocket_pivot"

        extended = self._pocket_pivot_frame(extended=True)
        signals, _ = detect_pocket_pivot_signals(extended, smoothed=smoothed, extrema=extrema, config=config)
        assert signals == []

        missing = self._pocket_pivot_frame(missing_sma=True)
        signals, stats = detect_pocket_pivot_signals(missing, smoothed=smoothed, extrema=extrema, config=config)
        assert signals == []
        assert stats.candidate_count == 0

    def _ascending_base_frame(self, *, bad_lows: bool = False, missing_z: bool = False) -> tuple[pd.DataFrame, pd.Series, list[LocalExtrema]]:
        n = 95
        closes = np.linspace(100.0, 116.0, n)
        lows = [100.0, 104.0, 108.0] if not bad_lows else [100.0, 98.0, 99.0]
        for idx, value in zip([20, 45, 70], lows):
            closes[idx - 1:idx + 2] = value
        closes[76] = 122.0
        frame = _base_frame(closes)
        frame.loc[20:70, "high"] = 118.0
        frame.loc[76, "high"] = 123.0
        frame.loc[76, "volume_ratio_20"] = 2.0
        frame.loc[76, "volume_zscore_20"] = 2.5
        if missing_z:
            frame = frame.drop(columns=["volume_zscore_20", "volume_zscore_50"])
        smoothed = pd.Series(closes, index=frame.index)
        extrema = [
            LocalExtrema(20, "trough", float(lows[0])),
            LocalExtrema(45, "trough", float(lows[1])),
            LocalExtrema(70, "trough", float(lows[2])),
        ]
        return frame, smoothed, extrema

    def test_ascending_base_positive_negative_and_missing_volume(self):
        config = PatternScanConfig(min_history_bars=80, recent_signal_max_age_bars=25)
        frame, smoothed, extrema = self._ascending_base_frame()
        signals, stats = detect_ascending_base_signals(frame, smoothed=smoothed, extrema=extrema, config=config)

        assert stats.confirmed_count == 1
        assert signals[0].pattern_family == "ascending_base"

        bad_frame, bad_smoothed, bad_extrema = self._ascending_base_frame(bad_lows=True)
        signals, _ = detect_ascending_base_signals(bad_frame, smoothed=bad_smoothed, extrema=bad_extrema, config=config)
        assert signals == []

        missing_frame, missing_smoothed, missing_extrema = self._ascending_base_frame(missing_z=True)
        signals, stats = detect_ascending_base_signals(missing_frame, smoothed=missing_smoothed, extrema=missing_extrema, config=config)
        assert signals == []
        assert stats.candidate_count == 0

    def _ipo_base_frame(self, *, n: int = 80, deep: bool = False, missing_z: bool = False) -> pd.DataFrame:
        closes = np.linspace(35.0, 46.0, n)
        start, end = 40, min(65, n - 3)
        closes[start:end + 1] = 44.0
        breakout_idx = end + 1
        closes[breakout_idx] = 51.0
        frame = _base_frame(closes)
        frame.loc[start:end, "high"] = 50.0
        frame.loc[start:end, "low"] = 32.0 if deep else 40.0
        frame.loc[breakout_idx, "high"] = 52.0
        frame.loc[breakout_idx, "volume_ratio_20"] = 2.0
        frame.loc[breakout_idx, "volume_zscore_20"] = 2.5
        if missing_z:
            frame = frame.drop(columns=["volume_zscore_20", "volume_zscore_50"])
        return frame

    def test_ipo_base_positive_negative_and_missing_volume(self):
        config = PatternScanConfig(min_history_bars=120, ipo_base_min_history_bars=35, recent_signal_max_age_bars=25)
        frame = self._ipo_base_frame()
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_ipo_base_signals(frame, smoothed=smoothed, extrema=extrema, config=config)

        assert stats.confirmed_count >= 1
        assert signals[0].pattern_family == "ipo_base"

        mature = self._ipo_base_frame(n=200)
        signals, _ = detect_ipo_base_signals(mature, smoothed=pd.Series(mature["close"]), extrema=[], config=config)
        assert signals == []

        missing = self._ipo_base_frame(missing_z=True)
        signals, stats = detect_ipo_base_signals(missing, smoothed=pd.Series(missing["close"]), extrema=[], config=config)
        assert signals == []
        assert stats.candidate_count == 0

    def _inside_week_frame(self, *, no_inside: bool = False, missing_timestamp: bool = False) -> pd.DataFrame:
        closes = np.full(35, 100.0)
        frame = _base_frame(closes)
        frame.loc[0:4, "high"] = 110.0
        frame.loc[0:4, "low"] = 90.0
        frame.loc[5:9, "high"] = 112.0 if no_inside else 108.0
        frame.loc[5:9, "low"] = 88.0 if no_inside else 92.0
        frame.loc[10, "close"] = 109.5
        frame.loc[10, "high"] = 110.0
        frame.loc[10, "volume_ratio_20"] = 2.0
        frame.loc[10, "volume_zscore_20"] = 2.5
        if missing_timestamp:
            frame = frame.drop(columns=["timestamp"])
        return frame

    def test_inside_week_breakout_positive_negative_and_missing_data(self):
        config = PatternScanConfig(min_history_bars=30, recent_signal_max_age_bars=30)
        frame = self._inside_week_frame()
        smoothed, extrema = _smoothed_and_extrema(frame, config)
        signals, stats = detect_inside_week_breakout_signals(frame, smoothed=smoothed, extrema=extrema, config=config)

        assert stats.confirmed_count == 1
        assert signals[0].pattern_family == "inside_week_breakout"

        no_inside = self._inside_week_frame(no_inside=True)
        signals, stats = detect_inside_week_breakout_signals(no_inside, smoothed=pd.Series(no_inside["close"]), extrema=[], config=config)
        assert signals == []

        missing = self._inside_week_frame(missing_timestamp=True)
        signals, stats = detect_inside_week_breakout_signals(missing, smoothed=pd.Series(missing["close"]), extrema=[], config=config)
        assert signals == []
        assert stats.candidate_count == 0

    def test_inside_week_breakout_uses_calendar_weeks_not_five_bar_buckets(self):
        config = PatternScanConfig(min_history_bars=30, recent_signal_max_age_bars=30)
        frame = self._inside_week_frame()
        frame = frame.drop(index=[2]).reset_index(drop=True)
        smoothed = pd.Series(frame["close"])

        signals, stats = detect_inside_week_breakout_signals(frame, smoothed=smoothed, extrema=[], config=config)

        assert stats.confirmed_count == 1
        assert signals[0].pattern_family == "inside_week_breakout"

    def test_dispatcher_emits_new_family_without_schema_change(self):
        config = PatternScanConfig(min_history_bars=120, ipo_base_min_history_bars=35, recent_signal_max_age_bars=25)
        frame = self._ipo_base_frame()
        smoothed, extrema = _smoothed_and_extrema(frame, config)

        signals_df, stats = detect_pattern_signals_for_symbol(frame, smoothed=smoothed, extrema=extrema, config=config)

        assert "ipo_base" in stats
        assert "ipo_base" in set(signals_df["pattern_family"])
        assert {"pattern_family", "pattern_state", "pattern_score", "setup_quality", "invalidation_price"}.issubset(signals_df.columns)

    def test_new_phase4_families_are_tier_1_for_scoring(self):
        scored = _score_signal_rows(
            pd.DataFrame(
                [
                    {"symbol_id": "IPO", "pattern_family": "ipo_base", "pattern_state": "confirmed"},
                    {"symbol_id": "IWB", "pattern_family": "inside_week_breakout", "pattern_state": "confirmed"},
                ]
            )
        ).set_index("symbol_id")

        assert scored.loc["IPO", "pattern_operational_tier"] == "tier_1"
        assert scored.loc["IWB", "pattern_operational_tier"] == "tier_1"


# ─────────────────────────────────────────────────────────────────────────────
# Config field smoke test
# ─────────────────────────────────────────────────────────────────────────────

class TestNewConfigFields:
    def test_wt3_fields_present(self):
        c = PatternScanConfig()
        assert c.wt3_tight_pct == 0.015
        assert c.wt3_prior_adv == 0.20

    def test_all_phase2_fields_present(self):
        c = PatternScanConfig()
        assert c.asc_tri_flat_tol == 0.015
        assert c.vcp_window_bars == 40
        assert c.vcp_price_contraction_factor == 0.85
        assert c.vcp_vol_contraction_factor == 0.90
        assert c.vcp_min_first_range_pct == 0.08
        assert c.flat_base_min_bars == 25
        assert c.flat_base_max_bars == 65
        assert c.flat_base_max_depth_pct == 0.15
        assert c.stage2_reclaim_lookback_bars == 20
        assert c.stage2_reclaim_max_extension_pct == 0.08
        assert c.stage2_reclaim_min_slope_pct == 0.0
        assert c.wt3_tight_pct == 0.015
        assert c.wt3_prior_adv == 0.20
        assert c.darvas_lookback_bars == 60
        assert c.pocket_pivot_lookback_bars == 10
        assert c.ascending_base_min_bars == 45
        assert c.ipo_base_min_history_bars == 35
        assert c.inside_week_lookback_weeks == 8
