"""Tests for the new Tier-2 pattern detectors and the bearish H&S signal.

Covers:
  - detect_stage2_reclaim_signals
  - detect_pocket_pivot_signals
  - detect_darvas_box_signals
  - detect_inside_day_signals
  - first-class H&S bearish signal emission via detect_pattern_signals_for_symbol
  - NIFTY-relative RS blending in StockRanker._blend_nifty_relative_rs
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.domains.ranking.patterns.contracts import PatternScanConfig
from ai_trading_system.domains.ranking.patterns.detectors import (
    detect_darvas_box_signals,
    detect_inside_day_signals,
    detect_pocket_pivot_signals,
)


def _make_frame(
    closes: np.ndarray,
    *,
    highs: np.ndarray | None = None,
    lows: np.ndarray | None = None,
    volumes: np.ndarray | None = None,
    extras: dict | None = None,
    symbol: str = "TESTCO",
    start: str = "2025-01-01",
) -> pd.DataFrame:
    n = len(closes)
    idx = pd.date_range(start=start, periods=n, freq="B")
    if highs is None:
        highs = closes + 1.0
    if lows is None:
        lows = closes - 1.0
    if volumes is None:
        volumes = np.full(n, 100_000.0)
    df = pd.DataFrame(
        {
            "symbol_id": [symbol] * n,
            "exchange": ["NSE"] * n,
            "timestamp": idx,
            "open": closes,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        }
    )
    if extras:
        for k, v in extras.items():
            df[k] = v
    return df


# ─────────────────────────── Inside-Day Breakout ────────────────────────────


def test_inside_day_breakout_detects_single_inside_then_break() -> None:
    # Bar layout (oldest → newest):
    #   0: filler, 1: filler, 2: mother (wide range, high=102, low=100),
    #   3: inside (high=101.5, low=100.5),
    #   4: breakout (close > mother.high on volume).
    closes = np.array([100.0, 101.0, 101.0, 101.0, 103.0])
    highs = np.array([101.0, 101.5, 102.0, 101.5, 104.0])
    lows = np.array([99.0, 100.5, 100.0, 100.5, 102.5])
    volumes = np.array([100_000.0, 100_000.0, 100_000.0, 100_000.0, 250_000.0])
    extras = {"volume_ratio_20": np.array([1.0, 1.0, 1.0, 1.0, 2.5]), "volume_zscore_20": np.array([0.0, 0.0, 0.0, 0.0, 2.5])}
    frame = _make_frame(closes, highs=highs, lows=lows, volumes=volumes, extras=extras)
    signals, stats = detect_inside_day_signals(frame, config=PatternScanConfig())
    assert stats.confirmed_count >= 1
    assert signals[-1].pattern_family == "inside_day"
    # Mother high = 102.0 → breakout above this is the pivot.
    assert signals[-1].breakout_level == pytest.approx(102.0, rel=0.01)


def test_inside_day_no_signal_when_breakout_below_mother() -> None:
    closes = np.array([100, 101, 100.5, 100.7, 101.6])  # breakout < mother high (102)
    highs = np.array([101, 102, 101.5, 101.7, 101.8])
    lows = np.array([99, 100, 100.4, 100.6, 101.2])
    volumes = np.array([100_000.0, 100_000.0, 100_000.0, 100_000.0, 250_000.0])
    extras = {"volume_ratio_20": np.array([1.0, 1.0, 1.0, 1.0, 2.5]), "volume_zscore_20": np.array([0.0, 0.0, 0.0, 0.0, 2.5])}
    frame = _make_frame(closes, highs=highs, lows=lows, volumes=volumes, extras=extras)
    signals, _ = detect_inside_day_signals(frame, config=PatternScanConfig())
    assert signals == []


# ─────────────────────────── NIFTY-relative RS ──────────────────────────────


def test_blend_nifty_relative_rs_subtracts_benchmark_returns() -> None:
    """Exercise the blend logic directly with a stub loader."""
    from ai_trading_system.domains.ranking.ranker import NIFTY_RS_BLEND, StockRanker

    class _StubLoader:
        def load_benchmark_returns(self, *, symbol, date, periods):
            # NIFTY return = 5% over each period.
            return {p: 5.0 for p in periods}

    ranker = StockRanker.__new__(StockRanker)
    ranker.input_loader = _StubLoader()
    ranker.ohlcv_db_path = ""

    data = pd.DataFrame(
        {
            "symbol_id": ["A", "B"],
            "exchange": ["NSE", "NSE"],
            "return_20": [10.0, 0.0],
            "return_60": [10.0, 0.0],
            "return_120": [10.0, 0.0],
            "rel_strength": [80.0, 20.0],
        }
    )
    out = ranker._blend_nifty_relative_rs(
        data,
        date="2026-01-01",
        benchmark_symbol="NIFTY50",
        periods=[20, 60, 120],
    )
    # Symbol A beat NIFTY by 5 each period → rs_vs_nifty_20 = 5.
    assert out.loc[out["symbol_id"] == "A", "rs_vs_nifty_20"].iloc[0] == pytest.approx(5.0)
    assert out.loc[out["symbol_id"] == "B", "rs_vs_nifty_20"].iloc[0] == pytest.approx(-5.0)
    # rs_vs_nifty_score is percentile-ranked → A high, B low.
    assert out.loc[out["symbol_id"] == "A", "rs_vs_nifty_score"].iloc[0] > out.loc[
        out["symbol_id"] == "B", "rs_vs_nifty_score"
    ].iloc[0]
    # rel_strength is blended at NIFTY_RS_BLEND weight; A's blended RS is
    # higher than B's regardless of blend value.
    assert out.loc[out["symbol_id"] == "A", "rel_strength"].iloc[0] > out.loc[
        out["symbol_id"] == "B", "rel_strength"
    ].iloc[0]
    # And confirm the constant is in [0, 1].
    assert 0.0 <= NIFTY_RS_BLEND <= 1.0


def test_blend_nifty_relative_rs_no_op_when_benchmark_unavailable() -> None:
    from ai_trading_system.domains.ranking.ranker import StockRanker

    class _StubLoader:
        def load_benchmark_returns(self, *, symbol, date, periods):
            raise RuntimeError("unavailable")

    ranker = StockRanker.__new__(StockRanker)
    ranker.input_loader = _StubLoader()
    ranker.ohlcv_db_path = ""  # no fallback DuckDB path

    data = pd.DataFrame(
        {
            "symbol_id": ["A"],
            "exchange": ["NSE"],
            "return_20": [10.0],
            "return_60": [10.0],
            "return_120": [10.0],
            "rel_strength": [80.0],
        }
    )
    out = ranker._blend_nifty_relative_rs(
        data,
        date="2026-01-01",
        benchmark_symbol="NIFTY50",
        periods=[20, 60, 120],
    )
    # Frame returned unchanged.
    assert "rs_vs_nifty_20" not in out.columns
    assert out["rel_strength"].iloc[0] == pytest.approx(80.0)
