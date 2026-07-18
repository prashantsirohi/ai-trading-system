"""Tests for the R0.1 measurement-repair analysis module."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.research.pattern_lane_calibration.r0_analysis import (
    _window_outcomes,
    assign_episodes,
    load_regime_series,
    repair_signals,
    signal_minus_control,
)


def _market_group(closes: list[float], *, start: str = "2026-01-01") -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=len(closes))
    closes_arr = np.asarray(closes, dtype=float)
    return pd.DataFrame({
        "date": dates,
        "high": closes_arr * 1.02,
        "low": closes_arr * 0.98,
        "close": closes_arr,
    })


def _bench(levels: list[float], *, start: str = "2026-01-01") -> pd.DataFrame:
    return pd.DataFrame({
        "date": pd.bdate_range(start, periods=len(levels)),
        "close": np.asarray(levels, dtype=float),
    })


def test_strict_confirmation_requires_close_not_high_touch() -> None:
    group = _market_group([100.0] * 10 + [104.0] + [100.0] * 9)
    as_of = group["date"].iloc[9]
    bench = _bench([1000.0] * 20)
    rows = _window_outcomes(
        group, as_of=as_of, horizons=[5],
        breakout_level=105.0, invalidation_price=90.0,
        bench_dates=bench["date"].to_numpy(), bench_levels=bench["close"].to_numpy(),
    )
    # high touches 104*1.02 > 105 but no close exceeds 105
    assert rows[0]["confirmed_breakout_strict"] is False
    rows = _window_outcomes(
        group, as_of=as_of, horizons=[5],
        breakout_level=103.0, invalidation_price=90.0,
        bench_dates=bench["date"].to_numpy(), bench_levels=bench["close"].to_numpy(),
    )
    assert rows[0]["confirmed_breakout_strict"] is True
    assert rows[0]["sessions_to_confirmation"] == 1
    # confirmed then window-end close back at/below the breakout level = failed
    assert rows[0]["failed_breakout"] is True


def test_invalidation_is_independent_of_breakout_failure() -> None:
    group = _market_group([100.0] * 10 + [88.0, 106.0, 106.0, 106.0, 106.0] + [106.0] * 5)
    as_of = group["date"].iloc[9]
    bench = _bench([1000.0] * 20)
    rows = _window_outcomes(
        group, as_of=as_of, horizons=[5],
        breakout_level=105.0, invalidation_price=90.0,
        bench_dates=bench["date"].to_numpy(), bench_levels=bench["close"].to_numpy(),
    )
    outcome = rows[0]
    assert outcome["invalidated_setup"] is True         # dipped to 88 first
    assert outcome["confirmed_breakout_strict"] is True  # later closed above 105
    assert outcome["failed_breakout"] is False           # window-end close held above


def test_benchmark_relative_return_computed() -> None:
    group = _market_group([100.0] * 10 + [110.0] * 10)
    as_of = group["date"].iloc[9]
    bench = _bench([1000.0] * 10 + [1050.0] * 10)
    rows = _window_outcomes(
        group, as_of=as_of, horizons=[5],
        breakout_level=np.nan, invalidation_price=np.nan,
        bench_dates=bench["date"].to_numpy(), bench_levels=bench["close"].to_numpy(),
    )
    assert rows[0]["forward_return"] == pytest.approx(0.10)
    assert rows[0]["benchmark_return"] == pytest.approx(0.05)
    assert rows[0]["benchmark_relative_return"] == pytest.approx(0.05)


def test_repair_signals_restores_as_of_and_lane_then_fails_without_context() -> None:
    signals = pd.DataFrame([
        {"signal_id": "A-hs", "symbol_id": "A", "as_of_date": None, "signal_date": "2026-01-05",
         "scan_lane_as_of": None, "history_band": None, "structure_observation_id": None, "exchange": None},
        {"signal_id": "B-ok", "symbol_id": "B", "as_of_date": "2026-01-05", "signal_date": "2026-01-05",
         "scan_lane_as_of": "stage2_continuation", "history_band": "180_plus",
         "structure_observation_id": "obs", "exchange": "NSE"},
    ])
    context = pd.DataFrame([
        {"symbol_id": "A", "as_of_date": "2026-01-05", "scan_lane_as_of": "no_lane",
         "history_band": "180_plus", "structure_observation_id": "obs-a", "exchange": "NSE"},
    ])
    repaired = repair_signals(signals, context)
    row = repaired.loc[repaired["signal_id"] == "A-hs"].iloc[0]
    assert row["as_of_date"] == "2026-01-05"
    assert row["scan_lane_as_of"] == "no_lane"
    assert bool(row["is_suppression_evidence"]) is True
    assert not repaired.loc[repaired["signal_id"] == "B-ok", "is_suppression_evidence"].iloc[0]

    with pytest.raises(RuntimeError, match="no structure-context"):
        repair_signals(signals, context.loc[context["symbol_id"] != "A"])


def test_assign_episodes_dedupes_and_fails_on_null_keys() -> None:
    signals = pd.DataFrame([
        {"symbol_id": "A", "pattern_family": "flat_base", "pattern_start": "2025-12-01", "as_of_date": "2026-01-05"},
        {"symbol_id": "A", "pattern_family": "flat_base", "pattern_start": "2025-12-01", "as_of_date": "2026-01-12"},
        {"symbol_id": "A", "pattern_family": "flat_base", "pattern_start": "2026-01-02", "as_of_date": "2026-01-12"},
    ])
    keyed = assign_episodes(signals)
    assert int(keyed["episode_first"].sum()) == 2
    first = keyed.loc[keyed["episode_first"] & (keyed["pattern_start"] == "2025-12-01")].iloc[0]
    assert first["as_of_date"] == "2026-01-05"

    with pytest.raises(RuntimeError, match="episode-key"):
        assign_episodes(signals.assign(pattern_start=[None, "2025-12-01", "2026-01-02"]))


def test_regime_series_requires_all_dates(tmp_path) -> None:
    frame = pd.DataFrame({"date": ["2026-01-05"], "regime": ["neutral"]})
    path = tmp_path / "regime.csv"
    frame.to_csv(path, index=False)
    required = pd.DatetimeIndex([pd.Timestamp("2026-01-05"), pd.Timestamp("2026-01-12")])
    with pytest.raises(RuntimeError, match="missing"):
        load_regime_series(path, required_dates=required)


def _pair_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    signal_outcomes = pd.DataFrame([
        {"signal_id": "S1", "symbol_id": "A", "scan_lane_as_of": "stage2_continuation",
         "pattern_family": "flat_base", "history_band": "180_plus", "pattern_state": "confirmed",
         "evidence_origin": "fresh", "market_regime": "neutral", "as_of_date": "2026-01-05",
         "episode_id": "A|flat_base|2025-12-01", "episode_first": True, "horizon_sessions": 5,
         "outcome_window_complete": True, "forward_return": 0.05, "benchmark_relative_return": 0.03},
    ])
    control_outcomes = pd.DataFrame([
        {"signal_id": "S1", "control_symbol_id": "Z", "scan_lane": "stage2_continuation",
         "pattern_family": "flat_base", "history_band": "180_plus", "as_of_date": "2026-01-05",
         "horizon_sessions": 5, "outcome_window_complete": True, "forward_return": 0.01,
         "benchmark_return": 0.0, "benchmark_relative_return": 0.01,
         "maximum_favourable_excursion": 0.02, "maximum_adverse_excursion": -0.01},
    ])
    return signal_outcomes, control_outcomes


def test_signal_minus_control_and_incomplete_guard() -> None:
    signal_outcomes, control_outcomes = _pair_frames()
    pairs = signal_minus_control(signal_outcomes, control_outcomes, allow_incomplete_control_pairs=0)
    assert pairs.iloc[0]["signal_minus_control_return"] == pytest.approx(0.04)

    broken = control_outcomes.assign(outcome_window_complete=False)
    with pytest.raises(RuntimeError, match="control pairs lack complete outcome windows"):
        signal_minus_control(signal_outcomes, broken, allow_incomplete_control_pairs=0)
    allowed = signal_minus_control(signal_outcomes, broken, allow_incomplete_control_pairs=1)
    assert allowed.empty
