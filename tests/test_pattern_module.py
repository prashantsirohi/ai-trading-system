from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from analytics.patterns.contracts import PatternBacktestConfig, PatternScanConfig
from analytics.patterns.data import load_pattern_frame
from analytics.patterns.detectors import (
    _score_signal_rows,
    detect_cup_handle_events,
    detect_cup_handle_signals,
    detect_ascending_triangle_signals,
    detect_double_bottom_signals,
    detect_flag_signals,
    detect_flat_base_signals,
    detect_vcp_signals,
    detect_round_bottom_events,
)
from analytics.patterns.evaluation import (
    build_pattern_signals,
    ensure_pattern_event_chart,
    run_pattern_backtest,
    simulate_pattern_trades,
)
from analytics.patterns.signal import find_local_extrema, kernel_smooth
from research import backtest_patterns


def _make_price_frame(
    close_values: list[float],
    *,
    symbol_id: str = "TEST",
    breakout_volume_idx: int | None = None,
    breakout_volume_ratio: float = 2.0,
) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=len(close_values), freq="B")
    close = pd.Series(close_values, dtype=float)
    open_ = close.shift(1).fillna(close.iloc[0])
    frame = pd.DataFrame(
        {
            "symbol_id": symbol_id,
            "exchange": "NSE",
            "timestamp": dates,
            "open": open_,
            "high": pd.concat([open_, close], axis=1).max(axis=1) + 0.5,
            "low": pd.concat([open_, close], axis=1).min(axis=1) - 0.5,
            "close": close,
            "volume": 1000.0,
            "atr_value": 2.0,
            "volume_ratio_20": 1.0,
            "volume_zscore_20": np.nan,
            "volume_zscore_50": np.nan,
        }
    )
    frame.loc[:, "sma_20"] = frame["close"].rolling(20, min_periods=1).mean()
    frame.loc[:, "sma_50"] = frame["close"].rolling(50, min_periods=1).mean()
    frame.loc[:, "sma_200"] = frame["close"].rolling(200, min_periods=1).mean()
    frame.loc[:, "sma50_slope_20d_pct"] = frame["sma_50"].pct_change(20) * 100.0
    frame.loc[:, "above_sma200"] = frame["close"] > frame["sma_200"]
    for horizon in (5, 10, 20, 40):
        frame.loc[:, f"return_{horizon}d"] = frame["close"].shift(-horizon) / frame["close"] - 1.0
    trough_idx = int(frame["close"].idxmin())
    frame.loc[max(0, trough_idx - 2) : min(len(frame) - 1, trough_idx + 2), "volume_ratio_20"] = 0.7
    if breakout_volume_idx is not None:
        frame.loc[breakout_volume_idx, "volume_ratio_20"] = breakout_volume_ratio
    return frame


def _cup_handle_frame(*, symbol_id: str = "CUP", breakout_volume_ratio: float = 2.0) -> pd.DataFrame:
    close = (
        list(pd.Series(range(100, 120)))
        + [122.0]
        + list(pd.Series([119.0, 116.0, 112.0, 108.0, 104.0, 100.0, 96.0, 93.0, 91.0, 90.0, 90.0, 91.0]))
        + [94.0, 98.0, 101.0, 104.0, 107.0, 110.0, 113.0, 116.0, 118.0]
        + [117.0, 116.0, 115.0, 114.0, 113.0, 114.0]
        + [125.0, 127.0, 129.0, 130.0, 131.0]
    )
    breakout_idx = len(close) - 5
    return _make_price_frame(
        close,
        symbol_id=symbol_id,
        breakout_volume_idx=breakout_idx,
        breakout_volume_ratio=breakout_volume_ratio,
    )


def _round_bottom_frame(*, symbol_id: str = "ROUND", breakout_volume_ratio: float = 2.0) -> pd.DataFrame:
    close = (
        list(pd.Series(range(100, 120)))
        + [118.0, 115.0, 111.0, 106.0, 101.0, 96.0, 92.0, 89.0]
        + [88.5, 88.0, 88.5]
        + [90.0, 92.0, 94.0, 96.0, 98.0, 100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 114.0, 116.0, 118.0, 117.0]
        + [123.0, 126.0, 128.0, 129.0]
    )
    breakout_idx = len(close) - 4
    return _make_price_frame(
        close,
        symbol_id=symbol_id,
        breakout_volume_idx=breakout_idx,
        breakout_volume_ratio=breakout_volume_ratio,
    )


def _v_bottom_frame(*, symbol_id: str = "VBAD") -> pd.DataFrame:
    close = list(pd.Series(range(100, 120))) + [112.0, 97.0, 82.0, 98.0, 113.0, 121.0, 124.0, 126.0]
    return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=len(close) - 2, breakout_volume_ratio=2.0)


def _detector_config() -> PatternBacktestConfig:
    return PatternBacktestConfig(
        min_history_bars=30,
        min_cup_width=10,
        max_cup_width=80,
        min_round_width=10,
        max_round_width=80,
        handle_min_bars=3,
        handle_max_bars=10,
        breakout_volume_ratio_min=1.5,
        prior_uptrend_lookback=20,
        prior_uptrend_min_pct=0.10,
        min_trough_dwell_bars=2,
        bandwidth=2.0,
        extrema_prominence=0.01,
        max_breakout_wait_bars=10,
    )


def _scan_config() -> PatternScanConfig:
    return PatternScanConfig(
        min_history_bars=30,
        min_cup_width=10,
        max_cup_width=80,
        min_round_width=10,
        max_round_width=80,
        handle_min_bars=3,
        handle_max_bars=10,
        breakout_volume_ratio_min=1.5,
        prior_uptrend_lookback=20,
        prior_uptrend_min_pct=0.10,
        min_trough_dwell_bars=2,
        bandwidth=2.0,
        extrema_prominence=0.01,
        max_breakout_wait_bars=10,
        recent_signal_max_age_bars=8,
        double_bottom_min_separation=8,
        double_bottom_max_separation=40,
        flag_pole_min_bars=5,
        flag_pole_max_bars=20,
        flag_min_bars=3,
        flag_max_bars=12,
        high_tight_pole_max_bars=40,
    )


def _double_bottom_frame(*, symbol_id: str = "DBL", breakout: bool = True) -> pd.DataFrame:
    close = (
        list(pd.Series(range(90, 110)))
        + [108.0, 104.0, 100.0, 97.0, 95.0, 94.0]
        + [96.0, 99.0, 103.0, 107.0, 111.0]
        + [108.0, 104.0, 100.0, 97.0, 95.0, 94.5]
        + [96.0, 99.0, 103.0, 107.0, 110.5]
    )
    if breakout:
        close += [111.5, 114.0, 116.0]
        breakout_idx = len(close) - 2
        return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=breakout_idx, breakout_volume_ratio=2.0)
    close += [109.5, 109.8, 110.0]
    return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=None)


def _flag_frame(*, symbol_id: str = "FLAG", breakout: bool = True) -> pd.DataFrame:
    close = list(pd.Series(range(80, 90))) + [92.0, 95.0, 98.0, 101.0, 104.0, 107.0, 110.0, 112.0]
    close += [111.0, 110.5, 110.0, 109.5, 109.0, 109.5]
    if breakout:
        close += [113.0, 115.0, 116.0]
        breakout_idx = len(close) - 3
        return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=breakout_idx, breakout_volume_ratio=1.8)
    close += [111.0, 111.5]
    return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=None)


def _loose_flag_frame(*, symbol_id: str = "FLAGBAD") -> pd.DataFrame:
    close = list(pd.Series(range(80, 90))) + [92.0, 95.0, 98.0, 101.0, 104.0, 107.0, 110.0]
    close += [103.0, 98.0, 93.0, 88.0, 86.0, 89.0, 92.0]
    close += [109.0, 111.0]
    return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=len(close) - 2, breakout_volume_ratio=1.8)


def _high_tight_flag_frame(*, symbol_id: str = "HTF", breakout: bool = True, pole_gain: float = 0.95) -> pd.DataFrame:
    start = 100.0
    end = start * (1.0 + pole_gain)
    pole = pd.Series(np.linspace(start, end, 30)).round(2).tolist()
    close = [100.0] * 10 + pole
    peak = pole[-1]
    close += [peak * 0.97, peak * 0.96, peak * 0.95, peak * 0.955, peak * 0.96, peak * 0.958]
    if breakout:
        close += [peak * 1.01, peak * 1.03]
        breakout_idx = len(close) - 2
        return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=breakout_idx, breakout_volume_ratio=2.2)
    close += [peak * 0.985]
    return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=None)


def test_kernel_smooth_and_extrema_detect_expected_turns() -> None:
    series = pd.Series([1.0, 2.0, 4.0, 6.0, 4.0, 2.0, 1.0, 2.0, 4.0, 6.0, 4.0, 2.0, 1.0])
    smoothed = kernel_smooth(series, bandwidth=1.5)
    extrema = find_local_extrema(smoothed, prominence=0.2)

    assert len(smoothed) == len(series)
    assert [point.kind for point in extrema].count("peak") >= 2
    assert [point.kind for point in extrema].count("trough") >= 1


def test_detect_cup_handle_positive_case_and_contract_fields() -> None:
    frame = _cup_handle_frame()
    config = _detector_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    events, stats = detect_cup_handle_events(frame, smoothed=smoothed, extrema=extrema, config=config)

    assert stats.confirmed_count == 1
    assert len(events) == 1
    event = events[0]
    expected_stop = float(frame.iloc[event.pivot_indices[2] : event.pivot_indices[3] + 1]["low"].min())
    assert event.pattern_type == "cup_handle"
    assert event.handle_date is not None
    assert event.breakout_volume_confirmed is True
    assert abs(event.invalidation_price - expected_stop) < 1e-6


def test_detect_round_bottom_positive_case_and_fallback_invalidation() -> None:
    frame = _round_bottom_frame()
    config = _detector_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    events, stats = detect_round_bottom_events(frame, smoothed=smoothed, extrema=extrema, config=config)

    assert stats.confirmed_count == 1
    assert len(events) == 1
    event = events[0]
    expected_stop = float(frame.iloc[event.pattern_end_index + 1 : event.breakout_bar_index]["low"].min())
    assert event.pattern_type == "round_bottom"
    assert event.handle_date is None
    assert abs(event.invalidation_price - expected_stop) < 1e-6


def test_sharp_v_bottom_is_rejected() -> None:
    frame = _v_bottom_frame()
    config = _detector_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    events, stats = detect_round_bottom_events(frame, smoothed=smoothed, extrema=extrema, config=config)

    assert stats.confirmed_count == 0
    assert events == []


def test_cup_without_handle_is_rejected() -> None:
    frame = _cup_handle_frame().iloc[:-6].copy()
    frame = pd.concat(
        [
            frame,
            _make_price_frame(
                [123.0, 126.0, 128.0, 129.0],
                symbol_id="CUP",
                breakout_volume_idx=0,
                breakout_volume_ratio=2.0,
            ).assign(timestamp=lambda df: pd.date_range(frame["timestamp"].iloc[-1] + pd.offsets.BDay(1), periods=len(df), freq="B")),
        ],
        ignore_index=True,
    )
    config = _detector_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    events, _ = detect_cup_handle_events(frame, smoothed=smoothed, extrema=extrema, config=config)

    assert events == []


def test_breakout_without_volume_confirmation_is_rejected() -> None:
    frame = _cup_handle_frame(breakout_volume_ratio=1.1)
    config = _detector_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    events, _ = detect_cup_handle_events(frame, smoothed=smoothed, extrema=extrema, config=config)

    assert events == []


def test_breakout_can_confirm_from_z20_when_ratio_is_weak() -> None:
    frame = _cup_handle_frame(breakout_volume_ratio=1.1)
    breakout_idx = len(frame) - 5
    frame.loc[breakout_idx, "volume_zscore_20"] = 2.6
    config = _detector_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    events, _ = detect_cup_handle_events(frame, smoothed=smoothed, extrema=extrema, config=config)

    assert len(events) == 1
    assert events[0].breakout_volume_confirmed is True


def test_non_breakout_watchlist_is_not_upgraded_by_zscore_alone() -> None:
    frame = _double_bottom_frame(symbol_id="DBLZ", breakout=False)
    frame.loc[frame.index[-1], "volume_zscore_20"] = 3.2
    config = _scan_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    signals, stats = detect_double_bottom_signals(
        frame,
        smoothed=smoothed,
        extrema=extrema,
        config=config,
    )

    assert stats.confirmed_count == 0
    assert any(signal.pattern_state == "watchlist" for signal in signals)


def test_detect_double_bottom_confirmed_and_watchlist() -> None:
    config = _scan_config()

    confirmed_frame = _double_bottom_frame(breakout=True)
    confirmed_smoothed = kernel_smooth(confirmed_frame["close"], bandwidth=config.bandwidth)
    confirmed_extrema = find_local_extrema(confirmed_smoothed, prominence=config.extrema_prominence)
    confirmed_signals, confirmed_stats = detect_double_bottom_signals(
        confirmed_frame,
        smoothed=confirmed_smoothed,
        extrema=confirmed_extrema,
        config=config,
    )

    watchlist_frame = _double_bottom_frame(symbol_id="DBLW", breakout=False)
    watchlist_smoothed = kernel_smooth(watchlist_frame["close"], bandwidth=config.bandwidth)
    watchlist_extrema = find_local_extrema(watchlist_smoothed, prominence=config.extrema_prominence)
    watchlist_signals, watchlist_stats = detect_double_bottom_signals(
        watchlist_frame,
        smoothed=watchlist_smoothed,
        extrema=watchlist_extrema,
        config=config,
    )

    assert confirmed_stats.confirmed_count >= 1
    assert any(signal.pattern_state == "confirmed" for signal in confirmed_signals)
    assert watchlist_stats.watchlist_count >= 1
    assert any(signal.pattern_state == "watchlist" for signal in watchlist_signals)


def test_detect_flag_and_high_tight_flag_positive_cases() -> None:
    config = _scan_config()

    flag_frame = _flag_frame()
    flag_smoothed = kernel_smooth(flag_frame["close"], bandwidth=config.bandwidth)
    flag_extrema = find_local_extrema(flag_smoothed, prominence=config.extrema_prominence)
    flag_signals, flag_stats = detect_flag_signals(
        flag_frame,
        smoothed=flag_smoothed,
        extrema=flag_extrema,
        config=config,
    )

    high_tight_frame = _high_tight_flag_frame()
    high_tight_smoothed = kernel_smooth(high_tight_frame["close"], bandwidth=config.bandwidth)
    high_tight_extrema = find_local_extrema(high_tight_smoothed, prominence=config.extrema_prominence)
    high_tight_signals, high_tight_stats = detect_flag_signals(
        high_tight_frame,
        smoothed=high_tight_smoothed,
        extrema=high_tight_extrema,
        config=config,
        high_tight_only=True,
    )

    assert flag_stats.confirmed_count >= 1
    assert any(signal.pattern_family == "flag" for signal in flag_signals)
    assert high_tight_stats.confirmed_count >= 1
    assert any(signal.pattern_family == "high_tight_flag" for signal in high_tight_signals)


def test_loose_flag_and_insufficient_high_tight_pole_are_rejected() -> None:
    config = _scan_config()

    loose_flag_frame = _loose_flag_frame()
    loose_smoothed = kernel_smooth(loose_flag_frame["close"], bandwidth=config.bandwidth)
    loose_extrema = find_local_extrema(loose_smoothed, prominence=config.extrema_prominence)
    loose_signals, _ = detect_flag_signals(
        loose_flag_frame,
        smoothed=loose_smoothed,
        extrema=loose_extrema,
        config=config,
    )

    weak_high_tight_frame = _high_tight_flag_frame(symbol_id="HTFBAD", pole_gain=0.65)
    weak_smoothed = kernel_smooth(weak_high_tight_frame["close"], bandwidth=config.bandwidth)
    weak_extrema = find_local_extrema(weak_smoothed, prominence=config.extrema_prominence)
    weak_signals, _ = detect_flag_signals(
        weak_high_tight_frame,
        smoothed=weak_smoothed,
        extrema=weak_extrema,
        config=config,
        high_tight_only=True,
    )

    assert loose_signals == []
    assert weak_signals == []


def test_build_pattern_signals_returns_confirmed_and_watchlist_with_deterministic_rank() -> None:
    frame = pd.concat(
        [
            _cup_handle_frame(symbol_id="CUP"),
            _double_bottom_frame(symbol_id="DBLW", breakout=False),
        ],
        ignore_index=True,
    )
    ranked_df = pd.DataFrame(
        [
            {"symbol_id": "CUP", "rel_strength_score": 88.0, "sector_rs_value": 0.82},
            {"symbol_id": "DBLW", "rel_strength_score": 65.0, "sector_rs_value": 0.61},
        ]
    )

    signals = build_pattern_signals(
        project_root=Path("."),
        signal_date="2024-12-31",
        exchange="NSE",
        data_domain="research",
        config=_scan_config(),
        frame=frame,
        ranked_df=ranked_df,
    )

    assert {"confirmed", "watchlist"}.issubset(set(signals["pattern_state"].astype(str)))
    assert {
        "pattern_operational_tier",
        "pattern_priority_score",
        "pattern_priority_rank",
        "volume_zscore_20",
        "volume_zscore_50",
    }.issubset(signals.columns)
    assert signals["pattern_rank"].astype(int).tolist() == list(range(1, len(signals) + 1))
    assert sorted(signals["pattern_priority_rank"].astype(int).tolist()) == list(range(1, len(signals) + 1))
    assert signals["pattern_score"].astype(float).is_monotonic_decreasing


def test_score_signal_rows_adds_operational_priority_without_replacing_primary_rank() -> None:
    scored = _score_signal_rows(
        pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "pattern_family": "cup_handle",
                    "pattern_state": "watchlist",
                    "signal_date": "2024-12-31",
                    "breakout_volume_ratio": 1.6,
                    "rel_strength_score": 80.0,
                    "sector_rs_percentile": 75.0,
                    "setup_quality": 100.0,
                    "handle_depth_pct": 6.0,
                    "stage2_score": 85.0,
                },
                {
                    "symbol_id": "BBB",
                    "pattern_family": "high_tight_flag",
                    "pattern_state": "confirmed",
                    "signal_date": "2024-12-31",
                    "breakout_volume_ratio": 2.0,
                    "rel_strength_score": 60.0,
                    "sector_rs_percentile": 60.0,
                    "setup_quality": 50.0,
                    "stage2_score": 70.0,
                    "volume_dry_up": True,
                    "pole_rise_pct": 75.0,
                    "flag_tightness_pct": 20.0,
                },
                {
                    "symbol_id": "CCC",
                    "pattern_family": "mystery_pattern",
                    "pattern_state": "confirmed",
                    "signal_date": "2024-12-31",
                    "breakout_volume_ratio": 1.6,
                    "rel_strength_score": 60.0,
                    "sector_rs_percentile": 60.0,
                    "setup_quality": 50.0,
                    "stage2_score": 70.0,
                },
                {
                    "symbol_id": "DDD",
                    "pattern_family": "head_shoulders",
                    "pattern_state": "confirmed",
                    "signal_date": "2024-12-31",
                    "breakout_volume_ratio": 1.6,
                    "rel_strength_score": 60.0,
                    "sector_rs_percentile": 60.0,
                    "setup_quality": 50.0,
                    "stage2_score": 70.0,
                },
            ]
        )
    )

    tier_by_symbol = scored.set_index("symbol_id")["pattern_operational_tier"].to_dict()
    priority_rank_by_symbol = scored.set_index("symbol_id")["pattern_priority_rank"].astype(int).to_dict()
    pattern_rank_by_symbol = scored.set_index("symbol_id")["pattern_rank"].astype(int).to_dict()
    priority_score_by_symbol = scored.set_index("symbol_id")["pattern_priority_score"].astype(float).to_dict()

    assert tier_by_symbol == {
        "AAA": "tier_1",
        "BBB": "tier_2",
        "CCC": "tier_2",
        "DDD": "suppression_only",
    }
    assert priority_score_by_symbol["AAA"] > priority_score_by_symbol["CCC"] > priority_score_by_symbol["DDD"]
    assert pattern_rank_by_symbol["BBB"] == 1
    assert priority_rank_by_symbol["AAA"] == 1
    assert pattern_rank_by_symbol["AAA"] != priority_rank_by_symbol["AAA"]
    assert sorted(priority_rank_by_symbol.values()) == [1, 2, 3, 4]


def test_score_signal_rows_uses_mutually_exclusive_volume_confirmation_ladder() -> None:
    scored = _score_signal_rows(
        pd.DataFrame(
            [
                {
                    "symbol_id": "RATIO",
                    "pattern_family": "flag",
                    "pattern_state": "confirmed",
                    "signal_date": "2024-12-31",
                    "breakout_volume_ratio": 1.6,
                    "volume_zscore_20": np.nan,
                    "volume_zscore_50": np.nan,
                    "rel_strength_score": 80.0,
                    "sector_rs_percentile": 75.0,
                    "setup_quality": 80.0,
                    "stage2_score": 85.0,
                },
                {
                    "symbol_id": "Z20",
                    "pattern_family": "flag",
                    "pattern_state": "confirmed",
                    "signal_date": "2024-12-31",
                    "breakout_volume_ratio": 1.1,
                    "volume_zscore_20": 2.6,
                    "volume_zscore_50": np.nan,
                    "rel_strength_score": 80.0,
                    "sector_rs_percentile": 75.0,
                    "setup_quality": 80.0,
                    "stage2_score": 85.0,
                },
                {
                    "symbol_id": "COMBINED",
                    "pattern_family": "flag",
                    "pattern_state": "confirmed",
                    "signal_date": "2024-12-31",
                    "breakout_volume_ratio": 1.6,
                    "volume_zscore_20": 2.6,
                    "volume_zscore_50": np.nan,
                    "rel_strength_score": 80.0,
                    "sector_rs_percentile": 75.0,
                    "setup_quality": 80.0,
                    "stage2_score": 85.0,
                },
            ]
        )
    )
    scores = scored.set_index("symbol_id")["pattern_score"].astype(float).to_dict()
    priorities = scored.set_index("symbol_id")["pattern_priority_score"].astype(float).to_dict()

    assert scores["COMBINED"] > scores["Z20"] > scores["RATIO"]
    assert priorities["COMBINED"] > priorities["Z20"] > priorities["RATIO"]
    assert scores["COMBINED"] - scores["RATIO"] <= 20.0


def test_build_pattern_signals_uses_parallel_helper_when_requested(monkeypatch) -> None:
    frame = pd.concat(
        [
            _cup_handle_frame(symbol_id="CUP"),
            _double_bottom_frame(symbol_id="DBLW", breakout=False),
        ],
        ignore_index=True,
    )
    called: dict[str, int] = {"parallel": 0}

    def fake_parallel(frame_arg, *, config, pattern_workers, progress_callback=None):
        called["parallel"] += 1
        assert pattern_workers == 4
        assert isinstance(config, PatternScanConfig)
        return (
            pd.DataFrame(
                [
                    {
                        "symbol_id": "CUP",
                        "pattern_family": "cup_handle",
                        "pattern_state": "confirmed",
                        "signal_date": "2024-12-31",
                        "pattern_score": 90.0,
                        "pattern_rank": 1,
                    }
                ]
            ),
            {},
            {},
        )

    monkeypatch.setattr("analytics.patterns.evaluation._scan_pattern_signals_parallel", fake_parallel)

    signals = build_pattern_signals(
        project_root=Path("."),
        signal_date="2024-12-31",
        exchange="NSE",
        data_domain="operational",
        config=_scan_config(),
        frame=frame,
        pattern_workers=4,
    )

    assert called["parallel"] == 1
    assert len(signals) == 1
    assert signals.iloc[0]["symbol_id"] == "CUP"


def test_build_pattern_signals_falls_back_to_serial_on_transient_parallel_resource_error(monkeypatch) -> None:
    frame = pd.concat(
        [
            _cup_handle_frame(symbol_id="CUP"),
            _double_bottom_frame(symbol_id="DBLW", breakout=False),
        ],
        ignore_index=True,
    )
    called: dict[str, int] = {"serial": 0}

    class _FailingPool:
        def __init__(self, *args, **kwargs):
            raise BlockingIOError(35, "Resource temporarily unavailable")

    def fake_serial(frame_arg, *, config, progress_callback=None):
        called["serial"] += 1
        return (
            pd.DataFrame(
                [
                    {
                        "symbol_id": "CUP",
                        "pattern_family": "cup_handle",
                        "pattern_state": "confirmed",
                        "signal_date": "2024-12-31",
                        "pattern_score": 90.0,
                        "pattern_rank": 1,
                    }
                ]
            ),
            {},
            {},
        )

    monkeypatch.setattr("analytics.patterns.evaluation.ProcessPoolExecutor", _FailingPool)
    monkeypatch.setattr("analytics.patterns.evaluation._scan_pattern_signals", fake_serial)

    signals = build_pattern_signals(
        project_root=Path("."),
        signal_date="2024-12-31",
        exchange="NSE",
        data_domain="operational",
        config=_scan_config(),
        frame=frame,
        pattern_workers=4,
    )

    assert called["serial"] == 1
    assert len(signals) == 1
    assert signals.iloc[0]["symbol_id"] == "CUP"


def test_build_pattern_signals_falls_back_to_serial_on_permission_error_from_process_pool(monkeypatch) -> None:
    frame = pd.concat(
        [
            _cup_handle_frame(symbol_id="CUP"),
            _double_bottom_frame(symbol_id="DBLW", breakout=False),
        ],
        ignore_index=True,
    )
    called: dict[str, int] = {"serial": 0}

    class _FailingPool:
        def __init__(self, *args, **kwargs):
            raise PermissionError(1, "Operation not permitted")

    def fake_serial(frame_arg, *, config, progress_callback=None):
        called["serial"] += 1
        return (
            pd.DataFrame(
                [
                    {
                        "symbol_id": "CUP",
                        "pattern_family": "cup_handle",
                        "pattern_state": "confirmed",
                        "signal_date": "2024-12-31",
                        "pattern_score": 90.0,
                        "pattern_rank": 1,
                    }
                ]
            ),
            {},
            {},
        )

    monkeypatch.setattr("analytics.patterns.evaluation.ProcessPoolExecutor", _FailingPool)
    monkeypatch.setattr("analytics.patterns.evaluation._scan_pattern_signals", fake_serial)

    signals = build_pattern_signals(
        project_root=Path("."),
        signal_date="2024-12-31",
        exchange="NSE",
        data_domain="operational",
        config=_scan_config(),
        frame=frame,
        pattern_workers=4,
    )

    assert called["serial"] == 1
    assert len(signals) == 1
    assert signals.iloc[0]["symbol_id"] == "CUP"


def test_load_pattern_frame_supports_operational_and_research_domains(monkeypatch, tmp_path: Path) -> None:
    raw = pd.DataFrame(
        {
            "symbol_id": ["AAA"] * 6,
            "exchange": ["NSE"] * 6,
            "timestamp": pd.date_range("2024-01-01", periods=6, freq="B"),
            "open": [10, 11, 12, 13, 14, 15],
            "high": [11, 12, 13, 14, 15, 16],
            "low": [9, 10, 11, 12, 13, 14],
            "close": [10, 11, 12, 13, 14, 15],
            "volume": [100, 110, 120, 130, 140, 150],
            "return_5d": [0.0] * 6,
            "return_10d": [0.0] * 6,
            "return_20d": [0.0] * 6,
            "return_40d": [0.0] * 6,
        }
    )

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(data_dir / "ohlcv.duckdb"))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )
        for row in raw[["symbol_id", "exchange", "timestamp", "open", "high", "low", "close", "volume"]].itertuples(index=False):
            conn.execute("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)", list(row))
    finally:
        conn.close()

    call_counter = {"count": 0}
    monkeypatch.setattr(
        "analytics.patterns.data.AlphaEngine.prepare_training_data",
        lambda self, **kwargs: call_counter.__setitem__("count", call_counter["count"] + 1) or raw.copy(),
    )

    operational = load_pattern_frame(
        tmp_path,
        from_date="2024-01-01",
        to_date="2024-01-31",
        data_domain="operational",
    )
    research = load_pattern_frame(
        tmp_path,
        from_date="2024-01-01",
        to_date="2024-01-31",
        data_domain="research",
    )

    for frame in (operational, research):
        assert {
            "symbol_id",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "volume_ratio_20",
            "volume_zscore_20",
            "volume_zscore_50",
            "above_sma200",
        }.issubset(frame.columns)
    assert call_counter["count"] == 1


def test_trade_simulation_covers_stop_target_and_timeout() -> None:
    config = PatternBacktestConfig(max_hold_bars=2, target_r_multiple=3.0, commission_rate=0.0)
    events = pd.DataFrame(
        [
            {
                "event_id": "STOP",
                "symbol_id": "STOP",
                "pattern_type": "cup_handle",
                "breakout_date": "2024-01-02",
                "breakout_bar_index": 1,
                "invalidation_price": 99.0,
            },
            {
                "event_id": "TARGET",
                "symbol_id": "TARGET",
                "pattern_type": "cup_handle",
                "breakout_date": "2024-01-02",
                "breakout_bar_index": 1,
                "invalidation_price": 99.0,
            },
            {
                "event_id": "TIMEOUT",
                "symbol_id": "TIMEOUT",
                "pattern_type": "round_bottom",
                "breakout_date": "2024-01-02",
                "breakout_bar_index": 1,
                "invalidation_price": 99.0,
            },
        ]
    )
    by_symbol = {
        "STOP": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=5, freq="B"),
                "open": [100.0, 101.0, 102.0, 101.0, 100.0],
                "high": [101.0, 102.0, 103.0, 102.0, 101.0],
                "low": [99.0, 100.0, 98.0, 99.0, 99.5],
                "close": [100.0, 101.0, 100.0, 100.0, 100.0],
            }
        ),
        "TARGET": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=5, freq="B"),
                "open": [100.0, 101.0, 102.0, 105.0, 106.0],
                "high": [101.0, 102.0, 112.0, 113.0, 114.0],
                "low": [99.0, 100.0, 101.0, 104.0, 105.0],
                "close": [100.0, 101.0, 111.0, 112.0, 113.0],
            }
        ),
        "TIMEOUT": pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=5, freq="B"),
                "open": [100.0, 101.0, 102.0, 102.5, 103.0],
                "high": [101.0, 102.0, 104.0, 104.5, 105.0],
                "low": [99.0, 100.0, 101.0, 101.5, 102.0],
                "close": [100.0, 101.0, 102.5, 103.0, 103.5],
            }
        ),
    }

    trades = simulate_pattern_trades(events, by_symbol=by_symbol, config=config)
    lookup = {row["event_id"]: row for _, row in trades.iterrows()}

    assert lookup["STOP"]["exit_reason"] == "stop"
    assert lookup["TARGET"]["exit_reason"] == "target"
    assert lookup["TIMEOUT"]["exit_reason"] == "timeout"


def test_run_pattern_backtest_writes_expected_artifacts(tmp_path: Path) -> None:
    research_frame = pd.concat(
        [
            _cup_handle_frame(symbol_id="CUP"),
            _round_bottom_frame(symbol_id="ROUND"),
        ],
        ignore_index=True,
    )
    bundle_dir = tmp_path / "reports" / "research" / "pattern_backtests" / "sample_bundle"
    result = run_pattern_backtest(
        project_root=tmp_path,
        from_date="2024-01-01",
        to_date="2024-12-31",
        config=_detector_config(),
        research_frame=research_frame,
        output_dir=bundle_dir,
    )

    assert Path(result["paths"]["pattern_events"]).exists()
    assert Path(result["paths"]["pattern_trades"]).exists()
    assert Path(result["paths"]["summary_csv"]).exists()
    assert Path(result["paths"]["summary_json"]).exists()

    payload = json.loads(Path(result["paths"]["summary_json"]).read_text(encoding="utf-8"))
    assert payload["summary_rows"]
    assert {"cup_handle", "round_bottom"}.issubset({row["pattern_type"] for row in payload["summary_rows"]})


def test_run_pattern_backtest_can_precompute_all_charts(tmp_path: Path) -> None:
    research_frame = pd.concat(
        [
            _cup_handle_frame(symbol_id="CUP"),
            _round_bottom_frame(symbol_id="ROUND"),
        ],
        ignore_index=True,
    )
    bundle_dir = tmp_path / "reports" / "research" / "pattern_backtests" / "all_charts_bundle"
    result = run_pattern_backtest(
        project_root=tmp_path,
        from_date="2024-01-01",
        to_date="2024-12-31",
        config=_detector_config(),
        research_frame=research_frame,
        output_dir=bundle_dir,
        precompute_all_charts=True,
    )

    event_count = len(result["events"])
    assert event_count >= 2
    assert len(result["paths"]["charts"]) == event_count
    for chart_path in result["paths"]["charts"]:
        assert Path(chart_path).exists()


def test_ensure_pattern_event_chart_generates_missing_chart_on_demand(tmp_path: Path) -> None:
    research_frame = _cup_handle_frame(symbol_id="CUP")
    bundle_dir = tmp_path / "reports" / "research" / "pattern_backtests" / "ondemand_bundle"
    result = run_pattern_backtest(
        project_root=tmp_path,
        from_date="2024-01-01",
        to_date="2024-12-31",
        config=_detector_config(),
        research_frame=research_frame,
        output_dir=bundle_dir,
        precompute_all_charts=False,
    )

    event_row = result["events"].iloc[0]
    expected_chart_path = bundle_dir / "charts" / f"{event_row['event_id']}.html"
    if expected_chart_path.exists():
        expected_chart_path.unlink()

    generated_chart = ensure_pattern_event_chart(
        project_root=tmp_path,
        bundle_dir=bundle_dir,
        event_row=event_row,
        config=_detector_config(),
        from_date="2024-01-01",
        to_date="2024-12-31",
        exchange="NSE",
        research_frame=research_frame,
    )

    assert Path(generated_chart).exists()
    assert Path(generated_chart).name == f"{event_row['event_id']}.html"


def test_cli_smoke_invokes_runner_and_prints_artifacts(monkeypatch, tmp_path: Path, capsys) -> None:
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    def _fake_run_pattern_backtest(**_: object) -> dict[str, object]:
        events_path = bundle_dir / "pattern_events.csv"
        trades_path = bundle_dir / "pattern_trades.csv"
        summary_csv = bundle_dir / "summary.csv"
        summary_json = bundle_dir / "summary.json"
        events_path.write_text("event_id\nE1\n", encoding="utf-8")
        trades_path.write_text("event_id\nE1\n", encoding="utf-8")
        summary_csv.write_text("pattern_type\ncup_handle\n", encoding="utf-8")
        summary_json.write_text("{}", encoding="utf-8")
        return {
            "paths": {
                "bundle_dir": str(bundle_dir),
                "pattern_events": str(events_path),
                "pattern_trades": str(trades_path),
                "summary_csv": str(summary_csv),
                "summary_json": str(summary_json),
            }
        }

    monkeypatch.setattr(backtest_patterns, "run_pattern_backtest", _fake_run_pattern_backtest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_patterns.py",
            "--project-root",
            str(tmp_path),
            "--output-dir",
            str(bundle_dir),
            "--from-date",
            "2024-01-01",
            "--to-date",
            "2024-12-31",
        ],
    )

    backtest_patterns.main()
    out = capsys.readouterr().out
    assert "Pattern events:" in out
    assert "Summary JSON:" in out


def _ascending_triangle_frame(*, symbol_id: str = "TRI", breakout: bool = True) -> pd.DataFrame:
    close = list(range(80, 100))
    close += [100, 102, 104, 105, 104, 103, 102, 101, 100, 100]
    close += list(range(100, 106))
    close += [103, 102, 101, 100, 99]
    close += [105, 106, 105, 104, 103, 102]
    close += [108, 110, 112]
    if breakout:
        close += [115, 118, 120]
        breakout_idx = len(close) - 2
        return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=breakout_idx, breakout_volume_ratio=2.0)
    close += [116]
    return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=None)


def _vcp_frame(*, symbol_id: str = "VCP", breakout: bool = True) -> pd.DataFrame:
    close = list(range(100, 120))
    close += [125, 128, 130, 132, 130, 128, 125]
    close += [122, 120, 118, 116, 114, 112]
    close += [108, 106, 104, 102, 100, 98]
    close += [103, 106, 110]
    if breakout:
        close += [115, 118]
        breakout_idx = len(close) - 2
        frame = _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=breakout_idx, breakout_volume_ratio=2.0)
    else:
        close += [112]
        frame = _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=None)
    frame.loc[:, "volume"] = [1000.0 * (1.0 - i * 0.015) for i in range(len(frame))]
    return frame


def _flat_base_frame(*, symbol_id: str = "FLAT", breakout: bool = True) -> pd.DataFrame:
    close = list(range(100, 120))
    close += [130, 131, 130, 131, 130, 131, 130, 131]
    close += [130, 131, 130, 131, 130, 131, 130]
    close += [131, 130, 131, 130, 131, 130]
    if breakout:
        close += [135, 138]
        breakout_idx = len(close) - 2
        return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=breakout_idx, breakout_volume_ratio=2.0)
    close += [134]
    return _make_price_frame(close, symbol_id=symbol_id, breakout_volume_idx=None)


def test_detect_ascending_triangle_positive_case() -> None:
    frame = _cup_handle_frame()
    config = _scan_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    signals, stats = detect_ascending_triangle_signals(
        frame, smoothed=smoothed, extrema=extrema, config=config
    )

    assert stats.candidate_count >= 0
    assert stats.confirmed_count >= 0
    for sig in signals:
        assert sig.pattern_family == "ascending_triangle"
        assert sig.breakout_level > 0


def test_detect_vcp_positive_case() -> None:
    frame = _cup_handle_frame()
    config = _scan_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    signals, stats = detect_vcp_signals(
        frame, smoothed=smoothed, extrema=extrema, config=config
    )

    assert stats.candidate_count >= 0
    for sig in signals:
        assert sig.pattern_family == "vcp"


def test_detect_flat_base_positive_case() -> None:
    frame = _cup_handle_frame()
    config = _scan_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    signals, stats = detect_flat_base_signals(
        frame, smoothed=smoothed, extrema=extrema, config=config
    )

    assert stats.candidate_count >= 0
    for sig in signals:
        assert sig.pattern_family == "flat_base"


def test_ascending_triangle_stage2_gate() -> None:
    frame = _ascending_triangle_frame()
    frame["stage2_score"] = 40.0
    config = _scan_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    signals, stats = detect_ascending_triangle_signals(
        frame, smoothed=smoothed, extrema=extrema, config=config
    )

    assert stats.confirmed_count == 0


def test_vcp_stage2_bonus() -> None:
    frame = _vcp_frame()
    frame["stage2_score"] = 85.0
    config = _scan_config()
    smoothed = kernel_smooth(frame["close"], bandwidth=config.bandwidth)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)

    signals, stats = detect_vcp_signals(
        frame, smoothed=smoothed, extrema=extrema, config=config
    )

    if stats.confirmed_count > 0:
        signal = [s for s in signals if s.pattern_state == "confirmed"][0]
        assert signal.setup_quality >= 60
