"""Rule-based pattern detectors for research backtests and live scans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from analytics.patterns.contracts import (
    PatternBacktestConfig,
    PatternEvent,
    PatternScanConfig,
    PatternSignal,
)
from analytics.patterns.signal import LocalExtrema


@dataclass(frozen=True)
class PatternScanStats:
    pattern_type: str
    candidate_count: int
    confirmed_count: int
    watchlist_count: int = 0


def _rounded(value: float | None) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 6)


def _calc_trough_dwell(smoothed: pd.Series, trough_idx: int, near_pct: float) -> int:
    trough = float(smoothed.iloc[trough_idx])
    threshold = trough * (1 + near_pct)
    return int((smoothed <= threshold).sum())


def _config_provenance(config: PatternBacktestConfig | PatternScanConfig) -> dict[str, object]:
    return {
        "bandwidth": config.bandwidth,
        "extrema_prominence": config.extrema_prominence,
        "breakout_volume_ratio_min": config.breakout_volume_ratio_min,
    }


def _scan_config_from_backtest(config: PatternBacktestConfig) -> PatternScanConfig:
    return PatternScanConfig(
        exchange=config.exchange,
        data_domain=config.data_domain,
        symbols=config.symbols,
        bandwidth=config.bandwidth,
        extrema_prominence=config.extrema_prominence,
        min_history_bars=config.min_history_bars,
        breakout_volume_ratio_min=config.breakout_volume_ratio_min,
        max_breakout_wait_bars=config.max_breakout_wait_bars,
        prior_uptrend_lookback=config.prior_uptrend_lookback,
        prior_uptrend_min_pct=config.prior_uptrend_min_pct,
        cup_depth_min=config.cup_depth_min,
        cup_depth_max=config.cup_depth_max,
        min_cup_width=config.min_cup_width,
        max_cup_width=config.max_cup_width,
        rim_tolerance_pct=config.rim_tolerance_pct,
        handle_min_bars=config.handle_min_bars,
        handle_max_bars=config.handle_max_bars,
        handle_max_depth_pct=config.handle_max_depth_pct,
        min_round_width=config.min_round_width,
        max_round_width=config.max_round_width,
        round_symmetry_min=config.round_symmetry_min,
        round_symmetry_max=config.round_symmetry_max,
        trough_near_pct=config.trough_near_pct,
        min_trough_dwell_bars=config.min_trough_dwell_bars,
        fallback_atr_stop_mult=config.fallback_atr_stop_mult,
    )


def _recent_enough(index: int, frame: pd.DataFrame, config: PatternScanConfig) -> bool:
    return index >= max(0, len(frame) - 1 - int(config.recent_signal_max_age_bars))


def _prior_uptrend_ok(frame: pd.DataFrame, *, left_idx: int, left_price: float, config: PatternBacktestConfig | PatternScanConfig) -> bool:
    prior_start = max(0, left_idx - config.prior_uptrend_lookback)
    prior_low = (
        float(frame.iloc[prior_start:left_idx]["close"].min())
        if left_idx > prior_start
        else float(frame.iloc[left_idx]["close"])
    )
    return left_price >= prior_low * (1 + config.prior_uptrend_min_pct)


def _find_breakout_confirmation(
    frame: pd.DataFrame,
    *,
    start_idx: int,
    resistance_level: float,
    config: PatternBacktestConfig | PatternScanConfig,
) -> tuple[int | None, float | None]:
    max_idx = min(len(frame) - 1, start_idx + config.max_breakout_wait_bars)
    for idx in range(start_idx + 1, max_idx + 1):
        close = float(frame.iloc[idx]["close"])
        volume_ratio = float(frame.iloc[idx].get("volume_ratio_20", 0.0) or 0.0)
        if close > resistance_level and volume_ratio >= config.breakout_volume_ratio_min:
            return idx, volume_ratio
    return None, None


def _latest_watchlist_volume(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    return float(frame.iloc[-1].get("volume_ratio_20", 0.0) or 0.0)


def _build_signal(
    *,
    frame: pd.DataFrame,
    pattern_family: str,
    pattern_state: str,
    signal_idx: int,
    pattern_start_idx: int,
    pattern_end_idx: int,
    breakout_level: float,
    invalidation_price: float,
    setup_quality: float,
    pivot_labels: tuple[str, ...],
    pivot_indices: tuple[int, ...],
    pivot_prices: tuple[float, ...],
    config: PatternScanConfig,
    volume_dry_up: bool = False,
    breakout_volume_ratio: float | None = None,
    width_bars: int | None = None,
    cup_depth_pct: float | None = None,
    handle_depth_pct: float | None = None,
    symmetry_ratio: float | None = None,
    trough_similarity_pct: float | None = None,
    pole_rise_pct: float | None = None,
    flag_tightness_pct: float | None = None,
    flag_retracement_pct: float | None = None,
) -> PatternSignal:
    timestamps = frame["timestamp"]
    signal_date = timestamps.iloc[signal_idx].date().isoformat()
    symbol = str(frame.iloc[0]["symbol_id"])
    watchlist_trigger_level = float(breakout_level)
    volume_ratio = (
        float(breakout_volume_ratio)
        if breakout_volume_ratio is not None
        else float(frame.iloc[signal_idx].get("volume_ratio_20", 0.0) or 0.0)
    )
    return PatternSignal(
        signal_id=f"{symbol}-{pattern_family}-{pattern_state}-{signal_date}",
        symbol_id=symbol,
        pattern_family=pattern_family,
        pattern_state=pattern_state,
        signal_direction="bullish",
        pattern_start=timestamps.iloc[pattern_start_idx].date().isoformat(),
        pattern_end=timestamps.iloc[pattern_end_idx].date().isoformat(),
        signal_date=signal_date,
        pattern_start_index=int(pattern_start_idx),
        pattern_end_index=int(pattern_end_idx),
        signal_bar_index=int(signal_idx),
        breakout_level=float(_rounded(breakout_level) or breakout_level),
        watchlist_trigger_level=float(_rounded(watchlist_trigger_level) or watchlist_trigger_level),
        invalidation_price=float(_rounded(invalidation_price) or invalidation_price),
        setup_quality=float(round(setup_quality, 6)),
        pivot_labels=pivot_labels,
        pivot_dates=tuple(timestamps.iloc[idx].date().isoformat() for idx in pivot_indices),
        pivot_prices=tuple(float(_rounded(price) or price) for price in pivot_prices),
        pivot_indices=tuple(int(idx) for idx in pivot_indices),
        volume_ratio_20=float(round(volume_ratio, 6)),
        breakout_volume_ratio=_rounded(breakout_volume_ratio) if breakout_volume_ratio is not None else None,
        width_bars=int(width_bars) if width_bars is not None else None,
        volume_dry_up=bool(volume_dry_up),
        cup_depth_pct=_rounded(cup_depth_pct),
        handle_depth_pct=_rounded(handle_depth_pct),
        symmetry_ratio=_rounded(symmetry_ratio),
        trough_similarity_pct=_rounded(trough_similarity_pct),
        pole_rise_pct=_rounded(pole_rise_pct),
        flag_tightness_pct=_rounded(flag_tightness_pct),
        flag_retracement_pct=_rounded(flag_retracement_pct),
        config_provenance=_config_provenance(config),
    )


def _signal_to_event(signal: PatternSignal) -> PatternEvent:
    pivot_dates = tuple(str(value) for value in signal.pivot_dates)
    pivot_prices = tuple(float(value) for value in signal.pivot_prices)
    pivot_indices = tuple(int(value) for value in signal.pivot_indices)
    labels = tuple(str(value) for value in signal.pivot_labels)
    trough_date = pivot_dates[1] if len(pivot_dates) > 1 else signal.pattern_start
    right_pivot_date = pivot_dates[2] if len(pivot_dates) > 2 else signal.pattern_end
    handle_date = pivot_dates[3] if len(pivot_dates) > 3 else None
    return PatternEvent(
        event_id=signal.signal_id.replace("-confirmed-", "-"),
        symbol_id=signal.symbol_id,
        pattern_type=signal.pattern_family,
        pattern_start=signal.pattern_start,
        pattern_end=signal.pattern_end,
        breakout_date=signal.signal_date,
        pattern_start_index=signal.pattern_start_index,
        pattern_end_index=signal.pattern_end_index,
        breakout_bar_index=signal.signal_bar_index,
        breakout_level=signal.breakout_level,
        invalidation_price=signal.invalidation_price,
        left_pivot_date=pivot_dates[0] if pivot_dates else signal.pattern_start,
        trough_date=trough_date,
        right_pivot_date=right_pivot_date,
        handle_date=handle_date,
        pivot_labels=labels,
        pivot_indices=pivot_indices,
        pivot_dates=pivot_dates,
        pivot_prices=pivot_prices,
        cup_depth_pct=float(signal.cup_depth_pct or 0.0),
        width_bars=int(signal.width_bars or 0),
        handle_depth_pct=signal.handle_depth_pct,
        handle_bars=(pivot_indices[3] - pivot_indices[2]) if len(pivot_indices) > 3 else None,
        symmetry_ratio=signal.symmetry_ratio,
        trough_dwell_bars=0,
        volume_dry_up=bool(signal.volume_dry_up),
        breakout_volume_confirmed=signal.pattern_state == "confirmed",
        breakout_volume_ratio=signal.breakout_volume_ratio,
        config_provenance=dict(signal.config_provenance),
    )


def _score_signal_rows(signals_df: pd.DataFrame) -> pd.DataFrame:
    if signals_df.empty:
        return signals_df
    scored = signals_df.copy()
    scored["pattern_score"] = 0.0

    state_bonus = np.where(scored["pattern_state"].astype(str) == "confirmed", 40.0, 20.0)
    scored["pattern_score"] += state_bonus

    breakout_volume_ratio = pd.to_numeric(scored.get("breakout_volume_ratio"), errors="coerce")
    scored["pattern_score"] += np.select(
        [breakout_volume_ratio >= 2.0, breakout_volume_ratio >= 1.5],
        [15.0, 10.0],
        default=0.0,
    )

    rel_strength = pd.to_numeric(scored.get("rel_strength_score"), errors="coerce")
    scored["pattern_score"] += np.select(
        [rel_strength >= 80.0, rel_strength >= 60.0],
        [15.0, 8.0],
        default=0.0,
    )

    sector_pct = pd.to_numeric(scored.get("sector_rs_percentile"), errors="coerce")
    scored["pattern_score"] += np.select(
        [sector_pct >= 70.0, sector_pct >= 60.0],
        [10.0, 5.0],
        default=0.0,
    )
    scored["pattern_score"] += np.where(scored.get("volume_dry_up", False).fillna(False).astype(bool), 10.0, 0.0)

    family_bonus = np.zeros(len(scored), dtype=float)
    family = scored["pattern_family"].astype(str)
    family_bonus += np.where(
        (family == "cup_handle") & (pd.to_numeric(scored.get("handle_depth_pct"), errors="coerce") <= 8.0),
        10.0,
        0.0,
    )
    symmetry = pd.to_numeric(scored.get("symmetry_ratio"), errors="coerce")
    family_bonus += np.where(
        (family == "round_bottom") & symmetry.between(0.75, 1.35, inclusive="both"),
        10.0,
        0.0,
    )
    trough_similarity = pd.to_numeric(scored.get("trough_similarity_pct"), errors="coerce")
    family_bonus += np.where(
        (family == "double_bottom") & (trough_similarity <= 3.0),
        10.0,
        0.0,
    )
    flag_retracement = pd.to_numeric(scored.get("flag_retracement_pct"), errors="coerce")
    family_bonus += np.where(
        (family == "flag") & (flag_retracement <= 25.0),
        10.0,
        0.0,
    )
    pole_rise = pd.to_numeric(scored.get("pole_rise_pct"), errors="coerce")
    flag_tightness = pd.to_numeric(scored.get("flag_tightness_pct"), errors="coerce")
    family_bonus += np.where(
        (family == "high_tight_flag") & (pole_rise >= 90.0) & (flag_tightness <= 15.0),
        10.0,
        0.0,
    )
    scored["pattern_score"] += family_bonus
    scored["pattern_score"] = scored["pattern_score"].clip(upper=100.0)
    scored = scored.sort_values(
        ["pattern_score", "setup_quality", "symbol_id"],
        ascending=[False, False, True],
        na_position="last",
    ).reset_index(drop=True)
    scored["pattern_rank"] = np.arange(1, len(scored) + 1)
    return scored


def detect_cup_handle_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    symbol = str(frame.iloc[0]["symbol_id"])
    extrema_list = list(extrema)
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    watchlist = 0
    used_signal_ids: set[str] = set()

    for idx in range(len(extrema_list) - 3):
        left, trough, right, handle = extrema_list[idx : idx + 4]
        if [left.kind, trough.kind, right.kind, handle.kind] != ["peak", "trough", "peak", "trough"]:
            continue
        left_idx, trough_idx, right_idx, handle_idx = left.index, trough.index, right.index, handle.index
        if not (left_idx < trough_idx < right_idx < handle_idx):
            continue
        width = right_idx - left_idx
        handle_bars = handle_idx - right_idx
        if width < config.min_cup_width or width > config.max_cup_width:
            continue
        if handle_bars < config.handle_min_bars or handle_bars > config.handle_max_bars:
            continue

        left_price = float(smoothed.iloc[left_idx])
        trough_price = float(smoothed.iloc[trough_idx])
        right_price = float(smoothed.iloc[right_idx])
        handle_price = float(smoothed.iloc[handle_idx])
        rim_avg = (left_price + right_price) / 2.0
        cup_depth = (rim_avg - trough_price) / max(rim_avg, 1e-9)
        if cup_depth < config.cup_depth_min or cup_depth > config.cup_depth_max:
            continue
        if abs(left_price - right_price) / max(left_price, right_price, 1e-9) > config.rim_tolerance_pct:
            continue
        if not _prior_uptrend_ok(frame, left_idx=left_idx, left_price=left_price, config=config):
            continue

        cup_mid = trough_price + (rim_avg - trough_price) / 2.0
        handle_depth = (right_price - handle_price) / max(right_price, 1e-9)
        if handle_price < cup_mid or handle_depth <= 0 or handle_depth > config.handle_max_depth_pct:
            continue

        trough_dwell = _calc_trough_dwell(
            smoothed.iloc[left_idx : right_idx + 1],
            trough_idx - left_idx,
            config.trough_near_pct,
        )
        if trough_dwell < config.min_trough_dwell_bars:
            continue

        resistance_level = float(frame.iloc[left_idx : right_idx + 1]["high"].max())
        breakout_idx, breakout_volume_ratio = _find_breakout_confirmation(
            frame,
            start_idx=handle_idx,
            resistance_level=resistance_level,
            config=config,
        )
        candidates += 1
        bottom_window = frame.iloc[max(0, trough_idx - 2) : min(len(frame), trough_idx + 3)]
        volume_dry_up = bool(bottom_window["volume_ratio_20"].mean() < 1.0)
        invalidation_price = float(frame.iloc[right_idx : handle_idx + 1]["low"].min())
        setup_quality = (
            45.0
            + max(0.0, 15.0 - abs(left_price - right_price) / max(rim_avg, 1e-9) * 100.0)
            + max(0.0, 18.0 - abs(handle_depth * 100.0))
            + (8.0 if volume_dry_up else 0.0)
        )
        if breakout_idx is not None and (not recent_only or _recent_enough(breakout_idx, frame, config)):
            signal = _build_signal(
                frame=frame,
                pattern_family="cup_handle",
                pattern_state="confirmed",
                signal_idx=breakout_idx,
                pattern_start_idx=left_idx,
                pattern_end_idx=handle_idx,
                breakout_level=resistance_level,
                invalidation_price=invalidation_price,
                setup_quality=setup_quality,
                pivot_labels=("left_rim", "trough", "right_rim", "handle_low"),
                pivot_indices=(left_idx, trough_idx, right_idx, handle_idx),
                pivot_prices=(left_price, trough_price, right_price, handle_price),
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=breakout_volume_ratio,
                width_bars=width,
                cup_depth_pct=cup_depth * 100.0,
                handle_depth_pct=handle_depth * 100.0,
                symmetry_ratio=((trough_idx - left_idx) / max(right_idx - trough_idx, 1)),
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
            continue

        latest_close = float(frame.iloc[-1]["close"])
        latest_low_since_handle = float(frame.iloc[handle_idx:]["low"].min())
        if (
            latest_close <= resistance_level
            and latest_close >= resistance_level * (1 - config.cup_watchlist_buffer_pct)
            and latest_low_since_handle > invalidation_price
        ):
            signal = _build_signal(
                frame=frame,
                pattern_family="cup_handle",
                pattern_state="watchlist",
                signal_idx=len(frame) - 1,
                pattern_start_idx=left_idx,
                pattern_end_idx=handle_idx,
                breakout_level=resistance_level,
                invalidation_price=invalidation_price,
                setup_quality=setup_quality - 10.0,
                pivot_labels=("left_rim", "trough", "right_rim", "handle_low"),
                pivot_indices=(left_idx, trough_idx, right_idx, handle_idx),
                pivot_prices=(left_price, trough_price, right_price, handle_price),
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=_latest_watchlist_volume(frame),
                width_bars=width,
                cup_depth_pct=cup_depth * 100.0,
                handle_depth_pct=handle_depth * 100.0,
                symmetry_ratio=((trough_idx - left_idx) / max(right_idx - trough_idx, 1)),
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                watchlist += 1

    return signals, PatternScanStats("cup_handle", candidates, confirmed, watchlist)


def detect_round_bottom_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    extrema_list = list(extrema)
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    watchlist = 0
    used_signal_ids: set[str] = set()
    for idx in range(len(extrema_list) - 1):
        left, trough = extrema_list[idx : idx + 2]
        if [left.kind, trough.kind] != ["peak", "trough"]:
            continue
        left_idx, trough_idx = left.index, trough.index
        if not (left_idx < trough_idx):
            continue
        left_price = float(smoothed.iloc[left_idx])
        trough_price = float(smoothed.iloc[trough_idx])
        recovery_threshold = left_price * (1 - config.rim_tolerance_pct)
        recovery_window = smoothed.iloc[trough_idx + 1 : min(len(smoothed), trough_idx + config.max_round_width + 1)]
        if recovery_window.empty:
            continue
        right_candidates = recovery_window[recovery_window >= recovery_threshold]
        if right_candidates.empty:
            continue
        right_idx = int(right_candidates.index[0])
        width = right_idx - left_idx
        if width < config.min_round_width or width > config.max_round_width:
            continue

        left_span = trough_idx - left_idx
        right_span = right_idx - trough_idx
        symmetry = left_span / max(right_span, 1)
        if symmetry < config.round_symmetry_min or symmetry > config.round_symmetry_max:
            continue

        right_price = float(smoothed.iloc[right_idx])
        rim_avg = (left_price + right_price) / 2.0
        depth = (rim_avg - trough_price) / max(rim_avg, 1e-9)
        if depth < config.cup_depth_min or depth > config.cup_depth_max:
            continue
        trough_dwell = int(
            (smoothed.iloc[left_idx : right_idx + 1] <= trough_price * (1 + config.trough_near_pct)).sum()
        )
        if trough_dwell < config.min_trough_dwell_bars:
            continue
        if not _prior_uptrend_ok(frame, left_idx=left_idx, left_price=left_price, config=config):
            continue

        resistance_level = float(frame.iloc[left_idx : right_idx + 1]["high"].max())
        breakout_idx, breakout_volume_ratio = _find_breakout_confirmation(
            frame,
            start_idx=right_idx,
            resistance_level=resistance_level,
            config=config,
        )
        candidates += 1
        post_right = frame.iloc[right_idx + 1 : breakout_idx] if breakout_idx is not None else frame.iloc[right_idx + 1 :]
        if not post_right.empty:
            invalidation_price = float(post_right["low"].min())
        else:
            atr_value = float(frame.iloc[min(right_idx, len(frame) - 1)].get("atr_value", 0.0) or 0.0)
            invalidation_price = resistance_level - (atr_value * config.fallback_atr_stop_mult)
        bottom_window = frame.iloc[max(0, trough_idx - 2) : min(len(frame), trough_idx + 3)]
        volume_dry_up = bool(bottom_window["volume_ratio_20"].mean() < 1.0)
        setup_quality = 48.0 + max(0.0, 14.0 - abs(1.0 - symmetry) * 20.0) + (8.0 if volume_dry_up else 0.0)

        if breakout_idx is not None and (not recent_only or _recent_enough(breakout_idx, frame, config)):
            signal = _build_signal(
                frame=frame,
                pattern_family="round_bottom",
                pattern_state="confirmed",
                signal_idx=breakout_idx,
                pattern_start_idx=left_idx,
                pattern_end_idx=right_idx,
                breakout_level=resistance_level,
                invalidation_price=invalidation_price,
                setup_quality=setup_quality,
                pivot_labels=("left_rim", "trough", "right_rim"),
                pivot_indices=(left_idx, trough_idx, right_idx),
                pivot_prices=(left_price, trough_price, right_price),
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=breakout_volume_ratio,
                width_bars=width,
                cup_depth_pct=depth * 100.0,
                symmetry_ratio=symmetry,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
            continue

        latest_close = float(frame.iloc[-1]["close"])
        latest_low_since_right = float(frame.iloc[right_idx:]["low"].min())
        if (
            latest_close <= resistance_level
            and latest_close >= resistance_level * (1 - config.round_watchlist_buffer_pct)
            and latest_low_since_right > invalidation_price
        ):
            signal = _build_signal(
                frame=frame,
                pattern_family="round_bottom",
                pattern_state="watchlist",
                signal_idx=len(frame) - 1,
                pattern_start_idx=left_idx,
                pattern_end_idx=right_idx,
                breakout_level=resistance_level,
                invalidation_price=invalidation_price,
                setup_quality=setup_quality - 10.0,
                pivot_labels=("left_rim", "trough", "right_rim"),
                pivot_indices=(left_idx, trough_idx, right_idx),
                pivot_prices=(left_price, trough_price, right_price),
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=_latest_watchlist_volume(frame),
                width_bars=width,
                cup_depth_pct=depth * 100.0,
                symmetry_ratio=symmetry,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                watchlist += 1

    return signals, PatternScanStats("round_bottom", candidates, confirmed, watchlist)


def detect_double_bottom_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    extrema_list = list(extrema)
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    watchlist = 0
    used_signal_ids: set[str] = set()
    for idx in range(len(extrema_list) - 2):
        if [item.kind for item in extrema_list[idx : idx + 3]] == ["trough", "peak", "trough"]:
            first, neckline_peak, second = extrema_list[idx : idx + 3]
        elif idx + 3 < len(extrema_list) and [item.kind for item in extrema_list[idx : idx + 4]] == ["peak", "trough", "peak", "trough"]:
            _, first, neckline_peak, second = extrema_list[idx : idx + 4]
        else:
            continue
        first_idx, peak_idx, second_idx = first.index, neckline_peak.index, second.index
        separation = second_idx - first_idx
        if not (first_idx < peak_idx < second_idx):
            continue
        if separation < config.double_bottom_min_separation or separation > config.double_bottom_max_separation:
            continue

        first_price = float(smoothed.iloc[first_idx])
        second_price = float(smoothed.iloc[second_idx])
        trough_avg = (first_price + second_price) / 2.0
        trough_spread = abs(first_price - second_price) / max(trough_avg, 1e-9)
        if trough_spread > config.double_bottom_trough_tolerance_pct:
            continue

        neckline = float(smoothed.iloc[peak_idx])
        neckline_gain = (neckline - trough_avg) / max(trough_avg, 1e-9)
        if neckline_gain < config.double_bottom_neckline_min_pct:
            continue
        if not _prior_uptrend_ok(frame, left_idx=max(0, first_idx - 1), left_price=neckline, config=config):
            continue

        breakout_level = float(frame.iloc[first_idx : second_idx + 1]["high"].max())
        breakout_idx, breakout_volume_ratio = _find_breakout_confirmation(
            frame,
            start_idx=second_idx,
            resistance_level=breakout_level,
            config=config,
        )
        candidates += 1
        invalidation_price = float(frame.iloc[first_idx : second_idx + 1]["low"].min())
        bottom_window = frame.iloc[max(0, second_idx - 2) : min(len(frame), second_idx + 3)]
        volume_dry_up = bool(bottom_window["volume_ratio_20"].mean() < 1.0)
        setup_quality = 50.0 + max(0.0, 15.0 - trough_spread * 100.0 * 2.0) + (8.0 if volume_dry_up else 0.0)

        if breakout_idx is not None and (not recent_only or _recent_enough(breakout_idx, frame, config)):
            signal = _build_signal(
                frame=frame,
                pattern_family="double_bottom",
                pattern_state="confirmed",
                signal_idx=breakout_idx,
                pattern_start_idx=first_idx,
                pattern_end_idx=second_idx,
                breakout_level=breakout_level,
                invalidation_price=invalidation_price,
                setup_quality=setup_quality,
                pivot_labels=("first_trough", "neckline_peak", "second_trough"),
                pivot_indices=(first_idx, peak_idx, second_idx),
                pivot_prices=(first_price, neckline, second_price),
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=breakout_volume_ratio,
                width_bars=separation,
                trough_similarity_pct=trough_spread * 100.0,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
            continue

        latest_close = float(frame.iloc[-1]["close"])
        if (
            latest_close <= breakout_level
            and latest_close >= breakout_level * (1 - config.round_watchlist_buffer_pct)
            and float(frame.iloc[second_idx:]["low"].min()) > invalidation_price
        ):
            signal = _build_signal(
                frame=frame,
                pattern_family="double_bottom",
                pattern_state="watchlist",
                signal_idx=len(frame) - 1,
                pattern_start_idx=first_idx,
                pattern_end_idx=second_idx,
                breakout_level=breakout_level,
                invalidation_price=invalidation_price,
                setup_quality=setup_quality - 10.0,
                pivot_labels=("first_trough", "neckline_peak", "second_trough"),
                pivot_indices=(first_idx, peak_idx, second_idx),
                pivot_prices=(first_price, neckline, second_price),
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=_latest_watchlist_volume(frame),
                width_bars=separation,
                trough_similarity_pct=trough_spread * 100.0,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                watchlist += 1
    return signals, PatternScanStats("double_bottom", candidates, confirmed, watchlist)


def _flag_candidate_from_window(
    frame: pd.DataFrame,
    *,
    pole_start_idx: int,
    pole_end_idx: int,
    flag_end_idx: int,
) -> tuple[float, float, float, float, float]:
    pole_start = float(frame.iloc[pole_start_idx]["close"])
    pole_end = float(frame.iloc[pole_end_idx]["close"])
    pole_rise_pct = (pole_end / max(pole_start, 1e-9) - 1.0) * 100.0
    flag_window = frame.iloc[pole_end_idx : flag_end_idx + 1]
    flag_high = float(flag_window["high"].max())
    flag_low = float(flag_window["low"].min())
    flag_range_pct = (flag_high / max(flag_low, 1e-9) - 1.0) * 100.0
    pole_height = pole_end - pole_start
    retracement_pct = ((pole_end - flag_low) / max(pole_height, 1e-9)) * 100.0 if pole_height > 0 else 999.0
    return pole_rise_pct, flag_high, flag_low, flag_range_pct, retracement_pct


def _flag_setup_quality(
    *,
    pole_rise_pct: float,
    retracement_pct: float,
    volume_dry_up: bool,
    high_tight: bool,
) -> float:
    score = 50.0 + min(20.0, pole_rise_pct / (3.0 if high_tight else 2.0))
    score += max(0.0, 20.0 - retracement_pct)
    if volume_dry_up:
        score += 8.0
    return float(score)


def detect_flag_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    high_tight_only: bool = False,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    pattern_family = "high_tight_flag" if high_tight_only else "flag"
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    watchlist = 0
    used_signal_ids: set[str] = set()
    max_pole_bars = config.high_tight_pole_max_bars if high_tight_only else config.flag_pole_max_bars
    min_pole_rise = config.high_tight_pole_min_pct * 100.0 if high_tight_only else config.flag_pole_min_pct * 100.0
    max_retracement_pct = (
        config.high_tight_flag_max_retracement_pct * 100.0
        if high_tight_only
        else config.flag_max_retracement_pct * 100.0
    )
    max_flag_range_pct = config.high_tight_flag_max_range_pct * 100.0 if high_tight_only else None

    for pole_end_idx in range(config.flag_pole_min_bars, len(frame) - config.flag_min_bars):
        for pole_bars in range(config.flag_pole_min_bars, max_pole_bars + 1):
            pole_start_idx = pole_end_idx - pole_bars
            if pole_start_idx < 0:
                continue
            for flag_bars in range(config.flag_min_bars, config.flag_max_bars + 1):
                flag_end_idx = pole_end_idx + flag_bars
                if flag_end_idx >= len(frame):
                    continue
                pole_rise_pct, flag_high, flag_low, flag_range_pct, retracement_pct = _flag_candidate_from_window(
                    frame,
                    pole_start_idx=pole_start_idx,
                    pole_end_idx=pole_end_idx,
                    flag_end_idx=flag_end_idx,
                )
                if pole_rise_pct < min_pole_rise:
                    continue
                if retracement_pct > max_retracement_pct:
                    continue
                if max_flag_range_pct is not None and flag_range_pct > max_flag_range_pct:
                    continue
                flag_window = smoothed.iloc[pole_end_idx : flag_end_idx + 1]
                if len(flag_window) < 2:
                    continue
                slope = float(flag_window.iloc[-1] - flag_window.iloc[0]) / max(len(flag_window) - 1, 1)
                if slope > 0.2:
                    continue

                breakout_volume_min = 2.0 if high_tight_only else config.breakout_volume_ratio_min
                breakout_idx = None
                breakout_volume_ratio = None
                max_idx = min(len(frame) - 1, flag_end_idx + config.max_breakout_wait_bars)
                for idx in range(flag_end_idx + 1, max_idx + 1):
                    close = float(frame.iloc[idx]["close"])
                    volume_ratio = float(frame.iloc[idx].get("volume_ratio_20", 0.0) or 0.0)
                    if close > flag_high and volume_ratio >= breakout_volume_min:
                        breakout_idx = idx
                        breakout_volume_ratio = volume_ratio
                        break
                candidates += 1
                invalidation_price = float(frame.iloc[pole_end_idx : flag_end_idx + 1]["low"].min())
                volume_dry_up = bool(frame.iloc[pole_end_idx : flag_end_idx + 1]["volume_ratio_20"].mean() < 1.0)
                setup_quality = _flag_setup_quality(
                    pole_rise_pct=pole_rise_pct,
                    retracement_pct=retracement_pct,
                    volume_dry_up=volume_dry_up,
                    high_tight=high_tight_only,
                )

                if breakout_idx is not None and (not recent_only or _recent_enough(breakout_idx, frame, config)):
                    signal = _build_signal(
                        frame=frame,
                        pattern_family=pattern_family,
                        pattern_state="confirmed",
                        signal_idx=breakout_idx,
                        pattern_start_idx=pole_start_idx,
                        pattern_end_idx=flag_end_idx,
                        breakout_level=flag_high,
                        invalidation_price=invalidation_price,
                        setup_quality=setup_quality,
                        pivot_labels=("pole_start", "pole_end", "flag_end"),
                        pivot_indices=(pole_start_idx, pole_end_idx, flag_end_idx),
                        pivot_prices=(
                            float(smoothed.iloc[pole_start_idx]),
                            float(smoothed.iloc[pole_end_idx]),
                            float(smoothed.iloc[flag_end_idx]),
                        ),
                        config=config,
                        volume_dry_up=volume_dry_up,
                        breakout_volume_ratio=breakout_volume_ratio,
                        width_bars=flag_end_idx - pole_start_idx,
                        pole_rise_pct=pole_rise_pct,
                        flag_tightness_pct=flag_range_pct,
                        flag_retracement_pct=retracement_pct,
                    )
                    if signal.signal_id not in used_signal_ids:
                        used_signal_ids.add(signal.signal_id)
                        signals.append(signal)
                        confirmed += 1
                    continue

                latest_close = float(frame.iloc[-1]["close"])
                if latest_close <= flag_high and latest_close >= flag_high * (1 - config.flag_watchlist_buffer_pct):
                    signal = _build_signal(
                        frame=frame,
                        pattern_family=pattern_family,
                        pattern_state="watchlist",
                        signal_idx=len(frame) - 1,
                        pattern_start_idx=pole_start_idx,
                        pattern_end_idx=flag_end_idx,
                        breakout_level=flag_high,
                        invalidation_price=invalidation_price,
                        setup_quality=setup_quality - 10.0,
                        pivot_labels=("pole_start", "pole_end", "flag_end"),
                        pivot_indices=(pole_start_idx, pole_end_idx, flag_end_idx),
                        pivot_prices=(
                            float(smoothed.iloc[pole_start_idx]),
                            float(smoothed.iloc[pole_end_idx]),
                            float(smoothed.iloc[flag_end_idx]),
                        ),
                        config=config,
                        volume_dry_up=volume_dry_up,
                        breakout_volume_ratio=_latest_watchlist_volume(frame),
                        width_bars=flag_end_idx - pole_start_idx,
                        pole_rise_pct=pole_rise_pct,
                        flag_tightness_pct=flag_range_pct,
                        flag_retracement_pct=retracement_pct,
                    )
                    if signal.signal_id not in used_signal_ids:
                        used_signal_ids.add(signal.signal_id)
                        signals.append(signal)
                        watchlist += 1

    return signals, PatternScanStats(pattern_family, candidates, confirmed, watchlist)


def detect_cup_handle_events(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternBacktestConfig,
) -> tuple[list[PatternEvent], PatternScanStats]:
    """Detect confirmed cup-and-handle breakouts for one symbol."""

    scan_config = _scan_config_from_backtest(config)
    signals, stats = detect_cup_handle_signals(
        frame,
        smoothed=smoothed,
        extrema=extrema,
        config=scan_config,
        recent_only=False,
    )
    events = [_signal_to_event(signal) for signal in signals if signal.pattern_state == "confirmed"]
    return events, PatternScanStats("cup_handle", stats.candidate_count, len(events), stats.watchlist_count)


def detect_round_bottom_events(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternBacktestConfig,
) -> tuple[list[PatternEvent], PatternScanStats]:
    """Detect confirmed round-bottom breakouts for one symbol."""

    scan_config = _scan_config_from_backtest(config)
    signals, stats = detect_round_bottom_signals(frame, smoothed=smoothed, extrema=extrema, config=scan_config, recent_only=False)
    events = [_signal_to_event(signal) for signal in signals if signal.pattern_state == "confirmed"]
    return events, PatternScanStats("round_bottom", stats.candidate_count, len(events), stats.watchlist_count)


def detect_pattern_signals_for_symbol(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
) -> tuple[pd.DataFrame, dict[str, PatternScanStats]]:
    """Return all bullish pattern signals for one ordered symbol frame."""

    detectors = [
        detect_cup_handle_signals,
        detect_round_bottom_signals,
        detect_double_bottom_signals,
        detect_flag_signals,
        lambda *args, **kwargs: detect_flag_signals(*args, **kwargs, high_tight_only=True),
    ]
    rows: list[dict[str, object]] = []
    stats: dict[str, PatternScanStats] = {}
    for detector in detectors:
        signals, detector_stats = detector(frame, smoothed=smoothed, extrema=extrema, config=config)
        stats[detector_stats.pattern_type] = detector_stats
        rows.extend(signal.to_record() for signal in signals)
    if not rows:
        return pd.DataFrame(), stats
    signals_df = pd.DataFrame(rows).drop_duplicates(subset=["signal_id"]).reset_index(drop=True)
    return _score_signal_rows(signals_df), stats
