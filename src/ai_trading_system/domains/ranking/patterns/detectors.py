"""Rule-based pattern detectors for research backtests and live scans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from ai_trading_system.analytics.patterns.contracts import (
    PatternBacktestConfig,
    PatternEvent,
    PatternScanConfig,
    PatternSignal,
)
from ai_trading_system.analytics.patterns.signal import LocalExtrema

TIER_1_PATTERNS = {
    "cup_handle",
    "flat_base",
    "vcp",
    "stage2_reclaim",
    "3wt",
    "ascending_triangle",
    "darvas_box",
    "pocket_pivot",
    "ascending_base",
    "ipo_base",
    "inside_week_breakout",
}
SUPPRESSION_ONLY_PATTERNS = {
    "head_shoulders",
}
VOLUME_Z20_CONFIRM_THRESHOLD = 2.0
VOLUME_Z50_CONFIRM_THRESHOLD = 2.0
VOLUME_Z20_STRONG_THRESHOLD = 3.0


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
        "volume_zscore_min": getattr(config, "volume_zscore_min", 2.0),
    }


def _scan_config_from_backtest(config: PatternBacktestConfig) -> PatternScanConfig:
    return PatternScanConfig(
        exchange=config.exchange,
        data_domain=config.data_domain,
        symbols=config.symbols,
        # Bug Fix 3: smoothing_method was missing — PatternBacktestConfig defaults
        # to 'kernel'; PatternScanConfig defaults to 'rolling'. Pass the backtest
        # value so callers that override it (e.g., research scripts) get the right
        # smoother in the derived scan config.
        smoothing_method=getattr(config, "smoothing_method", "rolling"),
        bandwidth=config.bandwidth,
        extrema_prominence=config.extrema_prominence,
        min_history_bars=config.min_history_bars,
        breakout_volume_ratio_min=config.breakout_volume_ratio_min,
        volume_zscore_min=config.volume_zscore_min,
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
        stage2_reclaim_lookback_bars=config.stage2_reclaim_lookback_bars,
        stage2_reclaim_max_extension_pct=config.stage2_reclaim_max_extension_pct,
        stage2_reclaim_min_slope_pct=config.stage2_reclaim_min_slope_pct,
        darvas_lookback_bars=config.darvas_lookback_bars,
        darvas_min_box_bars=config.darvas_min_box_bars,
        darvas_resistance_tolerance_pct=config.darvas_resistance_tolerance_pct,
        darvas_max_box_depth_pct=config.darvas_max_box_depth_pct,
        pocket_pivot_lookback_bars=config.pocket_pivot_lookback_bars,
        pocket_pivot_max_extension_pct=config.pocket_pivot_max_extension_pct,
        ascending_base_min_bars=config.ascending_base_min_bars,
        ascending_base_max_bars=config.ascending_base_max_bars,
        ascending_base_max_pullback_depth_pct=config.ascending_base_max_pullback_depth_pct,
        ascending_base_min_low_rise_pct=config.ascending_base_min_low_rise_pct,
        ipo_base_min_history_bars=config.ipo_base_min_history_bars,
        ipo_base_max_history_bars=config.ipo_base_max_history_bars,
        ipo_base_min_bars=config.ipo_base_min_bars,
        ipo_base_max_bars=config.ipo_base_max_bars,
        ipo_base_max_depth_pct=config.ipo_base_max_depth_pct,
        inside_week_lookback_weeks=config.inside_week_lookback_weeks,
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


def _coerce_optional_float(value: object) -> float | None:
    coerced = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(coerced):
        return None
    return float(coerced)


def _has_required_volume_ratio(frame: pd.DataFrame) -> bool:
    return "volume_ratio_20" in frame.columns


def _has_strict_volume_inputs(frame: pd.DataFrame) -> bool:
    return "volume_ratio_20" in frame.columns and (
        "volume_zscore_20" in frame.columns or "volume_zscore_50" in frame.columns
    )


def _volume_confirmation_details(
    row: pd.Series | dict[str, object],
    *,
    ratio_threshold: float,
    z_threshold: float = 2.0,
) -> dict[str, object]:
    volume_ratio = _coerce_optional_float(row.get("volume_ratio_20"))
    volume_zscore_20 = _coerce_optional_float(row.get("volume_zscore_20"))
    volume_zscore_50 = _coerce_optional_float(row.get("volume_zscore_50"))

    is_volume_ratio_confirmed = volume_ratio is not None and volume_ratio >= float(ratio_threshold)
    z_min = float(z_threshold)
    is_z20_confirmed = volume_zscore_20 is not None and volume_zscore_20 >= z_min
    is_z50_confirmed = volume_zscore_50 is not None and volume_zscore_50 >= z_min
    is_any_volume_confirmed = is_volume_ratio_confirmed and (is_z20_confirmed or is_z50_confirmed)
    is_strong_volume_confirmation = (
        (is_volume_ratio_confirmed and is_z20_confirmed)
        or (
            is_volume_ratio_confirmed
            and volume_zscore_20 is not None
            and volume_zscore_20 >= VOLUME_Z20_STRONG_THRESHOLD
        )
    )

    return {
        "volume_ratio_20": volume_ratio,
        "volume_zscore_20": volume_zscore_20,
        "volume_zscore_50": volume_zscore_50,
        "is_volume_ratio_missing": volume_ratio is None,
        "is_volume_zscore_missing": volume_zscore_20 is None and volume_zscore_50 is None,
        "is_volume_ratio_confirmed": is_volume_ratio_confirmed,
        "is_z20_confirmed": is_z20_confirmed,
        "is_z50_confirmed": is_z50_confirmed,
        "is_any_volume_confirmed": is_any_volume_confirmed,
        "is_strong_volume_confirmation": is_strong_volume_confirmation,
    }


def _find_breakout_confirmation(
    frame: pd.DataFrame,
    *,
    start_idx: int,
    resistance_level: float,
    config: PatternBacktestConfig | PatternScanConfig,
) -> tuple[int | None, dict[str, object] | None]:
    max_idx = min(len(frame) - 1, start_idx + config.max_breakout_wait_bars)
    for idx in range(start_idx + 1, max_idx + 1):
        close = float(frame.iloc[idx]["close"])
        confirmation = _volume_confirmation_details(
            frame.iloc[idx],
            ratio_threshold=float(config.breakout_volume_ratio_min),
            z_threshold=float(getattr(config, "volume_zscore_min", 2.0)),
        )
        if close > resistance_level and bool(confirmation["is_any_volume_confirmed"]):
            return idx, confirmation
    return None, None


def _latest_watchlist_volume(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    return float(frame.iloc[-1].get("volume_ratio_20", 0.0) or 0.0)


def _confirmed_volume_on_bar(
    frame: pd.DataFrame,
    idx: int,
    config: PatternBacktestConfig | PatternScanConfig,
) -> dict[str, object] | None:
    confirmation = _volume_confirmation_details(
        frame.iloc[idx],
        ratio_threshold=float(config.breakout_volume_ratio_min),
        z_threshold=float(getattr(config, "volume_zscore_min", 2.0)),
    )
    return confirmation if bool(confirmation["is_any_volume_confirmed"]) else None


def _positive_fallback_price(*values: float | None) -> float:
    for value in values:
        coerced = _coerce_optional_float(value)
        if coerced is not None and coerced > 0:
            return float(coerced)
    return 0.01


def _safe_invalidation_price(
    invalidation_price: float,
    *,
    breakout_level: float,
    frame: pd.DataFrame,
    pattern_start_idx: int,
    pattern_end_idx: int,
) -> float:
    raw = _coerce_optional_float(invalidation_price)
    if raw is not None and raw > 0:
        return raw
    window = frame.iloc[max(0, pattern_start_idx) : min(len(frame), pattern_end_idx + 1)]
    structural_low = None
    if "low" in window.columns and not window.empty:
        structural_low = _coerce_optional_float(pd.to_numeric(window["low"], errors="coerce").min())
    level = _coerce_optional_float(breakout_level)
    close = _coerce_optional_float(frame.iloc[min(pattern_end_idx, len(frame) - 1)].get("close"))
    fallback = _positive_fallback_price(structural_low, close, level)
    return max(0.01, fallback * 0.98)


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
        else _coerce_optional_float(frame.iloc[signal_idx].get("volume_ratio_20"))
    )
    volume_zscore_20 = _coerce_optional_float(frame.iloc[signal_idx].get("volume_zscore_20"))
    volume_zscore_50 = _coerce_optional_float(frame.iloc[signal_idx].get("volume_zscore_50"))
    safe_invalidation = _safe_invalidation_price(
        invalidation_price,
        breakout_level=breakout_level,
        frame=frame,
        pattern_start_idx=pattern_start_idx,
        pattern_end_idx=pattern_end_idx,
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
        invalidation_price=float(_rounded(safe_invalidation) or safe_invalidation),
        setup_quality=float(round(setup_quality, 6)),
        pivot_labels=pivot_labels,
        pivot_dates=tuple(timestamps.iloc[idx].date().isoformat() for idx in pivot_indices),
        pivot_prices=tuple(float(_rounded(price) or price) for price in pivot_prices),
        pivot_indices=tuple(int(idx) for idx in pivot_indices),
        volume_ratio_20=_rounded(volume_ratio),
        volume_zscore_20=_rounded(volume_zscore_20),
        volume_zscore_50=_rounded(volume_zscore_50),
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


def _operational_tier_for_family(family: str) -> str:
    normalized = str(family or "").strip().lower()
    if normalized in TIER_1_PATTERNS:
        return "tier_1"
    if normalized in SUPPRESSION_ONLY_PATTERNS:
        return "suppression_only"
    return "tier_2"


def _score_signal_rows(signals_df: pd.DataFrame) -> pd.DataFrame:
    if signals_df.empty:
        return signals_df
    scored = signals_df.copy()

    def _series_or_default(column: str, default: float | bool = float("nan")) -> pd.Series:
        value = scored.get(column)
        if value is None:
            return pd.Series(default, index=scored.index)
        return pd.Series(value, index=scored.index)

    state_bonus = np.where(scored["pattern_state"].astype(str) == "confirmed", 40.0, 20.0)
    pattern_score = pd.Series(state_bonus, index=scored.index, dtype=float)
    family = scored["pattern_family"].astype(str)
    scored.loc[:, "pattern_operational_tier"] = family.map(_operational_tier_for_family)

    breakout_volume_ratio = pd.to_numeric(_series_or_default("breakout_volume_ratio"), errors="coerce")
    volume_zscore_20 = pd.to_numeric(_series_or_default("volume_zscore_20"), errors="coerce")
    volume_zscore_50 = pd.to_numeric(_series_or_default("volume_zscore_50"), errors="coerce")
    is_volume_ratio_confirmed = breakout_volume_ratio >= 1.5
    is_z20_confirmed = volume_zscore_20 >= VOLUME_Z20_CONFIRM_THRESHOLD
    is_z50_confirmed = volume_zscore_50 >= VOLUME_Z50_CONFIRM_THRESHOLD
    is_combined_volume_confirmation = is_volume_ratio_confirmed & (is_z20_confirmed | is_z50_confirmed)
    is_strong_volume_confirmation = (
        is_combined_volume_confirmation
        & (is_z20_confirmed | (volume_zscore_20 >= VOLUME_Z20_STRONG_THRESHOLD))
    )
    pattern_score = pattern_score + np.select(
        [
            is_strong_volume_confirmation,
            is_combined_volume_confirmation,
        ],
        [20.0, 18.0],
        default=0.0,
    )

    rel_strength = pd.to_numeric(_series_or_default("rel_strength_score"), errors="coerce")
    pattern_score = pattern_score + np.select(
        [rel_strength >= 80.0, rel_strength >= 60.0],
        [15.0, 8.0],
        default=0.0,
    )

    sector_pct = pd.to_numeric(_series_or_default("sector_rs_percentile"), errors="coerce")
    pattern_score = pattern_score + np.select(
        [sector_pct >= 70.0, sector_pct >= 60.0],
        [10.0, 5.0],
        default=0.0,
    )
    volume_dry_up = _series_or_default("volume_dry_up", False)
    if volume_dry_up.dtype == object:
        volume_dry_up = volume_dry_up.infer_objects(copy=False)
    volume_dry_up = volume_dry_up.where(volume_dry_up.notna(), False).astype(bool)
    pattern_score = pattern_score + np.where(volume_dry_up, 10.0, 0.0)

    family_bonus = np.zeros(len(scored), dtype=float)
    family_bonus += np.where(
        (family == "cup_handle") & (pd.to_numeric(_series_or_default("handle_depth_pct"), errors="coerce") <= 8.0),
        10.0,
        0.0,
    )
    symmetry = pd.to_numeric(_series_or_default("symmetry_ratio"), errors="coerce")
    family_bonus += np.where(
        (family == "round_bottom") & symmetry.between(0.75, 1.35, inclusive="both"),
        10.0,
        0.0,
    )
    trough_similarity = pd.to_numeric(_series_or_default("trough_similarity_pct"), errors="coerce")
    family_bonus += np.where(
        (family == "double_bottom") & (trough_similarity <= 3.0),
        10.0,
        0.0,
    )
    flag_retracement = pd.to_numeric(_series_or_default("flag_retracement_pct"), errors="coerce")
    family_bonus += np.where(
        (family == "flag") & (flag_retracement <= 25.0),
        10.0,
        0.0,
    )
    pole_rise = pd.to_numeric(_series_or_default("pole_rise_pct"), errors="coerce")
    flag_tightness = pd.to_numeric(_series_or_default("flag_tightness_pct"), errors="coerce")
    family_bonus += np.where(
        (family == "high_tight_flag") & (pole_rise >= 90.0) & (flag_tightness <= 15.0),
        10.0,
        0.0,
    )
    pattern_score = pattern_score + family_bonus

    # ── Stage 2 bonus (Sprint 2) — rewards patterns on Stage 2 trend stocks ──
    # +15 pts for strong_stage2 (≥85), +10 for stage2 (≥70), +5 for transitional (≥50)
    if "stage2_score" in scored.columns:
        s2 = pd.to_numeric(scored["stage2_score"], errors="coerce").fillna(0.0)
        pattern_score = pattern_score + np.select(
            [s2 >= 85.0, s2 >= 70.0, s2 >= 50.0],
            [15.0, 10.0, 5.0],
            default=0.0,
        )
        scored.loc[:, "stage2_label"] = scored.get("stage2_label", pd.Series("non_stage2", index=scored.index))

    if "setup_quality" not in scored.columns:
        scored.loc[:, "setup_quality"] = np.nan
    scored.loc[:, "pattern_score"] = pattern_score.clip(upper=100.0)

    operational_tier = scored["pattern_operational_tier"].astype(str)
    tier_bonus = np.select(
        [
            operational_tier == "tier_1",
            operational_tier == "tier_2",
        ],
        [22.0, 12.0],
        default=0.0,
    )
    stage2_priority_bonus = np.zeros(len(scored), dtype=float)
    if "stage2_score" in scored.columns:
        s2_priority = pd.to_numeric(scored["stage2_score"], errors="coerce").fillna(0.0)
        stage2_priority_bonus = np.select(
            [s2_priority >= 85.0, s2_priority >= 70.0, s2_priority >= 50.0],
            [14.0, 9.0, 4.0],
            default=0.0,
        )
    rs_priority_bonus = np.select(
        [rel_strength >= 80.0, rel_strength >= 60.0],
        [10.0, 5.0],
        default=0.0,
    )
    sector_priority_bonus = np.select(
        [sector_pct >= 70.0, sector_pct >= 60.0],
        [6.0, 3.0],
        default=0.0,
    )
    confirmed_state = scored["pattern_state"].astype(str) == "confirmed"
    breakout_priority_bonus = np.select(
        [
            confirmed_state & is_strong_volume_confirmation,
            confirmed_state & is_combined_volume_confirmation,
            confirmed_state,
            is_combined_volume_confirmation,
        ],
        [10.0, 8.0, 4.0, 4.0],
        default=0.0,
    )
    setup_quality = pd.to_numeric(_series_or_default("setup_quality"), errors="coerce").fillna(0.0)
    setup_priority_component = np.clip(setup_quality * 0.12, 0.0, 12.0)

    clarity_bonus = np.zeros(len(scored), dtype=float)
    clarity_bonus += np.where(
        (family == "cup_handle") & (pd.to_numeric(_series_or_default("handle_depth_pct"), errors="coerce") <= 8.0),
        8.0,
        0.0,
    )
    clarity_bonus += np.where(
        (family == "round_bottom") & symmetry.between(0.85, 1.15, inclusive="both"),
        8.0,
        np.where(
            (family == "round_bottom") & symmetry.between(0.75, 1.35, inclusive="both"),
            4.0,
            0.0,
        ),
    )
    clarity_bonus += np.where(
        (family == "double_bottom") & (trough_similarity <= 3.0),
        8.0,
        np.where(
            (family == "double_bottom") & (trough_similarity <= 5.0),
            4.0,
            0.0,
        ),
    )
    clarity_bonus += np.where(
        (family == "flag") & (flag_retracement <= 25.0),
        8.0,
        np.where(
            (family == "flag") & (flag_retracement <= 35.0),
            4.0,
            0.0,
        ),
    )
    clarity_bonus += np.where(
        (family == "high_tight_flag") & (pole_rise >= 90.0) & (flag_tightness <= 15.0),
        10.0,
        np.where(
            (family == "high_tight_flag") & (pole_rise >= 75.0) & (flag_tightness <= 20.0),
            5.0,
            0.0,
        ),
    )
    priority_score = (
        pd.to_numeric(scored["pattern_score"], errors="coerce").fillna(0.0) * 0.35
        + tier_bonus
        + stage2_priority_bonus
        + rs_priority_bonus
        + sector_priority_bonus
        + breakout_priority_bonus
        + setup_priority_component
        + clarity_bonus
    )
    scored.loc[:, "pattern_priority_score"] = priority_score.clip(upper=100.0)
    scored = scored.sort_values(
        ["pattern_score", "setup_quality", "symbol_id"],
        ascending=[False, False, True],
        na_position="last",
    ).reset_index(drop=True)
    scored.loc[:, "pattern_rank"] = np.arange(1, len(scored) + 1)
    priority_view = scored.reset_index().sort_values(
        ["pattern_priority_score", "pattern_score", "setup_quality", "symbol_id"],
        ascending=[False, False, False, True],
        na_position="last",
    )
    priority_view.loc[:, "pattern_priority_rank"] = np.arange(1, len(priority_view) + 1)
    priority_ranks = priority_view.set_index("index")["pattern_priority_rank"]
    scored.loc[:, "pattern_priority_rank"] = scored.index.to_series().map(priority_ranks).astype(int)
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
    if not _has_required_volume_ratio(frame):
        return [], PatternScanStats("cup_handle", 0, 0, 0)
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
        breakout_idx, breakout_confirmation = _find_breakout_confirmation(
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
                breakout_volume_ratio=(breakout_confirmation or {}).get("volume_ratio_20"),
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

        # Bug Fix 4: skip stale CwH watchlist candidates whose handle is too old
        if not _recent_enough(handle_idx, frame, config):
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
    if not _has_required_volume_ratio(frame):
        return [], PatternScanStats("round_bottom", 0, 0, 0)
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
        # Bug Fix 2: use positional argmax — safe regardless of frame index type
        # (int-based or DatetimeIndex).  .index[0] was fragile on non-zero-start slices.
        recovery_offset = trough_idx + 1
        right_candidates_mask = recovery_window >= recovery_threshold
        if not right_candidates_mask.any():
            continue
        right_idx = recovery_offset + int(right_candidates_mask.values.argmax())
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
        breakout_idx, breakout_confirmation = _find_breakout_confirmation(
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
                breakout_volume_ratio=(breakout_confirmation or {}).get("volume_ratio_20"),
                width_bars=width,
                cup_depth_pct=depth * 100.0,
                symmetry_ratio=symmetry,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
            continue

        # Bug Fix 4: skip stale round bottom watchlist candidates
        if not _recent_enough(right_idx, frame, config):
            continue
        # Bug Fix 5: skip low-volume round bottom watchlist (below 50% of breakout threshold)
        if _latest_watchlist_volume(frame) < config.breakout_volume_ratio_min * 0.5:
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
    if not _has_required_volume_ratio(frame):
        return [], PatternScanStats("double_bottom", 0, 0, 0)
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
        breakout_idx, breakout_confirmation = _find_breakout_confirmation(
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
                breakout_volume_ratio=(breakout_confirmation or {}).get("volume_ratio_20"),
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
    """Detect bull-flag and high-tight-flag patterns.

    Bug Fix 1: replaced original O(n³) triple-nested loop with an optimised
    two-pass structure:
    - Outer loop iterates over *pole_bars* (constant ~5-20 range).
    - Per pole_bars, `pole_returns` is pre-computed as a numpy array (O(n))
      so the middle loop can skip non-qualifying poles with a single comparison.
    - Innermost flag_bars loop breaks after the first valid flag per
      (pole_bars, pole_end_idx) pair.  In practice this limits the innermost
      work to O(1) amortised per qualifying pole rather than O(max_flag_bars).
    """
    pattern_family = "high_tight_flag" if high_tight_only else "flag"
    if not _has_required_volume_ratio(frame):
        return [], PatternScanStats(pattern_family, 0, 0, 0)
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

    closes = frame["close"].to_numpy(dtype=float)
    n = len(closes)
    # Suppress divide-by-zero: replace 0-closes with nan
    closes_safe = np.where(closes > 1e-9, closes, np.nan)

    for pole_bars in range(config.flag_pole_min_bars, max_pole_bars + 1):
        if pole_bars >= n:
            break
        # Vectorised pole-return array: shape (n - pole_bars,)
        # pole_returns[i] = return from bar i to bar (i + pole_bars)
        pole_returns = (closes[pole_bars:] / closes_safe[:-pole_bars] - 1.0) * 100.0

        for pole_end_idx in range(pole_bars, n - config.flag_min_bars - 1):
            # Pre-screen: skip pole if return is below threshold
            pret = pole_returns[pole_end_idx - pole_bars]
            if np.isnan(pret) or pret < min_pole_rise:
                continue

            pole_start_idx = pole_end_idx - pole_bars

            for flag_bars in range(config.flag_min_bars, config.flag_max_bars + 1):
                flag_end_idx = pole_end_idx + flag_bars
                if flag_end_idx >= n:
                    break

                # Compute flag metrics using the shared helper
                _, flag_high, flag_low, flag_range_pct, retracement_pct = _flag_candidate_from_window(
                    frame,
                    pole_start_idx=pole_start_idx,
                    pole_end_idx=pole_end_idx,
                    flag_end_idx=flag_end_idx,
                )
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

                # Valid flag found — compute signal, then break (shortest valid flag wins).
                # HTF threshold sourced from config, not hard-coded.
                if high_tight_only:
                    breakout_volume_min = float(
                        getattr(config, "high_tight_breakout_volume_ratio_min", 2.0)
                    )
                else:
                    breakout_volume_min = float(config.breakout_volume_ratio_min)
                breakout_idx = None
                breakout_confirmation = None
                max_scan_idx = min(n - 1, flag_end_idx + config.max_breakout_wait_bars)
                for idx in range(flag_end_idx + 1, max_scan_idx + 1):
                    b_close = float(frame.iloc[idx]["close"])
                    confirmation = _volume_confirmation_details(
                        frame.iloc[idx],
                        ratio_threshold=float(breakout_volume_min),
                        z_threshold=float(getattr(config, "volume_zscore_min", 2.0)),
                    )
                    if b_close > flag_high and bool(confirmation["is_any_volume_confirmed"]):
                        breakout_idx = idx
                        breakout_confirmation = confirmation
                        break

                candidates += 1
                invalidation_price = float(frame.iloc[pole_end_idx : flag_end_idx + 1]["low"].min())
                volume_dry_up = bool(frame.iloc[pole_end_idx : flag_end_idx + 1]["volume_ratio_20"].mean() < 1.0)
                pole_rise_pct = float(pret)  # use pre-computed numpy value
                setup_quality = _flag_setup_quality(
                    pole_rise_pct=pole_rise_pct,
                    retracement_pct=retracement_pct,
                    volume_dry_up=volume_dry_up,
                    high_tight=high_tight_only,
                )

                pivot_prices = (
                    float(smoothed.iloc[pole_start_idx]),
                    float(smoothed.iloc[pole_end_idx]),
                    float(smoothed.iloc[flag_end_idx]),
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
                        pivot_prices=pivot_prices,
                        config=config,
                        volume_dry_up=volume_dry_up,
                        breakout_volume_ratio=(breakout_confirmation or {}).get("volume_ratio_20"),
                        width_bars=flag_end_idx - pole_start_idx,
                        pole_rise_pct=pole_rise_pct,
                        flag_tightness_pct=flag_range_pct,
                        flag_retracement_pct=retracement_pct,
                    )
                    if signal.signal_id not in used_signal_ids:
                        used_signal_ids.add(signal.signal_id)
                        signals.append(signal)
                        confirmed += 1
                else:
                    latest_close = float(frame.iloc[-1]["close"])
                    if (
                        latest_close <= flag_high
                        and latest_close >= flag_high * (1 - config.flag_watchlist_buffer_pct)
                    ):
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
                            pivot_prices=pivot_prices,
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

                # Break after first valid flag per (pole_bars, pole_end_idx)
                break

    return signals, PatternScanStats(pattern_family, candidates, confirmed, watchlist)


def _ascending_triangle_setup_quality(
    res_deviation_pct: float, trough_rise_pct: float, volume_dry_up: bool, width: int
) -> float:
    base = 45.0
    res_penalty = min(abs(res_deviation_pct) * 200.0, 15.0)
    trough_bonus = min(trough_rise_pct * 100.0, 10.0)
    vol_bonus = 8.0 if volume_dry_up else 0.0
    width_penalty = max(0, (width - 30) * 0.2) if width > 30 else 0
    return float(np.clip(base - res_penalty + trough_bonus + vol_bonus - width_penalty, 0, 100))


def detect_ascending_triangle_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect bullish ascending triangle patterns.

    Ascending Triangle = flat resistance line + rising troughs.
    Stage 2 gate: skip if stage2_score < 50 (pre-filter).
    """
    if not _has_required_volume_ratio(frame):
        return [], PatternScanStats("ascending_triangle", 0, 0, 0)
    extrema_list = list(extrema)
    peaks = [e for e in extrema_list if e.kind == "peak"]
    troughs = [e for e in extrema_list if e.kind == "trough"]

    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    watchlist = 0
    used_signal_ids: set[str] = set()
    flat_tol = getattr(config, "asc_tri_flat_tol", 0.015)

    if "stage2_score" in frame.columns:
        latest_s2 = float(frame["stage2_score"].iloc[-1]) if len(frame) > 0 else 0
        if latest_s2 < 50:
            return signals, PatternScanStats("ascending_triangle", 0, 0, 0)

    for i in range(len(peaks) - 1):
        p1, p2 = peaks[i], peaks[i + 1]
        if p2.index <= p1.index + 15:
            continue
        res_prices = [float(smoothed.iloc[p.index]) for p in [p1, p2]]
        res_mean = sum(res_prices) / 2
        if any(abs(p - res_mean) / res_mean > flat_tol for p in res_prices):
            continue

        span_troughs = [t for t in troughs if p1.index < t.index < p2.index]
        if len(span_troughs) < 2:
            continue

        t_prices = [float(smoothed.iloc[t.index]) for t in span_troughs]
        if not all(t_prices[j + 1] >= t_prices[j] * 1.005 for j in range(len(t_prices) - 1)):
            continue

        width = p2.index - p1.index
        if width < 15 or width > 90:
            continue

        candidates += 1
        resistance_level = float(frame.iloc[p1.index : p2.index + 1]["high"].max())
        invalidation = float(t_prices[-1]) * 0.98
        res_deviation = (res_prices[1] - res_prices[0]) / res_mean

        breakout_idx, breakout_confirmation = _find_breakout_confirmation(
            frame,
            start_idx=p2.index,
            resistance_level=resistance_level,
            config=config,
        )

        left_volume = float(frame.iloc[max(0, p1.index - 5) : p1.index]["volume_ratio_20"].mean())
        right_volume = float(frame.iloc[p2.index : min(len(frame), p2.index + 5)]["volume_ratio_20"].mean())
        volume_dry_up = right_volume < left_volume

        trough_rise_pct = (t_prices[-1] - t_prices[0]) / max(t_prices[0], 1e-9) if len(t_prices) > 1 else 0
        setup_quality = _ascending_triangle_setup_quality(res_deviation, trough_rise_pct, volume_dry_up, width)

        pivot_prices = (
            float(smoothed.iloc[p1.index]),
            float(smoothed.iloc[p2.index]),
        )
        pivot_indices = (p1.index, p2.index)

        if breakout_idx is not None and (not recent_only or _recent_enough(breakout_idx, frame, config)):
            signal = _build_signal(
                frame=frame,
                pattern_family="ascending_triangle",
                pattern_state="confirmed",
                signal_idx=breakout_idx,
                pattern_start_idx=p1.index,
                pattern_end_idx=p2.index,
                breakout_level=resistance_level,
                invalidation_price=invalidation,
                setup_quality=setup_quality,
                pivot_labels=("resistance_1", "resistance_2"),
                pivot_indices=pivot_indices,
                pivot_prices=pivot_prices,
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=(breakout_confirmation or {}).get("volume_ratio_20"),
                width_bars=width,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
            continue

        if not _recent_enough(p2.index, frame, config):
            continue
        latest_close = float(frame.iloc[-1]["close"])
        if latest_close <= resistance_level and latest_close >= resistance_level * 0.97:
            signal = _build_signal(
                frame=frame,
                pattern_family="ascending_triangle",
                pattern_state="watchlist",
                signal_idx=len(frame) - 1,
                pattern_start_idx=p1.index,
                pattern_end_idx=p2.index,
                breakout_level=resistance_level,
                invalidation_price=invalidation,
                setup_quality=setup_quality - 10.0,
                pivot_labels=("resistance_1", "resistance_2"),
                pivot_indices=pivot_indices,
                pivot_prices=pivot_prices,
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=_latest_watchlist_volume(frame),
                width_bars=width,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                watchlist += 1

    return signals, PatternScanStats("ascending_triangle", candidates, confirmed, watchlist)


def _vcp_setup_quality(
    price_contraction_pct: float, vol_contraction_pct: float, stage2_score: float
) -> float:
    base = 50.0
    pc_bonus = min(price_contraction_pct * 80.0, 15.0)
    vc_bonus = min(vol_contraction_pct * 60.0, 10.0)
    s2_bonus = 15.0 if stage2_score >= 85 else (10.0 if stage2_score >= 70 else 5.0)
    return (base + pc_bonus + vc_bonus + s2_bonus).clip(0, 100)


def detect_vcp_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect bullish VCP (Volatility Contraction Pattern).

    VCP = price range contracts across 3 sequential windows + volume contracts.
    Stage 2 bonus: +15 for strong_stage2, +10 for stage2, +5 for transitional.
    """
    if not _has_required_volume_ratio(frame):
        return [], PatternScanStats("vcp", 0, 0, 0)
    closes = frame["close"].to_numpy(float)
    vrat = frame["volume_ratio_20"].to_numpy(float)
    n = len(closes)
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    watchlist = 0
    used_signal_ids: set[str] = set()

    window = getattr(config, "vcp_window_bars", 40)
    p_fact = getattr(config, "vcp_price_contraction_factor", 0.85)
    v_fact = getattr(config, "vcp_vol_contraction_factor", 0.90)
    min_c1 = getattr(config, "vcp_min_first_range_pct", 0.08)

    has_stage2 = "stage2_score" in frame.columns

    for end in range(window, n):
        start = end - window
        third = window // 3
        if third < 3:
            continue

        ranges = []
        vols = []
        for i in range(3):
            segment_start = start + i * third
            segment_end = start + (i + 1) * third
            if segment_end > n:
                break
            seg_max = closes[segment_start:segment_end].max()
            seg_min = closes[segment_start:segment_end].min()
            seg_range = (seg_max - seg_min) / max(seg_min, 1e-9)
            ranges.append(seg_range)
            vols.append(vrat[segment_start:segment_end].mean())

        if len(ranges) < 3 or ranges[0] < min_c1:
            continue

        price_ok = all(ranges[j + 1] < ranges[j] * p_fact for j in range(2))
        vol_ok = all(vols[j + 1] < vols[j] * v_fact for j in range(2))
        if not (price_ok and vol_ok):
            continue

        candidates += 1
        pivot = float(frame.iloc[start:end + 1]["high"].max())
        invalidation = float(frame.iloc[start:end + 1]["low"].min())

        s2_score = float(frame["stage2_score"].iloc[end]) if has_stage2 and len(frame) > end else 0
        setup_quality = _vcp_setup_quality(
            ranges[0] - ranges[1], 1 - vols[0] / max(vols[1], 1e-9), s2_score
        )

        breakout_idx, breakout_confirmation = _find_breakout_confirmation(
            frame, start_idx=end, resistance_level=pivot, config=config
        )

        if breakout_idx is not None and (not recent_only or _recent_enough(breakout_idx, frame, config)):
            signal = _build_signal(
                frame=frame,
                pattern_family="vcp",
                pattern_state="confirmed",
                signal_idx=breakout_idx,
                pattern_start_idx=start,
                pattern_end_idx=end,
                breakout_level=pivot,
                invalidation_price=invalidation,
                setup_quality=setup_quality,
                pivot_labels=("vcp_start", "vcp_end"),
                pivot_indices=(start, end),
                pivot_prices=(float(smoothed.iloc[start]), float(smoothed.iloc[end])),
                config=config,
                volume_dry_up=vols[-1] < vols[0],
                breakout_volume_ratio=(breakout_confirmation or {}).get("volume_ratio_20"),
                width_bars=window,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
            continue

        if not _recent_enough(end, frame, config):
            continue
        latest_close = float(frame.iloc[-1]["close"])
        if latest_close <= pivot and latest_close >= pivot * 0.97:
            signal = _build_signal(
                frame=frame,
                pattern_family="vcp",
                pattern_state="watchlist",
                signal_idx=len(frame) - 1,
                pattern_start_idx=start,
                pattern_end_idx=end,
                breakout_level=pivot,
                invalidation_price=invalidation,
                setup_quality=setup_quality - 10.0,
                pivot_labels=("vcp_start", "vcp_end"),
                pivot_indices=(start, end),
                pivot_prices=(float(smoothed.iloc[start]), float(smoothed.iloc[end])),
                config=config,
                volume_dry_up=vols[-1] < vols[0],
                breakout_volume_ratio=_latest_watchlist_volume(frame),
                width_bars=window,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                watchlist += 1

    return signals, PatternScanStats("vcp", candidates, confirmed, watchlist)


def _flat_base_setup_quality(depth_pct: float, volume_dry_up: bool, width: int) -> float:
    base = 48.0
    depth_penalty = min(depth_pct * 200.0, 12.0)
    vol_bonus = 8.0 if volume_dry_up else 0.0
    width_bonus = min(width * 0.15, 8.0)
    return float(np.clip(base - depth_penalty + vol_bonus + width_bonus, 0, 100))


def _select_flat_base_signals(candidates: list[PatternSignal]) -> list[PatternSignal]:
    if not candidates:
        return []

    def _sort_key(signal: PatternSignal) -> tuple[int, int]:
        return (signal.pattern_start_index, signal.pattern_end_index)

    def _rank_key(signal: PatternSignal) -> tuple[int, float, int, int]:
        state_priority = 1 if signal.pattern_state == "confirmed" else 0
        return (
            state_priority,
            float(signal.setup_quality),
            int(signal.signal_bar_index),
            int(signal.pattern_end_index),
        )

    selected: list[PatternSignal] = []
    group: list[PatternSignal] = []
    group_end = -1
    for signal in sorted(candidates, key=_sort_key):
        if not group or signal.pattern_start_index <= group_end:
            group.append(signal)
            group_end = max(group_end, signal.pattern_end_index)
            continue
        selected.append(max(group, key=_rank_key))
        group = [signal]
        group_end = signal.pattern_end_index
    if group:
        selected.append(max(group, key=_rank_key))

    unique: dict[str, PatternSignal] = {}
    for signal in sorted(selected, key=lambda item: (item.signal_bar_index, item.pattern_end_index)):
        current = unique.get(signal.signal_id)
        if current is None or _rank_key(signal) > _rank_key(current):
            unique[signal.signal_id] = signal
    return list(unique.values())


def detect_flat_base_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect bullish flat base patterns.

    Flat Base = tight price range (max depth 15%) over 25-65 bars.
    """
    if not _has_required_volume_ratio(frame):
        return [], PatternScanStats("flat_base", 0, 0, 0)
    highs = frame["high"].to_numpy(float)
    lows = frame["low"].to_numpy(float)
    vrat = frame["volume_ratio_20"].to_numpy(float)
    n = len(highs)
    candidate_signals: list[PatternSignal] = []
    candidates = 0

    min_bars = getattr(config, "flat_base_min_bars", 25)
    max_bars = getattr(config, "flat_base_max_bars", 65)
    max_depth = getattr(config, "flat_base_max_depth_pct", 0.15)

    for end in range(min_bars, n):
        for span in range(min_bars, min(max_bars + 1, end + 1)):
            start = end - span
            wh = highs[start:end + 1].max()
            wl = lows[start:end + 1].min()
            depth = (wh - wl) / max(wh, 1e-9)
            if depth > max_depth:
                continue

            mid = start + span // 2
            if vrat[mid:end + 1].mean() >= vrat[start:mid].mean():
                continue

            candidates += 1
            pivot = wh
            invalidation = wl

            breakout_idx, breakout_confirmation = _find_breakout_confirmation(
                frame, start_idx=end, resistance_level=pivot, config=config
            )

            left_volume = float(frame.iloc[max(0, start - 5) : start]["volume_ratio_20"].mean())
            right_volume = float(frame.iloc[start:end]["volume_ratio_20"].mean())
            volume_dry_up = right_volume < left_volume

            setup_quality = _flat_base_setup_quality(depth, volume_dry_up, span)

            if breakout_idx is not None and (not recent_only or _recent_enough(breakout_idx, frame, config)):
                signal = _build_signal(
                    frame=frame,
                    pattern_family="flat_base",
                    pattern_state="confirmed",
                    signal_idx=breakout_idx,
                    pattern_start_idx=start,
                    pattern_end_idx=end,
                    breakout_level=pivot,
                    invalidation_price=invalidation,
                    setup_quality=setup_quality,
                    pivot_labels=("base_start", "base_end"),
                    pivot_indices=(start, end),
                    pivot_prices=(float(smoothed.iloc[start]), float(smoothed.iloc[end])),
                    config=config,
                    volume_dry_up=volume_dry_up,
                    breakout_volume_ratio=(breakout_confirmation or {}).get("volume_ratio_20"),
                    width_bars=span,
                )
                candidate_signals.append(signal)
                continue

            if not _recent_enough(end, frame, config):
                continue
            latest_close = float(frame.iloc[-1]["close"])
            if latest_close <= pivot and latest_close >= pivot * 0.97:
                signal = _build_signal(
                    frame=frame,
                    pattern_family="flat_base",
                    pattern_state="watchlist",
                    signal_idx=len(frame) - 1,
                    pattern_start_idx=start,
                    pattern_end_idx=end,
                    breakout_level=pivot,
                    invalidation_price=invalidation,
                    setup_quality=setup_quality - 10.0,
                    pivot_labels=("base_start", "base_end"),
                    pivot_indices=(start, end),
                    pivot_prices=(float(smoothed.iloc[start]), float(smoothed.iloc[end])),
                    config=config,
                    volume_dry_up=volume_dry_up,
                    breakout_volume_ratio=_latest_watchlist_volume(frame),
                    width_bars=span,
                )
                candidate_signals.append(signal)

    signals = _select_flat_base_signals(candidate_signals)
    confirmed = sum(1 for signal in signals if signal.pattern_state == "confirmed")
    watchlist = sum(1 for signal in signals if signal.pattern_state == "watchlist")
    return signals, PatternScanStats("flat_base", candidates, confirmed, watchlist)


def detect_stage2_reclaim_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect a Stage 1 to Stage 2 SMA-150 reclaim setup."""
    _ = extrema
    pattern_type = "stage2_reclaim"
    required = {"close", "sma_150", "sma150_slope_20d_pct"}
    if not _has_required_volume_ratio(frame) or not required.issubset(frame.columns):
        return [], PatternScanStats(pattern_type, 0, 0, 0)

    closes = pd.to_numeric(frame["close"], errors="coerce")
    sma150 = pd.to_numeric(frame["sma_150"], errors="coerce")
    slope = pd.to_numeric(frame["sma150_slope_20d_pct"], errors="coerce")
    lookback = int(getattr(config, "stage2_reclaim_lookback_bars", 20))
    max_extension = float(getattr(config, "stage2_reclaim_max_extension_pct", 0.08))
    min_slope = float(getattr(config, "stage2_reclaim_min_slope_pct", 0.0))
    start_scan = max(1, len(frame) - max(lookback, 1))

    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    used_signal_ids: set[str] = set()
    for idx in range(start_scan, len(frame)):
        close = _coerce_optional_float(closes.iloc[idx])
        reclaim_level = _coerce_optional_float(sma150.iloc[idx])
        prev_close = _coerce_optional_float(closes.iloc[idx - 1])
        prev_level = _coerce_optional_float(sma150.iloc[idx - 1])
        slope_value = _coerce_optional_float(slope.iloc[idx])
        if None in (close, reclaim_level, prev_close, prev_level, slope_value):
            continue
        if reclaim_level <= 0 or prev_level <= 0:
            continue
        crossed = prev_close <= prev_level and close > reclaim_level
        extension = (close / reclaim_level) - 1.0
        if not crossed or slope_value < min_slope or extension < 0 or extension > max_extension:
            continue

        candidates += 1
        confirmation = _volume_confirmation_details(
            frame.iloc[idx],
            ratio_threshold=float(config.breakout_volume_ratio_min),
            z_threshold=float(getattr(config, "volume_zscore_min", 2.0)),
        )
        if not bool(confirmation["is_any_volume_confirmed"]):
            continue
        if recent_only and not _recent_enough(idx, frame, config):
            continue

        pattern_start_idx = max(0, idx - lookback)
        invalidation = float(reclaim_level) * 0.97
        volume_ratio = float(confirmation["volume_ratio_20"] or 0.0)
        setup_quality = float(
            np.clip(
                52.0
                + min(12.0, max(0.0, slope_value) * 2.0)
                + min(12.0, max(0.0, volume_ratio - float(config.breakout_volume_ratio_min)) * 8.0)
                + max(0.0, 12.0 - extension * 100.0),
                0.0,
                100.0,
            )
        )
        signal = _build_signal(
            frame=frame,
            pattern_family=pattern_type,
            pattern_state="confirmed",
            signal_idx=idx,
            pattern_start_idx=pattern_start_idx,
            pattern_end_idx=idx,
            breakout_level=float(reclaim_level),
            invalidation_price=invalidation,
            setup_quality=setup_quality,
            pivot_labels=("reclaim_base", "sma150_reclaim"),
            pivot_indices=(pattern_start_idx, idx),
            pivot_prices=(float(smoothed.iloc[pattern_start_idx]), float(reclaim_level)),
            config=config,
            volume_dry_up=False,
            breakout_volume_ratio=confirmation.get("volume_ratio_20"),
            width_bars=idx - pattern_start_idx,
        )
        if signal.signal_id not in used_signal_ids:
            used_signal_ids.add(signal.signal_id)
            signals.append(signal)
            confirmed += 1

    return signals, PatternScanStats(pattern_type, candidates, confirmed, 0)


def detect_darvas_box_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect Darvas boxes: repeated resistance, controlled box risk, strict-volume breakout."""
    _ = extrema
    pattern_type = "darvas_box"
    required = {"high", "low", "close"}
    if not _has_strict_volume_inputs(frame) or not required.issubset(frame.columns):
        return [], PatternScanStats(pattern_type, 0, 0, 0)

    n = len(frame)
    min_box = int(getattr(config, "darvas_min_box_bars", 15))
    lookback = int(getattr(config, "darvas_lookback_bars", 60))
    if n < min_box + 1:
        return [], PatternScanStats(pattern_type, 0, 0, 0)

    highs = pd.to_numeric(frame["high"], errors="coerce")
    lows = pd.to_numeric(frame["low"], errors="coerce")
    closes = pd.to_numeric(frame["close"], errors="coerce")
    tol = float(getattr(config, "darvas_resistance_tolerance_pct", 0.02))
    max_depth = float(getattr(config, "darvas_max_box_depth_pct", 0.20))
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    used_signal_ids: set[str] = set()

    start_min = max(0, n - lookback - int(config.max_breakout_wait_bars))
    for end in range(max(min_box, start_min + min_box), n - 1):
        for span in range(min_box, min(lookback, end + 1) + 1):
            start = end - span + 1
            box_highs = highs.iloc[start : end + 1]
            box_lows = lows.iloc[start : end + 1]
            if box_highs.isna().any() or box_lows.isna().any():
                continue
            box_top = float(box_highs.max())
            box_bottom = float(box_lows.min())
            if box_top <= 0 or box_bottom <= 0:
                continue
            depth = (box_top - box_bottom) / box_top
            if depth > max_depth:
                continue
            touches = int((box_highs >= box_top * (1.0 - tol)).sum())
            if touches < 2:
                continue
            if float(closes.iloc[start : end + 1].max()) > box_top * (1.0 + tol):
                continue

            breakout_idx, breakout_confirmation = _find_breakout_confirmation(
                frame,
                start_idx=end,
                resistance_level=box_top,
                config=config,
            )
            candidates += 1
            if breakout_idx is None or breakout_confirmation is None:
                continue
            if recent_only and not _recent_enough(breakout_idx, frame, config):
                continue

            setup_quality = float(np.clip(55.0 + min(12.0, touches * 3.0) + max(0.0, 18.0 - depth * 100.0), 0, 100))
            signal = _build_signal(
                frame=frame,
                pattern_family=pattern_type,
                pattern_state="confirmed",
                signal_idx=breakout_idx,
                pattern_start_idx=start,
                pattern_end_idx=end,
                breakout_level=box_top,
                invalidation_price=box_bottom,
                setup_quality=setup_quality,
                pivot_labels=("box_start", "box_top", "box_bottom", "box_end"),
                pivot_indices=(start, int(box_highs.idxmax()), int(box_lows.idxmin()), end),
                pivot_prices=(float(smoothed.iloc[start]), box_top, box_bottom, float(smoothed.iloc[end])),
                config=config,
                volume_dry_up=bool(frame.iloc[start:end + 1]["volume_ratio_20"].mean() < 1.0),
                breakout_volume_ratio=breakout_confirmation.get("volume_ratio_20"),
                width_bars=span,
                cup_depth_pct=depth * 100.0,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
                break

    return signals, PatternScanStats(pattern_type, candidates, confirmed, 0)


def detect_pocket_pivot_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect pocket pivots on high-volume MA-supported thrusts."""
    _ = (smoothed, extrema)
    pattern_type = "pocket_pivot"
    required = {"open", "high", "low", "close", "volume", "sma_20", "sma_50"}
    if not _has_strict_volume_inputs(frame) or not required.issubset(frame.columns):
        return [], PatternScanStats(pattern_type, 0, 0, 0)

    lookback = int(getattr(config, "pocket_pivot_lookback_bars", 10))
    if len(frame) <= lookback:
        return [], PatternScanStats(pattern_type, 0, 0, 0)

    closes = pd.to_numeric(frame["close"], errors="coerce")
    opens = pd.to_numeric(frame["open"], errors="coerce")
    highs = pd.to_numeric(frame["high"], errors="coerce")
    lows = pd.to_numeric(frame["low"], errors="coerce")
    volumes = pd.to_numeric(frame["volume"], errors="coerce")
    sma20 = pd.to_numeric(frame["sma_20"], errors="coerce")
    sma50 = pd.to_numeric(frame["sma_50"], errors="coerce")
    max_extension = float(getattr(config, "pocket_pivot_max_extension_pct", 0.10))
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    used_signal_ids: set[str] = set()

    for idx in range(lookback, len(frame)):
        if recent_only and not _recent_enough(idx, frame, config):
            continue
        close = _coerce_optional_float(closes.iloc[idx])
        if close is None:
            continue
        prior = frame.iloc[idx - lookback : idx]
        prior_down = prior.loc[pd.to_numeric(prior["close"], errors="coerce") <= pd.to_numeric(prior["open"], errors="coerce")]
        if prior_down.empty:
            prior_down = prior.loc[pd.to_numeric(prior["close"], errors="coerce").diff().fillna(0.0) <= 0]
        if prior_down.empty:
            continue
        prior_highest_close = float(pd.to_numeric(prior["close"], errors="coerce").max())
        max_down_volume = float(pd.to_numeric(prior_down["volume"], errors="coerce").max())
        if not (close > prior_highest_close and volumes.iloc[idx] > max_down_volume):
            continue
        ma_ref = min(_positive_fallback_price(sma20.iloc[idx], close), _positive_fallback_price(sma50.iloc[idx], close))
        extension = (close / ma_ref) - 1.0
        above_or_reclaiming = (
            close >= sma20.iloc[idx]
            or close >= sma50.iloc[idx]
            or (closes.iloc[idx - 1] <= sma50.iloc[idx - 1] and close > sma50.iloc[idx])
        )
        if not bool(above_or_reclaiming) or extension > max_extension:
            continue
        confirmation = _confirmed_volume_on_bar(frame, idx, config)
        if confirmation is None:
            continue

        candidates += 1
        pattern_start = idx - lookback
        invalidation = float(min(lows.iloc[idx], sma20.iloc[idx], sma50.iloc[idx]))
        setup_quality = float(np.clip(56.0 + min(16.0, (volumes.iloc[idx] / max_down_volume - 1.0) * 10.0) + max(0.0, 12.0 - extension * 100.0), 0, 100))
        signal = _build_signal(
            frame=frame,
            pattern_family=pattern_type,
            pattern_state="confirmed",
            signal_idx=idx,
            pattern_start_idx=pattern_start,
            pattern_end_idx=idx,
            breakout_level=prior_highest_close,
            invalidation_price=invalidation,
            setup_quality=setup_quality,
            pivot_labels=("lookback_start", "prior_high_close", "pocket_pivot"),
            pivot_indices=(pattern_start, int(pd.to_numeric(prior["close"], errors="coerce").idxmax()), idx),
            pivot_prices=(float(closes.iloc[pattern_start]), prior_highest_close, close),
            config=config,
            volume_dry_up=False,
            breakout_volume_ratio=confirmation.get("volume_ratio_20"),
            width_bars=lookback,
        )
        if signal.signal_id not in used_signal_ids:
            used_signal_ids.add(signal.signal_id)
            signals.append(signal)
            confirmed += 1

    return signals, PatternScanStats(pattern_type, candidates, confirmed, 0)


def detect_ascending_base_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect ascending bases with three controlled, rising pullbacks."""
    pattern_type = "ascending_base"
    required = {"high", "low", "close"}
    if not _has_strict_volume_inputs(frame) or not required.issubset(frame.columns):
        return [], PatternScanStats(pattern_type, 0, 0, 0)

    min_bars = int(getattr(config, "ascending_base_min_bars", 45))
    max_bars = int(getattr(config, "ascending_base_max_bars", 80))
    if len(frame) < min_bars + 1:
        return [], PatternScanStats(pattern_type, 0, 0, 0)

    troughs = [item for item in list(extrema) if item.kind == "trough"]
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    used_signal_ids: set[str] = set()
    max_depth = float(getattr(config, "ascending_base_max_pullback_depth_pct", 0.20))
    min_low_rise = float(getattr(config, "ascending_base_min_low_rise_pct", 0.02))

    for i in range(len(troughs) - 2):
        t1, t2, t3 = troughs[i : i + 3]
        width = t3.index - t1.index
        if width < min_bars or width > max_bars:
            continue
        lows = [float(smoothed.iloc[t.index]) for t in (t1, t2, t3)]
        if not (lows[1] >= lows[0] * (1.0 + min_low_rise) and lows[2] >= lows[1] * (1.0 + min_low_rise)):
            continue
        window = frame.iloc[t1.index : t3.index + 1]
        pivot = float(window["high"].max())
        base_low = float(window["low"].min())
        if (pivot - base_low) / max(pivot, 1e-9) > max_depth:
            continue

        breakout_idx, breakout_confirmation = _find_breakout_confirmation(
            frame,
            start_idx=t3.index,
            resistance_level=pivot,
            config=config,
        )
        candidates += 1
        if breakout_idx is None or breakout_confirmation is None:
            continue
        if recent_only and not _recent_enough(breakout_idx, frame, config):
            continue

        setup_quality = float(np.clip(58.0 + min(18.0, ((lows[-1] / lows[0]) - 1.0) * 120.0), 0, 100))
        signal = _build_signal(
            frame=frame,
            pattern_family=pattern_type,
            pattern_state="confirmed",
            signal_idx=breakout_idx,
            pattern_start_idx=t1.index,
            pattern_end_idx=t3.index,
            breakout_level=pivot,
            invalidation_price=float(lows[-1]) * 0.98,
            setup_quality=setup_quality,
            pivot_labels=("pullback_1", "pullback_2", "pullback_3"),
            pivot_indices=(t1.index, t2.index, t3.index),
            pivot_prices=tuple(lows),
            config=config,
            volume_dry_up=bool(frame.iloc[t2.index:t3.index + 1]["volume_ratio_20"].mean() < frame.iloc[t1.index:t2.index]["volume_ratio_20"].mean()),
            breakout_volume_ratio=breakout_confirmation.get("volume_ratio_20"),
            width_bars=width,
        )
        if signal.signal_id not in used_signal_ids:
            used_signal_ids.add(signal.signal_id)
            signals.append(signal)
            confirmed += 1

    return signals, PatternScanStats(pattern_type, candidates, confirmed, 0)


def detect_ipo_base_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect first-stage bases on limited-history IPO/young-stock frames."""
    _ = extrema
    pattern_type = "ipo_base"
    required = {"high", "low", "close"}
    min_history = int(getattr(config, "ipo_base_min_history_bars", 35))
    max_history = int(getattr(config, "ipo_base_max_history_bars", 180))
    if not _has_strict_volume_inputs(frame) or not required.issubset(frame.columns):
        return [], PatternScanStats(pattern_type, 0, 0, 0)
    if len(frame) < min_history or len(frame) > max_history:
        return [], PatternScanStats(pattern_type, 0, 0, 0)

    min_bars = int(getattr(config, "ipo_base_min_bars", 15))
    max_bars = int(getattr(config, "ipo_base_max_bars", 65))
    max_depth = float(getattr(config, "ipo_base_max_depth_pct", 0.30))
    highs = pd.to_numeric(frame["high"], errors="coerce")
    lows = pd.to_numeric(frame["low"], errors="coerce")
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    used_signal_ids: set[str] = set()

    for end in range(min_bars, len(frame) - 1):
        for span in range(min_bars, min(max_bars, end + 1) + 1):
            start = end - span + 1
            window_high = float(highs.iloc[start:end + 1].max())
            window_low = float(lows.iloc[start:end + 1].min())
            if window_high <= 0 or window_low <= 0:
                continue
            depth = (window_high - window_low) / window_high
            if depth > max_depth:
                continue
            breakout_idx, breakout_confirmation = _find_breakout_confirmation(
                frame,
                start_idx=end,
                resistance_level=window_high,
                config=config,
            )
            candidates += 1
            if breakout_idx is None or breakout_confirmation is None:
                continue
            if recent_only and not _recent_enough(breakout_idx, frame, config):
                continue

            setup_quality = float(np.clip(55.0 + max(0.0, 20.0 - depth * 100.0) + min(8.0, span * 0.15), 0, 100))
            signal = _build_signal(
                frame=frame,
                pattern_family=pattern_type,
                pattern_state="confirmed",
                signal_idx=breakout_idx,
                pattern_start_idx=start,
                pattern_end_idx=end,
                breakout_level=window_high,
                invalidation_price=window_low,
                setup_quality=setup_quality,
                pivot_labels=("ipo_base_start", "ipo_base_low", "ipo_base_end"),
                pivot_indices=(start, int(lows.iloc[start:end + 1].idxmin()), end),
                pivot_prices=(float(smoothed.iloc[start]), window_low, float(smoothed.iloc[end])),
                config=config,
                volume_dry_up=bool(frame.iloc[start:end + 1]["volume_ratio_20"].mean() < 1.0),
                breakout_volume_ratio=breakout_confirmation.get("volume_ratio_20"),
                width_bars=span,
                cup_depth_pct=depth * 100.0,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
                break

    return signals, PatternScanStats(pattern_type, candidates, confirmed, 0)


def detect_inside_week_breakout_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect 5-bar inside-week compression followed by strict-volume breakout."""
    _ = (smoothed, extrema)
    pattern_type = "inside_week_breakout"
    required = {"timestamp", "high", "low", "close"}
    if not _has_strict_volume_inputs(frame) or not required.issubset(frame.columns):
        return [], PatternScanStats(pattern_type, 0, 0, 0)
    if len(frame) < 15:
        return [], PatternScanStats(pattern_type, 0, 0, 0)

    work = frame.reset_index(drop=True).copy()
    timestamps = pd.to_datetime(work["timestamp"], errors="coerce")
    if timestamps.isna().any():
        return [], PatternScanStats(pattern_type, 0, 0, 0)
    work.loc[:, "_row_idx"] = np.arange(len(work))
    work.loc[:, "_week_id"] = timestamps.dt.to_period("W-FRI").astype(str)
    weekly = work.groupby("_week_id", as_index=False).agg(
        start_idx=("_row_idx", "min"),
        end_idx=("_row_idx", "max"),
        high=("high", "max"),
        low=("low", "min"),
    )
    lookback_weeks = int(getattr(config, "inside_week_lookback_weeks", 8))
    weekly = weekly.tail(max(lookback_weeks + 2, 3)).reset_index(drop=True)

    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    used_signal_ids: set[str] = set()
    for pos in range(1, len(weekly)):
        prev = weekly.iloc[pos - 1]
        curr = weekly.iloc[pos]
        if not (float(curr["high"]) < float(prev["high"]) and float(curr["low"]) > float(prev["low"])):
            continue
        candidates += 1
        inside_end = int(curr["end_idx"])
        breakout_level = float(curr["high"])
        invalidation = float(curr["low"])
        max_idx = min(len(frame) - 1, inside_end + int(config.max_breakout_wait_bars))
        for idx in range(inside_end + 1, max_idx + 1):
            close = _coerce_optional_float(frame.iloc[idx].get("close"))
            if close is None or close <= breakout_level:
                continue
            confirmation = _confirmed_volume_on_bar(frame, idx, config)
            if confirmation is None:
                continue
            if recent_only and not _recent_enough(idx, frame, config):
                continue
            setup_quality = float(np.clip(55.0 + min(15.0, (float(prev["high"]) - float(curr["high"])) / max(float(prev["high"]), 1e-9) * 300.0), 0, 100))
            signal = _build_signal(
                frame=frame,
                pattern_family=pattern_type,
                pattern_state="confirmed",
                signal_idx=idx,
                pattern_start_idx=int(curr["start_idx"]),
                pattern_end_idx=inside_end,
                breakout_level=breakout_level,
                invalidation_price=invalidation,
                setup_quality=setup_quality,
                pivot_labels=("inside_week_start", "inside_week_high", "inside_week_low"),
                pivot_indices=(int(curr["start_idx"]), inside_end, inside_end),
                pivot_prices=(float(frame.iloc[int(curr["start_idx"])]["close"]), breakout_level, invalidation),
                config=config,
                volume_dry_up=bool(frame.iloc[int(curr["start_idx"]):inside_end + 1]["volume_ratio_20"].mean() < 1.0),
                breakout_volume_ratio=confirmation.get("volume_ratio_20"),
                width_bars=int(inside_end - int(curr["start_idx"]) + 1),
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
            break

    return signals, PatternScanStats(pattern_type, candidates, confirmed, 0)


def _3wt_setup_quality(tight_pct: float, prior_adv: float, volume_dry_up: bool) -> float:
    base = 52.0
    tightness_bonus = min((0.015 - tight_pct) * 2000.0, 15.0) if tight_pct < 0.015 else 0.0
    adv_bonus = min((prior_adv - 0.20) * 50.0, 10.0) if prior_adv > 0.20 else 0.0
    vol_bonus = 8.0 if volume_dry_up else 0.0
    return float(np.clip(base + tightness_bonus + adv_bonus + vol_bonus, 0, 100))


def detect_3wt_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect bullish 3-Weeks-Tight (3WT) patterns.

    3WT = last 3 'weekly' (5-bar) closing prices within a tight range (default 1.5%)
    following a significant prior advance (default ≥ 20%).
    """
    if not _has_required_volume_ratio(frame):
        return [], PatternScanStats("three_weeks_tight", 0, 0, 0)
    closes = frame["close"].to_numpy(float)
    n = len(closes)
    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    watchlist = 0
    used_signal_ids: set[str] = set()

    tight_pct = getattr(config, "wt3_tight_pct", 0.015)
    prior_adv = getattr(config, "wt3_prior_adv", 0.20)
    weeks = 3
    step = 5  # 5 bars ≈ 1 trading week

    if n < weeks * step + 20:
        return signals, PatternScanStats("three_weeks_tight", 0, 0, 0)

    for end in range(weeks * step, n):
        # Weekly close proxies: one close per 'week' at positions end, end-5, end-10
        w_closes = [closes[end - i * step] for i in range(weeks)]  # newest first
        w_max = max(w_closes)
        w_min = min(w_closes)
        if w_max < 1e-9:
            continue
        tightness = (w_max - w_min) / w_max
        if tightness > tight_pct:
            continue

        # Prior advance: from ~20 bars before the base start to the start of the 3WT
        base_start = end - weeks * step
        prior_start = max(0, base_start - 20)
        if closes[prior_start] < 1e-9:
            continue
        adv = (closes[base_start] - closes[prior_start]) / closes[prior_start]
        if adv < prior_adv:
            continue

        candidates += 1
        pivot = float(frame.iloc[base_start : end + 1]["high"].max())
        invalidation = float(w_closes[-1]) * 0.97  # 3% below oldest weekly close

        # Volume: check if volume is drying up in the base vs prior period
        vrat = frame.get("volume_ratio_20")
        volume_dry_up = False
        if vrat is not None and hasattr(vrat, "iloc"):
            base_vol = float(pd.to_numeric(vrat.iloc[base_start : end + 1], errors="coerce").mean())
            prior_vol = float(pd.to_numeric(vrat.iloc[prior_start : base_start], errors="coerce").mean())
            volume_dry_up = (base_vol < prior_vol) if (not np.isnan(base_vol) and not np.isnan(prior_vol)) else False

        setup_quality = _3wt_setup_quality(tightness, adv, volume_dry_up)

        breakout_idx, breakout_confirmation = _find_breakout_confirmation(
            frame, start_idx=end, resistance_level=pivot, config=config
        )

        pivot_prices = (
            float(smoothed.iloc[base_start]),
            float(smoothed.iloc[end]),
        )
        pivot_indices = (base_start, end)

        if breakout_idx is not None and (not recent_only or _recent_enough(breakout_idx, frame, config)):
            signal = _build_signal(
                frame=frame,
                pattern_family="three_weeks_tight",
                pattern_state="confirmed",
                signal_idx=breakout_idx,
                pattern_start_idx=base_start,
                pattern_end_idx=end,
                breakout_level=pivot,
                invalidation_price=invalidation,
                setup_quality=setup_quality,
                pivot_labels=("base_start", "base_end"),
                pivot_indices=pivot_indices,
                pivot_prices=pivot_prices,
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=(breakout_confirmation or {}).get("volume_ratio_20"),
                width_bars=weeks * step,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
            continue

        if not _recent_enough(end, frame, config):
            continue
        latest_close = float(frame.iloc[-1]["close"])
        if latest_close <= pivot and latest_close >= pivot * (1 - getattr(config, "cup_watchlist_buffer_pct", 0.03)):
            signal = _build_signal(
                frame=frame,
                pattern_family="three_weeks_tight",
                pattern_state="watchlist",
                signal_idx=len(frame) - 1,
                pattern_start_idx=base_start,
                pattern_end_idx=end,
                breakout_level=pivot,
                invalidation_price=invalidation,
                setup_quality=setup_quality - 10.0,
                pivot_labels=("base_start", "base_end"),
                pivot_indices=pivot_indices,
                pivot_prices=pivot_prices,
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=_latest_watchlist_volume(frame),
                width_bars=weeks * step,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                watchlist += 1

    return signals, PatternScanStats("three_weeks_tight", candidates, confirmed, watchlist)


def _sym_tri_setup_quality(peak_drop_pct: float, trough_rise_pct: float, volume_dry_up: bool, width: int) -> float:
    base = 45.0
    conv_bonus = min((peak_drop_pct + trough_rise_pct) * 50.0, 15.0)
    vol_bonus = 8.0 if volume_dry_up else 0.0
    width_penalty = max(0, (width - 40) * 0.2)
    return float(np.clip(base + conv_bonus + vol_bonus - width_penalty, 0, 100))


def detect_symmetrical_triangle_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Detect bullish symmetrical triangle patterns.

    Symmetrical Triangle = descending peaks + ascending troughs converging.
    Only emits BULLISH breakouts (close > upper line with volume).
    """
    if not _has_required_volume_ratio(frame):
        return [], PatternScanStats("symmetrical_triangle", 0, 0, 0)
    extrema_list = list(extrema)
    peaks = [e for e in extrema_list if e.kind == "peak"]
    troughs = [e for e in extrema_list if e.kind == "trough"]

    signals: list[PatternSignal] = []
    candidates = 0
    confirmed = 0
    watchlist = 0
    used_signal_ids: set[str] = set()

    for i in range(len(peaks) - 1):
        p1, p2 = peaks[i], peaks[i + 1]
        if p2.index <= p1.index + 15:
            continue

        p1_price = float(smoothed.iloc[p1.index])
        p2_price = float(smoothed.iloc[p2.index])

        # Descending peaks required
        if p2_price >= p1_price:
            continue

        # Find inner troughs between p1 and p2
        inner_troughs = [t for t in troughs if p1.index < t.index < p2.index]
        if len(inner_troughs) < 2:
            continue

        t1, t2 = inner_troughs[0], inner_troughs[-1]
        t1_price = float(smoothed.iloc[t1.index])
        t2_price = float(smoothed.iloc[t2.index])

        # Ascending troughs required
        if t2_price <= t1_price:
            continue

        width = p2.index - p1.index
        if width < 15 or width > 80:
            continue

        # Convergence check: upper line at p2 must still be above lower line at t2
        if p2_price <= t2_price:
            continue

        candidates += 1
        # Upper resistance is the smoothed price at p2 (the lower peak)
        resistance_level = float(frame.iloc[p1.index : p2.index + 1]["high"].max())
        # Invalidation: below the last trough
        invalidation = float(t2_price) * 0.98

        # Volume contraction check
        left_vol = float(pd.to_numeric(
            frame.get("volume_ratio_20", pd.Series(dtype=float)).iloc[max(0, p1.index - 5) : p1.index],
            errors="coerce",
        ).mean()) if "volume_ratio_20" in frame.columns else 1.0
        right_vol = float(pd.to_numeric(
            frame.get("volume_ratio_20", pd.Series(dtype=float)).iloc[p2.index : min(len(frame), p2.index + 5)],
            errors="coerce",
        ).mean()) if "volume_ratio_20" in frame.columns else 1.0
        volume_dry_up = right_vol < left_vol

        peak_drop_pct = (p1_price - p2_price) / max(p1_price, 1e-9)
        trough_rise_pct = (t2_price - t1_price) / max(t1_price, 1e-9)
        setup_quality = _sym_tri_setup_quality(peak_drop_pct, trough_rise_pct, volume_dry_up, width)

        breakout_idx, breakout_confirmation = _find_breakout_confirmation(
            frame, start_idx=p2.index, resistance_level=resistance_level, config=config
        )

        pivot_prices = (p1_price, t1_price, t2_price, p2_price)
        pivot_indices = (p1.index, t1.index, t2.index, p2.index)

        if breakout_idx is not None and (not recent_only or _recent_enough(breakout_idx, frame, config)):
            signal = _build_signal(
                frame=frame,
                pattern_family="symmetrical_triangle",
                pattern_state="confirmed",
                signal_idx=breakout_idx,
                pattern_start_idx=p1.index,
                pattern_end_idx=p2.index,
                breakout_level=resistance_level,
                invalidation_price=invalidation,
                setup_quality=setup_quality,
                pivot_labels=("upper_peak_1", "lower_trough_1", "upper_trough_2", "upper_peak_2"),
                pivot_indices=pivot_indices,
                pivot_prices=pivot_prices,
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=(breakout_confirmation or {}).get("volume_ratio_20"),
                width_bars=width,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                confirmed += 1
            continue

        if not _recent_enough(p2.index, frame, config):
            continue
        latest_close = float(frame.iloc[-1]["close"])
        buffer = getattr(config, "cup_watchlist_buffer_pct", 0.03)
        if latest_close <= resistance_level and latest_close >= resistance_level * (1 - buffer):
            signal = _build_signal(
                frame=frame,
                pattern_family="symmetrical_triangle",
                pattern_state="watchlist",
                signal_idx=len(frame) - 1,
                pattern_start_idx=p1.index,
                pattern_end_idx=p2.index,
                breakout_level=resistance_level,
                invalidation_price=invalidation,
                setup_quality=setup_quality - 10.0,
                pivot_labels=("upper_peak_1", "lower_trough_1", "upper_trough_2", "upper_peak_2"),
                pivot_indices=pivot_indices,
                pivot_prices=pivot_prices,
                config=config,
                volume_dry_up=volume_dry_up,
                breakout_volume_ratio=_latest_watchlist_volume(frame),
                width_bars=width,
            )
            if signal.signal_id not in used_signal_ids:
                used_signal_ids.add(signal.signal_id)
                signals.append(signal)
                watchlist += 1

    return signals, PatternScanStats("symmetrical_triangle", candidates, confirmed, watchlist)


def detect_inside_day_signals(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series | None = None,
    extrema: Iterable[LocalExtrema] | None = None,
    config: PatternScanConfig,
    recent_only: bool = True,
) -> tuple[list[PatternSignal], PatternScanStats]:
    """Inside-Day Breakout: 1-3 inside bars after a mother bar, then break.

    Inside bar := high ≤ prior high AND low ≥ prior low. The breakout bar
    must close above the high of the mother bar (the bar immediately
    preceding the first inside bar) on volume confirmation.
    """
    del smoothed, extrema, recent_only
    candidates = 0
    confirmed = 0
    signals: list[PatternSignal] = []
    used_signal_ids: set[str] = set()

    if not {"high", "low", "close"}.issubset(frame.columns):
        return signals, PatternScanStats("inside_day", 0, 0, 0)

    n = len(frame)
    max_lookback = int(config.inside_day_max_lookback_bars)
    if n < max_lookback + 2:
        return signals, PatternScanStats("inside_day", 0, 0, 0)

    high = pd.to_numeric(frame["high"], errors="coerce").to_numpy()
    low = pd.to_numeric(frame["low"], errors="coerce").to_numpy()
    close_arr = pd.to_numeric(frame["close"], errors="coerce").to_numpy()

    breakout_idx = n - 1
    breakout_close = close_arr[breakout_idx]
    if not np.isfinite(breakout_close):
        return signals, PatternScanStats("inside_day", 0, 0, 0)

    inside_count = 0
    for back in range(1, max_lookback + 1):
        cand = breakout_idx - back
        ref = cand - 1
        if ref < 0:
            break
        if not (
            np.isfinite(high[cand]) and np.isfinite(low[cand])
            and np.isfinite(high[ref]) and np.isfinite(low[ref])
        ):
            break
        if high[cand] <= high[ref] and low[cand] >= low[ref]:
            inside_count += 1
        else:
            break

    if inside_count == 0:
        return signals, PatternScanStats("inside_day", 0, 0, 0)
    candidates += 1

    first_inside_idx = breakout_idx - inside_count
    mother_idx = first_inside_idx - 1
    if mother_idx < 0:
        return signals, PatternScanStats("inside_day", candidates, 0, 0)
    mother_high = float(high[mother_idx])
    mother_low = float(low[mother_idx])

    confirmation = _volume_confirmation_details(
        frame.iloc[breakout_idx],
        ratio_threshold=float(config.breakout_volume_ratio_min),
    )
    if not (breakout_close > mother_high and bool(confirmation["is_any_volume_confirmed"])):
        return signals, PatternScanStats("inside_day", candidates, 0, 0)

    avg_inside_range = float(
        np.nanmean(high[first_inside_idx : breakout_idx] - low[first_inside_idx : breakout_idx])
    )
    mother_range = max(mother_high - mother_low, 1e-9)
    tightness = 1.0 - min(avg_inside_range / mother_range, 1.0)
    setup_quality = float(min(80.0, 45.0 + tightness * 25.0 + (5.0 if inside_count >= 2 else 0.0)))

    signal = _build_signal(
        frame=frame,
        pattern_family="inside_day",
        pattern_state="confirmed",
        signal_idx=breakout_idx,
        pattern_start_idx=mother_idx,
        pattern_end_idx=breakout_idx - 1,
        breakout_level=mother_high,
        invalidation_price=mother_low,
        setup_quality=setup_quality,
        pivot_labels=("mother_bar",),
        pivot_indices=(mother_idx,),
        pivot_prices=(mother_high,),
        config=config,
        volume_dry_up=False,
        breakout_volume_ratio=confirmation.get("volume_ratio_20"),
        width_bars=inside_count,
    )
    if signal.signal_id not in used_signal_ids:
        used_signal_ids.add(signal.signal_id)
        signals.append(signal)
        confirmed += 1

    return signals, PatternScanStats("inside_day", candidates, confirmed, 0)


def _build_head_shoulders_signal(
    frame: pd.DataFrame,
    *,
    neckline: float,
    config: PatternScanConfig,
) -> PatternSignal | None:
    """Materialize a first-class bearish H&S signal from the breakdown bar."""
    if frame.empty:
        return None
    timestamps = frame["timestamp"]
    breakdown_idx = len(frame) - 1
    signal_date = timestamps.iloc[breakdown_idx].date().isoformat()
    symbol = str(frame.iloc[0]["symbol_id"])
    look = frame.iloc[max(0, breakdown_idx - 60) : breakdown_idx + 1]
    invalidation = float(pd.to_numeric(look["high"], errors="coerce").max())
    if not np.isfinite(invalidation) or invalidation <= 0:
        invalidation = float(neckline) * 1.05
    return PatternSignal(
        signal_id=f"{symbol}-head_shoulders-confirmed-{signal_date}",
        symbol_id=symbol,
        pattern_family="head_shoulders",
        pattern_state="confirmed",
        signal_direction="bearish",
        pattern_start=timestamps.iloc[max(0, breakdown_idx - 60)].date().isoformat(),
        pattern_end=signal_date,
        signal_date=signal_date,
        pattern_start_index=int(max(0, breakdown_idx - 60)),
        pattern_end_index=int(breakdown_idx),
        signal_bar_index=int(breakdown_idx),
        breakout_level=float(_rounded(neckline) or neckline),
        watchlist_trigger_level=float(_rounded(neckline) or neckline),
        invalidation_price=float(_rounded(invalidation) or invalidation),
        setup_quality=50.0,
        pivot_labels=("neckline",),
        pivot_dates=(signal_date,),
        pivot_prices=(float(_rounded(neckline) or neckline),),
        pivot_indices=(int(breakdown_idx),),
        config_provenance=_config_provenance(config),
    )


def detect_head_shoulders_filter(
    frame: pd.DataFrame,
    *,
    smoothed: pd.Series,
    extrema: Iterable[LocalExtrema],
    config: PatternScanConfig,
) -> tuple[bool, float]:
    """Return (is_hs_top, neckline_price). Pure detection — no signal objects.

    H&S top = three consecutive peaks where the middle ('head') is strictly
    taller than both shoulders (ls, rs), shoulders within 5% of each other,
    and latest close has broken the neckline (< neckline * 0.99).
    """
    extrema_list = list(extrema)
    peaks = [e for e in extrema_list if e.kind == "peak"]
    troughs = [e for e in extrema_list if e.kind == "trough"]

    for i in range(len(peaks) - 2):
        ls, head, rs = peaks[i], peaks[i + 1], peaks[i + 2]

        ls_px = float(smoothed.iloc[ls.index])
        head_px = float(smoothed.iloc[head.index])
        rs_px = float(smoothed.iloc[rs.index])

        # Head must be clearly above both shoulders
        if head_px <= ls_px * 1.03 or head_px <= rs_px * 1.03:
            continue

        # Shoulders should be roughly balanced (within 5%)
        if abs(ls_px - rs_px) / max(ls_px, 1e-9) > 0.05:
            continue

        # Find troughs between ls→head and head→rs
        t1_candidates = [t for t in troughs if ls.index < t.index < head.index]
        t2_candidates = [t for t in troughs if head.index < t.index < rs.index]
        if not t1_candidates or not t2_candidates:
            continue

        t1 = t1_candidates[-1]  # trough closest to head on the left
        t2 = t2_candidates[0]   # trough closest to head on the right
        neckline = (float(smoothed.iloc[t1.index]) + float(smoothed.iloc[t2.index])) / 2.0

        # Confirmed breakdown: latest close below neckline
        latest_close = float(frame.iloc[-1]["close"])
        if latest_close < neckline * 0.99:
            return True, float(neckline)

    return False, 0.0


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
    """Return all bullish pattern signals for one ordered symbol frame.

    Applies the Head & Shoulders exclusion filter last: if an H&S top is
    detected and the latest close has broken the neckline, all bullish signals
    for this symbol are dropped.
    """
    # --- H&S exclusion check (run first so we can skip expensive detectors) ---
    extrema_list = list(extrema)  # materialise once; passed to all detectors
    is_hs, _neckline = detect_head_shoulders_filter(
        frame, smoothed=smoothed, extrema=extrema_list, config=config
    )
    if is_hs:
        # Symbol is in confirmed bearish H&S breakdown — emit a first-class
        # bearish signal so risk dashboards / short-watch surfaces can use it,
        # but still suppress all bullish signals for this symbol.
        hs_stats: dict[str, PatternScanStats] = {
            "head_shoulders": PatternScanStats("head_shoulders", 1, 1, 0)
        }
        hs_signal = _build_head_shoulders_signal(frame, neckline=_neckline, config=config)
        if hs_signal is None:
            return pd.DataFrame(), hs_stats
        signals_df = pd.DataFrame([hs_signal.to_record()])
        # Skip score normalisation (it assumes bullish primary scoring).
        signals_df["pattern_score"] = 0.0
        signals_df["pattern_rank"] = 1
        signals_df["pattern_priority_score"] = 0.0
        signals_df["pattern_priority_rank"] = 1
        signals_df["pattern_operational_tier"] = "suppression_only"
        return signals_df, hs_stats

    # --- Smoothed-extrema detectors ---
    se_detectors = [
        detect_cup_handle_signals,
        detect_round_bottom_signals,
        detect_double_bottom_signals,
        lambda *args, **kwargs: detect_flag_signals(*args, **kwargs, high_tight_only=False),
        lambda *args, **kwargs: detect_flag_signals(*args, **kwargs, high_tight_only=True),
        detect_ascending_triangle_signals,
        detect_symmetrical_triangle_signals,
        detect_ascending_base_signals,
    ]

    # --- Frame-only detectors (no smoothed/extrema needed) ---
    fo_detectors = [
        detect_vcp_signals,
        detect_flat_base_signals,
        detect_stage2_reclaim_signals,
        detect_3wt_signals,
        detect_darvas_box_signals,
        detect_pocket_pivot_signals,
        detect_inside_week_breakout_signals,
        detect_inside_day_signals,
    ]
    young_detectors = [
        detect_ipo_base_signals,
    ]

    rows: list[dict[str, object]] = []
    stats: dict[str, PatternScanStats] = {}

    if len(frame) >= int(config.min_history_bars):
        for detector in se_detectors:
            signals, detector_stats = detector(frame, smoothed=smoothed, extrema=extrema_list, config=config)
            stats[detector_stats.pattern_type] = detector_stats
            rows.extend(signal.to_record() for signal in signals)

        for detector in fo_detectors:
            signals, detector_stats = detector(frame, smoothed=smoothed, extrema=extrema_list, config=config)
            stats[detector_stats.pattern_type] = detector_stats
            rows.extend(signal.to_record() for signal in signals)

    if len(frame) >= int(getattr(config, "ipo_base_min_history_bars", 35)):
        for detector in young_detectors:
            signals, detector_stats = detector(frame, smoothed=smoothed, extrema=extrema_list, config=config)
            stats[detector_stats.pattern_type] = detector_stats
            rows.extend(signal.to_record() for signal in signals)

    if not rows:
        return pd.DataFrame(), stats
    signals_df = pd.DataFrame(rows).drop_duplicates(subset=["signal_id"]).reset_index(drop=True)
    return _score_signal_rows(signals_df), stats
