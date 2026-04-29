"""Contracts for research and operational chart-pattern workflows."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from typing import Any


@dataclass(frozen=True)
class PatternBacktestConfig:
    """Configuration for research pattern detection and evaluation."""

    exchange: str = "NSE"
    data_domain: str = "research"
    symbols: tuple[str, ...] = ()
    smoothing_method: str = "kernel"
    bandwidth: float = 3.0
    extrema_prominence: float = 0.02
    min_history_bars: int = 120
    breakout_volume_ratio_min: float = 1.5
    volume_zscore_min: float = 2.0
    max_breakout_wait_bars: int = 15
    event_horizons: tuple[int, ...] = (5, 10, 20, 40)
    commission_rate: float = 0.001
    max_hold_bars: int = 20
    target_r_multiple: float = 3.0
    fallback_atr_stop_mult: float = 2.0
    prior_uptrend_lookback: int = 30
    prior_uptrend_min_pct: float = 0.15
    cup_depth_min: float = 0.15
    cup_depth_max: float = 0.35
    min_cup_width: int = 20
    max_cup_width: int = 130
    rim_tolerance_pct: float = 0.08
    handle_min_bars: int = 3
    handle_max_bars: int = 20
    handle_max_depth_pct: float = 0.12
    min_round_width: int = 20
    max_round_width: int = 160
    round_symmetry_min: float = 0.6
    round_symmetry_max: float = 1.4
    trough_near_pct: float = 0.03
    min_trough_dwell_bars: int = 3
    sample_charts_per_pattern: int = 2

    asc_tri_flat_tol: float = 0.015
    vcp_window_bars: int = 40
    vcp_price_contraction_factor: float = 0.85
    vcp_vol_contraction_factor: float = 0.90
    vcp_min_first_range_pct: float = 0.08
    flat_base_min_bars: int = 25
    flat_base_max_bars: int = 65
    flat_base_max_depth_pct: float = 0.15
    stage2_reclaim_lookback_bars: int = 20
    stage2_reclaim_max_extension_pct: float = 0.08
    stage2_reclaim_min_slope_pct: float = 0.0
    wt3_tight_pct: float = 0.015
    wt3_prior_adv: float = 0.20

    def to_metadata(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["symbols"] = list(self.symbols)
        payload["event_horizons"] = list(self.event_horizons)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_metadata(), sort_keys=True)


@dataclass(frozen=True)
class PatternScanConfig:
    """Configuration for live pattern scanning across research or operational data."""

    exchange: str = "NSE"
    data_domain: str = "operational"
    symbols: tuple[str, ...] = ()
    smoothing_method: str = "rolling"
    bandwidth: float = 3.0
    extrema_prominence: float = 0.02
    min_history_bars: int = 120
    breakout_volume_ratio_min: float = 1.5
    volume_zscore_min: float = 2.0
    max_breakout_wait_bars: int = 15
    recent_signal_max_age_bars: int = 5
    prior_uptrend_lookback: int = 30
    prior_uptrend_min_pct: float = 0.15
    cup_depth_min: float = 0.15
    cup_depth_max: float = 0.35
    min_cup_width: int = 20
    max_cup_width: int = 130
    rim_tolerance_pct: float = 0.08
    handle_min_bars: int = 3
    handle_max_bars: int = 20
    handle_max_depth_pct: float = 0.12
    min_round_width: int = 20
    max_round_width: int = 160
    round_symmetry_min: float = 0.6
    round_symmetry_max: float = 1.4
    trough_near_pct: float = 0.03
    min_trough_dwell_bars: int = 3
    double_bottom_min_separation: int = 10
    double_bottom_max_separation: int = 60
    double_bottom_trough_tolerance_pct: float = 0.04
    double_bottom_neckline_min_pct: float = 0.08
    cup_watchlist_buffer_pct: float = 0.03
    round_watchlist_buffer_pct: float = 0.03
    flag_pole_min_pct: float = 0.12
    flag_pole_min_bars: int = 5
    flag_pole_max_bars: int = 20
    flag_min_bars: int = 3
    flag_max_bars: int = 15
    flag_max_retracement_pct: float = 0.38
    flag_watchlist_buffer_pct: float = 0.02
    high_tight_pole_min_pct: float = 0.90
    high_tight_pole_max_bars: int = 40
    high_tight_flag_max_range_pct: float = 0.15
    high_tight_flag_max_retracement_pct: float = 0.25
    fallback_atr_stop_mult: float = 2.0

    asc_tri_flat_tol: float = 0.015
    vcp_window_bars: int = 40
    vcp_price_contraction_factor: float = 0.85
    vcp_vol_contraction_factor: float = 0.90
    vcp_min_first_range_pct: float = 0.08
    flat_base_min_bars: int = 25
    flat_base_max_bars: int = 65
    flat_base_max_depth_pct: float = 0.15
    stage2_reclaim_lookback_bars: int = 20
    stage2_reclaim_max_extension_pct: float = 0.08
    stage2_reclaim_min_slope_pct: float = 0.0
    wt3_tight_pct: float = 0.015
    wt3_prior_adv: float = 0.20

    def to_metadata(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["symbols"] = list(self.symbols)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_metadata(), sort_keys=True)


@dataclass(frozen=True)
class PatternEvent:
    """A confirmed breakout event produced by a research detector."""

    event_id: str
    symbol_id: str
    pattern_type: str
    pattern_start: str
    pattern_end: str
    breakout_date: str
    pattern_start_index: int
    pattern_end_index: int
    breakout_bar_index: int
    breakout_level: float
    invalidation_price: float
    left_pivot_date: str
    trough_date: str
    right_pivot_date: str
    handle_date: str | None
    pivot_labels: tuple[str, ...]
    pivot_indices: tuple[int, ...]
    pivot_dates: tuple[str, ...]
    pivot_prices: tuple[float, ...]
    cup_depth_pct: float
    width_bars: int
    handle_depth_pct: float | None = None
    handle_bars: int | None = None
    symmetry_ratio: float | None = None
    trough_dwell_bars: int = 0
    volume_dry_up: bool = False
    breakout_volume_confirmed: bool = False
    breakout_volume_ratio: float | None = None
    config_provenance: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["pivot_labels"] = json.dumps(list(self.pivot_labels))
        record["pivot_indices"] = json.dumps(list(self.pivot_indices))
        record["pivot_dates"] = json.dumps(list(self.pivot_dates))
        record["pivot_prices"] = json.dumps([round(float(value), 6) for value in self.pivot_prices])
        record["config_provenance"] = json.dumps(self.config_provenance, sort_keys=True)
        return record


@dataclass(frozen=True)
class PatternSignal:
    """Operational or research live signal produced by the pattern scanner."""

    signal_id: str
    symbol_id: str
    pattern_family: str
    pattern_state: str
    signal_direction: str
    pattern_start: str
    pattern_end: str
    signal_date: str
    pattern_start_index: int
    pattern_end_index: int
    signal_bar_index: int
    breakout_level: float
    watchlist_trigger_level: float
    invalidation_price: float
    pattern_score: float = 0.0
    pattern_rank: int | None = None
    setup_quality: float = 0.0
    pivot_labels: tuple[str, ...] = ()
    pivot_dates: tuple[str, ...] = ()
    pivot_prices: tuple[float, ...] = ()
    pivot_indices: tuple[int, ...] = ()
    volume_ratio_20: float | None = None
    volume_zscore_20: float | None = None
    volume_zscore_50: float | None = None
    rel_strength_score: float | None = None
    sector_rs_percentile: float | None = None
    breakout_volume_ratio: float | None = None
    width_bars: int | None = None
    volume_dry_up: bool = False
    cup_depth_pct: float | None = None
    handle_depth_pct: float | None = None
    symmetry_ratio: float | None = None
    trough_similarity_pct: float | None = None
    pole_rise_pct: float | None = None
    flag_tightness_pct: float | None = None
    flag_retracement_pct: float | None = None
    config_provenance: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["pivot_labels"] = json.dumps(list(self.pivot_labels))
        record["pivot_dates"] = json.dumps(list(self.pivot_dates))
        record["pivot_prices"] = json.dumps([round(float(value), 6) for value in self.pivot_prices])
        record["pivot_indices"] = json.dumps(list(self.pivot_indices))
        record["config_provenance"] = json.dumps(self.config_provenance, sort_keys=True)
        return record


@dataclass(frozen=True)
class PatternTrade:
    """Normalized trade simulation output for a confirmed pattern event."""

    event_id: str
    symbol_id: str
    pattern_type: str
    breakout_date: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    stop_price: float
    target_price: float
    exit_reason: str
    holding_bars: int
    gross_return: float
    net_return: float
    r_multiple: float
    mfe: float
    mae: float

    def to_record(self) -> dict[str, Any]:
        return asdict(self)
