"""Contracts shared across ranking domain services."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RankFactorDefinition:
    """Describe a raw factor input and its normalized score column."""

    weight_key: str
    raw_column: str
    score_column: str


PRIMARY_FACTORS: tuple[RankFactorDefinition, ...] = (
    RankFactorDefinition("relative_strength", "rel_strength", "rel_strength_score"),
    RankFactorDefinition("volume_intensity", "volume_intensity_normalized", "vol_intensity_score"),
    RankFactorDefinition("trend_persistence", "trend_score", "trend_score_score"),
    RankFactorDefinition("momentum_acceleration", "momentum_acceleration", "momentum_acceleration_score"),
    RankFactorDefinition("proximity_highs", "prox_high", "prox_high_score"),
    RankFactorDefinition("delivery_pct", "delivery_pct", "delivery_pct_score"),
)

DEFAULT_FACTOR_WEIGHTS: dict[str, float] = {
    "relative_strength": 0.23,
    "volume_intensity": 0.18,
    "trend_persistence": 0.12,
    "momentum_acceleration": 0.08,
    "proximity_highs": 0.17,
    "delivery_pct": 0.12,
    "sector_strength": 0.10,
}

STAGE2_FRESH_BARS_MAX = 8
STAGE2_MID_BARS_MAX = 15
STAGE2_FRESHNESS_BONUS = 4.0
STAGE2_MID_FRESHNESS_BONUS = 2.0
STAGE2_TRANSITION_BONUS = 5.0
STAGE2_TRANSITION_BONUS_BARS_MAX = 8

RANK_MODES: tuple[str, ...] = (
    "default",
    "momentum",
    "breakout",
    "defensive",
    "watchlist",
    "stage2_breakout",  # Stage 2 uptrend filter + breakout focus
)

RANKED_SIGNAL_COLUMNS: tuple[str, ...] = (
    "symbol_id",
    "exchange",
    "close",
    "composite_score",
    "composite_score_adjusted",
    "rank_mode",
    "eligible_rank",
    "rejection_reasons",
    "penalty_score",
    "rank_confidence",
    "signal_age",
    "signal_decay_score",
    "previous_rank_position",
    "rank_delta",
    "score_delta",
    "rank_change_limit",
    "rel_strength_score",
    "vol_intensity_score",
    "trend_score_score",
    "momentum_acceleration_score",
    "prox_high_score",
    "delivery_pct_score",
    "sector_strength_score",
    "rel_strength",
    "vol_intensity",
    "volume_intensity_normalized",
    "momentum_acceleration",
    "trend_score",
    "prox_high",
    "delivery_pct",
    "sector_rs_value",
    "stock_vs_sector_value",
    "sector_name",
    "high_52w",
    "vol_20_avg",
    "adx_14",
    "sma_20",
    "sma_50",
    "volume",
    "timestamp",
    "return_5",
    "return_10",
    "return_20",
    "return_60",
    "return_120",
    "volume_zscore_20",
    "exhaustion_penalty",
    "exhaustion_flag",
    "pivot_distance_penalty",
    "distance_from_pivot_atr",
    "sector_rank_within_sector",
    "sector_total_symbols",
    # Stage 2 uptrend enrichment columns (added in Sprint 1)
    "stage2_score",
    "is_stage2_structural",
    "is_stage2_candidate",
    "is_stage2_uptrend",
    "stage2_label",
    "stage2_hard_fail_reason",
    "stage2_fail_reason",
    "stage2_score_bonus",
    "stage2_freshness_bonus",
    "stage2_transition_bonus",
    "stage2_age_warning",
    "weekly_stage_label",
    "weekly_stage_confidence",
    "weekly_stage_transition",
    "bars_in_stage",
    "stage_entry_date",
    "sma200_slope_20d_pct",
    "sma_150",
)
