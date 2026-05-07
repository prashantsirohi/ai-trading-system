"""Shared contracts for the fundamentals domain."""

from __future__ import annotations

FUNDAMENTAL_SCORE_COLUMNS = [
    "quality_score",
    "growth_score",
    "balance_sheet_score",
    "valuation_score",
    "ownership_score",
    "fundamental_score",
]

FUNDAMENTAL_OUTPUT_COLUMNS = [
    "snapshot_date",
    "symbol",
    "name",
    "industry_group",
    "industry",
    *FUNDAMENTAL_SCORE_COLUMNS,
    "fundamental_tier",
    "red_flags",
    "hard_red_flag",
    "screener_snapshot_date",
]

WATCHLIST_BUCKET_PRIORITY = {
    "ADD_TO_WATCHLIST": 0,
    "STUDY_ONLY": 1,
    "TECHNICAL_ONLY_RISK": 2,
    "AVOID_RED_FLAG": 3,
    "IGNORE_FOR_NOW": 4,
}

WATCHLIST_OUTPUT_COLUMNS = [
    "symbol",
    "name",
    "industry_group",
    "industry",
    "composite_score",
    "relative_strength",
    "volume_intensity",
    "trend_persistence",
    "proximity_to_highs",
    "delivery_pct",
    "sector_strength",
    "breakout_type",
    "breakout_score",
    "candidate_tier",
    "qualified",
    "pattern_family",
    "pattern_state",
    "pattern_score",
    "setup_quality",
    "quality_score",
    "growth_score",
    "balance_sheet_score",
    "valuation_score",
    "ownership_score",
    "fundamental_score",
    "fundamental_tier",
    "red_flags",
    "hard_red_flag",
    "fundamental_score_delta",
    "fundamental_trend_label",
    "trend_reason",
    "catalyst_score",
    "catalyst_type",
    "catalyst_summary",
    "evidence_source",
    "confidence",
    "final_watchlist_score",
    "watchlist_bucket",
    "watchlist_reason",
    "next_action",
]
