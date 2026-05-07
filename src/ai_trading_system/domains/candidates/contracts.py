"""Contracts for deterministic candidate selection."""

from __future__ import annotations

CANDIDATE_GROUPS = [
    "LEADING_SECTOR_BREAKOUT",
    "IMPROVING_SECTOR_STAGE2",
    "HIGH_RS_PULLBACK",
    "FUNDAMENTAL_IMPROVER",
    "RESULTS_OR_CATALYST_PENDING",
    "AVOID_RED_FLAG",
]

CANDIDATE_GROUP_PRIORITY = {
    "LEADING_SECTOR_BREAKOUT": 0,
    "FUNDAMENTAL_IMPROVER": 1,
    "RESULTS_OR_CATALYST_PENDING": 2,
    "IMPROVING_SECTOR_STAGE2": 3,
    "HIGH_RS_PULLBACK": 4,
    "AVOID_RED_FLAG": 5,
}

FINAL_CANDIDATE_COLUMNS = [
    "symbol",
    "name",
    "industry_group",
    "composite_score",
    "breakout_score",
    "pattern_score",
    "fundamental_score",
    "fundamental_tier",
    "fundamental_trend_label",
    "final_candidate_score",
    "candidate_group",
    "candidate_reason",
    "next_action",
]

DEFAULT_MIN_CANDIDATES = 10
DEFAULT_MAX_CANDIDATES = 25
DEFAULT_TECHNICAL_POOL_SIZE = 100
