"""Contracts for deterministic candidate selection."""

from __future__ import annotations

CANDIDATE_GROUPS = [
    "BLOWOUT_RESULT_BREAKOUT",
    "FUND_VALUE_TECH_READY",
    "RESULT_VALUE_ACCUMULATION",
    "FUNDAMENTAL_WATCH",
    "RESULT_DOWNTURN_AVOID",
    "LEADING_SECTOR_BREAKOUT",
    "IMPROVING_SECTOR_STAGE2",
    "HIGH_RS_PULLBACK",
    "FUNDAMENTAL_IMPROVER",
    "RESULTS_OR_CATALYST_PENDING",
    "AVOID_RED_FLAG",
]

CANDIDATE_GROUP_PRIORITY = {
    "BLOWOUT_RESULT_BREAKOUT": 0,
    "FUND_VALUE_TECH_READY": 1,
    "RESULT_VALUE_ACCUMULATION": 2,
    "FUNDAMENTAL_IMPROVER": 3,
    "RESULTS_OR_CATALYST_PENDING": 4,
    "LEADING_SECTOR_BREAKOUT": 5,
    "FUNDAMENTAL_WATCH": 6,
    "IMPROVING_SECTOR_STAGE2": 7,
    "HIGH_RS_PULLBACK": 8,
    "RESULT_DOWNTURN_AVOID": 9,
    "AVOID_RED_FLAG": 10,
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
    "quarterly_result_bucket",
    "valuation_history_bucket",
    "valuation_reason",
    "final_candidate_score",
    "candidate_group",
    "candidate_reason",
    "next_action",
]

DEFAULT_MIN_CANDIDATES = 10
DEFAULT_MAX_CANDIDATES = 25
DEFAULT_TECHNICAL_POOL_SIZE = 100
