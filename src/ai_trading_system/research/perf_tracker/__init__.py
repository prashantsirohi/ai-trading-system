"""Performance tracker for ranking system (Phase 0 of feedback loop).

Records every (date, symbol, rank_position, composite_score, watchlist_bucket)
from the rank stage and computes forward 5/10/20/60-day returns once they
mature. Provides cohort attribution, bucket attribution, and factor IC
metrics in a weekly digest.

This is the foundational measurement layer. Versioned configs, scheduled
backtests, paper trading, scorecards, and promotion lifecycle are layered
on top of this in later phases.
"""
