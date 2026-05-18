"""Performance tracker for ranking system (Phase 0 of feedback loop).

Records every (date, symbol, rank_position, composite_score, watchlist_bucket)
from the rank stage and computes forward 5/10/20/60-day returns once they
mature. Provides cohort attribution, bucket attribution, and factor IC
metrics in a weekly digest.

This is the foundational measurement layer. Versioned configs, scheduled
backtests, paper trading, scorecards, and promotion lifecycle are layered
on top of this in later phases.

Scope note — UNIV_TOP1000 regime overlay
----------------------------------------
The regime overlay introduced in commit ``a2a072e`` lives upstream of the
tracker, in ``analytics/regime/breadth.py`` + ``analytics/regime/profiles.py``,
and is consumed by ``ranking/service.py`` and ``pipeline/stages/execute.py``.
It influences which symbols are *ranked* (and at what min-score), but the
perf_tracker schema stores the resulting rank rows unchanged — there is no
regime column in ``rank_cohort_performance``. To slice digest metrics by
regime, add a regime-tag column to ranked_signals.csv and extend
``RANKED_TO_TRACKER`` in ``constants.py``.
"""
