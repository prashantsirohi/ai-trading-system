"""Single source of truth for perf_tracker factors and diagnostic thresholds.

Both the backfill (writes rows) and the digest / pipeline stage (reads + scores)
need to agree on which factor columns exist and how diagnostics interpret them.
Keeping them here avoids the 3-place sync that used to bite when a new factor
was added.
"""

from __future__ import annotations

# (source column in ranked_signals.csv → tracker column in rank_cohort_performance)
RANKED_TO_TRACKER: dict[str, str] = {
    "symbol_id": "symbol_id",
    "exchange": "exchange",
    "composite_score": "composite_score",
    "composite_score_adjusted": "composite_score_adjusted",
    "rank_mode": "rank_mode",
    "sector_name": "sector_name",
    "rel_strength_score": "factor_rs",
    "vol_intensity_score": "factor_vol",
    "trend_score_score": "factor_trend",
    "prox_high_score": "factor_prox",
    "delivery_pct_score": "factor_deliv",
    "sector_strength_score": "factor_sector",
    "momentum_acceleration_score": "factor_momentum_accel",
    "above_200dma_score": "factor_above_200dma",
    "liquidity_score": "factor_liquidity",
    "delivery_trend_score": "factor_delivery_trend",
}

# Tracker-side factor columns used by digest aggregations / IC computation.
# Order is preserved in the digest tables.
FACTOR_COLUMNS: tuple[str, ...] = (
    "factor_rs",
    "factor_vol",
    "factor_trend",
    "factor_prox",
    "factor_deliv",
    "factor_sector",
    "factor_momentum_accel",
    "factor_above_200dma",
    "factor_liquidity",
    "factor_delivery_trend",
)

IC_HORIZONS: tuple[int, ...] = (5, 10, 20)

WEIGHT_ACTIVATION_CANDIDATES: tuple[str, ...] = (
    "factor_liquidity",
    "factor_delivery_trend",
    "factor_above_200dma",
    "factor_momentum_accel",
)

WEIGHT_ACTIVATION_MIN_N = 30
WEIGHT_ACTIVATION_MIN_IC_20D = 0.02

# Drift detection thresholds.
DRIFT_THRESHOLD_PCT = 30.0          # recent IC drop vs baseline that triggers a flag
DRIFT_WARNING_MIN_RECENT_N = 1500   # below this, no alert is raised
DRIFT_CRITICAL_MIN_RECENT_N = 3000  # at/above this, critical can fire
DRIFT_CRITICAL_MIN_DELTA_IC = 0.03
DRIFT_CRITICAL_MIN_BASELINE_IC = 0.05

# Concentration interpretation thresholds (top-10 avg_20d - top-200 avg_20d, in pp).
CONCENTRATION_WEAK_DELTA = 0.50
CONCENTRATION_STRONG_DELTA = 1.50

# Factor coverage status thresholds.
COVERAGE_OK_PCT = 80.0

# Same-date bucket attribution: flag bucket rows below these as small-sample.
SAME_DATE_SMALL_SAMPLE_DAYS = 10
SAME_DATE_SMALL_SAMPLE_ROWS = 500

# Maturation guardrail: warn when matured rows / total rows is below this fraction.
MATURATION_WARNING_RATIO = 0.50

# Forward-return anomaly flag (raw close, no split adjustment).
# Returns whose absolute magnitude exceeds this in the 5-day window are most
# likely caused by corporate actions (splits/bonus) and should be reviewed.
FORWARD_RETURN_ANOMALY_5D_PCT = 50.0
FORWARD_RETURN_ANOMALY_THRESHOLDS: dict[int, float] = {
    5: 50.0,
    10: 75.0,
    20: 100.0,
    60: 200.0,
}

# Optional composition columns the digest displays per bucket when present.
COMPOSITION_OPTIONAL_COLUMNS: tuple[str, ...] = (
    "composite_score",
    "composite_score_adjusted",
    "factor_rs",
    "factor_sector",
    "factor_trend",
    "factor_prox",
    "factor_stage",
    "factor_conviction",
    "factor_vol",
    "factor_deliv",
    "factor_above_200dma",
    "factor_liquidity",
    "factor_delivery_trend",
    "volume_ratio_20",
    "delivery_pct",
    "prior_5d_return",
    "prior_20d_return",
)
