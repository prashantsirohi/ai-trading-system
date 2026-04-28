"""Weinstein stage classifier (S1/S2/S3/S4) on weekly OHLCV bars.

Pure logic. No I/O, no storage. Consumes the frame produced by
`weekly.to_weekly` and emits a row matching the weekly_stage_snapshot schema.

S3 uses the relaxed definition agreed for v1: flat 30W MA + price near highs
+ volatility expansion + not-clean-S2. Sub-labels (S3_EARLY/S3_CONFIRMED)
are deferred.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional

import numpy as np
import pandas as pd


# Tunable thresholds. Kept module-level so tests can monkeypatch.
FLAT_SLOPE = 0.005          # |ma30w_slope_4w| <= 0.5% counts as flat
TREND_SLOPE = 0.005         # slope above this is "trending"
NEAR_HIGH_PCT = 20.0        # within 20% of 52w high
NEAR_LOW_PCT = 30.0         # within 30% of 52w low
S1_MA_BAND = 0.05           # |close/ma30w - 1| < 5%
VOL_EXPANSION_RATIO = 1.1   # ATR%(10w) > 1.1 * ATR%(30w)
MIN_CONFIDENCE = 0.40       # below this -> UNDEFINED
MIN_WEEKS = 30              # need at least 30 bars for ma30w


@dataclass
class StageResult:
    symbol: Optional[str]
    week_end_date: pd.Timestamp
    stage_label: str            # S1/S2/S3/S4/UNDEFINED
    stage_confidence: float
    stage_transition: str       # NONE or e.g. S1_TO_S2
    ma10w: Optional[float]
    ma30w: Optional[float]
    ma40w: Optional[float]
    ma30w_slope_4w: Optional[float]
    weekly_rs_score: Optional[float]
    weekly_volume_ratio: Optional[float]
    support_level: Optional[float]
    resistance_level: Optional[float]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["week_end_date"] = pd.Timestamp(d["week_end_date"]).date()
        return d


def classify_latest(
    weekly: pd.DataFrame,
    *,
    symbol: Optional[str] = None,
    prior_stage: Optional[str] = None,
    weekly_rs_score: Optional[float] = None,
) -> StageResult:
    """Classify the most recent weekly bar in `weekly`."""
    if len(weekly) < MIN_WEEKS:
        last = weekly.index[-1] if len(weekly) else pd.NaT
        return StageResult(
            symbol=symbol,
            week_end_date=last,
            stage_label="UNDEFINED",
            stage_confidence=0.0,
            stage_transition=_transition(prior_stage, "UNDEFINED"),
            ma10w=None, ma30w=None, ma40w=None, ma30w_slope_4w=None,
            weekly_rs_score=weekly_rs_score,
            weekly_volume_ratio=None,
            support_level=None, resistance_level=None,
        )

    row = weekly.iloc[-1]
    scores = {
        "S1": _score_s1(weekly, row),
        "S2": _score_s2(weekly, row),
        "S3": _score_s3(weekly, row),
        "S4": _score_s4(weekly, row),
    }

    # Argmax with priority tiebreak: S2 > S4 > S3 > S1.
    priority = {"S2": 0, "S4": 1, "S3": 2, "S1": 3}
    best = max(scores.items(), key=lambda kv: (kv[1], -priority[kv[0]]))
    label, confidence = best
    if confidence < MIN_CONFIDENCE:
        label = "UNDEFINED"

    return StageResult(
        symbol=symbol,
        week_end_date=weekly.index[-1],
        stage_label=label,
        stage_confidence=round(float(confidence), 3),
        stage_transition=_transition(prior_stage, label),
        ma10w=_f(row.get("ma10w")),
        ma30w=_f(row.get("ma30w")),
        ma40w=_f(row.get("ma40w")),
        ma30w_slope_4w=_f(row.get("ma30w_slope_4w")),
        weekly_rs_score=weekly_rs_score,
        weekly_volume_ratio=_f(row.get("weekly_volume_ratio")),
        support_level=_support_level(weekly),
        resistance_level=_resistance_level(weekly),
    )


# ---- Per-stage scoring (each returns fraction of checks passed) ----

def _score_s2(weekly: pd.DataFrame, row: pd.Series) -> float:
    checks = [
        row["close"] > row["ma30w"],
        row["ma30w_slope_4w"] > TREND_SLOPE,
        row["ma10w"] > row["ma30w"],
        _safe(lambda: row["close"] > row["ma40w"], False),
        _near_high_pct(row) <= NEAR_HIGH_PCT * 0.75,  # inside 15% of 52w high
    ]
    return _frac(checks)


def _score_s4(weekly: pd.DataFrame, row: pd.Series) -> float:
    checks = [
        row["close"] < row["ma30w"],
        row["ma30w_slope_4w"] < -TREND_SLOPE,
        row["ma10w"] < row["ma30w"],
        _safe(lambda: row["close"] < row["ma40w"], False),
        _near_low_pct(row) <= 20.0,
    ]
    return _frac(checks)


def _score_s3(weekly: pd.DataFrame, row: pd.Series) -> float:
    near_high = _near_high_pct(row) <= NEAR_HIGH_PCT
    flat = abs(row["ma30w_slope_4w"]) <= FLAT_SLOPE
    vol_exp = _vol_expansion(row)
    not_clean_s2 = not (
        row["ma10w"] > row["ma30w"] * 1.03
        and row["ma30w_slope_4w"] > TREND_SLOPE
    )
    checks = [near_high, flat, vol_exp, not_clean_s2]
    return _frac(checks)


def _score_s1(weekly: pd.DataFrame, row: pd.Series) -> float:
    near_ma = abs(row["close"] / row["ma30w"] - 1.0) < S1_MA_BAND
    flat = abs(row["ma30w_slope_4w"]) < FLAT_SLOPE
    near_low = _near_low_pct(row) <= NEAR_LOW_PCT
    quiet_vol = _safe(lambda: row["weekly_volume_ratio"] < 1.0, False)
    ma10_near_ma30 = abs(row["ma10w"] / row["ma30w"] - 1.0) < 0.03
    checks = [near_ma, flat, near_low, quiet_vol, ma10_near_ma30]
    return _frac(checks)


# ---- Helpers ----

def _near_high_pct(row: pd.Series) -> float:
    hi = row.get("hi_52w")
    if hi is None or pd.isna(hi) or hi == 0:
        return 100.0
    return float((hi - row["close"]) / hi * 100.0)


def _near_low_pct(row: pd.Series) -> float:
    lo = row.get("lo_52w")
    if lo is None or pd.isna(lo) or lo == 0:
        return 100.0
    return float((row["close"] - lo) / lo * 100.0)


def _vol_expansion(row: pd.Series) -> bool:
    a10 = row.get("atr_pct_10w")
    a30 = row.get("atr_pct_30w")
    if a10 is None or a30 is None or pd.isna(a10) or pd.isna(a30) or a30 == 0:
        return False
    return float(a10) > VOL_EXPANSION_RATIO * float(a30)


def _support_level(weekly: pd.DataFrame) -> Optional[float]:
    # Min low of the prior 10 weeks (excluding the latest 2).
    if len(weekly) < 12:
        return None
    window = weekly["low"].iloc[-12:-2]
    return float(window.min()) if not window.empty else None


def _resistance_level(weekly: pd.DataFrame) -> Optional[float]:
    if len(weekly) < 12:
        return None
    window = weekly["high"].iloc[-12:-2]
    return float(window.max()) if not window.empty else None


def _transition(prior: Optional[str], current: str) -> str:
    if not prior or prior == current or prior == "UNDEFINED" or current == "UNDEFINED":
        return "NONE"
    return f"{prior}_TO_{current}"


def _frac(checks: list[bool]) -> float:
    if not checks:
        return 0.0
    return sum(1 for c in checks if bool(c)) / len(checks)


def _safe(fn, default):
    try:
        v = fn()
        if isinstance(v, float) and np.isnan(v):
            return default
        return bool(v)
    except (KeyError, TypeError, ValueError):
        return default


def _f(v) -> Optional[float]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        return None
    return float(v)
