"""Fixture-based tests for the Weinstein weekly stage classifier.

Synthetic daily OHLCV is generated for each canonical regime, resampled to
weekly via `to_weekly`, then fed to `classify_latest`. We assert the label
and that the schema is fully populated.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.domains.ranking.weekly import to_weekly
from ai_trading_system.domains.ranking.stage_classifier import (
    StageResult,
    classify_latest,
)
from ai_trading_system.domains.ranking import stage_classifier


def _make_daily(closes: np.ndarray, *, start: str = "2024-01-01",
                vol: float = 100_000.0, vol_mul: np.ndarray | None = None,
                noise_pct: float = 0.005, seed: int = 7) -> pd.DataFrame:
    """Build a daily OHLCV frame from a close-price array."""
    rng = np.random.default_rng(seed)
    n = len(closes)
    idx = pd.bdate_range(start=start, periods=n)
    closes = np.asarray(closes, dtype=float)
    noise = rng.normal(0.0, noise_pct, size=n) * closes
    opens = closes + noise * 0.3
    highs = np.maximum(opens, closes) + np.abs(noise) + closes * 0.002
    lows = np.minimum(opens, closes) - np.abs(noise) - closes * 0.002
    if vol_mul is None:
        vol_mul = np.ones(n)
    volumes = (vol * vol_mul).astype(float)
    return pd.DataFrame(
        {"open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes},
        index=idx,
    )


def _series_uptrend(n: int = 320, start: float = 100.0, daily_pct: float = 0.0035) -> np.ndarray:
    return start * (1.0 + daily_pct) ** np.arange(n)


def _series_downtrend(n: int = 320, start: float = 200.0, daily_pct: float = 0.0035) -> np.ndarray:
    return start * (1.0 - daily_pct) ** np.arange(n)


def _series_base(n: int = 320, start: float = 100.0, drop_pct: float = 0.004) -> np.ndarray:
    """Long downtrend, then flat near the lows."""
    drop_n = n // 2
    flat_n = n - drop_n
    drop = start * (1.0 - drop_pct) ** np.arange(drop_n)
    flat = np.full(flat_n, drop[-1])
    return np.concatenate([drop, flat])


def _topping_daily(start: str = "2023-01-01") -> pd.DataFrame:
    """Build daily OHLCV directly so ATR can expand only in the chop phase.

    ~60 weeks uptrend with tight ranges, then ~25 weeks of wider chop near
    the peak. This produces:
      - ma30w covering mostly chop -> slope_4w ~ 0
      - atr_pct_10w (recent chop) >> atr_pct_30w (mix)
    """
    rng = np.random.default_rng(42)
    up_days = 250       # ~50 weeks
    chop_days = 175     # ~35 weeks
    up = 100.0 * (1.0 + 0.004) ** np.arange(up_days)
    peak = up[-1]
    # Distribution: chop centered slightly below the peak so the 30W MA
    # actually flattens instead of drifting up as old trend bars roll out.
    chop_center = peak * 0.95
    chop_close = chop_center * (1.0 + rng.normal(0.0, 0.025, size=chop_days))

    closes = np.concatenate([up, chop_close])
    n = len(closes)
    idx = pd.bdate_range(start=start, periods=n)

    # Phase-dependent intra-day range: tight during trend, wide during chop.
    range_pct = np.concatenate([
        np.full(up_days, 0.006),    # 0.6% bar range in trend
        np.full(chop_days, 0.040),  # 4% bar range in chop -> ATR expansion
    ])
    rng2 = np.random.default_rng(11)
    bar_range = closes * range_pct
    opens = closes - rng2.normal(0.0, 0.3, n) * bar_range
    highs = np.maximum(opens, closes) + np.abs(rng2.normal(0.5, 0.2, n)) * bar_range
    lows = np.minimum(opens, closes) - np.abs(rng2.normal(0.5, 0.2, n)) * bar_range
    volumes = np.full(n, 100_000.0)
    return pd.DataFrame(
        {"open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes},
        index=idx,
    )


def _classify(daily: pd.DataFrame, **kw) -> StageResult:
    return classify_latest(to_weekly(daily), **kw)


# ---------- Stage tests ----------

def test_s2_strong_uptrend():
    daily = _make_daily(_series_uptrend())
    res = _classify(daily, symbol="UPTREND")
    assert res.stage_label == "S2"
    assert res.stage_confidence >= 0.6
    assert res.ma10w > res.ma30w
    assert res.ma30w_slope_4w > 0
    assert res.support_level is not None
    assert res.resistance_level is not None


def test_s4_strong_downtrend():
    daily = _make_daily(_series_downtrend())
    res = _classify(daily, symbol="DOWN")
    assert res.stage_label == "S4"
    assert res.stage_confidence >= 0.6
    assert res.ma10w < res.ma30w
    assert res.ma30w_slope_4w < 0


def test_s1_basing_after_decline():
    daily = _make_daily(_series_base(), noise_pct=0.001)
    res = _classify(daily, symbol="BASE")
    assert res.stage_label == "S1"
    assert res.stage_confidence >= 0.6
    # Price hugging the 30W MA in a base
    assert abs(res.ma10w / res.ma30w - 1.0) < 0.05


def test_s3_topping_pattern():
    daily = _topping_daily()
    res = _classify(daily, symbol="TOP")
    # Relaxed S3 should win over S2 once slope flattens and vol expands.
    assert res.stage_label == "S3", f"got {res.stage_label} (conf={res.stage_confidence})"
    assert res.stage_confidence >= 0.6


# ---------- Schema / edge cases ----------

def test_insufficient_history_returns_undefined():
    daily = _make_daily(_series_uptrend(n=80))  # ~16 weeks, < MIN_WEEKS
    res = _classify(daily, symbol="SHORT")
    assert res.stage_label == "UNDEFINED"
    assert res.stage_confidence == 0.0
    assert res.stage_transition == "NONE"


def test_transition_emits_when_prior_differs():
    daily = _make_daily(_series_uptrend())
    res = _classify(daily, symbol="X", prior_stage="S1")
    assert res.stage_label == "S2"
    assert res.stage_transition == "S1_TO_S2"
    assert res.bars_in_stage == 1
    assert res.stage_entry_date == res.week_end_date


def test_transition_none_when_same():
    daily = _make_daily(_series_uptrend())
    prior_entry = pd.Timestamp("2024-03-29")
    res = _classify(
        daily,
        symbol="X",
        prior_stage="S2",
        prior_bars_in_stage=7,
        prior_stage_entry_date=prior_entry,
    )
    assert res.stage_transition == "NONE"
    assert res.bars_in_stage == 8
    assert res.stage_entry_date == prior_entry


def test_s2_hysteresis_uses_lower_slope_threshold_for_existing_s2(monkeypatch):
    idx = pd.date_range("2024-01-05", periods=40, freq="W-FRI")
    weekly = pd.DataFrame(
        {
            "open": 100.0,
            "high": 106.0,
            "low": 98.0,
            "close": 105.0,
            "volume": 100_000.0,
            "ma10w": 104.0,
            "ma30w": 100.0,
            "ma40w": 99.0,
            "ma30w_slope_4w": 0.003,
            "weekly_volume_ratio": 1.2,
            "hi_52w": 105.0,
            "lo_52w": 80.0,
            "atr_pct_10w": 0.02,
            "atr_pct_30w": 0.02,
        },
        index=idx,
    )
    monkeypatch.setattr(stage_classifier, "MIN_CONFIDENCE", 0.9)

    entering = classify_latest(weekly, symbol="NEW", prior_stage="S1")
    remaining = classify_latest(weekly, symbol="OLD", prior_stage="S2")

    assert entering.stage_label == "UNDEFINED"
    assert remaining.stage_label == "S2"


def test_to_dict_full_schema():
    daily = _make_daily(_series_uptrend())
    res = _classify(daily, symbol="X", weekly_rs_score=82.5, prior_stage="S1")
    d = res.to_dict()
    expected_keys = {
        "symbol", "week_end_date", "stage_label", "stage_confidence",
        "stage_transition", "ma10w", "ma30w", "ma40w", "ma30w_slope_4w",
        "weekly_rs_score", "weekly_volume_ratio", "support_level",
        "resistance_level", "bars_in_stage", "stage_entry_date",
    }
    assert expected_keys.issubset(d.keys())
    assert d["weekly_rs_score"] == 82.5
    assert d["stage_label"] == "S2"
    assert d["stage_transition"] == "S1_TO_S2"


def test_weekly_resample_columns():
    daily = _make_daily(_series_uptrend())
    weekly = to_weekly(daily)
    for col in ("ma10w", "ma30w", "ma40w", "ma30w_slope_4w",
                "weekly_volume_ratio", "hi_52w", "lo_52w",
                "atr_pct_10w", "atr_pct_30w"):
        assert col in weekly.columns


def test_to_weekly_rejects_missing_columns():
    bad = pd.DataFrame(
        {"close": [1.0, 2.0]},
        index=pd.bdate_range("2024-01-01", periods=2),
    )
    with pytest.raises(ValueError):
        to_weekly(bad)


def test_analytics_shim_reexports_same_module():
    import analytics.weekly as a_weekly
    import analytics.stage_classifier as a_cls
    from ai_trading_system.domains.ranking import weekly as canon_weekly
    from ai_trading_system.domains.ranking import stage_classifier as canon_cls

    assert a_weekly is canon_weekly
    assert a_cls is canon_cls
