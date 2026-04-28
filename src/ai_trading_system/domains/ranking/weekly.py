"""Weekly resample + indicator helpers for Weinstein stage analysis.

Inputs are daily OHLCV. Outputs are W-FRI bars with derived columns used
downstream by stage_classifier.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


WEEKLY_RULE = "W-FRI"


def to_weekly(daily: pd.DataFrame) -> pd.DataFrame:
    """Resample a daily OHLCV frame to W-FRI weekly bars and attach indicators.

    The input must have a DatetimeIndex (or a 'date'/'timestamp' column) and
    columns: open, high, low, close, volume.
    """
    df = _ensure_dt_index(daily)
    required = {"open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"daily frame missing columns: {sorted(missing)}")

    weekly = (
        df.resample(WEEKLY_RULE)
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
        .dropna(subset=["close"])
        .copy()
    )

    weekly.loc[:, "ma10w"] = weekly["close"].rolling(10).mean()
    weekly.loc[:, "ma30w"] = weekly["close"].rolling(30).mean()
    weekly.loc[:, "ma40w"] = weekly["close"].rolling(40).mean()
    weekly.loc[:, "ma30w_slope_4w"] = weekly["ma30w"].pct_change(4, fill_method=None)
    weekly.loc[:, "vol20w_avg"] = weekly["volume"].rolling(20).mean()
    weekly.loc[:, "hi_52w"] = weekly["close"].rolling(52, min_periods=10).max()
    weekly.loc[:, "lo_52w"] = weekly["close"].rolling(52, min_periods=10).min()
    weekly.loc[:, "weekly_volume_ratio"] = weekly["volume"] / weekly["vol20w_avg"]

    # True range and ATR% (volatility expansion signal for S3)
    prev_close = weekly["close"].shift(1)
    tr = pd.concat(
        [
            weekly["high"] - weekly["low"],
            (weekly["high"] - prev_close).abs(),
            (weekly["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    weekly.loc[:, "atr10w"] = tr.rolling(10).mean()
    weekly.loc[:, "atr30w"] = tr.rolling(30).mean()
    weekly.loc[:, "atr_pct_10w"] = weekly["atr10w"] / weekly["close"]
    weekly.loc[:, "atr_pct_30w"] = weekly["atr30w"] / weekly["close"]

    return weekly


def _ensure_dt_index(daily: pd.DataFrame) -> pd.DataFrame:
    if isinstance(daily.index, pd.DatetimeIndex):
        return daily.sort_index()
    for col in ("date", "timestamp", "Date"):
        if col in daily.columns:
            out = daily.copy()
            out.loc[:, col] = pd.to_datetime(out[col])
            return out.set_index(col).sort_index()
    raise ValueError("daily frame needs DatetimeIndex or date/timestamp column")
