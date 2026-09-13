"""Shared recursive technical calculations, independent of storage.

EMA is seeded with the first close (pandas adjust=False). Supertrend retains
this repository's simple rolling ATR, with recursive final bands and +1/up,
-1/down direction. Warmup rows have no Supertrend or direction.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

KEY_COLUMNS = ["symbol_id", "exchange", "timestamp", "close"]


def supertrend_series(high, low, close, period=10, multiplier=3.0):
    if period < 1 or not np.isfinite(multiplier) or multiplier <= 0:
        raise ValueError("Supertrend requires a positive period and multiplier")
    previous_close = close.shift(1)
    tr = pd.concat([high - low, (high - previous_close).abs(),
                    (low - previous_close).abs()], axis=1).max(axis=1)
    valid = high.notna() & low.notna() & close.notna()
    atr = tr.where(valid).rolling(period, min_periods=period).mean()
    midpoint = (high + low) / 2
    upper = (midpoint + multiplier * atr).to_numpy(copy=True)
    lower = (midpoint - multiplier * atr).to_numpy(copy=True)
    prices = close.to_numpy()
    trend = np.full(len(close), np.nan)
    direction = np.full(len(close), np.nan)
    for i in range(len(close)):
        if not (np.isfinite(upper[i]) and np.isfinite(lower[i]) and np.isfinite(prices[i])):
            continue
        if i == 0 or not np.isfinite(trend[i - 1]):
            direction[i] = -1
        else:
            if not (upper[i] < upper[i - 1] or prices[i - 1] > upper[i - 1]):
                upper[i] = upper[i - 1]
            if not (lower[i] > lower[i - 1] or prices[i - 1] < lower[i - 1]):
                lower[i] = lower[i - 1]
            if direction[i - 1] == -1:
                direction[i] = 1 if prices[i] > upper[i] else -1
            else:
                direction[i] = -1 if prices[i] < lower[i] else 1
        trend[i] = lower[i] if direction[i] == 1 else upper[i]
    return pd.Series(trend, index=close.index), pd.Series(direction, index=close.index)


def compute_recursive_feature(prices, feature, *, periods=(12, 26, 50, 200),
                              fast=12, slow=26, signal=9, period=10, multiplier=3.0):
    """Calculate each listing independently in time order; round only outputs."""
    if feature == "ema":
        if not periods or any(int(p) != p or p < 1 for p in periods):
            raise ValueError("EMA periods must be positive integers")
        columns = [f"ema_{int(p)}" for p in periods]
    elif feature == "macd":
        if any(int(p) != p or p < 1 for p in (fast, slow, signal)):
            raise ValueError("MACD periods must be positive integers")
        columns = ["macd_line", f"macd_signal_{signal}", "macd_histogram"]
    elif feature == "supertrend":
        columns = [f"supertrend_{period}_{int(multiplier)}", f"supertrend_dir_{period}_{int(multiplier)}"]
    else:
        raise ValueError(f"Unsupported recursive feature: {feature}")
    frames = []
    for _, group in prices.groupby(["symbol_id", "exchange"], sort=True):
        group = group.sort_values("timestamp").reset_index(drop=True)
        result = group[KEY_COLUMNS].copy()
        close = group["close"]
        if feature == "ema":
            for p, column in zip(periods, columns):
                result[column] = close.ewm(span=int(p), adjust=False).mean()
        elif feature == "macd":
            line = close.ewm(span=fast, adjust=False).mean() - close.ewm(span=slow, adjust=False).mean()
            signal_line = line.ewm(span=signal, adjust=False).mean()
            result[columns[0]] = line
            result[columns[1]] = signal_line
            result[columns[2]] = line - signal_line
            result = result.iloc[slow - 1:]
        else:
            result[columns[0]], result[columns[1]] = supertrend_series(
                group["high"], group["low"], close, period, multiplier)
        result[columns] = result[columns].round(4)
        frames.append(result)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=KEY_COLUMNS + columns)
