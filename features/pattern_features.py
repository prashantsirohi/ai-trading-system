from __future__ import annotations

import numpy as np
import pandas as pd


def compute_pattern_preconditions(frame: pd.DataFrame) -> pd.DataFrame:
    """Compute additive pattern-precondition features for downstream scans."""
    output = frame.copy()
    if output.empty:
        for col in (
            "base_tightness",
            "consolidation_range_pct",
            "volatility_contraction",
            "pullback_depth_pct",
            "resistance_slope",
        ):
            output[col] = pd.Series(dtype=float)
        return output

    high = pd.to_numeric(output.get("high"), errors="coerce")
    low = pd.to_numeric(output.get("low"), errors="coerce")
    close = pd.to_numeric(output.get("close"), errors="coerce")

    range_pct = ((high - low) / close.replace(0, np.nan)) * 100.0
    rolling_range = range_pct.rolling(20, min_periods=5)
    output["consolidation_range_pct"] = rolling_range.mean()
    output["base_tightness"] = 1.0 / (1.0 + output["consolidation_range_pct"])

    if "atr_14" in output.columns:
        atr_series = pd.to_numeric(output["atr_14"], errors="coerce")
        atr_baseline = atr_series.rolling(20, min_periods=5).mean()
        output["volatility_contraction"] = atr_series / atr_baseline.replace(0, np.nan)
    else:
        tr = pd.concat(
            [
                high - low,
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr_series = tr.rolling(14, min_periods=5).mean()
        atr_baseline = atr_series.rolling(20, min_periods=5).mean()
        output["volatility_contraction"] = atr_series / atr_baseline.replace(0, np.nan)

    rolling_high = high.rolling(20, min_periods=5).max()
    output["pullback_depth_pct"] = ((rolling_high - close) / rolling_high.replace(0, np.nan)) * 100.0

    resistance = high.rolling(10, min_periods=5).max()
    output["resistance_slope"] = resistance.diff(5) / 5.0
    return output
