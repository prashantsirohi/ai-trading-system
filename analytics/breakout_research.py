"""Research helpers for evaluating breakout setup families over history."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from analytics.ml_engine import AlphaEngine
from analytics.training_dataset import TrainingDatasetBuilder


def build_breakout_dataset(
    project_root: str | Path,
    *,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """Build a research frame with OHLCV, indicators, and breakout-family features."""
    engine = AlphaEngine(data_domain="research")
    builder = TrainingDatasetBuilder(project_root=Path(project_root), data_domain="research")
    raw = engine.prepare_training_data(
        from_date=from_date,
        to_date=to_date,
        exchange=exchange,
        horizons=[5, 20],
    )
    df = builder._add_price_structure_features(raw.copy())
    df = df.sort_values(["symbol_id", "timestamp"]).reset_index(drop=True)
    by_symbol = df.groupby("symbol_id", group_keys=False)

    df["prior_base_high_30"] = by_symbol["high"].transform(
        lambda series: series.shift(1).rolling(30, min_periods=15).max()
    )
    df["prior_base_low_30"] = by_symbol["low"].transform(
        lambda series: series.shift(1).rolling(30, min_periods=15).min()
    )
    df["prior_base_high_60"] = by_symbol["high"].transform(
        lambda series: series.shift(1).rolling(60, min_periods=30).max()
    )
    df["prior_base_low_60"] = by_symbol["low"].transform(
        lambda series: series.shift(1).rolling(60, min_periods=30).min()
    )
    df["base_width_pct_30"] = (
        (df["prior_base_high_30"] - df["prior_base_low_30"])
        / df["prior_base_high_30"].replace(0, np.nan)
        * 100
    )
    df["base_width_pct_60"] = (
        (df["prior_base_high_60"] - df["prior_base_low_60"])
        / df["prior_base_high_60"].replace(0, np.nan)
        * 100
    )
    df["range_width_pct_20"] = (
        (df["prior_range_high_20"] - df["prior_range_low_20"])
        / df["prior_range_high_20"].replace(0, np.nan)
        * 100
    )
    df["breakout_pct_20"] = (
        (df["close"] - df["prior_range_high_20"])
        / df["prior_range_high_20"].replace(0, np.nan)
        * 100
    )
    df["breakout_pct_30"] = (
        (df["close"] - df["prior_base_high_30"])
        / df["prior_base_high_30"].replace(0, np.nan)
        * 100
    )
    df["atr_pct"] = df["atr_value"] / df["close"].replace(0, np.nan) * 100
    df["contraction_ratio"] = df["range_width_pct_20"] / df["base_width_pct_60"].replace(0, np.nan)
    df["near_52w_high_pct"] = df["dist_52w_high"] * 100
    df["supertrend_bullish"] = (df.get("st_signal", 0).fillna(0) > 0).astype(int)
    df["prev_st_signal"] = by_symbol["st_signal"].shift(1)
    df["supertrend_flip_up"] = (
        df["supertrend_bullish"].eq(1)
        & df["prev_st_signal"].fillna(0).le(0)
    )
    df["above_sma_20"] = df["dist_sma_20"] > 0
    df["above_sma_50"] = df["dist_sma_50"] > 0

    adx_usable = df["adx_value"].fillna(0).gt(0).mean() >= 0.05
    if adx_usable:
        df["trend_strength_ok"] = df["adx_value"].fillna(0).ge(18.0)
        df["trend_gate_source"] = "adx"
    else:
        df["trend_strength_ok"] = df["trend_alignment_score"].fillna(0).ge(2)
        df["trend_gate_source"] = "trend_alignment"

    common = (
        df["volume_ratio_20"].fillna(0).ge(1.2)
        & df["trend_strength_ok"]
        & df["supertrend_bullish"].eq(1)
        & df["above_sma_20"]
        & df["above_sma_50"]
    )

    df["legacy_range_breakout"] = (
        common
        & df["is_range_breakout_20"].eq(1)
        & df["breakout_pct_20"].between(0, 5, inclusive="both")
        & df["range_width_pct_20"].between(2, 35, inclusive="both")
        & df["near_52w_high_pct"].le(15)
    )
    df["base_breakout"] = (
        common
        & (df["close"] > df["prior_base_high_30"])
        & df["base_width_pct_30"].between(4, 18, inclusive="both")
        & df["base_width_pct_60"].between(6, 28, inclusive="both")
        & df["breakout_pct_20"].between(0.15, 4.0, inclusive="both")
        & df["contraction_ratio"].le(0.9)
        & df["near_52w_high_pct"].le(12)
    )
    df["contraction_breakout"] = (
        common
        & df["is_range_breakout_20"].eq(1)
        & df["range_width_pct_20"].between(2, 12, inclusive="both")
        & df["base_width_pct_60"].between(8, 30, inclusive="both")
        & df["contraction_ratio"].le(0.7)
        & df["breakout_pct_20"].between(0.5, 3.5, inclusive="both")
        & df["near_52w_high_pct"].le(10)
        & df["atr_pct"].le(5)
    )
    df["supertrend_flip_breakout"] = (
        common
        & df["supertrend_flip_up"]
        & df["is_range_breakout_20"].eq(1)
        & df["breakout_pct_20"].between(0.4, 3.0, inclusive="both")
        & df["range_width_pct_20"].between(3, 20, inclusive="both")
        & df["near_52w_high_pct"].le(14)
    )

    df["legacy_setup_quality"] = (
        df["breakout_pct_20"].clip(0, 5).fillna(0) * 8
        + df["volume_ratio_20"].clip(0, 4).fillna(0) * 14
        + df["adx_value"].clip(0, 60).fillna(0) * 0.6
        + (15 - df["near_52w_high_pct"].clip(0, 15).fillna(15)) * 2.0
        - df["range_width_pct_20"].clip(2, 35).fillna(35) * 0.8
    )
    df["base_setup_quality"] = (
        df["volume_ratio_20"].clip(0, 4).fillna(0) * 14
        + df["adx_value"].clip(0, 60).fillna(0) * 0.6
        + (12 - df["near_52w_high_pct"].clip(0, 12).fillna(12)) * 2.2
        + (18 - df["base_width_pct_30"].clip(4, 18).fillna(18)) * 1.2
        - df["breakout_pct_20"].clip(0, 4).fillna(0) * 1.5
    )
    df["contraction_setup_quality"] = (
        df["volume_ratio_20"].clip(0, 4).fillna(0) * 16
        + df["adx_value"].clip(0, 60).fillna(0) * 0.5
        + (10 - df["near_52w_high_pct"].clip(0, 10).fillna(10)) * 2.0
        + (0.8 - df["contraction_ratio"].clip(0, 0.8).fillna(0.8)) * 30
        - df["range_width_pct_20"].clip(2, 12).fillna(12) * 0.8
    )
    df["supertrend_flip_setup_quality"] = (
        df["volume_ratio_20"].clip(0, 4).fillna(0) * 12
        + df["adx_value"].clip(0, 60).fillna(0) * 0.55
        + (14 - df["near_52w_high_pct"].clip(0, 14).fillna(14)) * 1.8
        + df["breakout_pct_20"].clip(0, 3).fillna(0) * 6
    )
    return df


def summarize_breakout_period(
    df: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
    label: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Summarize breakout-family performance for a period."""
    period_df = df[
        (df["timestamp"] >= pd.Timestamp(start_date))
        & (df["timestamp"] <= pd.Timestamp(end_date))
    ].copy()
    families = [
        ("legacy_range_breakout", "legacy_setup_quality"),
        ("base_breakout", "base_setup_quality"),
        ("contraction_breakout", "contraction_setup_quality"),
        ("supertrend_flip_breakout", "supertrend_flip_setup_quality"),
    ]
    rows: list[dict[str, Any]] = []
    for family, score_col in families:
        signals = period_df[period_df[family].fillna(False)].copy()
        if signals.empty:
            rows.append(
                {
                    "period": label,
                    "setup_family": family,
                    "signal_count": 0,
                    "trading_days": 0,
                    "signals_per_month": 0.0,
                    "hit_rate_5d": np.nan,
                    "avg_return_5d": np.nan,
                    "median_return_5d": np.nan,
                    "hit_rate_20d": np.nan,
                    "avg_return_20d": np.nan,
                    "median_return_20d": np.nan,
                    "top3_hit_rate_5d": np.nan,
                    "top3_avg_return_5d": np.nan,
                    "top3_hit_rate_20d": np.nan,
                    "top3_avg_return_20d": np.nan,
                }
            )
            continue
        signals = signals.sort_values(["timestamp", score_col], ascending=[True, False])
        signals["rank_in_day"] = signals.groupby("timestamp")[score_col].rank(
            ascending=False,
            method="first",
        )
        top3 = signals[signals["rank_in_day"] <= 3].copy()
        trading_days = int(signals["timestamp"].dt.normalize().nunique())
        months = max(
            1,
            (pd.Timestamp(end_date).to_period("M") - pd.Timestamp(start_date).to_period("M")).n + 1,
        )
        rows.append(
            {
                "period": label,
                "setup_family": family,
                "signal_count": int(len(signals)),
                "trading_days": trading_days,
                "signals_per_month": round(len(signals) / months, 2),
                "hit_rate_5d": round(float((signals["return_5d"] > 0).mean()), 4),
                "avg_return_5d": round(float(signals["return_5d"].mean()), 4),
                "median_return_5d": round(float(signals["return_5d"].median()), 4),
                "hit_rate_20d": round(float((signals["return_20d"] > 0).mean()), 4),
                "avg_return_20d": round(float(signals["return_20d"].mean()), 4),
                "median_return_20d": round(float(signals["return_20d"].median()), 4),
                "top3_hit_rate_5d": round(float((top3["return_5d"] > 0).mean()), 4),
                "top3_avg_return_5d": round(float(top3["return_5d"].mean()), 4),
                "top3_hit_rate_20d": round(float((top3["return_20d"] > 0).mean()), 4),
                "top3_avg_return_20d": round(float(top3["return_20d"].mean()), 4),
            }
        )

    summary = pd.DataFrame(rows).sort_values(["period", "avg_return_20d"], ascending=[True, False])
    best = {}
    for horizon_col in ["avg_return_5d", "avg_return_20d", "top3_avg_return_5d", "top3_avg_return_20d"]:
        valid = summary.dropna(subset=[horizon_col])
        if not valid.empty:
            row = valid.sort_values(horizon_col, ascending=False).iloc[0]
            best[horizon_col] = {
                "setup_family": row["setup_family"],
                "value": float(row[horizon_col]),
            }
    return summary, best
