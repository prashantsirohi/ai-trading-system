"""Factor calculators for the ranking domain."""

from __future__ import annotations

import pandas as pd
import numpy as np


def apply_relative_strength(data: pd.DataFrame, *, return_frame: pd.DataFrame) -> pd.DataFrame:
    scores = data.copy()
    if return_frame is not None and not return_frame.empty:
        scores = scores.merge(
            return_frame[["symbol_id", "exchange", "return_pct"]],
            on=["symbol_id", "exchange"],
            how="left",
        )
    if "return_pct" not in scores.columns:
        scores["return_pct"] = 0.0
    scores["rel_strength"] = scores["return_pct"].fillna(0.0)
    return scores


def apply_volume_intensity(data: pd.DataFrame, *, volume_frame: pd.DataFrame) -> pd.DataFrame:
    scores = data.copy()
    if volume_frame is not None and not volume_frame.empty:
        scores = scores.merge(
            volume_frame[["symbol_id", "exchange", "vol_20_avg", "vol_20_max"]],
            on=["symbol_id", "exchange"],
            how="left",
        )
    scores["vol_intensity"] = (
        scores["volume"] / scores["vol_20_avg"].replace(0, np.nan)
    ).fillna(1.0)
    return scores


def apply_trend_persistence(
    data: pd.DataFrame,
    *,
    adx_frame: pd.DataFrame,
    sma_frame: pd.DataFrame,
) -> pd.DataFrame:
    scores = data.copy()
    if adx_frame is not None and not adx_frame.empty:
        scores = scores.merge(
            adx_frame[["symbol_id", "exchange", "adx_14"]],
            on=["symbol_id", "exchange"],
            how="left",
        )
    if sma_frame is not None and not sma_frame.empty:
        scores = scores.merge(
            sma_frame[["symbol_id", "exchange", "sma_20", "sma_50"]],
            on=["symbol_id", "exchange"],
            how="left",
        )

    if "adx_14" not in scores.columns:
        scores["adx_14"] = 50.0
    if "sma_20" not in scores.columns:
        scores["sma_20"] = scores["close"]
    if "sma_50" not in scores.columns:
        scores["sma_50"] = scores["close"]

    scores["adx_score"] = scores.get("adx_14", 50.0).fillna(50.0) * 2
    scores["sma20_aligned"] = (
        (scores["close"] > scores["sma_20"].replace(0, np.nan)).fillna(False).astype(int)
    )
    scores["sma50_aligned"] = (
        (scores["close"] > scores["sma_50"].replace(0, np.nan)).fillna(False).astype(int)
    )

    above_sma20 = scores["close"] > scores["sma_20"].replace(0, np.nan)
    above_sma50 = scores["close"] > scores["sma_50"].replace(0, np.nan)
    direction_multiplier = pd.Series(1.0, index=scores.index)
    direction_multiplier[~above_sma50] = 0.0
    direction_multiplier[~above_sma20 & above_sma50] = 0.5

    scores["trend_score"] = (
        scores["adx_score"].fillna(0.0) * 0.6 * direction_multiplier
        + scores["sma20_aligned"] * 25
        + scores["sma50_aligned"] * 15
    )
    return scores


def apply_proximity_highs(data: pd.DataFrame, *, highs_frame: pd.DataFrame) -> pd.DataFrame:
    scores = data.copy()
    if highs_frame is not None and not highs_frame.empty:
        scores = scores.merge(
            highs_frame[["symbol_id", "exchange", "high_52w"]],
            on=["symbol_id", "exchange"],
            how="left",
        )
    if "high_52w" not in scores.columns:
        scores["high_52w"] = scores["close"]
    scores["prox_high"] = (
        1 - (scores["close"] / scores["high_52w"].replace(0, np.nan))
    ).fillna(0.5) * 100
    return scores


def apply_delivery(data: pd.DataFrame, *, delivery_frame: pd.DataFrame) -> pd.DataFrame:
    scores = data.copy()
    if delivery_frame is not None and not delivery_frame.empty:
        scores = scores.merge(
            delivery_frame[["symbol_id", "exchange", "delivery_pct"]],
            on=["symbol_id", "exchange"],
            how="left",
        )
    if "delivery_pct" not in scores.columns:
        scores["delivery_pct"] = np.nan
    scores["delivery_pct"] = scores["delivery_pct"].fillna(20.0)
    return scores


def apply_sector_strength(
    data: pd.DataFrame,
    *,
    sector_rs: pd.DataFrame,
    stock_vs_sector: pd.DataFrame,
    sector_map: dict[str, str],
    date: str,
) -> pd.DataFrame:
    scores = data.copy()
    if sector_rs.empty or stock_vs_sector.empty or not sector_map:
        scores["sector_rs_value"] = 0.5
        scores["stock_vs_sector_value"] = 0.0
        return scores

    cutoff = pd.to_datetime(date).normalize()
    sector_slice = sector_rs.loc[sector_rs.index <= cutoff]
    stock_vs_slice = stock_vs_sector.loc[stock_vs_sector.index <= cutoff]
    if sector_slice.empty or stock_vs_slice.empty:
        scores["sector_rs_value"] = 0.5
        scores["stock_vs_sector_value"] = 0.0
        return scores

    latest_sector = sector_slice.ffill().iloc[-1]
    latest_stock_vs = stock_vs_slice.ffill().iloc[-1]

    scores["sector_name"] = scores["symbol_id"].map(sector_map)
    scores["sector_rs_value"] = scores["sector_name"].map(latest_sector.to_dict())
    scores["stock_vs_sector_value"] = scores["symbol_id"].map(latest_stock_vs.to_dict())
    scores["sector_rs_value"] = scores["sector_rs_value"].fillna(0.5)
    scores["stock_vs_sector_value"] = scores["stock_vs_sector_value"].fillna(0.0)
    return scores
