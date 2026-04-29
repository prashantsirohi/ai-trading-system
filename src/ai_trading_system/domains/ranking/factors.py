"""Factor calculators for the ranking domain."""

from __future__ import annotations

import pandas as pd
import numpy as np

TREND_STRENGTH_WEIGHT = 0.7
TREND_ALIGNMENT_WEIGHT = 0.3


def apply_relative_strength(data: pd.DataFrame, *, return_frame: pd.DataFrame) -> pd.DataFrame:
    scores = data.copy()
    if return_frame is not None and not return_frame.empty:
        scores = scores.merge(
            return_frame,
            on=["symbol_id", "exchange"],
            how="left",
        )
    period_cols = [c for c in ["return_20", "return_60", "return_120"] if c in scores.columns]
    if len(period_cols) >= 2:
        for col in period_cols:
            scores.loc[:, col] = scores[col].fillna(0.0)
        rs_20 = scores["return_20"].rank(pct=True) * 100 if "return_20" in scores.columns else pd.Series(50, index=scores.index)
        rs_60 = scores["return_60"].rank(pct=True) * 100 if "return_60" in scores.columns else pd.Series(50, index=scores.index)
        rs_120 = scores["return_120"].rank(pct=True) * 100 if "return_120" in scores.columns else pd.Series(50, index=scores.index)
        scores.loc[:, "rel_strength"] = 0.2 * rs_20 + 0.5 * rs_60 + 0.3 * rs_120
    else:
        if "return_pct" not in scores.columns:
            if "return_20" in scores.columns:
                scores.loc[:, "return_pct"] = scores["return_20"]
            else:
                scores.loc[:, "return_pct"] = 0.0
        scores.loc[:, "rel_strength"] = scores["return_pct"].fillna(0.0)
    return scores


def apply_momentum_acceleration(data: pd.DataFrame) -> pd.DataFrame:
    """Compute short-term momentum acceleration from existing ROC inputs."""
    scores = data.copy()
    for col in ("return_5", "return_10", "return_20"):
        if col not in scores.columns:
            scores.loc[:, col] = 0.0
        scores.loc[:, col] = pd.to_numeric(scores[col], errors="coerce").fillna(0.0)

    acceleration = (
        (scores["return_5"] - scores["return_20"]) * 0.6
        + (scores["return_10"] - scores["return_20"]) * 0.4
    )

    rs_delta = pd.Series(0.0, index=scores.index)
    for candidate in ("rel_strength_delta", "rel_strength_slope", "rs_delta", "rs_slope"):
        if candidate in scores.columns:
            rs_delta = pd.to_numeric(scores[candidate], errors="coerce").fillna(0.0)
            break

    scores.loc[:, "momentum_acceleration"] = acceleration + (rs_delta * 0.25)
    return scores


def apply_volume_intensity(data: pd.DataFrame, *, volume_frame: pd.DataFrame) -> pd.DataFrame:
    scores = data.copy()
    if volume_frame is not None and not volume_frame.empty:
        volume_cols = [
            column
            for column in [
                "symbol_id",
                "exchange",
                "vol_20_avg",
                "vol_20_max",
                "volume_zscore_20",
            ]
            if column in volume_frame.columns
        ]
        scores = scores.merge(
            volume_frame[volume_cols],
            on=["symbol_id", "exchange"],
            how="left",
        )
    if "vol_20_avg" not in scores.columns:
        scores.loc[:, "vol_20_avg"] = np.nan
    if "vol_20_max" not in scores.columns:
        scores.loc[:, "vol_20_max"] = np.nan
    if "volume_zscore_20" not in scores.columns:
        scores.loc[:, "volume_zscore_20"] = np.nan

    scores.loc[:, "vol_intensity"] = (
        pd.to_numeric(scores["volume"], errors="coerce")
        / pd.to_numeric(scores["vol_20_avg"], errors="coerce").replace(0, np.nan)
    ).fillna(1.0)
    zscore = pd.to_numeric(scores["volume_zscore_20"], errors="coerce").fillna(0.0)
    ratio_component = scores["vol_intensity"].clip(lower=0.0, upper=5.0)
    z_component = (1.0 + zscore.clip(lower=-2.0, upper=5.0) / 2.0).clip(lower=0.0, upper=3.5)
    scores.loc[:, "volume_intensity_normalized"] = (ratio_component * 0.6) + (z_component * 0.4)
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
        scores.loc[:, "adx_14"] = 50.0
    if "sma_20" not in scores.columns:
        scores.loc[:, "sma_20"] = scores["close"]
    if "sma_50" not in scores.columns:
        scores.loc[:, "sma_50"] = scores["close"]

    # ADX captures directional strength regardless of whether price is currently
    # above or below the moving averages, so keep it independent from SMA posture.
    scores.loc[:, "adx_score"] = (
        pd.to_numeric(scores.get("adx_14", 50.0), errors="coerce")
        .fillna(0.0)
        .clip(lower=0.0, upper=50.0) / 50.0 * 100.0
    )
    scores.loc[:, "sma20_aligned"] = (
        (scores["close"] > scores["sma_20"].replace(0, np.nan)).fillna(False).astype(int)
    )
    scores.loc[:, "sma50_aligned"] = (
        (scores["close"] > scores["sma_50"].replace(0, np.nan)).fillna(False).astype(int)
    )

    # Directional posture is a separate sub-score:
    # - 40 points for clearing SMA20
    # - 60 points for clearing SMA50
    # This keeps full alignment valuable without wiping out genuine ADX strength.
    scores.loc[:, "sma_alignment_score"] = (
        scores["sma20_aligned"] * 40.0
        + scores["sma50_aligned"] * 60.0
    )

    scores.loc[:, "trend_score"] = (
        scores["adx_score"].fillna(0.0) * TREND_STRENGTH_WEIGHT
        + scores["sma_alignment_score"].fillna(0.0) * TREND_ALIGNMENT_WEIGHT
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
        scores.loc[:, "high_52w"] = scores["close"]
    scores.loc[:, "prox_high"] = (
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
        scores.loc[:, "delivery_pct"] = np.nan

    sector_col = None
    for candidate in ("sector_name", "sector"):
        if candidate in scores.columns:
            sector_col = candidate
            break

    delivery_numeric = pd.to_numeric(scores["delivery_pct"], errors="coerce")
    scores.loc[:, "delivery_pct_imputed"] = delivery_numeric.isna()

    if sector_col is not None:
        sector_medians = delivery_numeric.groupby(scores[sector_col]).transform("median")
    else:
        sector_medians = pd.Series(np.nan, index=scores.index, dtype=float)

    universe_median = delivery_numeric.dropna().median()
    if pd.isna(universe_median):
        universe_median = 20.0

    scores.loc[:, "delivery_pct_filled"] = (
        delivery_numeric
        .fillna(sector_medians)
        .fillna(float(universe_median))
    )
    scores.loc[:, "delivery_pct"] = scores["delivery_pct_filled"]
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
        scores.loc[:, "sector_rs_value"] = 0.5
        scores.loc[:, "stock_vs_sector_value"] = 0.0
        return scores

    cutoff = pd.to_datetime(date).normalize()
    sector_slice = sector_rs.loc[sector_rs.index <= cutoff]
    stock_vs_slice = stock_vs_sector.loc[stock_vs_sector.index <= cutoff]
    if sector_slice.empty or stock_vs_slice.empty:
        scores.loc[:, "sector_rs_value"] = 0.5
        scores.loc[:, "stock_vs_sector_value"] = 0.0
        return scores

    latest_sector = sector_slice.ffill().iloc[-1]
    latest_stock_vs = stock_vs_slice.ffill().iloc[-1]

    scores.loc[:, "sector_name"] = scores["symbol_id"].map(sector_map)
    scores.loc[:, "sector_rs_value"] = scores["sector_name"].map(latest_sector.to_dict())
    scores.loc[:, "stock_vs_sector_value"] = scores["symbol_id"].map(latest_stock_vs.to_dict())
    scores.loc[:, "sector_rs_value"] = scores["sector_rs_value"].fillna(0.5)
    scores.loc[:, "stock_vs_sector_value"] = scores["stock_vs_sector_value"].fillna(0.0)
    return scores


def compute_penalty_score(frame: pd.DataFrame) -> pd.DataFrame:
    """Compute additive penalty metadata while preserving core factor scores."""
    output = frame.copy()
    output["penalty_score"] = 0.0
    output["exhaustion_penalty"] = 0.0
    output["exhaustion_flag"] = ""
    output["pivot_distance_penalty"] = 0.0
    output["distance_from_pivot_atr"] = np.nan

    if {"close", "sma_200"}.issubset(output.columns):
        close = pd.to_numeric(output["close"], errors="coerce")
        sma_200 = pd.to_numeric(output["sma_200"], errors="coerce")
        output.loc[close < sma_200, "penalty_score"] += 10.0

    if "liquidity_score" in output.columns:
        liquidity = pd.to_numeric(output["liquidity_score"], errors="coerce")
        output.loc[liquidity < 0.20, "penalty_score"] += 10.0

    if {"atr_14", "close"}.issubset(output.columns):
        atr = pd.to_numeric(output["atr_14"], errors="coerce")
        close = pd.to_numeric(output["close"], errors="coerce").replace(0, np.nan)
        output.loc[(atr / close) > 0.08, "penalty_score"] += 5.0

    close = pd.to_numeric(output.get("close", pd.Series(np.nan, index=output.index)), errors="coerce")
    volume_z = pd.to_numeric(output.get("volume_zscore_20", pd.Series(np.nan, index=output.index)), errors="coerce")
    volume_spike = volume_z > 3.0
    severe_volume_spike = volume_z > 4.0

    bb_width_extreme = pd.Series(False, index=output.index)
    for column in ("bb_width_percentile", "bollinger_band_width_percentile", "bb_width_pctile"):
        if column in output.columns:
            values = pd.to_numeric(output[column], errors="coerce")
            threshold = 0.95 if values.dropna().le(1.0).all() else 95.0
            bb_width_extreme = bb_width_extreme | (values > threshold)

    short_ma_extension = pd.Series(np.nan, index=output.index, dtype=float)
    if "sma_20" in output.columns:
        sma_20 = pd.to_numeric(output["sma_20"], errors="coerce").replace(0, np.nan)
        short_ma_extension = (close / sma_20) - 1.0
    short_ma_extended = short_ma_extension > 0.12
    short_ma_severe = short_ma_extension > 0.20

    exhaustion_count = (
        volume_spike.fillna(False).astype(int)
        + bb_width_extreme.fillna(False).astype(int)
        + short_ma_extended.fillna(False).astype(int)
    )
    strong_exhaustion = severe_volume_spike.fillna(False) | short_ma_severe.fillna(False) | (exhaustion_count >= 2)
    mild_exhaustion = (exhaustion_count >= 1) & ~strong_exhaustion
    output.loc[mild_exhaustion, "exhaustion_penalty"] = 3.0
    output.loc[strong_exhaustion, "exhaustion_penalty"] = 8.0
    output.loc[mild_exhaustion, "exhaustion_flag"] = "mild_exhaustion"
    output.loc[strong_exhaustion, "exhaustion_flag"] = "strong_exhaustion"
    output.loc[:, "penalty_score"] += output["exhaustion_penalty"]

    pivot = pd.Series(np.nan, index=output.index, dtype=float)
    for column in ("pivot_price", "breakout_level", "resistance_level", "watchlist_trigger_level"):
        if column in output.columns:
            candidate = pd.to_numeric(output[column], errors="coerce")
            pivot = pivot.where(pivot.notna(), candidate)

    atr = pd.Series(np.nan, index=output.index, dtype=float)
    for column in ("atr_14", "atr_value", "atr_20"):
        if column in output.columns:
            candidate = pd.to_numeric(output[column], errors="coerce")
            atr = atr.where(atr.notna(), candidate)

    valid_pivot_distance = pivot.gt(0) & atr.gt(0) & close.notna()
    distance_atr = (close - pivot) / atr.replace(0, np.nan)
    output.loc[valid_pivot_distance, "distance_from_pivot_atr"] = distance_atr.loc[valid_pivot_distance]
    output.loc[valid_pivot_distance & (distance_atr > 1.5), "pivot_distance_penalty"] = 3.0
    output.loc[valid_pivot_distance & (distance_atr > 2.5), "pivot_distance_penalty"] = 6.0
    output.loc[:, "penalty_score"] += output["pivot_distance_penalty"]

    output.loc[:, "penalty_score"] = output["penalty_score"].clip(lower=0.0)
    return output


def add_signal_freshness(frame: pd.DataFrame) -> pd.DataFrame:
    """Add signal age and a simple linear decay score for prioritization."""
    output = frame.copy()
    if output.empty:
        output["signal_age"] = pd.Series(dtype=int)
        output["signal_decay_score"] = pd.Series(dtype=float)
        return output

    timestamp_col = "timestamp" if "timestamp" in output.columns else "date" if "date" in output.columns else None
    if timestamp_col is None:
        output.loc[:, "signal_age"] = 0
        output.loc[:, "signal_decay_score"] = 1.0
        return output

    ts = pd.to_datetime(output[timestamp_col], errors="coerce")
    if "signal_start_date" in output.columns:
        start_ts = pd.to_datetime(output["signal_start_date"], errors="coerce")
        age_days = (ts - start_ts).dt.days
    else:
        reference = ts.max()
        age_days = (reference - ts).dt.days

    output.loc[:, "signal_age"] = age_days.fillna(0).clip(lower=0).astype(int)
    output.loc[:, "signal_decay_score"] = (1.0 - (output["signal_age"] / 30.0)).clip(lower=0.0, upper=1.0)
    return output
