import os
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from ai_trading_system.platform.logging.logger import logger

ta = None
talib = None

_VOLUME_ZSCORE_OUTPUT_COLS = (
    "volume_zscore_20",
    "volume_zscore_50",
)


def add_multi_timeframe_returns(frame: pd.DataFrame) -> pd.DataFrame:
    """Add standard trailing returns across multiple horizons."""
    output = frame.copy()
    if output.empty:
        for period in [5, 20, 60, 120, 252]:
            output[f"return_{period}d"] = pd.Series(dtype=float)
        return output

    if "symbol" in output.columns and "symbol_id" not in output.columns:
        output["symbol_id"] = output["symbol"]

    group_col = "symbol_id" if "symbol_id" in output.columns else None
    if group_col is None or "close" not in output.columns:
        for period in [5, 20, 60, 120, 252]:
            output[f"return_{period}d"] = np.nan
        return output

    grouped = output.groupby(group_col, sort=False)
    for period in [5, 20, 60, 120, 252]:
        output[f"return_{period}d"] = grouped["close"].pct_change(period)
    return output


# ---------------------------------------------------------------------------
# Stage 2 Uptrend scoring (Weinstein methodology, NSE-calibrated)
# ---------------------------------------------------------------------------
_STAGE2_OUTPUT_COLS = (
    "sma_150",
    "sma150_slope_20d_pct",
    "sma200_slope_20d_pct",
    "stage2_score",
    "is_stage2_structural",
    "is_stage2_candidate",
    "is_stage2_uptrend",
    "stage2_label",
    "stage2_hard_fail_reason",
    "stage2_fail_reason",
    "volume_zscore_20",
    "volume_zscore_50",
)


def add_volume_zscore_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add prior-window volume anomaly z-scores per symbol.

    Uses shifted rolling statistics so the current bar is never included in
    its own baseline. When there is insufficient history or zero variance, the
    z-score is left as ``NaN``.
    """
    output = frame.copy()
    if output.empty or "volume" not in output.columns:
        for col in _VOLUME_ZSCORE_OUTPUT_COLS:
            output[col] = pd.Series(dtype=float)
        return output

    if "symbol" in output.columns and "symbol_id" not in output.columns:
        output["symbol_id"] = output["symbol"]

    volume = pd.to_numeric(output["volume"], errors="coerce")
    if "symbol_id" in output.columns:
        grouped = volume.groupby(output["symbol_id"], sort=False)
        avg_20_prior = grouped.transform(lambda series: series.shift(1).rolling(20, min_periods=20).mean())
        std_20_prior = grouped.transform(lambda series: series.shift(1).rolling(20, min_periods=20).std(ddof=0))
        avg_50_prior = grouped.transform(lambda series: series.shift(1).rolling(50, min_periods=50).mean())
        std_50_prior = grouped.transform(lambda series: series.shift(1).rolling(50, min_periods=50).std(ddof=0))
    else:
        avg_20_prior = volume.shift(1).rolling(20, min_periods=20).mean()
        std_20_prior = volume.shift(1).rolling(20, min_periods=20).std(ddof=0)
        avg_50_prior = volume.shift(1).rolling(50, min_periods=50).mean()
        std_50_prior = volume.shift(1).rolling(50, min_periods=50).std(ddof=0)

    output.loc[:, "volume_zscore_20"] = (
        (volume - avg_20_prior) / std_20_prior.replace(0, np.nan)
    )
    output.loc[:, "volume_zscore_50"] = (
        (volume - avg_50_prior) / std_50_prior.replace(0, np.nan)
    )
    return output


def add_stage2_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute Stage 2 structural and score-based trend features per bar.

    Points-based scoring (max 100) calibrated for NSE large/mid-cap daily OHLCV.

    Required input columns
    ----------------------
    close                -- price series
    sma_50               -- 50-bar SMA (or computed internally if absent)
    sma_200              -- 200-bar SMA (or computed internally if absent)
    near_52w_high_pct    -- pct distance from 52-week high (0 = at high)
    rel_strength_score   -- cross-sectional RS percentile 0-100
    volume_ratio_20      -- current volume / 20-bar avg volume

    Added columns
    -------------
    sma_150              -- 150-bar SMA (min 100 periods)
    sma150_slope_20d_pct -- 20-bar slope of sma_150 (%)
    sma200_slope_20d_pct -- 20-bar slope of sma_200 (%)
    stage2_score         -- 0-100 composite score
    is_stage2_structural -- strict Weinstein structural Stage 2 gate
    is_stage2_candidate  -- soft score-led discovery flag (stage2_score >= 50)
    is_stage2_uptrend    -- compatibility alias for is_stage2_structural
    stage2_label         -- 'strong_stage2' | 'stage2' | 'stage1_to_stage2' | 'non_stage2'
    stage2_hard_fail_reason -- comma-separated failed structural predicates
    stage2_fail_reason   -- hard fail reasons first, then softer score-only deficiencies
    """
    out = df.copy()

    # Guard: empty frame — add typed empty columns and return
    if out.empty or "close" not in out.columns:
        for col in _STAGE2_OUTPUT_COLS:
            if col in ("stage2_label", "stage2_hard_fail_reason", "stage2_fail_reason"):
                dtype = object
            elif col in ("is_stage2_structural", "is_stage2_candidate", "is_stage2_uptrend"):
                dtype = bool
            else:
                dtype = float
            out[col] = pd.Series(dtype=dtype)
        return out

    close = pd.to_numeric(out["close"], errors="coerce")
    out = add_volume_zscore_features(out)

    # ── SMA-150 (primary Stage 2 MA) ─────────────────────────────────────
    out.loc[:, "sma_150"] = close.rolling(150, min_periods=100).mean()

    # ── SMA references (use existing columns when present) ───────────────
    sma_150 = out["sma_150"]
    sma_200 = pd.to_numeric(
        out.get("sma_200", close.rolling(200, min_periods=100).mean()),
        errors="coerce",
    )
    # Ensure sma_200 column is in frame for downstream scoring
    if "sma_200" not in out.columns:
        out.loc[:, "sma_200"] = sma_200

    sma_150_ready = close.rolling(150, min_periods=150).count() >= 150
    sma_200_ready = close.rolling(200, min_periods=200).count() >= 200

    # ── SMA slopes (20-bar look-back, expressed as %) ────────────────────
    out.loc[:, "sma150_slope_20d_pct"] = ((sma_150 / sma_150.shift(20)) - 1.0) * 100.0
    out.loc[:, "sma200_slope_20d_pct"] = ((sma_200 / sma_200.shift(20)) - 1.0) * 100.0

    # ── Optional fields — use defaults when absent ────────────────────────
    near_52w = pd.to_numeric(
        out.get("near_52w_high_pct", pd.Series(999.0, index=out.index)),
        errors="coerce",
    ).fillna(999.0)

    rs_score = pd.to_numeric(
        out.get("rel_strength_score", pd.Series(0.0, index=out.index)),
        errors="coerce",
    ).fillna(0.0)

    vol_ratio = pd.to_numeric(
        out.get("volume_ratio_20", pd.Series(1.0, index=out.index)),
        errors="coerce",
    ).fillna(1.0)

    # ── Scoring (9 conditions, max 100 pts) ──────────────────────────────
    # Condition 1:  close > sma_150          → 15 pts
    # Condition 2:  close > sma_200          → 15 pts
    # Condition 3:  sma_150 > sma_200        → 15 pts  (MA alignment)
    # Condition 4:  sma200_slope_20d_pct > 0 → 15 pts  (SMA-200 rising)
    # Condition 5:  near_52w_high_pct ≤ 25%  → 10 pts  (upper half of range)
    # Condition 6:  near_52w_high_pct ≤ 15%  → 10 pts  (near breakout zone)
    # Condition 7:  rel_strength_score ≥ 70   → 10 pts  (top 30th pct)
    # Condition 8:  rel_strength_score ≥ 85   → 10 pts  (top 15th pct, cumulative)
    # Condition 9:  volume_ratio_20 > 1.2     → 10 pts  (demand confirmation)
    slope_200 = out["sma200_slope_20d_pct"].fillna(-1.0)

    score = pd.Series(0.0, index=out.index)
    score += np.where(close > sma_150, 15.0, 0.0)
    score += np.where(close > sma_200, 15.0, 0.0)
    score += np.where(sma_150 > sma_200, 15.0, 0.0)
    score += np.where(slope_200 > 0.0, 15.0, 0.0)
    score += np.where(near_52w <= 25.0, 10.0, 0.0)
    score += np.where(near_52w <= 15.0, 10.0, 0.0)  # cumulative with ≤25
    score += np.where(rs_score >= 70.0, 10.0, 0.0)
    score += np.where(rs_score >= 85.0, 10.0, 0.0)  # cumulative with ≥70
    score += np.where(vol_ratio > 1.2, 10.0, 0.0)

    out.loc[:, "stage2_score"] = score.clip(0.0, 100.0)

    structural_checks = {
        "below_sma150": sma_150_ready & (close > sma_150),
        "below_sma200": sma_200_ready & (close > sma_200),
        "sma150_below_sma200": sma_150_ready & sma_200_ready & (sma_150 > sma_200),
        "sma200_slope_negative": sma_200_ready & sma_200.shift(20).notna() & (out["sma200_slope_20d_pct"] > 0.0),
        "far_from_52w_high": near_52w.notna() & (near_52w <= 25.0),
    }

    structural_pass = pd.Series(True, index=out.index, dtype=bool)
    for passed in structural_checks.values():
        structural_pass &= passed.fillna(False)

    out.loc[:, "is_stage2_structural"] = structural_pass
    out.loc[:, "is_stage2_candidate"] = out["stage2_score"] >= 50.0
    out.loc[:, "is_stage2_uptrend"] = out["is_stage2_structural"]

    out.loc[:, "stage2_label"] = np.select(
        [
            out["is_stage2_structural"] & (out["stage2_score"] >= 85.0),
            out["is_stage2_structural"] & (out["stage2_score"] >= 70.0),
            (~out["is_stage2_structural"]) & (out["stage2_score"] >= 50.0),
        ],
        ["strong_stage2", "stage2", "stage1_to_stage2"],
        default="non_stage2",
    )

    hard_reasons_per_row: list[str] = []
    fail_reasons_per_row: list[str] = []
    for i in out.index:
        hard_reasons: list[str] = []
        for reason, passed in structural_checks.items():
            passed_value = passed.loc[i]
            if pd.isna(passed_value) or not bool(passed_value):
                hard_reasons.append(reason)
        if not hard_reasons:
            hard_reason_text = ""
        else:
            hard_reason_text = ",".join(hard_reasons)
        hard_reasons_per_row.append(hard_reason_text)

        soft_reasons: list[str] = []
        rs_value = rs_score.loc[i]
        vol_value = vol_ratio.loc[i]
        near_high_value = near_52w.loc[i]

        if pd.isna(rs_value) or rs_value < 70.0:
            soft_reasons.append("rs_below_70th_pctile")
        elif rs_value < 85.0:
            soft_reasons.append("rs_below_85th_pctile")

        if pd.isna(vol_value) or vol_value <= 1.2:
            soft_reasons.append("weak_volume")

        if pd.isna(near_high_value) or near_high_value > 15.0:
            soft_reasons.append("not_near_breakout_zone")

        ordered_reasons = hard_reasons + [reason for reason in soft_reasons if reason not in hard_reasons]
        fail_reasons_per_row.append(",".join(ordered_reasons))

    out.loc[:, "stage2_hard_fail_reason"] = hard_reasons_per_row
    out.loc[:, "stage2_fail_reason"] = fail_reasons_per_row

    return out


class FeatureEngine:
    """
    Feature Engine - Calculates technical indicators for trading signals.
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir

    def load_raw_data(self, symbol: str) -> pd.DataFrame:
        """Load raw OHLCV data for a symbol"""
        filepath = os.path.join(self.data_dir, "raw", "NSE_EQ", f"{symbol}.parquet")
        if os.path.exists(filepath):
            df = pd.read_parquet(filepath)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.set_index("timestamp")
            return df
        return pd.DataFrame()

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all technical indicators"""
        if df.empty or "close" not in df.columns:
            return df

        result = df.copy()

        if "open" in df.columns and "high" in df.columns and "low" in df.columns:
            result = self._calculate_ohlc_indicators(result)

        result = self._calculate_custom_indicators(result)

        return result

    def _calculate_ohlc_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate indicators using pure pandas (pandas-ta not available)"""
        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"] if "volume" in df.columns else None

        df["RSI"] = self._calculate_rsi(close, 14)

        macd, macd_signal, macd_hist = self._calculate_macd(close)
        df["MACD"] = macd
        df["MACD_signal"] = macd_signal
        df["MACD_hist"] = macd_hist

        df["ATR"] = self._calculate_atr(high, low, close, 14)

        df["EMA_20"] = close.ewm(span=20, adjust=False).mean()
        df["EMA_50"] = close.ewm(span=50, adjust=False).mean()
        df["EMA_200"] = close.ewm(span=200, adjust=False).mean()

        df["SMA_20"] = close.rolling(20).mean()
        df["SMA_50"] = close.rolling(50).mean()
        df["SMA_200"] = close.rolling(200).mean()

        supert, supert_d = self._calculate_supertrend(high, low, close, 10, 3)
        df["SUPERT_10_3"] = supert
        df["SUPERTd_10_3"] = supert_d

        if volume is not None:
            df["VWAP"] = ((high + low + close) / 3 * volume).cumsum() / volume.cumsum()

        return df

    def _calculate_rsi(self, close: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI"""
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def _calculate_macd(
        self, close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
    ):
        """Calculate MACD"""
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=signal, adjust=False).mean()
        macd_hist = macd - macd_signal
        return macd, macd_signal, macd_hist

    def _calculate_atr(
        self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14
    ) -> pd.Series:
        """Calculate ATR"""
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        return atr

    def _calculate_supertrend(
        self,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 10,
        multiplier: float = 3.0,
    ):
        """Calculate Supertrend"""
        tr = self._calculate_atr(high, low, close, period)
        atr = tr.rolling(window=period).mean()

        upper_band = (high + low) / 2 + multiplier * atr
        lower_band = (high + low) / 2 - multiplier * atr

        supert = pd.Series(index=close.index, dtype=float)
        supert_d = pd.Series(1, index=close.index)

        for i in range(1, len(close)):
            if close.iloc[i] > upper_band.iloc[i - 1]:
                supert.iloc[i] = lower_band.iloc[i]
                supert_d.iloc[i] = 1
            elif close.iloc[i] < lower_band.iloc[i - 1]:
                supert.iloc[i] = upper_band.iloc[i]
                supert_d.iloc[i] = -1
            else:
                supert.iloc[i] = supert.iloc[i - 1]
                supert_d.iloc[i] = supert_d.iloc[i - 1]

                if (
                    supert_d.iloc[i] == 1
                    and lower_band.iloc[i] < lower_band.iloc[i - 1]
                ):
                    lower_band.iloc[i] = lower_band.iloc[i - 1]
                if (
                    supert_d.iloc[i] == -1
                    and upper_band.iloc[i] > upper_band.iloc[i - 1]
                ):
                    upper_band.iloc[i] = upper_band.iloc[i - 1]

        return supert, supert_d

    def _calculate_custom_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate custom indicators"""
        if "close" not in df.columns:
            return df

        df["returns"] = df["close"].pct_change()
        df["log_returns"] = np.log(df["close"] / df["close"].shift(1))

        df["volatility_20"] = df["returns"].rolling(20).std()
        df["volatility_60"] = df["returns"].rolling(60).std()

        df["price_change_1d"] = df["close"].pct_change(1)
        df["price_change_5d"] = df["close"].pct_change(5)
        df["price_change_20d"] = df["close"].pct_change(20)

        if "volume" in df.columns:
            df["volume_sma_20"] = df["volume"].rolling(20).mean()
            df["volume_ratio"] = df["volume"] / df["volume_sma_20"]
            df["volume_spike"] = (df["volume"] > df["volume_sma_20"] * 2).astype(int)

        if "high" in df.columns and "low" in df.columns:
            df["range"] = df["high"] - df["low"]
            df["range_pct"] = df["range"] / df["close"] * 100

        for window in [5, 10, 20]:
            df[f"high_{window}d"] = df["high"].rolling(window).max()
            df[f"low_{window}d"] = df["low"].rolling(window).min()

        return df

    def calculate_trend_strength(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate trend strength indicators"""
        if "close" not in df.columns:
            return df

        if "EMA_20" in df.columns:
            ema_20 = df["EMA_20"]
        else:
            ema_20 = df["close"].ewm(span=20, adjust=False).mean()

        if "EMA_50" in df.columns:
            ema_50 = df["EMA_50"]
        else:
            ema_50 = df["close"].ewm(span=50, adjust=False).mean()

        df["trend_strength"] = (ema_20 - ema_50) / ema_50 * 100

        if "SMA_200" in df.columns:
            sma_200 = df["SMA_200"]
        elif "EMA_200" in df.columns:
            sma_200 = df["EMA_200"]
        else:
            sma_200 = df["close"].ewm(span=200, adjust=False).mean()
        df["distance_from_sma200"] = (df["close"] - sma_200) / sma_200 * 100

        return df

    def calculate_momentum(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate momentum indicators"""
        if "close" not in df.columns:
            return df

        df["momentum_10"] = df["close"] / df["close"].shift(10) - 1
        df["momentum_20"] = df["close"] / df["close"].shift(20) - 1

        for period in [12, 26]:
            df[f"roc_{period}"] = (df["close"] - df["close"].shift(period)) / df[
                "close"
            ].shift(period)

        return df

    def calculate_volume_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate volume-based indicators"""
        if "volume" not in df.columns:
            return df

        df["obv"] = (np.sign(df["close"].diff()) * df["volume"]).fillna(0).cumsum()

        if "high" in df.columns and "low" in df.columns and "close" in df.columns:
            typical_price = (df["high"] + df["low"] + df["close"]) / 3
            df["vwap_calc"] = (typical_price * df["volume"]).cumsum() / df[
                "volume"
            ].cumsum()

        return df

    def get_latest_features(self, symbol: str) -> Dict:
        """Get latest calculated features for a symbol"""
        df = self.load_raw_data(symbol)
        if df.empty:
            return {}

        df = self.calculate_indicators(df)
        df = self.calculate_trend_strength(df)
        df = self.calculate_momentum(df)
        df = self.calculate_volume_indicators(df)

        latest = df.iloc[-1].to_dict()
        return latest

    def save_features(self, df: pd.DataFrame, symbol: str) -> str:
        """Save calculated features to parquet"""
        os.makedirs(os.path.join(self.data_dir, "features"), exist_ok=True)
        filepath = os.path.join(self.data_dir, "features", f"{symbol}_features.parquet")
        df.to_parquet(filepath, index=True)
        logger.info(f"Saved features to {filepath}")
        return filepath

    def get_feature_list(self) -> List[str]:
        """Get list of available features"""
        return [
            "RSI",
            "MACD",
            "MACD_signal",
            "MACD_hist",
            "SUPERT_10_3",
            "ATR",
            "EMA_20",
            "EMA_50",
            "SMA_20",
            "SMA_50",
            "SMA_200",
            "VWAP",
            "returns",
            "log_returns",
            "volatility_20",
            "volatility_60",
            "price_change_1d",
            "price_change_5d",
            "price_change_20d",
            "volume_sma_20",
            "volume_ratio",
            "volume_spike",
            "trend_strength",
            "momentum_10",
            "momentum_20",
            "obv",
            "range",
            "range_pct",
        ]
