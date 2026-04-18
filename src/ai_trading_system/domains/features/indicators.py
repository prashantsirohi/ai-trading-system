import os
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from core.logging import logger

ta = None
talib = None


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
