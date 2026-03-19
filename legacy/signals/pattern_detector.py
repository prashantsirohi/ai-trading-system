import os
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SignalDetector:
    """
    Signal Engine - Detects trading patterns and generates signals.
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir

    def load_features(self, symbol: str) -> pd.DataFrame:
        """Load feature data for a symbol"""
        filepath = os.path.join(self.data_dir, "features", f"{symbol}_features.parquet")
        if os.path.exists(filepath):
            return pd.read_parquet(filepath)
        return pd.DataFrame()

    def detect_all_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect all trading signals"""
        if df.empty or "close" not in df.columns:
            return pd.DataFrame()

        signals = pd.DataFrame(index=df.index)
        signals["supertrend_breakout"] = self._detect_supertrend_breakout(df)
        signals["rsi_reversal"] = self._detect_rsi_reversal(df)
        signals["ma_crossover"] = self._detect_ma_crossover(df)
        signals["volume_breakout"] = self._detect_volume_breakout(df)
        signals["support_resistance"] = self._detect_sr_breakout(df)
        signals["rsi_oversold"] = self._detect_rsi_oversold(df)
        signals["rsi_overbought"] = self._detect_rsi_overbought(df)

        return signals

    def _detect_supertrend_breakout(self, df: pd.DataFrame) -> pd.Series:
        """Detect Supertrend breakout"""
        if "SUPERT_10_3" not in df.columns and "SUPERTd_10_3" not in df.columns:
            return pd.Series(0, index=df.index)

        close = df["close"]
        if "SUPERT_10_3" in df.columns:
            supert = df["SUPERT_10_3"]
            supert_direction = df.get("SUPERTd_10_3", pd.Series(0, index=df.index))
        else:
            return pd.Series(0, index=df.index)

        breakout = ((close > supert) & (supert_direction.shift(1) <= 0)).astype(int)
        return breakout.fillna(0)

    def _detect_rsi_reversal(self, df: pd.DataFrame) -> pd.Series:
        """Detect RSI reversal (bullish/bearish divergence)"""
        if "RSI" not in df.columns:
            return pd.Series(0, index=df.index)

        rsi = df["RSI"]
        rsi_ma = rsi.rolling(5).mean()

        bullish = ((rsi < 35) & (rsi_ma > rsi_ma.shift(1))).astype(int)
        bearish = ((rsi > 65) & (rsi_ma < rsi_ma.shift(1))).astype(int)

        return bullish - bearish

    def _detect_ma_crossover(self, df: pd.DataFrame) -> pd.Series:
        """Detect moving average crossover"""
        if "EMA_20" not in df.columns or "EMA_50" not in df.columns:
            return pd.Series(0, index=df.index)

        ema_20 = df["EMA_20"]
        ema_50 = df["EMA_50"]

        golden_cross = ((ema_20 > ema_50) & (ema_20.shift(1) <= ema_50.shift(1))).astype(int)
        death_cross = ((ema_20 < ema_50) & (ema_20.shift(1) >= ema_50.shift(1))).astype(int)

        return golden_cross - death_cross

    def _detect_volume_breakout(self, df: pd.DataFrame) -> pd.Series:
        """Detect volume breakout"""
        if "volume" not in df.columns or "volume_sma_20" not in df.columns:
            return pd.Series(0, index=df.index)

        volume_ratio = df["volume"] / df["volume_sma_20"]
        price_up = df["close"] > df["close"].shift(1)

        breakout = ((volume_ratio > 2) & price_up).astype(int)
        return breakout.fillna(0)

    def _detect_sr_breakout(self, df: pd.DataFrame) -> pd.Series:
        """Detect support/resistance breakout"""
        if "high" not in df.columns or "low" not in df.columns:
            return pd.Series(0, index=df.index)

        high_20 = df["high"].rolling(20).max()
        low_20 = df["low"].rolling(20).min()

        resistance_break = (df["close"] > high_20.shift(1)).astype(int)
        support_break = (df["close"] < low_20.shift(1)).astype(int)

        return resistance_break - support_break

    def _detect_rsi_oversold(self, df: pd.DataFrame) -> pd.Series:
        """Detect RSI oversold condition"""
        if "RSI" not in df.columns:
            return pd.Series(0, index=df.index)
        return (df["RSI"] < 30).astype(int)

    def _detect_rsi_overbought(self, df: pd.DataFrame) -> pd.Series:
        """Detect RSI overbought condition"""
        if "RSI" not in df.columns:
            return pd.Series(0, index=df.index)
        return (df["RSI"] > 70).astype(int)

    def generate_signals(self, symbol: str, min_strength: int = 1) -> pd.DataFrame:
        """Generate trading signals for a symbol"""
        df = self.load_features(symbol)
        if df.empty:
            return pd.DataFrame()

        signals = self.detect_all_signals(df)
        signals["symbol"] = symbol
        signals["close"] = df["close"]

        signal_cols = [col for col in signals.columns if col not in ["symbol", "close"]]
        signals["signal_strength"] = signals[signal_cols].sum(axis=1)

        filtered = signals[signals["signal_strength"] >= min_strength].copy()
        filtered["timestamp"] = filtered.index

        return filtered

    def get_latest_signals(self, symbol: str) -> Dict:
        """Get latest signals for a symbol"""
        signals = self.generate_signals(symbol)
        if signals.empty:
            return {}

        latest = signals.iloc[-1].to_dict()
        return latest

    def scan_all_symbols(self, symbols: List[str], min_strength: int = 1) -> pd.DataFrame:
        """Scan multiple symbols for signals"""
        all_signals = []

        for symbol in symbols:
            signals = self.generate_signals(symbol, min_strength)
            if not signals.empty:
                all_signals.append(signals)

        if not all_signals:
            return pd.DataFrame()

        combined = pd.concat(all_signals, ignore_index=True)
        combined = combined.sort_values("signal_strength", ascending=False)

        return combined

    def calculate_signal_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate overall signal score"""
        if "close" not in df.columns:
            return df

        signals = self.detect_all_signals(df)
        signal_cols = [col for col in signals.columns]

        df["signal_score"] = signals[signal_cols].sum(axis=1)

        df["signal_type"] = "neutral"
        for idx in df.index:
            if df.loc[idx, "signal_score"] > 0:
                df.loc[idx, "signal_type"] = "bullish"
            elif df.loc[idx, "signal_score"] < 0:
                df.loc[idx, "signal_type"] = "bearish"

        return df

    def save_signals(self, df: pd.DataFrame, symbol: str) -> str:
        """Save signals to parquet"""
        os.makedirs(os.path.join(self.data_dir, "signals"), exist_ok=True)
        filepath = os.path.join(self.data_dir, "signals", f"{symbol}_signals.parquet")
        df.to_parquet(filepath, index=True)
        logger.info(f"Saved signals to {filepath}")
        return filepath
