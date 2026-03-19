import os
import logging
import duckdb
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Literal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RegimeDetector:
    """
    Regime Detection via ADX + trend alignment.
    Classifies each trading day as:
      - TREND      : ADX >= 20 (strong trend, use trend-following strategy)
      - MEAN_REV   : ADX < 20  (weak/no trend, switch to mean-reversion)
    """

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
    ):
        if ohlcv_db_path is None:
            ohlcv_db_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "ohlcv.duckdb",
            )
        if feature_store_dir is None:
            feature_store_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "feature_store",
            )
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path)

    def detect_regime(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        date: str = None,
        adx_threshold: float = 20.0,
    ) -> Literal["TREND", "MEAN_REV"]:
        """
        Detect regime for a single symbol on a given date.
        Reads ADX from the feature store (Parquet) or computes from DuckDB.
        """
        adx_path = os.path.join(
            self.feature_store_dir, "adx", exchange, f"{symbol_id}.parquet"
        )

        if os.path.exists(adx_path):
            df = pd.read_parquet(adx_path)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if date:
                cutoff = pd.to_datetime(date)
                df = df[df["timestamp"] <= cutoff]
            if df.empty:
                return "MEAN_REV"
            latest = df.sort_values("timestamp").iloc[-1]
            adx = latest.get("adx_14", 0)
        else:
            conn = self._get_conn()
            try:
                result = conn.execute(
                    """
                    SELECT timestamp FROM _catalog
                    WHERE symbol_id = ? AND exchange = ?
                    ORDER BY timestamp DESC LIMIT 1
                """,
                    (symbol_id, exchange),
                ).fetchone()
                if not result:
                    return "MEAN_REV"
                adx = self._compute_adx_single(conn, symbol_id, exchange)
            finally:
                conn.close()

        return "TREND" if adx >= adx_threshold else "MEAN_REV"

    def _compute_adx_single(
        self, conn, symbol_id: str, exchange: str, period: int = 14
    ) -> float:
        result = conn.execute(f"""
            WITH ohlc AS (
                SELECT
                    symbol_id, exchange, timestamp,
                    high, low, close,
                    LAG(close) OVER w AS prev_close
                FROM _catalog
                WHERE symbol_id = '{symbol_id}' AND exchange = '{exchange}'
                WINDOW w AS (ORDER BY timestamp)
            ),
            tr_dm AS (
                SELECT
                    MAX(high) - MIN(low) AS tr,
                    GREATEST(
                        ABS(high - prev_close),
                        ABS(low - prev_close),
                        ABS(high - low)
                    ) AS true_range,
                    GREATEST(high - prev_close, prev_close - low, 0) AS plus_dm,
                    GREATEST(prev_close - low, high - prev_close, 0) AS minus_dm
                FROM ohlc
                GROUP BY symbol_id, exchange, timestamp, high, low, close, prev_close
            ),
            smoothed AS (
                SELECT
                    AVG(true_range) OVER w AS atr,
                    AVG(plus_dm) OVER w AS atr_plus_dm,
                    AVG(minus_dm) OVER w AS atr_minus_dm
                FROM tr_dm
                WINDOW w AS (ORDER BY ROW_NUMBER() OVER (ORDER BY true_range)
                             ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
            ),
            di AS (
                SELECT
                    ROUND(100.0 * atr_plus_dm / NULLIF(atr, 0), 4) AS plus_di,
                    ROUND(100.0 * atr_minus_dm / NULLIF(atr, 0), 4) AS minus_di,
                    ABS(plus_di - minus_di) AS di_diff,
                    plus_di + minus_di AS di_sum
                FROM smoothed
                WHERE atr > 0
            )
            SELECT ROUND(100.0 * AVG(di_diff / NULLIF(di_sum, 0)), 4) AS adx
            FROM di
        """).fetchone()
        return float(result[0]) if result and result[0] is not None else 0.0

    def detect_bulk_regimes(
        self,
        symbols: List[str],
        exchanges: List[str] = None,
        date: str = None,
        adx_threshold: float = 20.0,
    ) -> pd.DataFrame:
        """
        Detect regime for multiple symbols on the same date.
        Returns DataFrame with symbol_id, exchange, regime.
        """
        if exchanges is None:
            exchanges = ["NSE"]

        results = []
        for exc in exchanges:
            for sym in symbols:
                regime = self.detect_regime(sym, exc, date, adx_threshold)
                results.append(
                    {
                        "symbol_id": sym,
                        "exchange": exc,
                        "regime": regime,
                        "date": date,
                    }
                )

        return pd.DataFrame(results)

    def get_market_regime(
        self,
        benchmark_symbol: str = "^NSEI",
        exchange: str = "NSE",
        date: str = None,
        adx_threshold: float = 20.0,
    ) -> Dict:
        """
        Compute aggregate market regime from Nifty 50 (synthetic).
        Returns dict with market_regime, avg_adx, pct_trending.
        """
        conn = self._get_conn()
        try:
            df = conn.execute(
                """
                SELECT symbol_id, exchange, timestamp, close
                FROM _catalog
                WHERE exchange = ?
                AND timestamp IS NOT NULL
                ORDER BY timestamp
            """,
                (exchange,),
            ).fetchdf()
        finally:
            conn.close()

        if df.empty:
            return {"market_regime": "UNKNOWN", "avg_adx": 0, "pct_trending": 0}

        regime_counts = {"TREND": 0, "MEAN_REV": 0}
        sample_syms = df["symbol_id"].unique()[:100]

        for sym in sample_syms:
            r = self.detect_regime(sym, exchange, date, adx_threshold=adx_threshold)
            regime_counts[r] += 1

        total = regime_counts["TREND"] + regime_counts["MEAN_REV"]
        pct_trend = regime_counts["TREND"] / max(total, 1) * 100

        if pct_trend >= 60:
            market_regime = "STRONG_TREND"
        elif pct_trend >= 40:
            market_regime = "MIXED"
        else:
            market_regime = "RANGE_BOUND"

        logger.info(
            f"Market regime: {market_regime} ({regime_counts['TREND']}/{total} trending)"
        )

        return {
            "market_regime": market_regime,
            "pct_trending": round(pct_trend, 2),
            "symbols_analysed": total,
            "date": date,
        }

    def regime_summary(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        lookback_days: int = 60,
    ) -> pd.DataFrame:
        """
        Return regime time series for a symbol over lookback_days.
        """
        conn = self._get_conn()
        try:
            df = conn.execute(
                """
                SELECT timestamp FROM _catalog
                WHERE symbol_id = ? AND exchange = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """,
                (symbol_id, exchange, lookback_days),
            ).fetchdf()
        finally:
            conn.close()

        if df.empty:
            return pd.DataFrame()

        results = []
        for _, row in df.iterrows():
            ts = row["timestamp"]
            date_str = str(ts.date()) if hasattr(ts, "date") else str(ts)[:10]
            regime = self.detect_regime(symbol_id, exchange, date_str)
            results.append({"timestamp": ts, "regime": regime})

        return pd.DataFrame(results)
