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
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
        )
        if ohlcv_db_path is None:
            ohlcv_db_path = os.path.join(
                project_root,
                "data",
                "ohlcv.duckdb",
            )
        if feature_store_dir is None:
            feature_store_dir = os.path.join(
                project_root,
                "data",
                "feature_store",
            )
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path, read_only=True)

    def _latest_market_adx_snapshot(
        self,
        exchange: str = "NSE",
        date: str | None = None,
    ) -> list[float]:
        """Load the latest ADX value per symbol from the feature store."""
        adx_dir = os.path.join(self.feature_store_dir, "adx", exchange)
        if not os.path.isdir(adx_dir):
            return []

        pattern = os.path.join(adx_dir, "*.parquet").replace("\\", "/")
        cutoff_clause = ""
        if date:
            cutoff_clause = f"WHERE CAST(timestamp AS DATE) <= DATE '{date}'"

        conn = duckdb.connect()
        try:
            query = f"""
                WITH ranked AS (
                    SELECT
                        symbol_id,
                        adx_14 AS latest_adx,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol_id
                            ORDER BY timestamp DESC
                        ) AS rn
                    FROM read_parquet('{pattern}')
                    {cutoff_clause}
                )
                SELECT latest_adx
                FROM ranked
                WHERE rn = 1 AND latest_adx IS NOT NULL
            """
            rows = conn.execute(query).fetchall()
            return [float(row[0]) for row in rows if row and row[0] is not None]
        finally:
            conn.close()

    def _market_breadth_snapshot(
        self,
        exchange: str = "NSE",
        date: str | None = None,
    ) -> Dict[str, float]:
        """Compute directional breadth for the latest market snapshot."""
        conn = self._get_conn()
        try:
            date_filter = ""
            if date:
                date_filter = f"AND CAST(timestamp AS DATE) <= DATE '{date}'"

            query = f"""
                WITH latest AS (
                  SELECT symbol_id, exchange, MAX(CAST(timestamp AS DATE)) AS d
                  FROM _catalog
                  WHERE exchange = ? {date_filter}
                  GROUP BY symbol_id, exchange
                ),
                px AS (
                  SELECT c.symbol_id, c.exchange, CAST(c.timestamp AS DATE) AS d, c.close
                  FROM _catalog c
                  JOIN latest l
                    ON c.symbol_id = l.symbol_id
                   AND c.exchange = l.exchange
                   AND CAST(c.timestamp AS DATE) = l.d
                ),
                sma AS (
                  SELECT
                    symbol_id,
                    exchange,
                    CAST(timestamp AS DATE) AS d,
                    AVG(close) OVER (
                        PARTITION BY symbol_id, exchange
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS sma50,
                    AVG(close) OVER (
                        PARTITION BY symbol_id, exchange
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS sma200,
                    LAG(close, 20) OVER (
                        PARTITION BY symbol_id, exchange
                        ORDER BY CAST(timestamp AS DATE)
                    ) AS close_20,
                    LAG(close, 50) OVER (
                        PARTITION BY symbol_id, exchange
                        ORDER BY CAST(timestamp AS DATE)
                    ) AS close_50
                  FROM _catalog
                  WHERE exchange = ? {date_filter}
                )
                SELECT
                  COUNT(*) AS n,
                  AVG(CASE WHEN p.close > s.sma50 THEN 1.0 ELSE 0.0 END) * 100 AS pct_above_50,
                  AVG(CASE WHEN p.close > s.sma200 THEN 1.0 ELSE 0.0 END) * 100 AS pct_above_200,
                  AVG(CASE WHEN s.close_20 IS NOT NULL AND p.close > s.close_20 THEN 1.0 ELSE 0.0 END) * 100 AS pct_up_20,
                  AVG(CASE WHEN s.close_50 IS NOT NULL AND p.close > s.close_50 THEN 1.0 ELSE 0.0 END) * 100 AS pct_up_50
                FROM px p
                JOIN sma s
                  ON p.symbol_id = s.symbol_id
                 AND p.exchange = s.exchange
                 AND p.d = s.d
            """
            row = conn.execute(query, (exchange, exchange)).fetchone()
        finally:
            conn.close()

        if not row:
            return {
                "symbols_analysed": 0,
                "pct_above_50": 0.0,
                "pct_above_200": 0.0,
                "pct_up_20": 0.0,
                "pct_up_50": 0.0,
                "breadth_score": 0.0,
            }

        pct_above_50 = float(row[1] or 0.0)
        pct_above_200 = float(row[2] or 0.0)
        pct_up_20 = float(row[3] or 0.0)
        pct_up_50 = float(row[4] or 0.0)
        breadth_score = float(
            np.nanmean([pct_above_50, pct_above_200, pct_up_20, pct_up_50])
        )
        return {
            "symbols_analysed": int(row[0] or 0),
            "pct_above_50": round(pct_above_50, 2),
            "pct_above_200": round(pct_above_200, 2),
            "pct_up_20": round(pct_up_20, 2),
            "pct_up_50": round(pct_up_50, 2),
            "breadth_score": round(breadth_score, 2),
        }

    def _latest_symbol_adx(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        date: str | None = None,
    ) -> float:
        """Return the latest available ADX value for a symbol."""
        adx_path = os.path.join(
            self.feature_store_dir, "adx", exchange, f"{symbol_id}.parquet"
        )
        if os.path.exists(adx_path):
            df = pd.read_parquet(adx_path)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                if date:
                    cutoff = pd.to_datetime(date)
                    df = df[df["timestamp"] <= cutoff]
            if not df.empty:
                latest = df.sort_values("timestamp").iloc[-1]
                return float(latest.get("adx_14", latest.get("adx_value", 0)) or 0)

        conn = self._get_conn()
        try:
            return self._compute_adx_single(conn, symbol_id, exchange)
        finally:
            conn.close()

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
            adx = latest.get("adx_14", latest.get("adx_value", 0))
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
                    LAG(close) OVER w AS prev_close,
                    LAG(high) OVER w AS prev_high,
                    LAG(low) OVER w AS prev_low
                FROM _catalog
                WHERE symbol_id = '{symbol_id}' AND exchange = '{exchange}'
                WINDOW w AS (ORDER BY timestamp)
            ),
            tr_dm AS (
                SELECT
                    GREATEST(
                        ABS(high - prev_close),
                        ABS(low - prev_close),
                        ABS(high - low)
                    ) AS true_range,
                    CASE
                        WHEN (high - prev_high) > (prev_low - low) AND (high - prev_high) > 0
                        THEN (high - prev_high)
                        ELSE 0
                    END AS plus_dm,
                    CASE
                        WHEN (prev_low - low) > (high - prev_high) AND (prev_low - low) > 0
                        THEN (prev_low - low)
                        ELSE 0
                    END AS minus_dm
                FROM ohlc
                WHERE prev_close IS NOT NULL
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
        Compute aggregate market regime from Nifty 50 breadth and ADX snapshots.
        Returns dict with market_regime, avg_adx, pct_trending.
        """
        breadth = self._market_breadth_snapshot(exchange=exchange, date=date)
        if breadth["symbols_analysed"] == 0:
            return {"market_regime": "UNKNOWN", "avg_adx": 0, "pct_trending": 0}

        adx_values = self._latest_market_adx_snapshot(exchange=exchange, date=date)
        total = len(adx_values)
        pct_trend = (
            sum(value >= adx_threshold for value in adx_values) / max(total, 1) * 100
        )
        adx_median = float(np.nanmedian(adx_values)) if adx_values else 0.0
        breadth_score = breadth["breadth_score"]

        if breadth_score >= 60:
            market_bias = "BULLISH"
        elif breadth_score <= 40:
            market_bias = "BEARISH"
        else:
            market_bias = "NEUTRAL"

        if pct_trend >= 60 and adx_median >= 22:
            if market_bias == "BULLISH":
                market_regime = "STRONG_BULL_TREND"
            elif market_bias == "BEARISH":
                market_regime = "STRONG_BEAR_TREND"
            else:
                market_regime = "STRONG_TREND"
        elif market_bias == "BULLISH":
            market_regime = "BULLISH_MIXED"
        elif market_bias == "BEARISH":
            market_regime = "BEARISH_MIXED"
        elif 45 <= breadth["pct_above_50"] <= 55 and 45 <= breadth["pct_up_20"] <= 55:
            market_regime = "RANGE_BOUND"
        else:
            market_regime = "MIXED"

        logger.info(
            "Market regime: %s | bias=%s | trend_pct=%.1f | breadth=%.1f",
            market_regime,
            market_bias,
            pct_trend,
            breadth_score,
        )

        return {
            "market_regime": market_regime,
            "market_bias": market_bias,
            "avg_adx": round(float(np.nanmean(adx_values)) if adx_values else 0.0, 2),
            "adx_median": round(adx_median, 2),
            "pct_trending": round(pct_trend, 2),
            "trending_pct": round(pct_trend, 2),
            "symbols_analysed": breadth["symbols_analysed"],
            **breadth,
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
