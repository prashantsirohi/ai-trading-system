import os
import time
import sqlite3
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureStore:
    """
    Feature Store & Compute Layer.

    Responsibilities:
    - Vectorized technical indicator computation via DuckDB SQL (RSI, ADX, MACD, SMA, etc.)
    - Persisted feature storage in partitioned Parquet (feature_store/<symbol>/<feature>.parquet)
    - Feature registry tracking metadata, versions, and staleness
    - Point-in-time joins to merge features with OHLCV data without look-ahead bias

    Architecture:
    - Computes features directly from the OHLCV catalog in ohlcv.duckdb
    - Writes feature snapshots to feature_store/ as partitioned Parquet files
    - Maintains _feature_registry table in ohlcv.duckdb for feature catalog
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
        os.makedirs(self.feature_store_dir, exist_ok=True)

        self._init_feature_registry()

    # ------------------------------------------------------------------ #
    #  DuckDB helpers                                                    #
    # ------------------------------------------------------------------ #

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path)

    # ------------------------------------------------------------------ #
    #  Feature Registry                                                  #
    # ------------------------------------------------------------------ #

    def _init_feature_registry(self):
        conn = self._get_conn()
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS _feat_id_seq START 1
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _feature_registry (
                feature_id      BIGINT  PRIMARY KEY DEFAULT nextval('_feat_id_seq'),
                feature_name    TEXT    NOT NULL,
                symbol_id      TEXT,
                exchange       TEXT,
                computed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rows_computed  BIGINT,
                lookback_days  INTEGER,
                params         TEXT,
                feature_file   TEXT,
                status         TEXT    DEFAULT 'pending',
                note           TEXT
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_feat_name
            ON _feature_registry(feature_name, symbol_id, exchange)
        """)
        conn.commit()
        conn.close()
        logger.info("Feature registry initialized")

    def register_feature(
        self,
        feature_name: str,
        symbol_id: str = None,
        exchange: str = None,
        rows_computed: int = 0,
        lookback_days: int = 0,
        params: dict = None,
        feature_file: str = None,
        status: str = "completed",
        note: str = None,
    ) -> int:
        conn = self._get_conn()
        feat_id_raw = conn.execute("SELECT nextval('_feat_id_seq')").fetchone()
        feat_id = int(feat_id_raw[0]) if feat_id_raw else 1

        conn.execute(
            """
            INSERT INTO _feature_registry
                (feature_id, feature_name, symbol_id, exchange, rows_computed,
                 lookback_days, params, feature_file, status, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                feat_id,
                feature_name,
                symbol_id,
                exchange,
                rows_computed,
                lookback_days,
                str(params) if params else None,
                feature_file,
                status,
                note,
            ),
        )
        conn.commit()
        conn.close()
        return feat_id

    def list_features(self, feature_name: str = None) -> pd.DataFrame:
        conn = self._get_conn()
        try:
            if feature_name:
                df = conn.execute(
                    """
                    SELECT feature_id, feature_name, symbol_id, exchange,
                           computed_at, rows_computed, lookback_days, params,
                           feature_file, status, note
                    FROM _feature_registry
                    WHERE feature_name = ?
                    ORDER BY computed_at DESC
                """,
                    (feature_name,),
                ).fetchdf()
            else:
                df = conn.execute("""
                    SELECT feature_id, feature_name, symbol_id, exchange,
                           computed_at, rows_computed, lookback_days, params,
                           feature_file, status, note
                    FROM _feature_registry
                    ORDER BY feature_name, computed_at DESC
                """).fetchdf()
            return df
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    #  Core: compute features via DuckDB vectorized SQL                  #
    # ------------------------------------------------------------------ #

    def _sql_feature(
        self,
        feature_name: str,
        sql_template: str,
        symbol_id: str = None,
        exchange: str = "NSE",
        params: dict = None,
    ) -> pd.DataFrame:
        """
        Execute a DuckDB SQL template over the OHLCV catalog.
        Template receives {symbol} placeholder. Must return:
          symbol_id, exchange, timestamp, and at least one feature column.
        """
        conn = self._get_conn()
        try:
            if symbol_id:
                query = sql_template.replace("{symbol}", f"'{symbol_id}'")
                order_pos = query.upper().rfind("ORDER BY")
                insert_pos = order_pos if order_pos != -1 else len(query)
                query = (
                    query[:insert_pos]
                    + f" AND exchange = '{exchange}'"
                    + query[insert_pos:]
                )
                df = conn.execute(query).fetchdf()
            else:
                query = sql_template.replace("{symbol}", "symbol_id")
                df = conn.execute(query).fetchdf()
            return df
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    #  Individual feature computations                                   #
    # ------------------------------------------------------------------ #

    def compute_rsi(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        period: int = 14,
    ) -> pd.DataFrame:
        """
        Relative Strength Index (RSI).
        Formula: 100 - (100 / (1 + RS)), where RS = avg_gain / avg_loss.
        """
        df = self._sql_feature(
            "RSI",
            f"""
            WITH prices AS (
                SELECT
                    symbol_id,
                    exchange,
                    timestamp,
                    close,
                    LAG(close) OVER w AS prev_close
                FROM _catalog
                WHERE symbol_id = {{symbol}}
                  AND exchange = '{exchange}'
                  AND timestamp IS NOT NULL
                WINDOW w AS (ORDER BY timestamp)
            ),
            gains_losses AS (
                SELECT
                    symbol_id,
                    exchange,
                    timestamp,
                    close,
                    prev_close,
                    CASE WHEN close - prev_close > 0 THEN close - prev_close ELSE 0 END AS gain,
                    CASE WHEN prev_close - close > 0 THEN prev_close - close ELSE 0 END AS loss
                FROM prices
            ),
            smoothed AS (
                SELECT
                    symbol_id,
                    exchange,
                    timestamp,
                    close,
                    gain,
                    loss,
                    AVG(gain) OVER w AS avg_gain,
                    AVG(loss) OVER w AS avg_loss
                FROM gains_losses
                WINDOW w AS (ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
            )
            SELECT
                symbol_id,
                exchange,
                timestamp,
                close,
                ROUND(avg_gain::DOUBLE / NULLIF(avg_loss::DOUBLE, 0), 6) AS rs,
                ROUND(
                    100.0 - (100.0 / (1.0 + NULLIF(avg_gain::DOUBLE / NULLIF(avg_loss::DOUBLE, 0), 0))),
                4) AS rsi_{period}
            FROM smoothed
            WHERE avg_loss IS NOT NULL
            ORDER BY timestamp
            """,
            symbol_id=symbol_id,
            exchange=exchange,
            params={"period": period},
        )
        return df

    def compute_adx(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        period: int = 14,
    ) -> pd.DataFrame:
        """
        Average Directional Index (ADX) with +DI and -DI.
        Uses Wilder smoothing (exponential weighted avg).
        """
        df = self._sql_feature(
            "ADX",
            f"""
            WITH ohlc AS (
                SELECT
                    symbol_id,
                    exchange,
                    timestamp,
                    high, low, close,
                    LAG(close) OVER w AS prev_close,
                    LAG(low) OVER w AS prev_low,
                    LAG(high) OVER w AS prev_high
                FROM _catalog
                WHERE symbol_id = {{symbol}}
                  AND exchange = '{exchange}'
                  AND timestamp IS NOT NULL
                WINDOW w AS (ORDER BY timestamp)
            ),
            tr_dm AS (
                SELECT
                    symbol_id, exchange, timestamp,
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
                    symbol_id, exchange, timestamp,
                    true_range, plus_dm, minus_dm,
                    AVG(true_range) OVER w AS atr,
                    AVG(plus_dm) OVER w AS atr_plus_dm,
                    AVG(minus_dm) OVER w AS atr_minus_dm
                FROM tr_dm
                WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
            ),
            di AS (
                SELECT
                    symbol_id, exchange, timestamp,
                    true_range, atr,
                    ROUND(100.0 * atr_plus_dm / NULLIF(atr, 0), 6) AS plus_di,
                    ROUND(100.0 * atr_minus_dm / NULLIF(atr, 0), 6) AS minus_di,
                    ABS(plus_di - minus_di) AS di_diff,
                    plus_di + minus_di AS di_sum
                FROM smoothed
                WHERE atr IS NOT NULL AND atr > 0
            ),
            dx AS (
                SELECT
                    symbol_id, exchange, timestamp,
                    plus_di, minus_di,
                    ROUND(100.0 * di_diff / NULLIF(di_sum, 0), 4) AS dx
                FROM di
                WHERE di_sum IS NOT NULL AND di_sum > 0
            ),
            adx_base AS (
                SELECT
                    symbol_id, exchange, timestamp,
                    plus_di, minus_di, dx,
                    AVG(dx) OVER w AS avg_dx
                FROM dx
                WINDOW w AS (PARTITION BY symbol_id ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
            )
            SELECT
                symbol_id, exchange, timestamp,
                ROUND(plus_di, 4) AS plus_di_{period},
                ROUND(minus_di, 4) AS minus_di_{period},
                ROUND(avg_dx, 4) AS adx_{period}
            FROM adx_base
            WHERE avg_dx IS NOT NULL
            ORDER BY timestamp
            """,
            symbol_id=symbol_id,
            exchange=exchange,
            params={"period": period},
        )
        return df

    def compute_sma(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        windows: List[int] = None,
    ) -> pd.DataFrame:
        """
        Simple Moving Average for multiple windows via DuckDB vectorized SQL.
        """
        if windows is None:
            windows = [5, 10, 20, 50, 100, 200]

        window_defs = [
            f"w{w} AS (ORDER BY timestamp ROWS BETWEEN {w - 1} PRECEDING AND CURRENT ROW)"
            for w in windows
        ]
        window_clause = ", ".join(window_defs)
        sma_cols = ",\n                ".join(
            f"ROUND(AVG(close) OVER w{w}, 4) AS sma_{w}" for w in windows
        )

        sql = f"""
            WITH ranked AS (
                SELECT
                    symbol_id,
                    exchange,
                    timestamp,
                    close,
                    ROW_NUMBER() OVER w AS rn
                FROM _catalog
                WHERE symbol_id = {{symbol}}
                  AND exchange = '{exchange}'
                  AND timestamp IS NOT NULL
                WINDOW w AS (ORDER BY timestamp)
            )
            SELECT
                symbol_id, exchange, timestamp, close,
                {sma_cols}
            FROM ranked
            WINDOW {window_clause}
            QUALIFY rn >= {max(windows)}
            ORDER BY timestamp
        """

        conn = self._get_conn()
        try:
            if symbol_id:
                query = sql.replace("{symbol}", f"'{symbol_id}'")
            else:
                query = sql.replace("{symbol}", "symbol_id")
            df = conn.execute(query).fetchdf()
            return df
        finally:
            conn.close()

    def compute_ema(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        windows: List[int] = None,
    ) -> pd.DataFrame:
        """
        Exponential Moving Average using DuckDB's EMA via exponential smoothing.
        """
        if windows is None:
            windows = [12, 26, 50, 200]

        conn = self._get_conn()
        try:
            base_sql = f"""
                SELECT
                    symbol_id, exchange, timestamp, close,
                    LAG(close) OVER w AS prev_close
                FROM _catalog
                WHERE symbol_id = {{symbol}}
                  AND exchange = '{exchange}'
                  AND timestamp IS NOT NULL
                WINDOW w AS (ORDER BY timestamp)
            """
            if symbol_id:
                base_sql = base_sql.replace("{symbol}", f"'{symbol_id}'")

            result_dfs = []
            for w in windows:
                alpha = 2.0 / (w + 1)
                query = f"""
                    WITH prices AS ({base_sql}),
                    ema AS (
                        SELECT
                            symbol_id, exchange, timestamp, close, prev_close,
                            CASE
                                WHEN prev_close IS NULL THEN close
                                ELSE prev_close + {alpha} * (close - prev_close)
                            END AS ema_{w}
                        FROM prices
                    )
                    SELECT symbol_id, exchange, timestamp, close,
                           ROUND(ema_{w}, 4) AS ema_{w}
                    FROM ema
                    ORDER BY timestamp
                """
                df = conn.execute(query).fetchdf()
                result_dfs.append(df)

            if not result_dfs:
                return pd.DataFrame()

            df = result_dfs[0]
            for other in result_dfs[1:]:
                cols = [c for c in other.columns if c not in df.columns]
                df = df.merge(
                    other[cols + ["symbol_id", "exchange", "timestamp"]], how="left"
                )

            return df
        finally:
            conn.close()

    def compute_macd(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> pd.DataFrame:
        """
        MACD (Moving Average Convergence Divergence).
        MACD_line = EMA_fast - EMA_slow
        Signal_line = EMA(MACD_line, signal)
        Histogram = MACD_line - Signal_line
        """
        conn = self._get_conn()
        try:
            base = f"""
                SELECT
                    symbol_id, exchange, timestamp, close
                FROM _catalog
                WHERE symbol_id = {{symbol}}
                  AND exchange = '{exchange}'
                  AND timestamp IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp) >= {slow}
                ORDER BY timestamp
            """
            if symbol_id:
                base = base.replace("{symbol}", f"'{symbol_id}'")

            fast_alpha = 2.0 / (fast + 1)
            slow_alpha = 2.0 / (slow + 1)
            sig_alpha = 2.0 / (signal + 1)

            query = f"""
                WITH prices AS ({base}),
                ema_fast AS (
                    SELECT
                        symbol_id, exchange, timestamp, close,
                        LAG(close) OVER w AS prev,
                        CASE WHEN LAG(close) OVER w IS NULL THEN close
                             ELSE LAG(close) OVER w + {fast_alpha} * (close - LAG(close) OVER w)
                        END AS ema_f
                    FROM prices
                    WINDOW w AS (ORDER BY timestamp)
                ),
                ema_slow AS (
                    SELECT
                        symbol_id, exchange, timestamp, close,
                        LAG(close) OVER w AS prev,
                        CASE WHEN LAG(close) OVER w IS NULL THEN close
                             ELSE LAG(close) OVER w + {slow_alpha} * (close - LAG(close) OVER w)
                        END AS ema_s
                    FROM prices
                    WINDOW w AS (ORDER BY timestamp)
                ),
                macd_line AS (
                    SELECT
                        f.symbol_id, f.exchange, f.timestamp, f.close,
                        f.ema_f, s.ema_s,
                        f.ema_f - s.ema_s AS macd_line
                    FROM ema_fast f
                    JOIN ema_slow s USING (symbol_id, exchange, timestamp)
                ),
                signal_line AS (
                    SELECT
                        symbol_id, exchange, timestamp, close, macd_line,
                        LAG(macd_line) OVER w AS prev_macd,
                        CASE WHEN LAG(macd_line) OVER w IS NULL THEN macd_line
                             ELSE LAG(macd_line) OVER w + {sig_alpha} * (macd_line - LAG(macd_line) OVER w)
                        END AS signal_line
                    FROM macd_line
                    WINDOW w AS (ORDER BY timestamp)
                )
                SELECT
                    symbol_id, exchange, timestamp, close,
                    ROUND(macd_line, 4) AS macd_line,
                    ROUND(signal_line, 4) AS macd_signal_{signal},
                    ROUND(macd_line - signal_line, 4) AS macd_histogram
                FROM signal_line
                WHERE macd_line IS NOT NULL
                ORDER BY timestamp
            """
            return conn.execute(query).fetchdf()
        finally:
            conn.close()

    def compute_atr(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        period: int = 14,
    ) -> pd.DataFrame:
        """
        Average True Range.
        TR = MAX(H-L, |H-PC|, |L-PC|)
        ATR = Wilder smoothing of TR
        """
        df = self._sql_feature(
            "ATR",
            f"""
            WITH ohlc AS (
                SELECT
                    symbol_id, exchange, timestamp,
                    high, low, close,
                    LAG(close) OVER w AS prev_close
                FROM _catalog
                WHERE symbol_id = {{symbol}}
                  AND exchange = '{exchange}'
                  AND timestamp IS NOT NULL
                WINDOW w AS (ORDER BY timestamp)
            ),
            tr_calc AS (
                SELECT
                    symbol_id, exchange, timestamp,
                    GREATEST(
                        high - low,
                        ABS(high - prev_close),
                        ABS(low - prev_close)
                    ) AS tr
                FROM ohlc
            ),
            atr_calc AS (
                SELECT
                    symbol_id, exchange, timestamp,
                    tr,
                    AVG(tr) OVER w AS raw_atr
                FROM tr_calc
                WINDOW w AS (ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
            )
            SELECT
                symbol_id, exchange, timestamp,
                ROUND(raw_atr, 4) AS atr_{period}
            FROM atr_calc
            WHERE raw_atr IS NOT NULL
            ORDER BY timestamp
            """,
            symbol_id=symbol_id,
            exchange=exchange,
            params={"period": period},
        )
        return df

    def compute_bollinger_bands(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        period: int = 20,
        std_dev: float = 2.0,
    ) -> pd.DataFrame:
        """
        Bollinger Bands.
        Middle = SMA(close, period)
        Upper = Middle + std_dev * STDDEV(close, period)
        Lower = Middle - std_dev * STDDEV(close, period)
        """
        df = self._sql_feature(
            "BB",
            f"""
            WITH ranked AS (
                SELECT
                    symbol_id, exchange, timestamp, close,
                    AVG(close) OVER w AS sma_mid,
                    STDDEV(close) OVER w AS sd,
                    ROW_NUMBER() OVER w AS rn
                FROM _catalog
                WHERE symbol_id = {{symbol}}
                  AND exchange = '{exchange}'
                  AND timestamp IS NOT NULL
                WINDOW w AS (ORDER BY timestamp ROWS BETWEEN {period - 1} PRECEDING AND CURRENT ROW)
            )
            SELECT
                symbol_id, exchange, timestamp, close,
                ROUND(sma_mid, 4) AS bb_middle_{period},
                ROUND(sma_mid + {std_dev} * sd, 4) AS bb_upper_{period}_{int(std_dev)}sd,
                ROUND(sma_mid - {std_dev} * sd, 4) AS bb_lower_{period}_{int(std_dev)}sd
            FROM ranked
            WHERE rn >= {period}
            ORDER BY timestamp
            """,
            symbol_id=symbol_id,
            exchange=exchange,
            params={"period": period, "std_dev": std_dev},
        )
        return df

    def compute_roc(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        periods: List[int] = None,
    ) -> pd.DataFrame:
        """
        Rate of Change.
        ROC(n) = (close - close[n periods ago]) / close[n periods ago] * 100
        """
        if periods is None:
            periods = [1, 5, 10, 20]

        conn = self._get_conn()
        try:
            all_dfs = []
            for p in periods:
                base_sql = f"""
                    SELECT
                        symbol_id, exchange, timestamp, close,
                        LAG(close, {p}) OVER w AS close_{p},
                        ROUND(
                            100.0 * (close - LAG(close, {p}) OVER w)
                            / NULLIF(LAG(close, {p}) OVER w, 0),
                        4) AS roc_{p}
                    FROM _catalog
                    WHERE symbol_id = {{symbol}}
                      AND exchange = '{exchange}'
                      AND timestamp IS NOT NULL
                    WINDOW w AS (ORDER BY timestamp)
                """
                if symbol_id:
                    base_sql = base_sql.replace("{symbol}", f"'{symbol_id}'")

                df = conn.execute(f"""
                    SELECT symbol_id, exchange, timestamp, close,
                           roc_{p} AS roc_{p}
                    FROM ({base_sql}) t
                    WHERE close_{p} IS NOT NULL
                    ORDER BY timestamp
                """).fetchdf()
                all_dfs.append(df)

            if not all_dfs:
                return pd.DataFrame()

            result = all_dfs[0][["symbol_id", "exchange", "timestamp", "close"]].copy()
            for other_df in all_dfs:
                cols = [c for c in other_df.columns if c not in result.columns]
                result = result.merge(
                    other_df[cols + ["symbol_id", "exchange", "timestamp"]], how="left"
                )
            return result
        finally:
            conn.close()

    def compute_all_technicals(
        self,
        symbol_id: str,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Compute all technical indicators for a single symbol and merge them.
        """
        t0 = time.time()
        features = ["close"]

        rsi = self.compute_rsi(symbol_id, exchange, period=14)
        if not rsi.empty:
            rsi = rsi.rename(columns={"close": "close"})
            features.append("rsi_14")
        else:
            rsi = pd.DataFrame()

        adx = self.compute_adx(symbol_id, exchange, period=14)
        if not adx.empty:
            features.extend(["plus_di_14", "minus_di_14", "adx_14"])
        else:
            adx = pd.DataFrame()

        sma = self.compute_sma(symbol_id, exchange, windows=[20, 50, 200])
        if not sma.empty:
            features.extend(["sma_20", "sma_50", "sma_200"])
        else:
            sma = pd.DataFrame()

        ema = self.compute_ema(symbol_id, exchange, windows=[12, 26])
        if not ema.empty:
            features.extend(["ema_12", "ema_26"])
        else:
            ema = pd.DataFrame()

        macd = self.compute_macd(symbol_id, exchange)
        if not macd.empty:
            features.extend(["macd_line", "macd_signal_9", "macd_histogram"])
        else:
            macd = pd.DataFrame()

        atr = self.compute_atr(symbol_id, exchange, period=14)
        if not atr.empty:
            features.append("atr_14")
        else:
            atr = pd.DataFrame()

        bb = self.compute_bollinger_bands(symbol_id, exchange)
        if not bb.empty:
            features.extend(["bb_middle_20", "bb_upper_20_2sd", "bb_lower_20_2sd"])
        else:
            bb = pd.DataFrame()

        roc = self.compute_roc(symbol_id, exchange, periods=[1, 5, 20])
        if not roc.empty:
            features.extend(["roc_1", "roc_5", "roc_20"])
        else:
            roc = pd.DataFrame()

        df = rsi
        for other, merge_cols in [
            (adx, ["symbol_id", "exchange", "timestamp"]),
            (sma, ["symbol_id", "exchange", "timestamp"]),
            (ema, ["symbol_id", "exchange", "timestamp"]),
            (macd, ["symbol_id", "exchange", "timestamp"]),
            (atr, ["symbol_id", "exchange", "timestamp"]),
            (bb, ["symbol_id", "exchange", "timestamp"]),
            (roc, ["symbol_id", "exchange", "timestamp"]),
        ]:
            if not other.empty:
                other_cols = [c for c in other.columns if c not in df.columns]
                df = df.merge(other[other_cols + merge_cols], on=merge_cols, how="left")

        elapsed = time.time() - t0
        logger.info(
            f"Computed {len(df.columns) - 3} features for {symbol_id} in {elapsed:.2f}s"
        )
        return df

    # ------------------------------------------------------------------ #
    #  Point-in-time join (no look-ahead bias)                          #
    # ------------------------------------------------------------------ #

    def point_in_time_join(
        self,
        features_df: pd.DataFrame,
        ohlcv_symbol_id: str,
        exchange: str = "NSE",
        feature_timestamp_col: str = "timestamp",
        ohlcv_timestamp_col: str = "timestamp",
        how: str = "left",
    ) -> pd.DataFrame:
        """
        Point-in-time join: for each OHLCV row, attach the most recent
        feature values available *before or at* that timestamp.

        This prevents look-ahead bias — features computed from future data
        cannot leak into training signals.

        Args:
            features_df: DataFrame with (symbol_id, exchange, timestamp, feature_* cols)
            ohlcv_symbol_id: Symbol to fetch OHLCV rows for
            exchange: Exchange
            feature_timestamp_col: Name of timestamp column in features_df
            ohlcv_timestamp_col: Name of timestamp column in OHLCV data
            how: Join type ('left', 'inner')

        Returns:
            Merged DataFrame with OHLCV + latest features as of each row's timestamp.
        """
        if features_df.empty:
            return pd.DataFrame()

        conn = self._get_conn()
        try:
            ohlcv_df = conn.execute(
                """
                SELECT
                    symbol_id, exchange, timestamp, open, high, low, close, volume
                FROM _catalog
                WHERE symbol_id = ? AND exchange = ?
                ORDER BY timestamp
            """,
                (ohlcv_symbol_id, exchange),
            ).fetchdf()

            if ohlcv_df.empty:
                return pd.DataFrame()

            feature_cols = [
                c
                for c in features_df.columns
                if c not in ("symbol_id", "exchange", "timestamp", "close")
            ]
            if not feature_cols:
                return ohlcv_df

            feat_ts = features_df[["timestamp"] + feature_cols].rename(
                columns={"timestamp": "feat_ts"}
            )

            result = []
            for _, ohlcv_row in ohlcv_df.iterrows():
                ts = ohlcv_row[ohlcv_timestamp_col]
                applicable = feat_ts[feat_ts["feat_ts"] <= ts]
                if applicable.empty:
                    row = ohlcv_row.to_dict()
                else:
                    latest = applicable.loc[applicable["feat_ts"].idxmax()]
                    row = {**ohlcv_row.to_dict()}
                    for col in feature_cols:
                        row[col] = latest[col]
                result.append(row)

            return pd.DataFrame(result)

        finally:
            conn.close()

    def as_of_join_sql(
        self,
        features_table: str,
        ohlcv_symbol_id: str,
        exchange: str = "NSE",
        as_of_ts: str = None,
    ) -> pd.DataFrame:
        """
        SQL-based AS OF join using DuckDB QUALIFY.
        Returns OHLCV rows with the latest feature values available
        at or before each OHLCV timestamp.
        """
        conn = self._get_conn()
        try:
            if as_of_ts:
                query = f"""
                    WITH ohlcv AS (
                        SELECT
                            symbol_id, exchange, timestamp,
                            open, high, low, close, volume
                        FROM _catalog
                        WHERE symbol_id = '{ohlcv_symbol_id}'
                          AND exchange = '{exchange}'
                          AND timestamp <= TIMESTAMP '{as_of_ts}'
                    ),
                    feat_latest AS (
                        SELECT DISTINCT ON (timestamp)
                            timestamp, open, high, low, close, volume
                        FROM _catalog
                        WHERE symbol_id = '{ohlcv_symbol_id}'
                          AND exchange = '{exchange}'
                          AND timestamp <= TIMESTAMP '{as_of_ts}'
                        ORDER BY timestamp
                    )
                    SELECT o.symbol_id, o.exchange, o.timestamp,
                           o.open, o.high, o.low, o.close, o.volume,
                           f.open AS feat_open, f.high AS feat_high,
                           f.low AS feat_low, f.close AS feat_close
                    FROM ohlcv o
                    LEFT JOIN feat_latest f
                        ON f.timestamp = (
                            SELECT MAX(timestamp)
                            FROM _catalog
                            WHERE symbol_id = '{ohlcv_symbol_id}'
                              AND exchange = '{exchange}'
                              AND timestamp <= o.timestamp
                              AND timestamp <= TIMESTAMP '{as_of_ts}'
                        )
                    ORDER BY o.timestamp
                """
            else:
                query = f"""
                    WITH ohlcv AS (
                        SELECT
                            symbol_id, exchange, timestamp,
                            open, high, low, close, volume
                        FROM _catalog
                        WHERE symbol_id = '{ohlcv_symbol_id}'
                          AND exchange = '{exchange}'
                    ),
                    feat_with_row AS (
                        SELECT
                            symbol_id, exchange, timestamp,
                            close AS feat_close,
                            ROW_NUMBER() OVER w AS rn,
                            COUNT(*) OVER () AS total
                        FROM _catalog
                        WHERE symbol_id = '{ohlcv_symbol_id}'
                          AND exchange = '{exchange}'
                        WINDOW w AS (ORDER BY timestamp)
                    )
                    SELECT
                        o.symbol_id, o.exchange, o.timestamp,
                        o.open, o.high, o.low, o.close, o.volume,
                        f.feat_close AS close_lag_1
                    FROM ohlcv o
                    LEFT JOIN feat_with_row f
                        ON o.symbol_id = f.symbol_id
                       AND f.rn = (
                            SELECT MAX(rn)
                            FROM feat_with_row f2
                            WHERE f2.timestamp <= o.timestamp
                              AND f2.symbol_id = '{ohlcv_symbol_id}'
                        )
                    ORDER BY o.timestamp
                """
            return conn.execute(query).fetchdf()
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    #  Bulk feature computation + persistence                            #
    # ------------------------------------------------------------------ #

    def compute_and_store_features(
        self,
        symbols: List[str] = None,
        exchanges: List[str] = None,
        feature_types: List[str] = None,
        warehouse_dir: str = None,
    ) -> Dict[str, int]:
        """
        Compute all technical features for all (or specified) symbols
        and write them to the feature store as partitioned Parquet.

        Args:
            symbols: List of symbol_ids. Defaults to all in OHLCV catalog.
            exchanges: List of exchanges. Defaults to ['NSE', 'BSE'].
            feature_types: List of features to compute.
                          Defaults to all: ['rsi', 'adx', 'sma', 'ema', 'macd', 'atr', 'bb', 'roc'].
            warehouse_dir: Where to write Parquet files. Defaults to feature_store_dir.

        Returns:
            Dict mapping feature_type -> number of rows written.
        """
        if exchanges is None:
            exchanges = ["NSE", "BSE"]
        if feature_types is None:
            feature_types = ["rsi", "adx", "sma", "ema", "macd", "atr", "bb", "roc"]
        if warehouse_dir is None:
            warehouse_dir = self.feature_store_dir

        if symbols is None:
            conn = self._get_conn()
            try:
                sym_rows = conn.execute(
                    """
                    SELECT DISTINCT symbol_id FROM _catalog
                    WHERE exchange IN ({exchanges})
                    ORDER BY symbol_id
                """.format(exchanges=",".join(f"'{e}'" for e in exchanges))
                ).fetchdf()
                symbols = sym_rows["symbol_id"].tolist()
            finally:
                conn.close()

        logger.info(f"Computing features for {len(symbols)} symbols: {feature_types}")

        feature_methods = {
            "rsi": lambda sid, exc: self.compute_rsi(sid, exc),
            "adx": lambda sid, exc: self.compute_adx(sid, exc),
            "sma": lambda sid, exc: self.compute_sma(sid, exc),
            "ema": lambda sid, exc: self.compute_ema(sid, exc),
            "macd": lambda sid, exc: self.compute_macd(sid, exc),
            "atr": lambda sid, exc: self.compute_atr(sid, exc),
            "bb": lambda sid, exc: self.compute_bollinger_bands(sid, exc),
            "roc": lambda sid, exc: self.compute_roc(sid, exc),
            "supertrend": lambda sid, exc: self.compute_supertrend(sid, exc),
        }

        rows_written = {}
        for feat_type in feature_types:
            if feat_type not in feature_methods:
                logger.warning(f"Unknown feature type: {feat_type}")
                continue

            method = feature_methods[feat_type]
            total_rows = 0

            for exc in exchanges:
                for sym in symbols:
                    try:
                        df = method(sym, exc)
                        if df.empty:
                            continue

                        feat_dir = os.path.join(warehouse_dir, feat_type, exc)
                        os.makedirs(feat_dir, exist_ok=True)

                        out_path = os.path.join(feat_dir, f"{sym}.parquet")
                        df.to_parquet(out_path, index=False)
                        total_rows += len(df)

                        self.register_feature(
                            feature_name=feat_type,
                            symbol_id=sym,
                            exchange=exc,
                            rows_computed=len(df),
                            feature_file=out_path,
                            status="completed",
                        )
                    except Exception as e:
                        logger.warning(
                            f"Error computing {feat_type} for {sym}/{exc}: {e}"
                        )

            rows_written[feat_type] = total_rows
            logger.info(
                f"{feat_type}: wrote {total_rows:,} rows across {len(symbols)} symbols"
            )

        return rows_written

    def load_feature(
        self,
        feature_name: str,
        symbol_id: str,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Load pre-computed features from Parquet store.
        """
        path = os.path.join(
            self.feature_store_dir, feature_name, exchange, f"{symbol_id}.parquet"
        )
        if os.path.exists(path):
            return pd.read_parquet(path)
        return pd.DataFrame()

    def compute_all_technicals_store(
        self,
        symbol_id: str,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Compute all technicals and store to Parquet in one call.
        Returns the merged DataFrame.
        """
        df = self.compute_all_technicals(symbol_id, exchange)
        if df.empty:
            return df

        out_dir = os.path.join(self.feature_store_dir, "all_technicals", exchange)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{symbol_id}.parquet")
        df.to_parquet(out_path, index=False)

        self.register_feature(
            feature_name="all_technicals",
            symbol_id=symbol_id,
            exchange=exchange,
            rows_computed=len(df),
            feature_file=out_path,
            status="completed",
        )
        return df

    # ------------------------------------------------------------------ #
    #  Supertrend (hybrid: DuckDB fetch + pandas stateful compute)         #
    # ------------------------------------------------------------------ #

    def compute_supertrend(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        period: int = 10,
        multiplier: float = 3.0,
    ) -> pd.DataFrame:
        """
        Supertrend indicator using hybrid approach:
        - OHLCV fetched from DuckDB (vectorized)
        - Stateful Supertrend logic computed in pandas (requires row-by-row state)
        - Result returned as DataFrame with: symbol_id, exchange, timestamp, close,
          supertrend_<p>_<m>, supertrend_dir_<p>_<m>
        """
        conn = self._get_conn()
        try:
            ohlcv = conn.execute(
                """
                SELECT symbol_id, exchange, timestamp, high, low, close
                FROM _catalog
                WHERE symbol_id = ? AND exchange = ?
                  AND timestamp IS NOT NULL
                ORDER BY timestamp
            """,
                (symbol_id, exchange),
            ).fetchdf()
        finally:
            conn.close()

        if ohlcv.empty:
            return pd.DataFrame()

        high = ohlcv["high"]
        low = ohlcv["low"]
        close = ohlcv["close"]

        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.rolling(window=period, min_periods=period).mean()

        upper_band = (high + low) / 2 + multiplier * atr
        lower_band = (high + low) / 2 - multiplier * atr

        supertrend = pd.Series(index=ohlcv.index, dtype=float)
        direction = pd.Series(1, index=ohlcv.index, dtype=int)

        for i in range(len(close)):
            if i == 0:
                supertrend.iloc[i] = lower_band.iloc[i]
                direction.iloc[i] = 1
                continue

            prev_supert = supertrend.iloc[i - 1]
            prev_dir = direction.iloc[i - 1]
            prev_upper = upper_band.iloc[i - 1]
            prev_lower = lower_band.iloc[i - 1]
            curr_close = close.iloc[i]
            curr_upper = upper_band.iloc[i]
            curr_lower = lower_band.iloc[i]

            if curr_close > prev_upper:
                direction.iloc[i] = 1
                supertrend.iloc[i] = curr_lower
            elif curr_close < prev_lower:
                direction.iloc[i] = -1
                supertrend.iloc[i] = curr_upper
            else:
                direction.iloc[i] = prev_dir
                supertrend.iloc[i] = prev_supert

                if prev_dir == 1 and curr_lower < prev_lower:
                    supertrend.iloc[i] = prev_lower
                if prev_dir == -1 and curr_upper > prev_upper:
                    supertrend.iloc[i] = prev_upper

        suffix = f"_{period}_{int(multiplier)}"
        result = ohlcv[["symbol_id", "exchange", "timestamp"]].copy()
        result["close"] = close.values
        result[f"supertrend{suffix}"] = supertrend.values
        result[f"supertrend_dir{suffix}"] = direction.values

        logger.info(
            f"Supertrend{period}x{multiplier}: {len(result)} rows for {symbol_id}"
        )
        return result

    # ------------------------------------------------------------------ #
    #  Fundamental features from stock_details                            #
    # ------------------------------------------------------------------ #

    def compute_fundamental_features(
        self,
        masterdb_path: str = None,
        exchanges: List[str] = None,
    ) -> pd.DataFrame:
        """
        Compute static fundamental features per symbol from stock_details.
        Returns one row per symbol with: symbol_id, exchange, name,
        industry_group, industry, mcap, mcap_category.
        """
        if masterdb_path is None:
            masterdb_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "masterdata.db",
            )
        if exchanges is None:
            exchanges = ["NSE", "BSE"]
        if not os.path.exists(masterdb_path):
            logger.warning(f"masterdb not found: {masterdb_path}")
            return pd.DataFrame()

        conn_sqlite = sqlite3.connect(masterdb_path)
        cur = conn_sqlite.cursor()
        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name IN ('stock_details', 'symbols')
        """)
        available = {r[0] for r in cur.fetchall()}
        conn_sqlite.close()

        if "stock_details" not in available:
            logger.warning("stock_details table not found in masterdb")
            return pd.DataFrame()

        conn_sqlite = sqlite3.connect(masterdb_path)
        exc_placeholders = ",".join("?" * len(exchanges))

        df = pd.read_sql(
            f"""
            SELECT
                sd.Security_id,
                sd.Symbol    AS symbol_id,
                sd.Name      AS name,
                sd."Industry Group" AS industry_group,
                sd."Industry"       AS industry,
                sd.MCAP,
                s.exchange
            FROM stock_details sd
            LEFT JOIN symbols s ON s.security_id = sd.Security_id
            WHERE sd.Security_id IS NOT NULL
              AND sd.Security_id != ''
              AND s.exchange IN ({exc_placeholders})
        """,
            conn_sqlite,
            params=exchanges,
        )
        conn_sqlite.close()

        if df.empty:
            return pd.DataFrame()

        def mcap_category(mcap_val):
            try:
                m = float(str(mcap_val).replace(",", "").replace(" ", ""))
            except (ValueError, TypeError):
                return "Unknown"
            if mcap_val is None or str(mcap_val).strip() == "":
                return "Unknown"
            if m >= 2_000_000:
                return "Mega Cap"
            elif m >= 200_000:
                return "Large Cap"
            elif m >= 20_000:
                return "Mid Cap"
            elif m >= 5_000:
                return "Small Cap"
            elif m >= 1_000:
                return "Micro Cap"
            else:
                return "Nano Cap"

        df["mcap_category"] = df["MCAP"].apply(mcap_category)

        return df

    def store_fundamental_features(
        self,
        masterdb_path: str = None,
        exchanges: List[str] = None,
    ) -> int:
        """
        Store fundamental features as Parquet files (one per symbol).
        Returns number of symbols stored.
        """
        df = self.compute_fundamental_features(
            masterdb_path=masterdb_path, exchanges=exchanges
        )
        if df.empty:
            return 0

        total = 0
        for _, row in df.iterrows():
            sym = row["symbol_id"]
            exc = row["exchange"] or "NSE"
            feat_dir = os.path.join(self.feature_store_dir, "fundamental", exc)
            os.makedirs(feat_dir, exist_ok=True)
            out_path = os.path.join(feat_dir, f"{sym}.parquet")
            row.to_frame().T.to_parquet(out_path, index=False)
            self.register_feature(
                feature_name="fundamental",
                symbol_id=sym,
                exchange=exc,
                rows_computed=1,
                params={
                    "industry_group": row.get("industry_group"),
                    "industry": row.get("industry"),
                    "mcap_category": row.get("mcap_category"),
                },
                feature_file=out_path,
                status="completed",
            )
            total += 1

        logger.info(f"Stored fundamental features for {total} symbols")
        return total

    def load_fundamental_features(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        masterdb_path: str = None,
    ) -> pd.DataFrame:
        """
        Load fundamental features for a symbol from Parquet store,
        falling back to masterdb directly if not found.
        """
        path = os.path.join(
            self.feature_store_dir, "fundamental", exchange, f"{symbol_id}.parquet"
        )
        if os.path.exists(path):
            return pd.read_parquet(path)

        if masterdb_path is None:
            masterdb_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "masterdata.db",
            )
        if not os.path.exists(masterdb_path):
            return pd.DataFrame()

        conn_sqlite = sqlite3.connect(masterdb_path)
        df = pd.read_sql(
            """
            SELECT
                sd.Security_id,
                sd.Symbol    AS symbol_id,
                sd.Name      AS name,
                sd."Industry Group" AS industry_group,
                sd."Industry"       AS industry,
                sd.MCAP
            FROM stock_details sd
            LEFT JOIN symbols s ON s.security_id = sd.Security_id
            WHERE sd.Symbol = ? AND s.exchange = ?
              AND sd.Security_id IS NOT NULL AND sd.Security_id != ''
        """,
            conn_sqlite,
            params=(symbol_id, exchange),
        )
        conn_sqlite.close()
        return df
