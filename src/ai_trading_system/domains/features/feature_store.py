import os
import time
import sqlite3
import duckdb
import pandas as pd
import numpy as np
from ai_trading_system.domains.features import repository as features_repository
from ai_trading_system.domains.features import snapshot as features_snapshot
from ai_trading_system.domains.features.indicators import add_stage2_features
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Callable
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger

# Columns produced by the Stage 2 computation — persisted to feature parquet
STAGE2_FEATURE_COLUMNS: tuple[str, ...] = (
    "sma_150",
    "sma150_slope_20d_pct",
    "sma200_slope_20d_pct",
    "stage2_score",
    "is_stage2_uptrend",
    "stage2_label",
    "stage2_fail_reason",
    "near_52w_high_pct",
    "volume_ratio_20",
)


def add_feature_readiness(frame: pd.DataFrame, min_lookback: int = 50) -> pd.DataFrame:
    """Mark rows with sufficient per-symbol history for robust feature usage."""
    output = frame.copy(deep=True)
    if output.empty:
        output.loc[:, "feature_ready"] = pd.Series(dtype=bool)
        return output
    if "symbol" in output.columns and "symbol_id" not in output.columns:
        output.loc[:, "symbol_id"] = output["symbol"]
    if "symbol_id" not in output.columns:
        output.loc[:, "feature_ready"] = False
        return output
    output.loc[:, "feature_ready"] = output.groupby("symbol_id").cumcount() >= (int(min_lookback) - 1)
    return output


def add_feature_confidence(frame: pd.DataFrame) -> pd.DataFrame:
    """Propagate readiness/provider confidence into a bounded feature confidence score."""
    output = frame.copy(deep=True)
    if output.empty:
        output.loc[:, "feature_confidence"] = pd.Series(dtype=float)
        return output

    output.loc[:, "feature_confidence"] = 1.0

    if "feature_ready" in output.columns:
        output.loc[~output["feature_ready"].fillna(False), "feature_confidence"] = 0.0

    if "provider_confidence" in output.columns:
        provider = pd.to_numeric(output["provider_confidence"], errors="coerce").clip(lower=0.0, upper=1.0)
        output.loc[:, "feature_confidence"] = pd.concat(
            [output["feature_confidence"], provider], axis=1
        ).min(axis=1)

    output.loc[:, "feature_confidence"] = output["feature_confidence"].clip(lower=0.0, upper=1.0)
    return output


def add_liquidity_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add basic cross-sectional liquidity signals."""
    output = frame.copy(deep=True)
    if output.empty:
        output.loc[:, "turnover"] = pd.Series(dtype=float)
        output.loc[:, "liquidity_score"] = pd.Series(dtype=float)
        return output

    close = pd.to_numeric(output.get("close"), errors="coerce")
    volume = pd.to_numeric(output.get("volume"), errors="coerce")
    output.loc[:, "turnover"] = close * volume

    if "date" not in output.columns and "timestamp" in output.columns:
        output.loc[:, "date"] = pd.to_datetime(output["timestamp"]).dt.normalize()
    if "date" in output.columns:
        output.loc[:, "liquidity_score"] = output.groupby("date")["turnover"].rank(pct=True)
    else:
        output.loc[:, "liquidity_score"] = output["turnover"].rank(pct=True)
    return output


def add_cross_sectional_features(frame: pd.DataFrame, metric: str = "return_20d") -> pd.DataFrame:
    """Add per-date universe and sector ranks for explainability and screening."""
    output = frame.copy(deep=True)
    if output.empty:
        output.loc[:, "rank_in_universe"] = pd.Series(dtype=float)
        output.loc[:, "percentile_score"] = pd.Series(dtype=float)
        return output
    if metric not in output.columns:
        output.loc[:, "rank_in_universe"] = np.nan
        output.loc[:, "percentile_score"] = np.nan
        if "sector" in output.columns:
            output.loc[:, "rank_in_sector"] = np.nan
        return output

    if "date" not in output.columns and "timestamp" in output.columns:
        output.loc[:, "date"] = pd.to_datetime(output["timestamp"]).dt.normalize()
    if "date" not in output.columns:
        output.loc[:, "rank_in_universe"] = output[metric].rank(ascending=False, method="dense")
        output.loc[:, "percentile_score"] = output[metric].rank(pct=True)
        if "sector" in output.columns:
            output.loc[:, "rank_in_sector"] = output.groupby("sector")[metric].rank(ascending=False, method="dense")
        return output

    output.loc[:, "rank_in_universe"] = output.groupby("date")[metric].rank(ascending=False, method="dense")
    output.loc[:, "percentile_score"] = output.groupby("date")[metric].rank(pct=True)
    if "sector" in output.columns:
        output.loc[:, "rank_in_sector"] = output.groupby(["date", "sector"])[metric].rank(
            ascending=False,
            method="dense",
        )
    return output


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
        data_domain: str = "operational",
    ):
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
        )
        paths = ensure_domain_layout(
            project_root=project_root,
            data_domain=data_domain,
        )
        if ohlcv_db_path is None:
            ohlcv_db_path = str(paths.ohlcv_db_path)
        if feature_store_dir is None:
            feature_store_dir = str(paths.feature_store_dir)

        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        self.db_path = ohlcv_db_path
        self.data_domain = data_domain
        os.makedirs(self.feature_store_dir, exist_ok=True)

        self._init_feature_registry()
        self._init_metadata_tables()

    # ------------------------------------------------------------------ #
    #  DuckDB helpers                                                    #
    # ------------------------------------------------------------------ #

    def _get_conn(self):
        return features_repository.get_conn(self.ohlcv_db_path)

    # ------------------------------------------------------------------ #
    #  Feature Registry                                                  #
    # ------------------------------------------------------------------ #

    def _init_feature_registry(self):
        features_repository.init_feature_registry(self.ohlcv_db_path)

    def create_snapshot(self, description: str = None) -> int:
        """Create snapshot of current state."""
        conn = self._get_conn()

        # Get OHLCV range
        ohlcv_range = conn.execute("""
            SELECT MIN(timestamp)::date, MAX(timestamp)::date, COUNT(DISTINCT symbol_id)
            FROM _catalog
        """).fetchone()

        # Get features count
        features_count = conn.execute("""
            SELECT COUNT(*) FROM _feature_registry WHERE status = 'completed'
        """).fetchone()[0]

        # Get next snapshot_id
        conn.execute("SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM _snapshots")
        result = conn.execute(
            "SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM _snapshots"
        ).fetchone()[0]

        # Update existing running snapshot to completed
        conn.execute("""
            UPDATE _snapshots 
            SET status = 'completed', snapshot_ts = CURRENT_TIMESTAMP
            WHERE status = 'running'
        """)

        # Create new snapshot
        conn.execute(
            """
            INSERT INTO _snapshots (snapshot_id, snapshot_ts, symbols_processed, rows_written, from_date, to_date, status, note)
            VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, 'completed', ?)
        """,
            (
                result,
                ohlcv_range[2],
                features_count,
                str(ohlcv_range[0]),
                str(ohlcv_range[1]),
                description or f"Daily snapshot",
            ),
        )

        # Update all features with snapshot_id
        conn.execute(f"""
            UPDATE _feature_registry 
            SET snapshot_id = {result}
            WHERE snapshot_id IS NULL
        """)

        conn.commit()

        logger.info(
            f"Created snapshot: {result} ({ohlcv_range[2]} symbols, OHLCV: {ohlcv_range[0]} to {ohlcv_range[1]})"
        )

        return result
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
        return features_repository.register_feature(
            self.ohlcv_db_path,
            feature_name=feature_name,
            symbol_id=symbol_id,
            exchange=exchange,
            rows_computed=rows_computed,
            lookback_days=lookback_days,
            params=params,
            feature_file=feature_file,
            status=status,
            note=note,
        )

    # ------------------------------------------------------------------ #
    #  Iceberg-lite Metadata Tables                                       #
    # ------------------------------------------------------------------ #

    def _init_metadata_tables(self):
        """Initialize metadata tables for Iceberg-lite architecture."""
        features_repository.init_metadata_tables(self.ohlcv_db_path)

    # ------------------------------------------------------------------ #
    #  Partitioned Storage (Iceberg-lite)                                 #
    # ------------------------------------------------------------------ #

    def _get_partition_path(self, table_name: str, year: int, month: int) -> str:
        """Get partition path: data/features/table_name/year=YYYY/month=MM/"""
        return features_snapshot.get_partition_path(
            self.feature_store_dir,
            table_name,
            year,
            month,
        )

    def store_partitioned(
        self,
        table_name: str,
        df: pd.DataFrame,
        snapshot_id: int = None,
    ) -> int:
        """
        Store data in partitioned Parquet format (Iceberg-lite).
        Path: table_name/year=YYYY/month=MM/symbol.parquet

        Atomic write: write to temp, then rename.
        """
        return features_snapshot.store_partitioned(
            get_conn=self._get_conn,
            feature_store_dir=self.feature_store_dir,
            table_name=table_name,
            df=df,
            snapshot_id=snapshot_id,
        )

    def load_partitioned(
        self,
        table_name: str,
        symbol_id: str = None,
        start_date: str = None,
        end_date: str = None,
        snapshot_id: int = None,
    ) -> pd.DataFrame:
        """Load data from partitioned storage with optional time travel."""
        return features_snapshot.load_partitioned(
            get_conn=self._get_conn,
            feature_store_dir=self.feature_store_dir,
            table_name=table_name,
            symbol_id=symbol_id,
            start_date=start_date,
            end_date=end_date,
            snapshot_id=snapshot_id,
        )

    def get_table_info(self, table_name: str = None) -> pd.DataFrame:
        """Get info about partitioned tables."""
        return features_snapshot.get_table_info(
            get_conn=self._get_conn,
            table_name=table_name,
        )

    def create_snapshot(self, description: str = None) -> int:
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
    #  Incremental computation helpers                                   #
    # ------------------------------------------------------------------ #

    def get_last_feature_date(
        self, feature_name: str, symbol_id: str = None, exchange: str = "NSE"
    ) -> str:
        """Get the last date for which a feature was computed."""
        return features_repository.get_last_feature_date(
            self.ohlcv_db_path,
            feature_name=feature_name,
            symbol_id=symbol_id,
            exchange=exchange,
        )

    def compute_incremental(
        self,
        feature_name: str,
        symbol_id: str,
        exchange: str = "NSE",
        compute_method=None,
        lookback_days: int = 50,
    ) -> int:
        """
        Compute features incrementally - only for new dates since last computation.
        Returns number of new rows computed.

        Args:
            feature_name: Name of the feature table
            symbol_id: Stock symbol
            exchange: Exchange (default NSE)
            compute_method: Function to compute the feature
            lookback_days: Days of historical data to include for rolling calculations
        """
        import datetime

        last_date = self.get_last_feature_date(feature_name, symbol_id, exchange)

        if last_date:
            # Add lookback days for rolling calculations
            last_dt = datetime.datetime.strptime(
                last_date, "%Y-%m-%d"
            ) - datetime.timedelta(days=lookback_days)
            start_date = last_dt.strftime("%Y-%m-%d")
            df = compute_method(symbol_id, exchange, start_date=start_date)
        else:
            df = compute_method(symbol_id, exchange)

        if df.empty:
            return 0

        # Add date column if not present
        if "date" not in df.columns:
            df["date"] = pd.to_datetime(df["timestamp"]).dt.date

        # Filter to only new rows (after last_date) if incremental
        if last_date:
            last_date_dt = pd.to_datetime(last_date).date()
            df = df[df["date"] > last_date_dt]

        if df.empty:
            return 0

        rows = self.store_features_duckdb(feature_name, df)
        return rows

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
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        Execute a DuckDB SQL template over the OHLCV catalog.
        Template receives {symbol_predicate} and {exchange_predicate} placeholders. Must return:
          symbol_id, exchange, timestamp, and at least one feature column.
        """
        conn = self._get_conn()
        try:
            query = sql_template.replace(
                "{symbol_predicate}",
                "symbol_id = ?" if symbol_id else "TRUE",
            ).replace(
                "{exchange_predicate}",
                "exchange = ?" if exchange else "TRUE",
            )

            bind_params: list[Any] = []
            if symbol_id:
                bind_params.append(symbol_id)
            if exchange:
                bind_params.append(exchange)

            date_conditions = []
            if start_date:
                date_conditions.append("timestamp > CAST(? AS TIMESTAMP)")
                bind_params.append(start_date)
            if end_date:
                date_conditions.append("timestamp <= CAST(? AS TIMESTAMP)")
                bind_params.append(end_date)

            if date_conditions:
                order_pos = query.upper().rfind("ORDER BY")
                insert_pos = order_pos if order_pos != -1 else len(query)
                query = (
                    query[:insert_pos]
                    + " AND "
                    + " AND ".join(date_conditions)
                    + " "
                    + query[insert_pos:]
                )

            df = conn.execute(query, bind_params).fetchdf()
            return df
        finally:
            conn.close()

    def _append_to_parquet(self, new_df: pd.DataFrame, path: str) -> pd.DataFrame:
        """
        Append new rows to existing parquet file, avoiding duplicates by timestamp.
        Returns the final DataFrame (existing + new).
        """
        if new_df.empty:
            return pd.DataFrame()

        if os.path.exists(path):
            existing = pd.read_parquet(path)
            if not existing.empty and "timestamp" in existing.columns:
                max_ts = existing["timestamp"].max()
                new_df = new_df[new_df["timestamp"] > max_ts]
                if new_df.empty:
                    existing.attrs["rows_appended"] = 0
                    return existing
                combined = pd.concat([existing, new_df], ignore_index=True)
                combined.to_parquet(path, index=False)
                combined.attrs["rows_appended"] = len(new_df)
                return combined

        new_df.to_parquet(path, index=False)
        new_df.attrs["rows_appended"] = len(new_df)
        return new_df

    def _overwrite_parquet(self, df: pd.DataFrame, path: str) -> int:
        """Overwrite a parquet file with a deterministic full rebuild."""
        if df.empty:
            return 0
        ordered = df.sort_values("timestamp").reset_index(drop=True)
        ordered.to_parquet(path, index=False)
        return len(ordered)

    def _replace_tail_in_parquet(
        self,
        new_df: pd.DataFrame,
        path: str,
        replace_from_ts: pd.Timestamp | None,
    ) -> int:
        """Replace the recent tail of a parquet file with recomputed rows."""
        if new_df.empty:
            return 0

        new_df = new_df.copy(deep=True)
        new_df.loc[:, "timestamp"] = pd.to_datetime(new_df["timestamp"])
        new_df = new_df.sort_values("timestamp").drop_duplicates(
            subset=["symbol_id", "exchange", "timestamp"],
            keep="last",
        )

        if not os.path.exists(path) or replace_from_ts is None:
            new_df.to_parquet(path, index=False)
            return len(new_df)

        existing = pd.read_parquet(path)
        if not existing.empty and "timestamp" in existing.columns:
            existing = existing.copy(deep=True)
            existing.loc[:, "timestamp"] = pd.to_datetime(existing["timestamp"])
            existing = existing[existing["timestamp"] < replace_from_ts]

        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.sort_values("timestamp").drop_duplicates(
            subset=["symbol_id", "exchange", "timestamp"],
            keep="last",
        )
        combined.to_parquet(path, index=False)
        return len(new_df)

    def _get_incremental_window(
        self,
        symbol_id: str,
        exchange: str,
        tail_bars: int,
        warmup_bars: int,
    ) -> tuple[str | None, pd.Timestamp | None]:
        """
        Return:
        - compute_start_date: where raw OHLCV should start for safe recomputation
        - replace_from_ts: feature rows at/after this timestamp should be replaced
        """
        conn = self._get_conn()
        try:
            replace_row = conn.execute(
                """
                SELECT MIN(timestamp) FROM (
                    SELECT timestamp
                    FROM _catalog
                    WHERE symbol_id = ? AND exchange = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                ) tail
                """,
                [symbol_id, exchange, int(tail_bars)],
            ).fetchone()
            replace_from_ts = pd.to_datetime(replace_row[0]) if replace_row and replace_row[0] else None
            if replace_from_ts is None:
                return None, None

            total_bars = int(tail_bars + warmup_bars)
            compute_row = conn.execute(
                """
                SELECT MIN(timestamp) FROM (
                    SELECT timestamp
                    FROM _catalog
                    WHERE symbol_id = ? AND exchange = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                ) windowed
                """,
                [symbol_id, exchange, total_bars],
            ).fetchone()
            compute_from_ts = pd.to_datetime(compute_row[0]) if compute_row and compute_row[0] else None
            return (
                compute_from_ts.strftime("%Y-%m-%d %H:%M:%S") if compute_from_ts is not None else None,
                replace_from_ts,
            )
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
        start_date: str = None,
        end_date: str = None,
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
                WHERE {{symbol_predicate}}
                  AND {{exchange_predicate}}
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
            start_date=start_date,
            end_date=end_date,
        )
        return df

    def compute_adx(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        period: int = 14,
        start_date: str = None,
        end_date: str = None,
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
                WHERE {{symbol_predicate}}
                  AND {{exchange_predicate}}
                  AND timestamp IS NOT NULL
                WINDOW w AS (ORDER BY timestamp)
            ),
            tr_dm AS (
                SELECT
                    symbol_id, exchange, timestamp,
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
            start_date=start_date,
            end_date=end_date,
        )
        return df

    def compute_sma(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        windows: List[int] = None,
        start_date: str = None,
        end_date: str = None,
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

        # Build date filter
        date_filter = ""
        if start_date or end_date:
            conditions = []
            if start_date:
                conditions.append(f"timestamp > '{start_date}'")
            if end_date:
                conditions.append(f"timestamp <= '{end_date}'")
            date_filter = " AND " + " AND ".join(conditions)

        sql = f"""
            WITH ranked AS (
                SELECT
                    symbol_id,
                    exchange,
                    timestamp,
                    close,
                    ROW_NUMBER() OVER w AS rn
                FROM _catalog
                WHERE {{symbol_predicate}}
                  AND {{exchange_predicate}}
                  AND timestamp IS NOT NULL
                  {date_filter}
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
            query = sql.replace(
                "{symbol_predicate}",
                "symbol_id = ?" if symbol_id else "TRUE",
            ).replace(
                "{exchange_predicate}",
                "exchange = ?" if exchange else "TRUE",
            )
            bind_params: list[Any] = []
            if symbol_id:
                bind_params.append(symbol_id)
            if exchange:
                bind_params.append(exchange)
            df = conn.execute(query, bind_params).fetchdf()
            return df
        finally:
            conn.close()

    def compute_ema(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        windows: List[int] = None,
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        Exponential Moving Average using DuckDB's EMA via exponential smoothing.
        """
        if windows is None:
            windows = [12, 26, 50, 200]

        # Build date filter
        date_filter = ""
        if start_date or end_date:
            conditions = []
            if start_date:
                conditions.append(f"timestamp > '{start_date}'")
            if end_date:
                conditions.append(f"timestamp <= '{end_date}'")
            date_filter = " AND " + " AND ".join(conditions)

        conn = self._get_conn()
        try:
            base_sql = f"""
                SELECT
                    symbol_id, exchange, timestamp, close,
                    LAG(close) OVER w AS prev_close
                FROM _catalog
                WHERE {"symbol_id = ?" if symbol_id else "TRUE"}
                  AND {"exchange = ?" if exchange else "TRUE"}
                  AND timestamp IS NOT NULL
                  {date_filter}
                WINDOW w AS (ORDER BY timestamp)
            """
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
                params: list[Any] = []
                if symbol_id:
                    params.append(symbol_id)
                if exchange:
                    params.append(exchange)
                df = conn.execute(query, params).fetchdf()
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
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        MACD (Moving Average Convergence Divergence).
        MACD_line = EMA_fast - EMA_slow
        Signal_line = EMA(MACD_line, signal)
        Histogram = MACD_line - Signal_line
        """
        conn = self._get_conn()
        try:
            date_filter = ""
            if start_date:
                date_filter += f" AND timestamp > '{start_date}'"
            if end_date:
                date_filter += f" AND timestamp <= '{end_date}'"

            base = f"""
                SELECT
                    symbol_id, exchange, timestamp, close
                FROM _catalog
                WHERE {"symbol_id = ?" if symbol_id else "TRUE"}
                  AND {"exchange = ?" if exchange else "TRUE"}
                  AND timestamp IS NOT NULL
                  {date_filter}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp) >= {slow}
                ORDER BY timestamp
            """
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
            params: list[Any] = []
            if symbol_id:
                params.append(symbol_id)
            if exchange:
                params.append(exchange)
            return conn.execute(query, params).fetchdf()
        finally:
            conn.close()

    def compute_atr(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        period: int = 14,
        start_date: str = None,
        end_date: str = None,
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
                WHERE {{symbol_predicate}}
                  AND {{exchange_predicate}}
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
            start_date=start_date,
            end_date=end_date,
        )
        return df

    def compute_bollinger_bands(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        period: int = 20,
        std_dev: float = 2.0,
        start_date: str = None,
        end_date: str = None,
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
                WHERE {{symbol_predicate}}
                  AND {{exchange_predicate}}
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
            start_date=start_date,
            end_date=end_date,
        )
        return df

    def compute_roc(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        periods: List[int] = None,
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        Rate of Change.
        ROC(n) = (close - close[n periods ago]) / close[n periods ago] * 100
        """
        if periods is None:
            periods = [1, 5, 10, 20]

        # Build date filter
        date_filter = ""
        if start_date or end_date:
            conditions = []
            if start_date:
                conditions.append(f"timestamp > '{start_date}'")
            if end_date:
                conditions.append(f"timestamp <= '{end_date}'")
            date_filter = " AND " + " AND ".join(conditions)

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
                    WHERE {"symbol_id = ?" if symbol_id else "TRUE"}
                      AND {"exchange = ?" if exchange else "TRUE"}
                      AND timestamp IS NOT NULL
                      {date_filter}
                    WINDOW w AS (ORDER BY timestamp)
                """
                params: list[Any] = []
                if symbol_id:
                    params.append(symbol_id)
                if exchange:
                    params.append(exchange)
                df = conn.execute(f"""
                    SELECT symbol_id, exchange, timestamp, close,
                           roc_{p} AS roc_{p}
                    FROM ({base_sql}) t
                    WHERE close_{p} IS NOT NULL
                    ORDER BY timestamp
                """, params).fetchdf()
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

    def compute_stage2(
        self,
        symbol_id: str = None,
        exchange: str = "NSE",
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """Compute Stage 2 uptrend features for a single symbol.

        Fetches OHLCV data with SMA-150 and SMA-200 from DuckDB, derives
        ``near_52w_high_pct`` and ``volume_ratio_20``, then delegates to
        ``add_stage2_features()`` in *indicators.py*.

        Note: ``rel_strength_score`` is cross-sectional and not available
        inside the per-symbol feature loop; it defaults to 0 in the scoring
        function and will be enriched by the ranker at rank time.
        """
        date_filter = ""
        if start_date or end_date:
            conds: list[str] = []
            if start_date:
                conds.append(f"timestamp > '{start_date}'")
            if end_date:
                conds.append(f"timestamp <= '{end_date}'")
            date_filter = " AND " + " AND ".join(conds)

        sym_pred = "symbol_id = ?" if symbol_id else "TRUE"
        exc_pred = "exchange = ?" if exchange else "TRUE"

        # Pull OHLCV with SMA-150 and SMA-200 in a single SQL pass.
        # min_periods handled in Python via add_stage2_features (min_periods=100).
        sql = f"""
            SELECT
                symbol_id, exchange, timestamp,
                open, high, low, close, volume,
                AVG(close) OVER w150 AS sma_150,
                AVG(close) OVER w200 AS sma_200,
                MAX(high)  OVER w252 AS high_252
            FROM _catalog
            WHERE {sym_pred}
              AND {exc_pred}
              AND timestamp IS NOT NULL
              {date_filter}
            WINDOW
                w150 AS (ORDER BY timestamp ROWS BETWEEN 149 PRECEDING AND CURRENT ROW),
                w200 AS (ORDER BY timestamp ROWS BETWEEN 199 PRECEDING AND CURRENT ROW),
                w252 AS (ORDER BY timestamp ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
            ORDER BY timestamp
        """

        conn = self._get_conn()
        try:
            bind_params: list[Any] = []
            if symbol_id:
                bind_params.append(symbol_id)
            if exchange:
                bind_params.append(exchange)
            df = conn.execute(sql, bind_params).fetchdf()
        finally:
            conn.close()

        if df.empty:
            return df

        # ── Derived fields ──────────────────────────────────────────────
        close = pd.to_numeric(df["close"], errors="coerce")
        high_252 = pd.to_numeric(df["high_252"], errors="coerce").replace(0, pd.NA)
        df["near_52w_high_pct"] = ((1.0 - close / high_252) * 100.0).clip(0.0, 100.0)
        df.drop(columns=["high_252"], inplace=True)

        if "volume" in df.columns:
            vol = pd.to_numeric(df["volume"], errors="coerce")
            vol_avg = vol.rolling(20, min_periods=10).mean().replace(0, pd.NA)
            df["volume_ratio_20"] = vol / vol_avg

        # rel_strength_score not available here — add_stage2_features defaults to 0
        df = add_stage2_features(df)

        # Keep only columns useful for the feature store
        keep_cols = ["symbol_id", "exchange", "timestamp", "close"] + list(STAGE2_FEATURE_COLUMNS)
        return df[[c for c in keep_cols if c in df.columns]]

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
                query = """
                    WITH ohlcv AS (
                        SELECT
                            symbol_id, exchange, timestamp,
                            open, high, low, close, volume
                        FROM _catalog
                        WHERE symbol_id = ?
                          AND exchange = ?
                          AND timestamp <= CAST(? AS TIMESTAMP)
                    ),
                    feat_latest AS (
                        SELECT DISTINCT ON (timestamp)
                            timestamp, open, high, low, close, volume
                        FROM _catalog
                        WHERE symbol_id = ?
                          AND exchange = ?
                          AND timestamp <= CAST(? AS TIMESTAMP)
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
                            WHERE symbol_id = ?
                              AND exchange = ?
                              AND timestamp <= o.timestamp
                              AND timestamp <= CAST(? AS TIMESTAMP)
                        )
                    ORDER BY o.timestamp
                """
                params = [
                    ohlcv_symbol_id,
                    exchange,
                    as_of_ts,
                    ohlcv_symbol_id,
                    exchange,
                    as_of_ts,
                    ohlcv_symbol_id,
                    exchange,
                    as_of_ts,
                ]
            else:
                query = """
                    WITH ohlcv AS (
                        SELECT
                            symbol_id, exchange, timestamp,
                            open, high, low, close, volume
                        FROM _catalog
                        WHERE symbol_id = ?
                          AND exchange = ?
                    ),
                    feat_with_row AS (
                        SELECT
                            symbol_id, exchange, timestamp,
                            close AS feat_close,
                            ROW_NUMBER() OVER w AS rn,
                            COUNT(*) OVER () AS total
                        FROM _catalog
                        WHERE symbol_id = ?
                          AND exchange = ?
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
                              AND f2.symbol_id = ?
                        )
                    ORDER BY o.timestamp
                """
                params = [ohlcv_symbol_id, exchange, ohlcv_symbol_id, exchange, ohlcv_symbol_id]
            return conn.execute(query, params).fetchdf()
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    #  DuckDB-based Feature Storage (Phase 1 restructuring)             #
    # ------------------------------------------------------------------ #

    def _ensure_feature_table(self, feature_name: str, df: pd.DataFrame = None):
        """Create feature table if not exists, with columns from df schema."""
        conn = self._get_conn()
        table_name = f"feat_{feature_name}"
        try:
            conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            conn.close()
            return
        except Exception as exc:
            logger.debug("Feature table %s does not exist yet: %s", table_name, exc)

        conn = self._get_conn()
        try:
            if df is not None and not df.empty:
                feat_cols = self._get_feature_columns(df)
                col_defs = [
                    "symbol_id VARCHAR",
                    "exchange VARCHAR",
                    "timestamp TIMESTAMP",
                    "date DATE",
                ]
                for col in feat_cols:
                    col_defs.append(f'"{col}" DOUBLE')

                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {", ".join(col_defs)},
                        PRIMARY KEY (symbol_id, exchange, timestamp)
                    )
                """
                conn.execute(create_sql)
            else:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        symbol_id VARCHAR,
                        exchange VARCHAR,
                        timestamp TIMESTAMP,
                        date DATE,
                        PRIMARY KEY (symbol_id, exchange, timestamp)
                    )
                """)
            conn.commit()
        finally:
            conn.close()

    def _get_feature_columns(self, df: pd.DataFrame) -> List[str]:
        """Get feature columns (exclude metadata)."""
        return [
            c
            for c in df.columns
            if c not in ("symbol_id", "exchange", "timestamp", "date")
        ]

    def store_features_duckdb(
        self,
        feature_name: str,
        df: pd.DataFrame,
    ) -> int:
        """Store features in DuckDB table using DuckDB's native append."""
        if df.empty:
            return 0

        df = df.copy(deep=True)
        if "date" not in df.columns:
            df.loc[:, "date"] = pd.to_datetime(df["timestamp"]).dt.date

        self._ensure_feature_table(feature_name, df)

        conn = self._get_conn()
        try:
            conn.execute(f"INSERT INTO feat_{feature_name} BY NAME SELECT * FROM df")
            conn.commit()
            rows = len(df)
        except Exception as exc:
            logger.exception("Failed writing feature rows into feat_%s: %s", feature_name, exc)
            raise
        finally:
            conn.close()

        return rows

    def load_features_duckdb(
        self,
        feature_name: str,
        symbol_id: str = None,
        exchange: str = None,
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """Load features from DuckDB table."""
        conn = self._get_conn()
        table_name = f"feat_{feature_name}"

        try:
            conn.execute(f"SELECT * FROM {table_name} LIMIT 1")
        except Exception as exc:
            logger.warning("Feature table %s read skipped: %s", table_name, exc)
            conn.close()
            return pd.DataFrame()

        conn = self._get_conn()
        try:
            conditions = []
            params: list[Any] = []
            if symbol_id:
                conditions.append("symbol_id = ?")
                params.append(symbol_id)
            if exchange:
                conditions.append("exchange = ?")
                params.append(exchange)
            if start_date:
                conditions.append("date >= ?")
                params.append(start_date)
            if end_date:
                conditions.append("date <= ?")
                params.append(end_date)

            where_clause = " AND ".join(conditions) if conditions else "1=1"
            df = conn.execute(f"""
                SELECT * FROM {table_name}
                WHERE {where_clause}
                ORDER BY symbol_id, timestamp
            """, params).fetchdf()
            return df
        finally:
            conn.close()

    def migrate_parquet_to_duckdb(self, feature_name: str = None) -> Dict[str, int]:
        """Migrate existing parquet files to DuckDB tables."""
        imported = {}
        feature_dirs = []

        if feature_name:
            for exc in ["NSE", "BSE"]:
                path = os.path.join(self.feature_store_dir, feature_name, exc)
                if os.path.exists(path):
                    feature_dirs.append((feature_name, exc, path))
        else:
            for root, dirs, files in os.walk(self.feature_store_dir):
                parts = root.split(os.sep)
                if len(parts) >= 3 and parts[-2] in ["NSE", "BSE"]:
                    feat_name = parts[-3]
                    exc = parts[-2]
                    if any(f.endswith(".parquet") for f in files):
                        feature_dirs.append((feat_name, exc, root))

        for feat_name, exc, dir_path in feature_dirs:
            logger.info(f"Migrating {feat_name}/{exc}...")
            parquet_files = [f for f in os.listdir(dir_path) if f.endswith(".parquet")]

            self._ensure_feature_table(feat_name)
            conn = self._get_conn()
            total_rows = 0

            try:
                max_ts = conn.execute(
                    f"SELECT MAX(timestamp) FROM feat_{feat_name}"
                ).fetchone()[0]
            except Exception as exc:
                logger.debug("Could not fetch max timestamp for feat_%s: %s", feat_name, exc)
                max_ts = None

            for pf in parquet_files:
                try:
                    df = pd.read_parquet(os.path.join(dir_path, pf))
                    if "date" not in df.columns:
                        df["date"] = pd.to_datetime(df["timestamp"]).dt.date
                    if max_ts:
                        df = df[df["timestamp"] > max_ts]
                    if df.empty:
                        continue

                    feat_cols = self._get_feature_columns(df)
                    cols_sql = ", ".join(
                        ["symbol_id", "exchange", "timestamp", "date"] + feat_cols
                    )

                    for _, row in df.iterrows():
                        vals = [
                            row["symbol_id"],
                            row["exchange"],
                            row["timestamp"],
                            row["date"],
                        ]
                        vals += [row.get(c) for c in feat_cols]
                        placeholders = ",".join(["?"] * len(vals))
                        conn.execute(
                            f"INSERT OR IGNORE INTO feat_{feat_name} ({cols_sql}) VALUES ({placeholders})",
                            vals,
                        )
                        total_rows += 1

                except Exception as e:
                    logger.warning(f"Error migrating {pf}: {e}")

            conn.commit()
            imported[f"{feat_name}/{exc}"] = total_rows
            logger.info(f"Migrated {total_rows} rows for {feat_name}/{exc}")
            conn.close()

        return imported

    # ------------------------------------------------------------------ #
    #  Bulk feature computation + persistence                            #
    # ------------------------------------------------------------------ #

    def compute_and_store_features(
        self,
        symbols: List[str] = None,
        exchanges: List[str] = None,
        feature_types: List[str] = None,
        warehouse_dir: str = None,
        use_duckdb: bool = False,
        incremental: bool = False,
        tail_bars: int = 252,
        warmup_bars: int | None = None,
        full_rebuild: bool = False,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, int]:
        """
        Compute all technical features for all (or specified) symbols
        and write them to the feature store.

        Args:
            symbols: List of symbol_ids. Defaults to all in OHLCV catalog.
            exchanges: List of exchanges. Defaults to ['NSE', 'BSE'].
            feature_types: List of features to compute.
                          Defaults to all: ['rsi', 'adx', 'sma', 'ema', 'macd', 'atr', 'bb', 'roc'].
            warehouse_dir: Where to write Parquet files. Defaults to feature_store_dir.
            use_duckdb: If True, store in DuckDB tables. If False, use Parquet files.

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
                    WHERE exchange IN (SELECT UNNEST(?))
                    ORDER BY symbol_id
                """,
                    [list(exchanges)],
                ).fetchdf()
                symbols = sym_rows["symbol_id"].tolist()
            finally:
                conn.close()

        warmup_bars = int(warmup_bars or tail_bars)
        incremental = bool(incremental and not full_rebuild)
        mode = "incremental" if incremental else "full_rebuild" if full_rebuild else "full"

        logger.info(
            f"Computing features for {len(symbols)} symbols: {feature_types} "
            f"(mode={mode}, tail_bars={tail_bars}, warmup_bars={warmup_bars})"
        )

        feature_methods = {
            "rsi": lambda sid, exc, start, end: self.compute_rsi(sid, exc, start_date=start, end_date=end),
            "adx": lambda sid, exc, start, end: self.compute_adx(sid, exc, start_date=start, end_date=end),
            "sma": lambda sid, exc, start, end: self.compute_sma(sid, exc, start_date=start, end_date=end),
            "ema": lambda sid, exc, start, end: self.compute_ema(sid, exc, start_date=start, end_date=end),
            "macd": lambda sid, exc, start, end: self.compute_macd(sid, exc, start_date=start, end_date=end),
            "atr": lambda sid, exc, start, end: self.compute_atr(sid, exc, start_date=start, end_date=end),
            "bb": lambda sid, exc, start, end: self.compute_bollinger_bands(sid, exc, start_date=start, end_date=end),
            "roc": lambda sid, exc, start, end: self.compute_roc(sid, exc, start_date=start, end_date=end),
            "supertrend": lambda sid, exc, start, end: self.compute_supertrend(sid, exc, start_date=start, end_date=end),
            # Stage 2 uptrend scoring (Weinstein methodology, Sprint 1)
            "stage2": lambda sid, exc, start, end: self.compute_stage2(sid, exc, start_date=start, end_date=end),
        }

        total_steps = max(1, len(feature_types) * len(exchanges) * len(symbols))
        processed_steps = 0
        progress_start = time.time()
        progress_interval = max(1, total_steps // 100)  # ~1% increments
        if callable(progress_callback):
            progress_callback(
                {
                    "status": "started",
                    "mode": mode,
                    "total_steps": total_steps,
                    "completed_steps": 0,
                    "feature_types": list(feature_types),
                    "exchanges": list(exchanges),
                    "symbols_count": len(symbols),
                }
            )

        rows_written = {}
        for feat_type in feature_types:
            if feat_type not in feature_methods:
                logger.warning(f"Unknown feature type: {feat_type}")
                continue

            method = feature_methods[feat_type]
            total_rows = 0

            for exc in exchanges:
                for sym in symbols:
                    rows_added = 0
                    step_status = "ok"
                    step_error = None
                    try:
                        compute_start = None
                        replace_from_ts = None
                        if incremental:
                            compute_start, replace_from_ts = self._get_incremental_window(
                                symbol_id=sym,
                                exchange=exc,
                                tail_bars=tail_bars,
                                warmup_bars=warmup_bars,
                            )

                        df = method(sym, exc, compute_start, None)
                        if df.empty:
                            step_status = "empty"
                            continue

                        if use_duckdb:
                            rows_added = self.store_features_duckdb(feat_type, df)
                            total_rows += rows_added
                            self.register_feature(
                                feature_name=feat_type,
                                symbol_id=sym,
                                exchange=exc,
                                rows_computed=rows_added,
                                feature_file=f"duckdb:feat_{feat_type}",
                                status="completed",
                            )
                        else:
                            feat_dir = os.path.join(warehouse_dir, feat_type, exc)
                            os.makedirs(feat_dir, exist_ok=True)
                            out_path = os.path.join(feat_dir, f"{sym}.parquet")
                            if incremental and os.path.exists(out_path) and replace_from_ts is not None:
                                df = df.copy(deep=True)
                                df.loc[:, "timestamp"] = pd.to_datetime(df["timestamp"])
                                df = df[df["timestamp"] >= replace_from_ts].copy()
                                rows_added = self._replace_tail_in_parquet(
                                    df,
                                    out_path,
                                    replace_from_ts,
                                )
                            elif full_rebuild or not os.path.exists(out_path):
                                rows_added = self._overwrite_parquet(df, out_path)
                            else:
                                combined = self._append_to_parquet(df, out_path)
                                rows_added = int(combined.attrs.get("rows_appended", 0))

                            total_rows += rows_added
                            self.register_feature(
                                feature_name=feat_type,
                                symbol_id=sym,
                                exchange=exc,
                                rows_computed=rows_added,
                                lookback_days=tail_bars if incremental else 0,
                                params={
                                    "mode": mode,
                                    "tail_bars": tail_bars if incremental else None,
                                    "warmup_bars": warmup_bars if incremental else None,
                                },
                                feature_file=out_path,
                                status="completed",
                            )
                    except Exception as e:
                        step_status = "error"
                        step_error = str(e)
                        logger.warning(
                            f"Error computing {feat_type} for {sym}/{exc}: {e}"
                        )
                    finally:
                        processed_steps += 1
                        if callable(progress_callback):
                            should_emit = (
                                processed_steps == 1
                                or processed_steps == total_steps
                                or (processed_steps % progress_interval == 0)
                                or step_status == "error"
                            )
                            if should_emit:
                                elapsed = max(0.0, time.time() - progress_start)
                                rate = (processed_steps / elapsed) if elapsed > 0 else 0.0
                                remaining = max(0, total_steps - processed_steps)
                                eta_seconds = int(remaining / rate) if rate > 0 else None
                                progress_callback(
                                    {
                                        "status": "running",
                                        "mode": mode,
                                        "feature_type": feat_type,
                                        "exchange": exc,
                                        "symbol_id": sym,
                                        "total_steps": total_steps,
                                        "completed_steps": processed_steps,
                                        "rows_added": int(rows_added),
                                        "step_status": step_status,
                                        "error": step_error,
                                        "elapsed_seconds": int(elapsed),
                                        "eta_seconds": eta_seconds,
                                    }
                                )

            rows_written[feat_type] = total_rows
            logger.info(
                f"{feat_type}: wrote {total_rows:,} rows across {len(symbols)} symbols"
            )

        if callable(progress_callback):
            total_rows_written = int(sum(rows_written.values()))
            progress_callback(
                {
                    "status": "completed",
                    "mode": mode,
                    "total_steps": total_steps,
                    "completed_steps": processed_steps,
                    "rows_written_total": total_rows_written,
                    "elapsed_seconds": int(max(0.0, time.time() - progress_start)),
                }
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
        use_duckdb: bool = False,
    ) -> pd.DataFrame:
        """
        Compute all technicals and store to feature store.
        Returns the merged DataFrame.
        """
        df = self.compute_all_technicals(symbol_id, exchange)
        if df.empty:
            return df

        if use_duckdb:
            rows_added = self.store_features_duckdb("all_technicals", df)
            self.register_feature(
                feature_name="all_technicals",
                symbol_id=symbol_id,
                exchange=exchange,
                rows_computed=rows_added,
                feature_file="duckdb:feat_all_technicals",
                status="completed",
            )
        else:
            out_dir = os.path.join(self.feature_store_dir, "all_technicals", exchange)
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"{symbol_id}.parquet")
            df = self._append_to_parquet(df, out_path)
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
        start_date: str = None,
        end_date: str = None,
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
                  AND (? IS NULL OR timestamp > CAST(? AS TIMESTAMP))
                  AND (? IS NULL OR timestamp <= CAST(? AS TIMESTAMP))
                ORDER BY timestamp
            """,
                (symbol_id, exchange, start_date, start_date, end_date, end_date),
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
        result = ohlcv[["symbol_id", "exchange", "timestamp"]].copy(deep=True)
        result.loc[:, "close"] = close.values
        result.loc[:, f"supertrend{suffix}"] = supertrend.values
        result.loc[:, f"supertrend_dir{suffix}"] = direction.values

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
                os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")),
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
            WHERE type='table' AND name = 'symbols'
        """)
        available = {r[0] for r in cur.fetchall()}
        conn_sqlite.close()

        if "symbols" not in available:
            logger.warning("symbols table not found in masterdb")
            return pd.DataFrame()

        conn_sqlite = sqlite3.connect(masterdb_path)
        exc_placeholders = ",".join("?" * len(exchanges))

        df = pd.read_sql(
            f"""
            SELECT
                s.security_id,
                s.symbol_id,
                s.symbol_name   AS name,
                s.sector        AS industry_group,
                s.industry      AS industry,
                s.mcap,
                s.exchange
            FROM symbols s
            WHERE s.exchange IN ({exc_placeholders})
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

        df["mcap_category"] = df["mcap"].apply(mcap_category)

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
                os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")),
                "data",
                "masterdata.db",
            )
        if not os.path.exists(masterdb_path):
            return pd.DataFrame()

        conn_sqlite = sqlite3.connect(masterdb_path)
        df = pd.read_sql(
            """
            SELECT
                s.security_id,
                s.symbol_id,
                s.symbol_name   AS name,
                s.sector        AS industry_group,
                s.industry      AS industry,
                s.mcap
            FROM symbols s
            WHERE s.symbol_id = ? AND s.exchange = ?
        """,
            conn_sqlite,
            params=(symbol_id, exchange),
        )
        conn_sqlite.close()
        return df
