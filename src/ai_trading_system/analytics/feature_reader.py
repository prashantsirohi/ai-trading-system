"""
Feature reader for analytics components.
Reads from DuckDB-backed partitioned parquet files efficiently.
Handles both per-symbol parquet files and DuckDB partitioned parquet files.
"""

import os
import duckdb
import pandas as pd
from typing import Optional, List


class FeatureReader:
    """
    Read features from feature_store parquet files.
    Supports DuckDB partitioned parquet (fast SQL queries) and
    per-symbol parquet files (for visualizations/backtester).
    """

    def __init__(self, feature_store_dir: str, ohlcv_db_path: str):
        self.feature_store_dir = feature_store_dir
        self.ohlcv_db_path = ohlcv_db_path

    def _conn(self):
        return duckdb.connect(self.ohlcv_db_path)

    def _glob_pattern(self, feature: str, exchange: str) -> str:
        return os.path.join(self.feature_store_dir, feature, exchange, "*.parquet")

    def read_feature(
        self,
        feature: str,
        exchange: str = "NSE",
        symbols: Optional[List[str]] = None,
        date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Read feature data from partitioned parquet using DuckDB.
        Much faster than loading per-symbol parquet files.
        """
        pattern = self._glob_pattern(feature, exchange)
        conn = self._conn()
        try:
            query = f"SELECT * FROM read_parquet('{pattern}')"
            if symbols:
                sym_list = ",".join(f"'{s}'" for s in symbols)
                query += f" WHERE symbol_id IN ({sym_list})"
            if date:
                query += (
                    f" WHERE timestamp <= '{date}'"
                    if not symbols
                    else f" AND timestamp <= '{date}'"
                )
            if limit:
                query += f" LIMIT {limit}"
            return conn.execute(query).fetchdf()
        finally:
            conn.close()

    def read_latest(
        self,
        feature: str,
        exchange: str = "NSE",
        symbols: Optional[List[str]] = None,
        cutoff_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Read latest feature value for each symbol.
        """
        pattern = self._glob_pattern(feature, exchange)
        conn = self._conn()
        try:
            query = f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) AS rn
                    FROM read_parquet('{pattern}')
                """
            conditions = []
            if symbols:
                sym_list = ",".join(f"'{s}'" for s in symbols)
                conditions.append(f"symbol_id IN ({sym_list})")
            if cutoff_date:
                conditions.append(f"timestamp <= '{cutoff_date}'")
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += ") sub WHERE rn = 1"
            return conn.execute(query).fetchdf()
        finally:
            conn.close()

    def read_ohlcv(
        self,
        exchange: str = "NSE",
        symbols: Optional[List[str]] = None,
        date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read raw OHLCV data from DuckDB."""
        conn = self._conn()
        try:
            query = f"SELECT * FROM _catalog WHERE exchange = '{exchange}'"
            if symbols:
                sym_list = ",".join(f"'{s}'" for s in symbols)
                query += f" AND symbol_id IN ({sym_list})"
            if date:
                query += f" AND timestamp <= '{date}'"
            return conn.execute(query).fetchdf()
        finally:
            conn.close()

    def read_per_symbol(
        self,
        feature: str,
        symbol_id: str,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Read feature for a single symbol.
        Tries per-symbol parquet first, falls back to DuckDB partitioned query.
        """
        per_sym = os.path.join(
            self.feature_store_dir, feature, exchange, f"{symbol_id}.parquet"
        )
        if os.path.exists(per_sym):
            return pd.read_parquet(per_sym)
        pattern = self._glob_pattern(feature, exchange)
        conn = self._conn()
        try:
            return conn.execute(
                f"SELECT * FROM read_parquet('{pattern}') WHERE symbol_id = '{symbol_id}'"
            ).fetchdf()
        finally:
            conn.close()
