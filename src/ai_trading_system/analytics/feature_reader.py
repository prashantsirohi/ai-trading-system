"""
Feature reader for analytics components.
Reads from DuckDB-backed partitioned parquet files efficiently.
Handles both per-symbol parquet files and DuckDB partitioned parquet files.
"""

from pathlib import Path
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
        return str(self._contained_path(feature, exchange) / "*.parquet")

    def _contained_path(self, *parts: str) -> Path:
        root = Path(self.feature_store_dir).expanduser().resolve()
        candidate = root.joinpath(*(str(part) for part in parts)).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise ValueError("Feature path escapes the configured feature store") from exc
        return candidate

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
            query = "SELECT * FROM read_parquet(?)"
            params: list[object] = [pattern]
            conditions: list[str] = []
            if symbols:
                placeholders = ", ".join("?" for _ in symbols)
                conditions.append(f"symbol_id IN ({placeholders})")
                params.extend(symbols)
            if date:
                conditions.append("timestamp <= CAST(? AS TIMESTAMP)")
                params.append(date)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            if limit is not None:
                if int(limit) < 0 or int(limit) > 1_000_000:
                    raise ValueError("limit must be between 0 and 1000000")
                query += " LIMIT ?"
                params.append(int(limit))
            return conn.execute(query, params).fetchdf()
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
            query = """
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) AS rn
                    FROM read_parquet(?)
                """
            params: list[object] = [pattern]
            conditions = []
            if symbols:
                placeholders = ", ".join("?" for _ in symbols)
                conditions.append(f"symbol_id IN ({placeholders})")
                params.extend(symbols)
            if cutoff_date:
                conditions.append("timestamp <= CAST(? AS TIMESTAMP)")
                params.append(cutoff_date)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += ") sub WHERE rn = 1"
            return conn.execute(query, params).fetchdf()
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
            query = "SELECT * FROM _catalog WHERE exchange = ?"
            params: list[object] = [exchange]
            if symbols:
                placeholders = ", ".join("?" for _ in symbols)
                query += f" AND symbol_id IN ({placeholders})"
                params.extend(symbols)
            if date:
                query += " AND timestamp <= CAST(? AS TIMESTAMP)"
                params.append(date)
            return conn.execute(query, params).fetchdf()
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
        per_sym = self._contained_path(feature, exchange, f"{symbol_id}.parquet")
        if per_sym.exists():
            return pd.read_parquet(per_sym)
        pattern = self._glob_pattern(feature, exchange)
        conn = self._conn()
        try:
            return conn.execute(
                "SELECT * FROM read_parquet(?) WHERE symbol_id = ?",
                [pattern, symbol_id],
            ).fetchdf()
        finally:
            conn.close()
