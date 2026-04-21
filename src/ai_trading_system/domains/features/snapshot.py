"""Snapshot and partitioned parquet I/O helpers."""

from __future__ import annotations

import glob
import os
from typing import Callable

import pandas as pd

from ai_trading_system.platform.logging.logger import logger


def get_partition_path(feature_store_dir: str, table_name: str, year: int, month: int) -> str:
    """Get partition path: data/features/table_name/year=YYYY/month=MM/."""
    return os.path.join(feature_store_dir, table_name, f"year={year}", f"month={month:02d}")


def store_partitioned(
    *,
    get_conn: Callable[[], object],
    feature_store_dir: str,
    table_name: str,
    df: pd.DataFrame,
    snapshot_id: int = None,
) -> int:
    """
    Store data in partitioned Parquet format (Iceberg-lite).
    Path: table_name/year=YYYY/month=MM/symbol.parquet

    Atomic write: write to temp, then rename.
    """
    if df.empty:
        return 0

    # Add date column if missing
    if "date" not in df.columns:
        df["date"] = pd.to_datetime(df["timestamp"]).dt.date

    rows_written = 0
    conn = get_conn()

    # Group by partition
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["month"] = pd.to_datetime(df["date"]).dt.month

    for (year, month), partition_df in df.groupby(["year", "month"]):
        partition_path = get_partition_path(feature_store_dir, table_name, year, month)
        os.makedirs(partition_path, exist_ok=True)

        # Group by symbol within partition
        for symbol in partition_df["symbol_id"].unique():
            sym_df = partition_df[partition_df["symbol_id"] == symbol].copy()

            # Atomic write: temp file then rename
            temp_path = os.path.join(partition_path, f"{symbol}.tmp.parquet")
            final_path = os.path.join(partition_path, f"{symbol}.parquet")

            sym_df.drop(columns=["year", "month"], errors="ignore").to_parquet(
                temp_path, index=False
            )

            # Atomic rename
            if os.path.exists(final_path):
                os.remove(final_path)
            os.rename(temp_path, final_path)

            # Register file
            try:
                conn.execute(
                    """
                    INSERT INTO _file_registry (file_path, table_name, feature_name, min_date, max_date, row_count, snapshot_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        final_path,
                        table_name,
                        table_name,
                        sym_df["date"].min(),
                        sym_df["date"].max(),
                        len(sym_df),
                        snapshot_id,
                    ),
                )
            except Exception as exc:
                logger.debug("File registry insert skipped for %s: %s", final_path, exc)

            rows_written += len(sym_df)

    # Update ingestion status
    for symbol in df["symbol_id"].unique():
        sym_df = df[df["symbol_id"] == symbol]
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO _ingestion_status
                (symbol_id, exchange, table_name, last_updated, last_date, status)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, 'completed')
            """,
                (
                    symbol,
                    sym_df["exchange"].iloc[0]
                    if "exchange" in sym_df.columns
                    else "NSE",
                    table_name,
                    sym_df["date"].max(),
                ),
            )
        except Exception as exc:
            logger.debug("Ingestion status upsert skipped for %s/%s: %s", table_name, symbol, exc)

    conn.commit()
    conn.close()

    # Cleanup temp files
    for f in os.listdir(feature_store_dir):
        if f.endswith(".tmp.parquet"):
            try:
                os.remove(os.path.join(feature_store_dir, f))
            except Exception as exc:
                logger.debug("Temp parquet cleanup skipped for %s: %s", f, exc)

    return rows_written


def load_partitioned(
    *,
    get_conn: Callable[[], object],
    feature_store_dir: str,
    table_name: str,
    symbol_id: str = None,
    start_date: str = None,
    end_date: str = None,
    snapshot_id: int = None,
) -> pd.DataFrame:
    """Load data from partitioned storage with optional time travel."""
    # If snapshot_id specified, load from that snapshot's files
    if snapshot_id:
        conn = get_conn()
        files = conn.execute(
            """
            SELECT file_path FROM _file_registry
            WHERE table_name = ? AND snapshot_id = ?
        """,
            (table_name, snapshot_id),
        ).fetchall()
        conn.close()

        if files:
            dfs = [pd.read_parquet(f[0]) for f in files]
            df = pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()
    else:
        # Load from current data
        pattern = os.path.join(
            feature_store_dir, table_name, "**", "*.parquet"
        )
        files = glob.glob(pattern, recursive=True)

        if not files:
            return pd.DataFrame()

        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)

    # Filter by symbol
    if symbol_id:
        df = df[df["symbol_id"] == symbol_id]

    # Filter by date range
    if start_date:
        df = df[df["date"] >= pd.to_datetime(start_date).date()]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date).date()]

    return df


def get_table_info(
    *,
    get_conn: Callable[[], object],
    table_name: str = None,
) -> pd.DataFrame:
    """Get info about partitioned tables."""
    conn = get_conn()
    try:
        if table_name:
            df = conn.execute(
                """
                SELECT
                    table_name,
                    COUNT(*) as num_files,
                    SUM(row_count) as total_rows,
                    MIN(min_date) as earliest_date,
                    MAX(max_date) as latest_date
                FROM _file_registry
                WHERE table_name = ?
                GROUP BY table_name
            """,
                (table_name,),
            ).fetchdf()
        else:
            df = conn.execute(
                """
                SELECT
                    table_name,
                    COUNT(*) as num_files,
                    SUM(row_count) as total_rows,
                    MIN(min_date) as earliest_date,
                    MAX(max_date) as latest_date
                FROM _file_registry
                GROUP BY table_name
            """
            ).fetchdf()
        return df
    finally:
        conn.close()

