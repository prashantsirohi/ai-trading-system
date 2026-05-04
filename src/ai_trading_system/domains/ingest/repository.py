"""DuckDB access helpers for ingest domain modules."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.analytics.data_trust import ensure_data_trust_schema
from ai_trading_system.platform.logging.logger import logger


def get_duckdb_conn(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(db_path))
    conn.execute("SET home_directory = '.'")
    return conn


def get_table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        """,
        (table_name,),
    ).fetchall()
    return {row[0] for row in rows}


def ensure_catalog_compatibility(conn: duckdb.DuckDBPyConnection) -> None:
    """Log legacy schema gaps without mutating live tables that may have dependencies."""
    expected_columns = {
        "_catalog": {
            "security_id",
            "parquet_file",
            "ingestion_version",
            "ingestion_ts",
            "provider",
            "provider_priority",
            "validation_status",
            "validated_against",
            "ingest_run_id",
            "repair_batch_id",
            "provider_confidence",
            "provider_discrepancy_flag",
            "provider_discrepancy_note",
            "adjusted_open",
            "adjusted_high",
            "adjusted_low",
            "adjusted_close",
            "adjustment_factor",
            "adjustment_source",
            "instrument_type",
            "is_benchmark",
            "benchmark_label",
            "isin",
            "series",
            "trading_segment",
        },
        "_catalog_history": {
            "security_id",
            "parquet_file",
            "ingestion_version",
            "ingestion_ts",
            "provider",
            "provider_priority",
            "validation_status",
            "validated_against",
            "ingest_run_id",
            "repair_batch_id",
            "provider_confidence",
            "provider_discrepancy_flag",
            "provider_discrepancy_note",
            "adjusted_open",
            "adjusted_high",
            "adjusted_low",
            "adjusted_close",
            "adjustment_factor",
            "adjustment_source",
            "instrument_type",
            "is_benchmark",
            "benchmark_label",
            "isin",
            "series",
            "trading_segment",
        },
    }

    for table_name, expected in expected_columns.items():
        actual = get_table_columns(conn, table_name)
        missing = sorted(expected - actual)
        if missing:
            logger.warning(
                "Legacy DuckDB schema detected for %s; compatibility mode will skip missing columns: %s",
                table_name,
                ", ".join(missing),
            )


def initialize_ingest_duckdb(db_path: str | Path) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute("SET home_directory = '.'")

    conn.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS snapshot_id_seq START 1
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _catalog (
            symbol_id           TEXT    NOT NULL,
            security_id         TEXT,
            exchange            TEXT    NOT NULL,
            timestamp           TIMESTAMP NOT NULL,
            open                DOUBLE,
            high                DOUBLE,
            low                 DOUBLE,
            close               DOUBLE,
            volume              BIGINT,
            parquet_file        TEXT,
            ingestion_version    BIGINT  DEFAULT nextval('snapshot_id_seq'),
            ingestion_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol_id, exchange, timestamp)
        )
        """
    )

    conn.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS _snap_id_seq START 1
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _snapshots (
            snapshot_id         BIGINT  PRIMARY KEY DEFAULT nextval('_snap_id_seq'),
            snapshot_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            symbols_processed   INTEGER,
            rows_written        BIGINT,
            from_date          TEXT,
            to_date            TEXT,
            status             TEXT    DEFAULT 'running',
            note               TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS _hist_id_seq START 1
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _catalog_history (
            hist_id            BIGINT  PRIMARY KEY DEFAULT nextval('_hist_id_seq'),
            snapshot_id        BIGINT,
            symbol_id          TEXT,
            security_id        TEXT,
            exchange           TEXT,
            timestamp          TIMESTAMP,
            open               DOUBLE,
            high               DOUBLE,
            low                DOUBLE,
            close              DOUBLE,
            volume             BIGINT,
            parquet_file       TEXT,
            ingestion_version   BIGINT,
            ingestion_ts        TIMESTAMP,
            archived_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS _pfile_id_seq START 1
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _parquet_files (
            pfile_id           BIGINT  PRIMARY KEY DEFAULT nextval('_pfile_id_seq'),
            parquet_file       TEXT    UNIQUE,
            symbol_id          TEXT,
            exchange           TEXT,
            rows_count         BIGINT,
            created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            active             BOOLEAN DEFAULT TRUE
        )
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_catalog_symbol
        ON _catalog(symbol_id, exchange)
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_snapshots_ts
        ON _snapshots(snapshot_ts)
        """
    )

    ensure_data_trust_schema(conn)
    ensure_catalog_compatibility(conn)

    conn.commit()
    conn.close()
    logger.info(f"DuckDB initialized: {db_path}")


def fetch_catalog_summary(db_path: str | Path) -> tuple[int, int, object]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        catalog_rows, symbol_count, latest_ts = conn.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT symbol_id), MAX(timestamp)
            FROM _catalog
            """
        ).fetchone()
        return int(catalog_rows or 0), int(symbol_count or 0), latest_ts
    finally:
        conn.close()


def fetch_catalog_close_frame(db_path: str | Path, validation_date: str) -> pd.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        frame = conn.execute(
            """
            WITH latest AS (
                SELECT symbol_id, MAX(timestamp) AS max_ts
                FROM _catalog
                WHERE exchange = 'NSE'
                  AND CAST(timestamp AS DATE) = ?
                GROUP BY symbol_id
            )
            SELECT c.symbol_id, c.close AS close_catalog
            FROM _catalog c
            INNER JOIN latest l
                    ON c.symbol_id = l.symbol_id
                   AND c.timestamp = l.max_ts
            WHERE c.exchange = 'NSE'
            """,
            [validation_date],
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        return pd.DataFrame(columns=["symbol_id", "close_catalog"])
    frame = frame.copy(deep=True)
    frame.loc[:, "symbol_id"] = frame["symbol_id"].astype(str).str.strip()
    frame.loc[:, "close_catalog"] = pd.to_numeric(frame["close_catalog"], errors="coerce")
    return frame.dropna(subset=["symbol_id", "close_catalog"]).drop_duplicates("symbol_id", keep="last")
