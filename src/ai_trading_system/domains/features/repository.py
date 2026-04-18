"""Feature registry and metadata DB helpers."""

from __future__ import annotations

import duckdb

from core.logging import logger


def get_conn(ohlcv_db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(ohlcv_db_path)


def init_feature_registry(ohlcv_db_path: str) -> None:
    conn = get_conn(ohlcv_db_path)

    # Create sequence if not exists
    try:
        conn.execute("CREATE SEQUENCE IF NOT EXISTS _feat_id_seq START 1")
    except Exception as exc:
        logger.debug("Feature id sequence bootstrap skipped: %s", exc)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _feature_registry (
            feature_id BIGINT PRIMARY KEY DEFAULT nextval('_feat_id_seq'),
            feature_name VARCHAR NOT NULL,
            symbol_id VARCHAR,
            exchange VARCHAR,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            rows_computed INTEGER DEFAULT 0,
            lookback_days INTEGER DEFAULT 0,
            params VARCHAR,
            feature_file VARCHAR,
            snapshot_id BIGINT,
            status VARCHAR DEFAULT 'completed',
            note VARCHAR
        )
    """
    )

    conn.commit()
    conn.close()


def init_metadata_tables(ohlcv_db_path: str) -> None:
    """Initialize metadata tables for Iceberg-lite architecture."""
    conn = get_conn(ohlcv_db_path)

    # File registry - tracks all parquet files
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _file_registry (
            file_id INTEGER PRIMARY KEY,
            file_path VARCHAR,
            table_name VARCHAR,
            feature_name VARCHAR,
            min_date DATE,
            max_date DATE,
            row_count INTEGER,
            snapshot_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Ingestion status - tracks what's been updated
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _ingestion_status (
            symbol_id VARCHAR,
            exchange VARCHAR,
            table_name VARCHAR,
            last_updated TIMESTAMP,
            last_date DATE,
            status VARCHAR DEFAULT 'pending',
            PRIMARY KEY (symbol_id, exchange, table_name)
        )
    """
    )

    conn.commit()
    conn.close()


def register_feature(
    ohlcv_db_path: str,
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
    conn = get_conn(ohlcv_db_path)
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


def get_last_feature_date(
    ohlcv_db_path: str,
    feature_name: str,
    symbol_id: str = None,
    exchange: str = "NSE",
) -> str | None:
    """Get the last date for which a feature was computed."""
    conn = get_conn(ohlcv_db_path)
    try:
        if symbol_id:
            result = conn.execute(
                f"""
                SELECT MAX(date) FROM feat_{feature_name}
                WHERE symbol_id = ? AND exchange = ?
            """,
                (symbol_id, exchange),
            ).fetchone()[0]
        else:
            result = conn.execute(
                f"SELECT MAX(date) FROM feat_{feature_name}"
            ).fetchone()[0]
        return str(result) if result else None
    except Exception as exc:
        logger.debug(
            "Last feature date unavailable for %s (%s/%s): %s",
            feature_name,
            symbol_id,
            exchange,
            exc,
        )
        return None
    finally:
        conn.close()

