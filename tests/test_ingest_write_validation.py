from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from collectors.ingest_full import write_dfs_to_duckdb
from collectors.ingest_validation import IngestValidationError, validate_delivery_frame, validate_ohlcv_frame
from scripts.repair_ingest_schema import run as run_repair_ingest_schema


def _create_catalog_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            security_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            PRIMARY KEY (symbol_id, exchange, timestamp)
        )
        """
    )


def _create_delivery_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE _delivery (
            symbol_id VARCHAR,
            exchange VARCHAR,
            timestamp DATE,
            delivery_pct DOUBLE,
            volume BIGINT,
            delivery_qty BIGINT
        )
        """
    )


def test_validate_ohlcv_frame_rejects_swapped_symbol_exchange() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "NSE",
                "security_id": "123",
                "exchange": "RELIANCE",
                "timestamp": "2026-04-10",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1000,
            }
        ]
    )

    with pytest.raises(IngestValidationError, match="swapped symbol_id/exchange"):
        validate_ohlcv_frame(frame, source_label="test")


def test_validate_delivery_frame_rejects_out_of_range_delivery_pct() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "RELIANCE",
                "exchange": "NSE",
                "timestamp": "2026-04-10",
                "delivery_pct": 145.0,
                "volume": 10,
                "delivery_qty": 5,
            }
        ]
    )

    with pytest.raises(IngestValidationError, match="delivery_pct must be numeric between 0 and 100"):
        validate_delivery_frame(frame, source_label="test")


def test_write_dfs_to_duckdb_fails_closed_on_invalid_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        _create_catalog_table(conn)
        bad_rows = pd.DataFrame(
            [
                {
                    "symbol_id": "NSE",
                    "security_id": "123",
                    "exchange": "INFY",
                    "timestamp": "2026-04-10",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000,
                }
            ]
        )
        with pytest.raises(IngestValidationError):
            write_dfs_to_duckdb(conn, [bad_rows], "2026-04-10", "2026-04-10")
    finally:
        conn.close()


def test_repair_ingest_schema_repairs_swapped_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        _create_catalog_table(conn)
        _create_delivery_table(conn)
        conn.execute(
            """
            INSERT INTO _catalog VALUES
            ('NSE', '123', 'RELIANCE', '2026-04-10', 100.0, 102.0, 99.0, 101.0, 1000)
            """
        )
        conn.execute(
            """
            INSERT INTO _delivery VALUES
            ('NSE', 'INFY', '2026-04-10', 55.0, 500, 275)
            """
        )
    finally:
        conn.close()

    assert run_repair_ingest_schema(db_path=str(db_path), apply=False, fail_on_drift=True) == 2
    assert run_repair_ingest_schema(db_path=str(db_path), apply=True, fail_on_drift=True) == 0

    verify_conn = duckdb.connect(str(db_path), read_only=True)
    try:
        row = verify_conn.execute("SELECT symbol_id, exchange FROM _catalog").fetchone()
        assert row == ("RELIANCE", "NSE")
        delivery_row = verify_conn.execute("SELECT symbol_id, exchange FROM _delivery").fetchone()
        assert delivery_row == ("INFY", "NSE")
    finally:
        verify_conn.close()
