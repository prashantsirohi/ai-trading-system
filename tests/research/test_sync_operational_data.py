"""Research data sync tests."""

from __future__ import annotations

import duckdb
import sqlite3

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.sync_operational_data import sync_operational_to_research


CREATE_CATALOG = """
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
    parquet_file VARCHAR,
    ingestion_version BIGINT,
    ingestion_ts TIMESTAMP
)
"""


def test_sync_operational_to_research_refreshes_source_range_without_duplicates(tmp_path):
    op = ensure_domain_layout(project_root=tmp_path, data_domain="operational")
    research = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    master = sqlite3.connect(op.master_db_path)
    master.execute("CREATE TABLE stock_details (Symbol TEXT PRIMARY KEY, Sector TEXT)")
    master.execute("INSERT INTO stock_details VALUES ('AAA', 'TECH')")
    master.commit()
    master.close()

    op_conn = duckdb.connect(str(op.ohlcv_db_path))
    op_conn.execute(CREATE_CATALOG)
    op_conn.execute(
        "INSERT INTO _catalog VALUES ('AAA', NULL, 'NSE', '2026-01-02', 10, 11, 9, 10.5, 1000, NULL, 1, '2026-01-02')"
    )
    op_conn.execute(
        "INSERT INTO _catalog VALUES ('AAA', NULL, 'NSE', '2026-01-02', 10, 11, 9, 10.7, 1000, NULL, 2, '2026-01-03')"
    )
    op_conn.execute(
        "INSERT INTO _catalog VALUES ('BBB', NULL, 'NSE', '2026-02-02', 20, 21, 19, 20.5, 1000, NULL, 1, '2026-02-02')"
    )
    op_conn.close()
    research_conn = duckdb.connect(str(research.ohlcv_db_path))
    research_conn.execute(CREATE_CATALOG)
    research_conn.execute(
        "INSERT INTO _catalog VALUES ('OLD', NULL, 'NSE', '2025-01-01', 1, 1, 1, 1, 1, NULL, 1, '2025-01-01')"
    )
    research_conn.execute(
        "INSERT INTO _catalog VALUES ('STALE', NULL, 'NSE', '2026-01-02', 1, 1, 1, 1, 1, NULL, 1, '2026-01-02')"
    )
    research_conn.close()

    dry = sync_operational_to_research(
        project_root=tmp_path,
        apply=False,
    )
    assert dry["status"] == "dry_run"
    assert dry["rows_to_copy"] == 3
    assert dry["refresh_mode"] == "replace_research_rows_inside_operational_date_range"

    applied = sync_operational_to_research(
        project_root=tmp_path,
        apply=True,
    )
    assert applied["status"] == "applied"
    assert applied["target_rows_in_source_range"] == 2
    assert applied["total_target_rows"] == 3
    assert applied["masterdata"]["status"] == "applied"

    conn = duckdb.connect(str(research.ohlcv_db_path), read_only=True)
    rows = conn.execute("SELECT symbol_id, close FROM _catalog ORDER BY symbol_id").fetchall()
    conn.close()
    assert rows == [("AAA", 10.7), ("BBB", 20.5), ("OLD", 1.0)]

    master_copy = sqlite3.connect(research.root_dir / "masterdata.db")
    try:
        master_rows = master_copy.execute("SELECT Symbol, Sector FROM stock_details").fetchall()
    finally:
        master_copy.close()
    assert master_rows == [("AAA", "TECH")]
