from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from ai_trading_system.domains.ingest.price_continuity import BulkRawPriceBasisShiftError
from ai_trading_system.interfaces.cli.backfill_operational_valuation import (
    copy_ohlcv_backfill,
    copy_ohlcv_chunk,
    inspect_ohlcv_coverage,
    validation_summary,
)


def _create_source(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
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
                parquet_file VARCHAR,
                ingestion_version BIGINT,
                ingestion_ts TIMESTAMP
            )
            """
        )
        conn.executemany(
            "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("AAA", "1", "NSE", "2016-01-01 00:00:00", 10.0, 12.0, 9.0, 11.0, 1000, "research/a.parquet", 7, "2026-01-01"),
                ("AAA", "1", "NSE", "2016-01-01 15:30:00", 11.0, 13.0, 10.0, 12.0, 1200, "research/a2.parquet", 8, "2026-01-02"),
                ("BBB", "2", "NSE", "2016-01-02 00:00:00", 20.0, 22.0, 19.0, 21.0, 2000, "research/b.parquet", 7, "2026-01-01"),
                ("CCC", "3", "BSE", "2016-01-02 00:00:00", 30.0, 32.0, 29.0, 31.0, 3000, "research/c.parquet", 7, "2026-01-01"),
            ],
        )
    finally:
        conn.close()


def _create_target(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                security_id VARCHAR,
                parquet_file VARCHAR,
                ingestion_version BIGINT,
                ingestion_ts TIMESTAMP,
                provider VARCHAR,
                provider_priority INTEGER,
                validation_status VARCHAR,
                validated_against VARCHAR,
                ingest_run_id VARCHAR,
                repair_batch_id VARCHAR,
                provider_confidence DOUBLE,
                provider_discrepancy_flag BOOLEAN,
                provider_discrepancy_note VARCHAR,
                adjusted_open DOUBLE,
                adjusted_high DOUBLE,
                adjusted_low DOUBLE,
                adjusted_close DOUBLE,
                adjustment_factor DOUBLE,
                adjustment_source VARCHAR,
                instrument_type VARCHAR,
                is_benchmark BOOLEAN,
                benchmark_label VARCHAR,
                isin VARCHAR,
                series VARCHAR,
                trading_segment VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog (
                symbol_id, exchange, timestamp, open, high, low, close, volume,
                security_id, parquet_file, ingestion_version, ingestion_ts,
                provider, validation_status
            )
            VALUES ('AAA', 'NSE', '2016-01-01 09:15:00', 99, 99, 99, 99, 99, 'old', 'operational.parquet', 1, '2026-01-03', 'nse', 'validated')
            """
        )
    finally:
        conn.close()


def test_copy_ohlcv_chunk_is_idempotent_and_preserves_existing_rows(tmp_path: Path) -> None:
    source = tmp_path / "research.duckdb"
    target = tmp_path / "operational.duckdb"
    _create_source(source)
    _create_target(target)

    dry_run = copy_ohlcv_chunk(
        source_db_path=source,
        target_db_path=target,
        from_date="2016-01-01",
        to_date="2016-01-02",
        dry_run=True,
        run_id="test_run",
    )

    assert dry_run.source_rows == 2
    assert dry_run.missing_rows == 1
    assert dry_run.inserted_rows == 0

    result = copy_ohlcv_chunk(
        source_db_path=source,
        target_db_path=target,
        from_date="2016-01-01",
        to_date="2016-01-02",
        run_id="test_run",
    )
    rerun = copy_ohlcv_chunk(
        source_db_path=source,
        target_db_path=target,
        from_date="2016-01-01",
        to_date="2016-01-02",
        run_id="test_run",
    )

    assert result.inserted_rows == 1
    assert rerun.missing_rows == 0
    assert rerun.inserted_rows == 0

    conn = duckdb.connect(str(target), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol_id, CAST(timestamp AS DATE) AS trade_date, close, provider, validation_status
            FROM _catalog
            ORDER BY symbol_id, trade_date
            """
        ).fetchall()
        duplicates = conn.execute(
            """
            SELECT symbol_id, exchange, CAST(timestamp AS DATE), COUNT(*)
            FROM _catalog
            GROUP BY 1, 2, 3
            HAVING COUNT(*) > 1
            """
        ).fetchall()
    finally:
        conn.close()

    normalized_rows = [(symbol, str(trade_date), close, provider, status) for symbol, trade_date, close, provider, status in rows]
    assert normalized_rows == [
        ("AAA", "2016-01-01", 99.0, "nse", "validated"),
        ("BBB", "2016-01-02", 21.0, "research_ohlcv_backfill", "research_backfill"),
    ]
    assert duplicates == []


def test_coverage_and_yearly_copy_report_missing_rows(tmp_path: Path) -> None:
    source = tmp_path / "research.duckdb"
    target = tmp_path / "operational.duckdb"
    _create_source(source)
    _create_target(target)

    coverage = inspect_ohlcv_coverage(
        source_db_path=source,
        target_db_path=target,
        from_date="2016-01-01",
        to_date="2016-01-02",
    )
    chunks = copy_ohlcv_backfill(
        source_db_path=source,
        target_db_path=target,
        from_date="2016-01-01",
        to_date="2016-01-02",
        dry_run=True,
    )

    assert coverage.source_rows == 2
    assert coverage.source_symbols == 2
    assert coverage.missing_rows == 1
    assert coverage.missing_rows_by_year == {2016: 1}
    assert len(chunks) == 1
    assert chunks[0].missing_rows == 1


def test_copy_normalizes_dhan_utc_evening_timestamp_to_ist_trade_date(tmp_path: Path) -> None:
    source = tmp_path / "research.duckdb"
    target = tmp_path / "operational.duckdb"
    _create_source(source)
    _create_target(target)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO _catalog
            VALUES ('DDD', '4', 'NSE', '2016-01-02 18:30:00', 30, 32, 29, 31, 3000,
                    'research/d.parquet', 9, '2026-01-03')
            """
        )
    finally:
        conn.close()

    result = copy_ohlcv_chunk(
        source_db_path=source,
        target_db_path=target,
        from_date="2016-01-03",
        to_date="2016-01-03",
        run_id="test_run",
    )

    conn = duckdb.connect(str(target), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT symbol_id, timestamp, close
            FROM _catalog
            WHERE symbol_id = 'DDD'
            """
        ).fetchone()
    finally:
        conn.close()
    assert result.inserted_rows == 1
    assert row is not None
    assert row[0] == "DDD"
    assert str(row[1]) == "2016-01-03 00:00:00"
    assert row[2] == 31.0


def test_validation_summary_handles_missing_valuation_tables(tmp_path: Path) -> None:
    target = tmp_path / "operational.duckdb"
    _create_target(target)

    summaries = validation_summary(target)

    by_table = {summary.table_name: summary for summary in summaries}
    assert by_table["_catalog"].rows == 1
    assert by_table["_catalog"].min_date == "2016-01-01"
    assert by_table["fundamental_ttm"].rows == 0
    assert by_table["valuation_cycle_features"].max_date is None


def test_copy_rejects_broad_candidate_basis_shift_without_writing(tmp_path: Path) -> None:
    source = tmp_path / "research.duckdb"
    target = tmp_path / "operational.duckdb"
    _create_source(source)
    _create_target(target)
    source_conn = duckdb.connect(str(source))
    target_conn = duckdb.connect(str(target))
    try:
        source_conn.execute("DELETE FROM _catalog")
        target_conn.execute("DELETE FROM _catalog")
        rows = []
        for index in range(10):
            symbol = f"SYM{index:02d}"
            security_id = str(index)
            rows.extend(
                [
                    (
                        symbol,
                        security_id,
                        "NSE",
                        "2026-01-01",
                        10.0,
                        10.0,
                        10.0,
                        10.0,
                        100,
                        f"research/{symbol}.parquet",
                        index * 2,
                        "2026-01-03",
                    ),
                    (
                        symbol,
                        security_id,
                        "NSE",
                        "2026-01-02",
                        100.0,
                        100.0,
                        100.0,
                        100.0,
                        100,
                        f"research/{symbol}.parquet",
                        index * 2 + 1,
                        "2026-01-03",
                    ),
                ]
            )
        source_conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        source_conn.close()
        target_conn.close()

    with pytest.raises(BulkRawPriceBasisShiftError, match=r"2026-01-02 \(10 symbols\)"):
        copy_ohlcv_chunk(
            source_db_path=source,
            target_db_path=target,
            from_date="2026-01-01",
            to_date="2026-01-02",
            run_id="test_run",
        )

    conn = duckdb.connect(str(target), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM _catalog").fetchone() == (0,)
    finally:
        conn.close()
