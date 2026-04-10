from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from collectors import repair_ohlcv_window
from collectors.repair_ohlcv_window import (
    _backup_current_rows,
    _build_comparison_results,
    _compare_trade_frames,
    _delete_window_rows,
)


def _init_catalog(db_path: Path, rows: list[tuple]) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _catalog (
                symbol_id VARCHAR,
                security_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )
        conn.execute("DELETE FROM _catalog")
        for row in rows:
            conn.execute("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)
    finally:
        conn.close()


def test_compare_trade_frames_detects_only_changed_dates() -> None:
    db_frame = pd.DataFrame(
        [
            {"timestamp": "2026-04-01", "open": 100.0, "high": 110.0, "low": 95.0, "close": 105.0, "volume": 1000},
            {"timestamp": "2026-04-02", "open": 106.0, "high": 111.0, "low": 101.0, "close": 107.0, "volume": 1100},
        ]
    )
    api_frame = pd.DataFrame(
        [
            {"timestamp": "2026-04-01", "open": 100.0, "high": 110.0, "low": 95.0, "close": 105.0, "volume": 1000},
            {"timestamp": "2026-04-02", "open": 206.0, "high": 211.0, "low": 201.0, "close": 207.0, "volume": 2100},
        ]
    )

    result = _compare_trade_frames("ABC", "1", db_frame, api_frame)

    assert result.symbol_id == "ABC"
    assert result.mismatch_dates == ["2026-04-02"]
    assert result.mismatches[0]["fields"]["close"] == {"db": 107.0, "api": 207.0}


def test_backup_current_rows_writes_only_target_symbols(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    _init_catalog(
        db_path,
        [
            ("AAA", "1", "NSE", "2026-04-01 00:00:00", 10.0, 11.0, 9.0, 10.5, 100),
            ("BBB", "2", "NSE", "2026-04-01 00:00:00", 20.0, 21.0, 19.0, 20.5, 200),
        ],
    )

    backup_path = _backup_current_rows(
        db_path=db_path,
        report_dir=report_dir,
        symbol_ids=["AAA"],
        exchange="NSE",
        from_date="2026-04-01",
        to_date="2026-04-01",
    )

    assert backup_path.exists()
    backed_up = pd.read_parquet(backup_path)
    assert list(backed_up["symbol_id"]) == ["AAA"]


def test_build_comparison_results_ignores_symbols_with_no_api_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog(
        db_path,
        [
            ("AAA", "1", "NSE", "2026-04-01 00:00:00", 10.0, 11.0, 9.0, 10.5, 100),
        ],
    )

    results = _build_comparison_results(
        db_path=db_path,
        available_symbols=[{"symbol_id": "AAA", "security_id": "1"}],
        api_frame_map={},
        exchange="NSE",
        from_date="2026-04-01",
        to_date="2026-04-01",
    )

    assert len(results) == 1
    assert results[0].db_rows == 1
    assert results[0].api_rows == 0
    assert results[0].mismatch_dates == []


def test_delete_window_rows_removes_stale_rows_for_target_symbols(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog(
        db_path,
        [
            ("AAA", "1", "NSE", "2026-04-01 00:00:00", 10.0, 11.0, 9.0, 10.5, 100),
            ("AAA", "1", "NSE", "2026-04-02 00:00:00", 11.0, 12.0, 10.0, 11.5, 110),
            ("BBB", "2", "NSE", "2026-04-01 00:00:00", 20.0, 21.0, 19.0, 20.5, 200),
        ],
    )

    deleted = _delete_window_rows(
        db_path=db_path,
        symbol_ids=["AAA"],
        exchange="NSE",
        from_date="2026-04-01",
        to_date="2026-04-02",
    )

    assert deleted == 2

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        remaining = conn.execute(
            "SELECT symbol_id, CAST(timestamp AS DATE) AS trade_date FROM _catalog ORDER BY symbol_id, trade_date"
        ).fetchall()
    finally:
        conn.close()

    assert remaining == [("BBB", pd.Timestamp("2026-04-01").date())]


def test_repair_window_requires_symbols_for_repair(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="No symbols available for repair window"):
        repair_ohlcv_window.repair_window(
            project_root=tmp_path,
            from_date="2026-03-31",
            to_date="2026-04-06",
            apply_changes=False,
        )
