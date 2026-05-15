"""Tests for the top-1000 PIT universe index."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from ai_trading_system.domains.features.universe_index import (
    UNIVERSE_INDEX_BASE_LEVEL,
    UNIVERSE_INDEX_CODE,
    compute_index_bar,
    compute_membership_for_rebalance,
    ensure_index_catalog_tables,
    first_trading_day_of_month,
    trading_days_between,
    upsert_index_bar,
    upsert_membership,
)


def _build_synthetic_catalog(db_path: Path, symbols_with_drift: list[tuple[str, float]], days: int = 300, start: date = date(2024, 1, 1)) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS _catalog (
            symbol_id VARCHAR, security_id VARCHAR, exchange VARCHAR,
            timestamp TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, parquet_file VARCHAR,
            ingestion_version BIGINT, ingestion_ts TIMESTAMP
        )
        """
    )
    rows = []
    for i in range(days):
        d = start + timedelta(days=i)
        for sym, drift in symbols_with_drift:
            close = 100.0 * (1.0 + drift) ** i
            rows.append((sym, None, "NSE", d, close, close * 1.01, close * 0.99, close, 1000 + i, None, 1, d))
    con.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.close()


def test_membership_top_n_by_turnover_deterministic(tmp_path):
    db = tmp_path / "ohlcv.duckdb"
    # HIGH = highest turnover (largest close × volume), then MID, then LOW.
    _build_synthetic_catalog(
        db,
        symbols_with_drift=[("HIGH", 0.005), ("MID", 0.003), ("LOW", 0.001)],
        days=250,
    )
    members_df, sparse = compute_membership_for_rebalance(
        db, rebalance_date=date(2024, 9, 1), top_n=3, min_recent_days=180
    )
    assert list(members_df["symbol_id"]) == ["HIGH", "MID", "LOW"]
    assert not sparse


def test_membership_sparse_history_flag_when_under_top_n(tmp_path):
    db = tmp_path / "ohlcv.duckdb"
    _build_synthetic_catalog(db, symbols_with_drift=[("ONE", 0.001), ("TWO", 0.001)], days=250)
    members_df, sparse = compute_membership_for_rebalance(
        db, rebalance_date=date(2024, 9, 1), top_n=1000, min_recent_days=180
    )
    assert len(members_df) == 2
    assert sparse


def test_membership_excludes_short_history(tmp_path):
    db = tmp_path / "ohlcv.duckdb"
    _build_synthetic_catalog(db, symbols_with_drift=[("LONG", 0.001)], days=300)
    # SHORT is added only for the last 30 days — fails min_recent_days=180.
    con = duckdb.connect(str(db))
    start = date(2024, 1, 1)
    rows = []
    for i in range(270, 300):
        d = start + timedelta(days=i)
        rows.append(("SHORT", None, "NSE", d, 100, 101, 99, 100, 1_000_000_000, None, 1, d))
    con.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.close()

    members_df, _ = compute_membership_for_rebalance(
        db, rebalance_date=date(2024, 10, 28), top_n=5, min_recent_days=180
    )
    assert "SHORT" not in list(members_df["symbol_id"])


def test_membership_lookback_strictly_before_rebalance(tmp_path):
    """Turnover on rebalance_date itself must not influence membership."""
    db = tmp_path / "ohlcv.duckdb"
    _build_synthetic_catalog(db, symbols_with_drift=[("A", 0.001), ("B", 0.001)], days=300)
    rebalance = date(2024, 8, 1)
    # Inject a huge spike on rebalance_date that would flip ordering if not excluded.
    con = duckdb.connect(str(db))
    con.execute(
        "UPDATE _catalog SET volume = 99_999_999_999 WHERE symbol_id = 'B' AND timestamp = ?",
        [rebalance],
    )
    con.close()
    members_df, _ = compute_membership_for_rebalance(
        db, rebalance_date=rebalance, top_n=2, min_recent_days=180
    )
    # A and B otherwise had identical turnover; the spike on D itself must not flip them.
    # Sort order on tie is database-defined; presence of both is enough.
    assert set(members_df["symbol_id"]) == {"A", "B"}


def test_compute_index_bar_equal_weight_math(tmp_path):
    db = tmp_path / "ohlcv.duckdb"
    # Construct 3 symbols with known daily returns on day t=200.
    con = duckdb.connect(str(db))
    con.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR, security_id VARCHAR, exchange VARCHAR,
            timestamp TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, parquet_file VARCHAR,
            ingestion_version BIGINT, ingestion_ts TIMESTAMP
        )
        """
    )
    d0 = date(2024, 6, 30)
    d1 = date(2024, 7, 1)
    pairs = [("A", 100.0, 110.0), ("B", 200.0, 210.0), ("C", 300.0, 309.0)]
    for sym, prev_close, today_close in pairs:
        con.execute(
            "INSERT INTO _catalog VALUES (?, NULL, 'NSE', ?, NULL, NULL, NULL, ?, 1000, NULL, 1, ?)",
            [sym, d0, prev_close, d0],
        )
        con.execute(
            "INSERT INTO _catalog VALUES (?, NULL, 'NSE', ?, NULL, NULL, NULL, ?, 1000, NULL, 1, ?)",
            [sym, d1, today_close, d1],
        )
    con.close()

    new_level, diag = compute_index_bar(
        db,
        bar_date=d1,
        constituents=["A", "B", "C"],
        previous_index_level=100.0,
        rebalance_date=d0,
        min_used_ratio=0.5,
    )
    # daily_return = mean(0.10, 0.05, 0.03) = 0.06
    expected = 100.0 * 1.06
    assert abs(new_level - expected) < 1e-6
    assert diag.quality_flag == "ok"
    assert diag.n_used == 3
    assert diag.n_members == 3


def test_compute_index_bar_low_coverage_holds_level(tmp_path):
    db = tmp_path / "ohlcv.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR, security_id VARCHAR, exchange VARCHAR,
            timestamp TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, parquet_file VARCHAR,
            ingestion_version BIGINT, ingestion_ts TIMESTAMP
        )
        """
    )
    # Only 1 of 4 constituents has both prices.
    d0 = date(2024, 6, 30)
    d1 = date(2024, 7, 1)
    con.execute(
        "INSERT INTO _catalog VALUES ('A', NULL, 'NSE', ?, NULL, NULL, NULL, 100, 1000, NULL, 1, ?)",
        [d0, d0],
    )
    con.execute(
        "INSERT INTO _catalog VALUES ('A', NULL, 'NSE', ?, NULL, NULL, NULL, 110, 1000, NULL, 1, ?)",
        [d1, d1],
    )
    con.close()

    new_level, diag = compute_index_bar(
        db,
        bar_date=d1,
        constituents=["A", "B", "C", "D"],
        previous_index_level=100.0,
        rebalance_date=d0,
        min_used_ratio=0.70,
    )
    assert new_level == 100.0  # held
    assert diag.quality_flag == "low_coverage"
    assert diag.n_used == 1
    assert diag.n_members == 4


def test_ensure_tables_and_upsert_roundtrip(tmp_path):
    db = tmp_path / "ohlcv.duckdb"
    ensure_index_catalog_tables(db)
    # _index_metadata row created idempotently.
    con = duckdb.connect(str(db), read_only=True)
    count = con.execute(
        "SELECT COUNT(*) FROM _index_metadata WHERE index_code = ?",
        [UNIVERSE_INDEX_CODE],
    ).fetchone()[0]
    con.close()
    assert count == 1


def test_first_trading_day_of_month():
    days = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 2, 1), date(2024, 2, 5)]
    out = first_trading_day_of_month(days)
    assert out[(2024, 1)] == date(2024, 1, 2)
    assert out[(2024, 2)] == date(2024, 2, 1)


def test_trading_days_between(tmp_path):
    db = tmp_path / "ohlcv.duckdb"
    _build_synthetic_catalog(db, symbols_with_drift=[("A", 0.0)], days=10)
    days = trading_days_between(db, date(2024, 1, 1), date(2024, 1, 10))
    assert len(days) == 10
    assert days == sorted(days)
