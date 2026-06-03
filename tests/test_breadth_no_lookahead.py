from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from ai_trading_system.analytics.regime.breadth import compute_market_regime_snapshot


def _seed_breadth_db(path: Path, *, future_crash: bool) -> None:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            close DOUBLE,
            volume BIGINT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE _index_catalog (
            index_code VARCHAR,
            date DATE,
            close DOUBLE
        )
        """
    )
    start = date(2025, 1, 1)
    rows = []
    idx_rows = []
    for i in range(230):
        d = start + timedelta(days=i)
        base = 100.0 + i
        if future_crash and i > 210:
            base = 20.0
        for symbol in ("AAA", "BBB", "CCC"):
            rows.append((symbol, "NSE", d.isoformat(), base, 1000))
        idx_rows.append(("UNIV_TOP1000", d.isoformat(), base))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany("INSERT INTO _index_catalog VALUES (?, ?, ?)", idx_rows)
    conn.close()


def test_breadth_snapshot_does_not_use_future_rows(tmp_path: Path) -> None:
    clean = tmp_path / "clean.duckdb"
    crashed = tmp_path / "crashed.duckdb"
    _seed_breadth_db(clean, future_crash=False)
    _seed_breadth_db(crashed, future_crash=True)

    as_of = "2025-07-20"
    clean_snapshot = compute_market_regime_snapshot(clean, as_of=as_of)
    crashed_snapshot = compute_market_regime_snapshot(crashed, as_of=as_of)

    assert clean_snapshot.pct_above_200dma == crashed_snapshot.pct_above_200dma
    assert clean_snapshot.top1000_above_200dma == crashed_snapshot.top1000_above_200dma
    assert clean_snapshot.regime == crashed_snapshot.regime


def test_breadth_snapshot_prefers_adjusted_close(tmp_path: Path) -> None:
    db = tmp_path / "adjusted.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            close DOUBLE,
            adjusted_close DOUBLE,
            volume BIGINT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE _index_catalog (
            index_code VARCHAR,
            date DATE,
            close DOUBLE
        )
        """
    )
    start = date(2025, 1, 1)
    rows = []
    idx_rows = []
    for i in range(260):
        d = start + timedelta(days=i)
        adjusted = 100.0 + i
        raw_aaa = 10.0 if i == 259 else adjusted
        rows.append(("AAA", "NSE", d.isoformat(), raw_aaa, adjusted, 1000))
        rows.append(("BBB", "NSE", d.isoformat(), adjusted, adjusted, 1000))
        idx_rows.append(("UNIV_TOP1000", d.isoformat(), adjusted))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?)", rows)
    conn.executemany("INSERT INTO _index_catalog VALUES (?, ?, ?)", idx_rows)
    conn.close()

    snapshot = compute_market_regime_snapshot(db, as_of=str(start + timedelta(days=259)))

    assert snapshot.pct_above_200dma == 1.0
    assert snapshot.advancers == 2
    assert snapshot.decliners == 0
    assert snapshot.ad_pct == 1.0


def test_breadth_snapshot_requires_252_bars_for_new_highs(tmp_path: Path) -> None:
    db = tmp_path / "eligible_252.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            close DOUBLE,
            volume BIGINT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE _index_catalog (
            index_code VARCHAR,
            date DATE,
            close DOUBLE
        )
        """
    )
    start = date(2025, 1, 1)
    rows = []
    idx_rows = []
    for i in range(260):
        d = start + timedelta(days=i)
        rows.append(("OLD", "NSE", d.isoformat(), 500.0 - i, 1000))
        if i >= 250:
            rows.append(("FRESH", "NSE", d.isoformat(), 1000.0 + i, 1000))
        idx_rows.append(("UNIV_TOP1000", d.isoformat(), 100.0 + i))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany("INSERT INTO _index_catalog VALUES (?, ?, ?)", idx_rows)
    conn.close()

    snapshot = compute_market_regime_snapshot(db, as_of=str(start + timedelta(days=259)))

    assert snapshot.new_52w_highs == 0
    assert snapshot.pct_at_52w_high == 0.0


def test_breadth_snapshot_uses_latest_universe_membership(tmp_path: Path) -> None:
    db = tmp_path / "membership.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            close DOUBLE,
            volume BIGINT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE _index_catalog (
            index_code VARCHAR,
            date DATE,
            close DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE _universe_membership (
            rebalance_date DATE,
            symbol_id VARCHAR,
            rank_by_turnover INTEGER,
            median_turnover DOUBLE,
            recent_days INTEGER,
            sparse_history BOOLEAN
        )
        """
    )
    start = date(2025, 1, 1)
    rows = []
    idx_rows = []
    for i in range(260):
        d = start + timedelta(days=i)
        rows.append(("AAA", "NSE", d.isoformat(), 100.0 + i, 1000))
        rows.append(("BBB", "NSE", d.isoformat(), 500.0 - i, 1000))
        rows.append(("CCC", "NSE", d.isoformat(), 400.0 - i, 1000))
        idx_rows.append(("UNIV_TOP1000", d.isoformat(), 100.0 + i))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany("INSERT INTO _index_catalog VALUES (?, ?, ?)", idx_rows)
    conn.execute(
        "INSERT INTO _universe_membership VALUES (?, 'AAA', 1, 1000.0, 200, FALSE)",
        [start + timedelta(days=200)],
    )
    conn.close()

    snapshot = compute_market_regime_snapshot(db, as_of=str(start + timedelta(days=259)))

    assert snapshot.total_symbols_count == 1
    assert snapshot.eligible_200dma_count == 1
    assert snapshot.pct_above_200dma == 1.0
