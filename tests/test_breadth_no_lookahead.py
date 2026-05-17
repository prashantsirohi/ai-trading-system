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
