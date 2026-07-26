from __future__ import annotations

from pathlib import Path

import duckdb

from ai_trading_system.ui.execution_api.services.readmodels.sector_detail import (
    _load_latest_technicals,
)


def test_latest_technicals_binds_market_symbol_values(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP,
                close DOUBLE, volume DOUBLE, high DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog
            VALUES ('SAFE', 'NSE', '2026-07-24', 100.0, 1000.0, 101.0)
            """
        )

    malicious = "SAFE') OR 1=1 --"
    result = _load_latest_technicals(str(db_path), [malicious])

    assert result.empty
    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute("SELECT COUNT(*) FROM _catalog").fetchone()[0] == 1
