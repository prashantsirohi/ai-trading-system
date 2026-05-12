"""Research dynamic backtest loader tests."""

from __future__ import annotations

from datetime import date, timedelta

import duckdb

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.backtesting.research_loader import load_research_ranked_by_date


def test_research_loader_computes_engine_columns(tmp_path):
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
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
    start = date(2025, 1, 1)
    rows = []
    for i in range(240):
        d = start + timedelta(days=i)
        rows.append(("AAA", None, "NSE", d, 100 + i, 101 + i, 99 + i, 100 + i, 1000 + i, None, 1, d))
        rows.append(("BBB", None, "NSE", d, 200 - i * 0.1, 201 - i * 0.1, 199 - i * 0.1, 200 - i * 0.1, 900 + i, None, 1, d))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()

    ranked = load_research_ranked_by_date(
        tmp_path,
        from_date=start + timedelta(days=220),
        to_date=start + timedelta(days=239),
    )

    assert ranked
    frame = ranked[start + timedelta(days=239)]
    for column in ["sma_11", "sma_200", "atr_14", "volume_ratio_20", "swing_low_20"]:
        assert column in frame.columns
    assert "AAA" in set(frame["symbol_id"])
    assert int(frame.loc[frame["symbol_id"] == "AAA", "eligible_rank"].iloc[0]) == 1
