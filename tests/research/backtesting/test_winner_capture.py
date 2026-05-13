"""Winner-capture research backtest tests."""

from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.backtesting.winner_capture import (
    WinnerCaptureConfig,
    run_winner_capture_analysis,
)


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


def test_winner_capture_identifies_top_gainers_and_capture_status(tmp_path, monkeypatch):
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    conn.execute(CREATE_CATALOG)
    rows = [
        ("AAA", "2025-01-01", 100.0),
        ("AAA", "2025-12-31", 200.0),
        ("BBB", "2025-01-01", 100.0),
        ("BBB", "2025-12-31", 250.0),
        ("CCC", "2025-01-01", 10.0),
        ("CCC", "2025-12-31", 12.0),
        ("BAD", "2025-01-01", 0.0),
        ("BAD", "2025-12-31", 999.0),
        ("NIFTY50", "2025-01-01", 1000.0),
        ("NIFTY50", "2025-12-31", 3000.0),
    ]
    conn.executemany(
        """
        INSERT INTO _catalog VALUES (?, NULL, 'NSE', ?, ?, ?, ?, ?, 1000, NULL, 1, ?)
        """,
        [(symbol, d, close, close, close, close, d) for symbol, d, close in rows],
    )
    conn.close()

    def _ranked(*_, **__):
        return {
            date(2025, 1, 3): pd.DataFrame(
                [
                    {
                        "symbol_id": "AAA",
                        "eligible_rank": 10,
                        "composite_score_adjusted": 88.0,
                        "close": 110.0,
                    },
                    {
                        "symbol_id": "BBB",
                        "eligible_rank": 60,
                        "composite_score_adjusted": 70.0,
                        "close": 105.0,
                    },
                ]
            ),
            date(2025, 1, 4): pd.DataFrame(
                [
                    {
                        "symbol_id": "AAA",
                        "eligible_rank": 20,
                        "composite_score_adjusted": 80.0,
                        "close": 115.0,
                    }
                ]
            ),
        }

    monkeypatch.setattr(
        "ai_trading_system.research.backtesting.winner_capture.load_research_ranked_by_date",
        _ranked,
    )

    result = run_winner_capture_analysis(
        tmp_path,
        config=WinnerCaptureConfig(year=2025, top_gainers=2, rank_cutoff=50, persist=False),
    )

    assert result["status"] == "ok"
    assert result["summary"]["winner_count"] == 2
    assert result["summary"]["captured_count"] == 1
    assert result["summary"]["missed_count"] == 1
    assert result["summary"]["capture_rate"] == 0.5
    assert [row["symbol_id"] for row in result["winners"]] == ["BBB", "AAA"]
    bbb, aaa = result["winners"]
    assert bbb["captured"] is False
    assert bbb["best_rank"] == 60
    assert aaa["captured"] is True
    assert aaa["first_capture_date"] == "2025-01-03"
    assert aaa["first_capture_rank"] == 10
    assert aaa["days_to_capture"] == 2
    assert aaa["return_at_capture"] == 0.1
