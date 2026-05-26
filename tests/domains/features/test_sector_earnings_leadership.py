from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb

from ai_trading_system.domains.features.fundamental_growth import refresh_fundamental_growth
from ai_trading_system.domains.features.fundamental_period_facts import refresh_fundamental_period_facts
from ai_trading_system.domains.features.sector_earnings_leadership import refresh_sector_earnings_leadership


def _create_screener(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE screener_financials (
                symbol TEXT,
                period_type TEXT,
                report_date DATE,
                metric_id TEXT,
                value REAL,
                available_at DATE,
                source TEXT,
                sync_batch_id TEXT,
                synced_at TIMESTAMP
            )
            """
        )
        rows = []
        reports = ["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"]
        payload = {
            "AUTO1": {
                "sales": [100, 105, 110, 115, 130],
                "net_profit": [10, 11, 12, 13, 18],
                "operating_profit": [20, 21, 22, 23, 32],
            },
            "AUTO2": {
                "sales": [80, 84, 88, 92, 104],
                "net_profit": [8, 8, 9, 10, 14],
                "operating_profit": [16, 16, 17, 18, 26],
            },
            "IT1": {
                "sales": [100, 99, 98, 97, 96],
                "net_profit": [-5, 4, 4, 3, 2],
                "operating_profit": [12, 11, 11, 10, 8],
            },
        }
        for symbol, metrics in payload.items():
            for idx, report in enumerate(reports):
                available = f"{report[:4]}-{int(report[5:7]) % 12 + 1:02d}-15"
                for metric, values in metrics.items():
                    rows.append((symbol, "quarterly", report, metric, values[idx], available, "screener", "b1", "2026-01-01"))
                rows.append((symbol, "quarterly", report, "expenses", metrics["sales"][idx] - metrics["operating_profit"][idx], available, "screener", "b1", "2026-01-01"))
        rows.extend(
            [
                ("AUTO1", "annual", "2025-03-31", "sales", 400, "2025-06-30", "screener", "b1", "2026-01-01"),
                ("AUTO1", "annual", "2025-03-31", "net_profit", 40, "2025-06-30", "screener", "b1", "2026-01-01"),
                ("AUTO1", "annual", "2025-03-31", "operating_profit", 80, "2025-06-30", "screener", "b1", "2026-01-01"),
                ("AUTO1", "annual", "2025-03-31", "expenses", 320, "2025-06-30", "screener", "b1", "2026-01-01"),
            ]
        )
        conn.executemany("INSERT INTO screener_financials VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    finally:
        conn.close()


def _create_master(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_details (
                Symbol TEXT,
                Name TEXT,
                Sector TEXT,
                [Industry Group] TEXT,
                Industry TEXT,
                MCAP REAL,
                exchange TEXT
            )
            """
        )
        conn.execute("CREATE TABLE symbols (symbol_id TEXT, symbol_name TEXT, exchange TEXT, sector TEXT, industry TEXT, mcap REAL)")
        conn.execute("CREATE TABLE sector_mapping (industry TEXT, system_sector TEXT)")
        conn.executemany(
            "INSERT INTO stock_details VALUES (?, ?, ?, ?, ?, ?, 'NSE')",
            [
                ("AUTO1", "Auto One", "Auto", "Automobiles", "Passenger Cars", 1000),
                ("AUTO2", "Auto Two", "Auto", "Automobiles", "Components", 800),
            ],
        )
        conn.execute("INSERT INTO symbols VALUES ('IT1', 'IT One', 'NSE', 'IT - Software', 'Software', 500)")
        conn.execute("INSERT INTO sector_mapping VALUES ('IT - Software', 'IT')")
        conn.commit()
    finally:
        conn.close()


def test_period_facts_pivot_and_symbols_dim_prefers_stock_details(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    screener = tmp_path / "screener.db"
    master = tmp_path / "master.db"
    _create_screener(screener)
    _create_master(master)

    result = refresh_fundamental_period_facts(ohlcv_db_path=ohlcv, screener_db_path=screener, master_db_path=master)

    assert result.facts_rows == 16
    conn = duckdb.connect(str(ohlcv), read_only=True)
    try:
        auto = conn.execute("SELECT * FROM fundamental_period_facts_enriched WHERE symbol='AUTO1' AND report_date='2026-03-31'").df().iloc[0]
        it = conn.execute("SELECT * FROM symbols_dim WHERE symbol='IT1'").df().iloc[0]
    finally:
        conn.close()
    assert auto["sector_name"] == "Auto"
    assert round(float(auto["opm_pct"]), 4) == round(32 / 130 * 100, 4)
    assert round(float(auto["npm_pct"]), 4) == round(18 / 130 * 100, 4)
    assert it["sector_name"] == "IT"


def test_company_and_sector_growth_use_positive_profit_bases(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    screener = tmp_path / "screener.db"
    master = tmp_path / "master.db"
    _create_screener(screener)
    _create_master(master)
    refresh_fundamental_period_facts(ohlcv_db_path=ohlcv, screener_db_path=screener, master_db_path=master)

    result = refresh_fundamental_growth(ohlcv_db_path=ohlcv)

    assert result.company_rows == 15
    conn = duckdb.connect(str(ohlcv), read_only=True)
    try:
        it_q2 = conn.execute("SELECT profit_qoq_growth FROM company_fundamental_growth WHERE symbol='IT1' AND report_date='2025-06-30'").fetchone()[0]
        auto_sector = conn.execute("SELECT * FROM sector_fundamental_growth WHERE sector_name='Auto' AND report_date='2026-03-31'").df().iloc[0]
    finally:
        conn.close()
    assert it_q2 is None
    assert round(float(auto_sector["sector_sales_yoy_growth"]), 4) == round((130 + 104) / (100 + 80) - 1, 4)
    assert float(auto_sector["sales_yoy_positive_pct"]) == 100.0


def test_sector_earnings_leadership_refresh_writes_latest_csv(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    screener = tmp_path / "screener.db"
    master = tmp_path / "master.db"
    output = tmp_path / "sector_earnings_leadership.csv"
    _create_screener(screener)
    _create_master(master)

    result = refresh_sector_earnings_leadership(
        ohlcv_db_path=ohlcv,
        screener_db_path=screener,
        master_db_path=master,
        output_csv=output,
    )

    assert result["status"] == "completed"
    assert result["latest_report_date"] == "2026-03-31"
    assert output.exists()
    conn = duckdb.connect(str(ohlcv), read_only=True)
    try:
        latest = conn.execute("SELECT * FROM sector_earnings_leadership WHERE report_date='2026-03-31' ORDER BY sector_earnings_growth_score DESC").df()
    finally:
        conn.close()
    assert latest.iloc[0]["sector_name"] == "Auto"
    assert latest.iloc[0]["earnings_trend_label"] == "accelerating_leader"
