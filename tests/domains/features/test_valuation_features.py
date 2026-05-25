from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.features.valuation_cycle import refresh_valuation_cycle_features
from ai_trading_system.domains.features.valuation_index import refresh_valuation_index
from ai_trading_system.domains.features.valuation_schema import ensure_valuation_schema
from ai_trading_system.domains.features.valuation_ttm import refresh_fundamental_ttm


def _create_ohlcv(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                close DOUBLE,
                adjusted_close DOUBLE
            )
            """
        )
        rows = [
            ("AAA", "NSE", "2026-01-10", 100.0, 110.0),
            ("BBB", "NSE", "2026-01-10", 50.0, None),
            ("CCC", "NSE", "2026-01-10", 80.0, None),
            ("AAA", "NSE", "2026-01-11", 121.0, 121.0),
            ("BBB", "NSE", "2026-01-11", 45.0, None),
            ("CCC", "NSE", "2026-01-11", 88.0, None),
        ]
        conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


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
        for symbol, base_profit, shares in [("AAA", 10.0, 10.0), ("BBB", -5.0, 20.0)]:
            for idx, report_date in enumerate(["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"]):
                available_at = ["2025-05-15", "2025-08-14", "2025-11-14", "2026-01-09"][idx]
                rows.extend(
                    [
                        (symbol, "quarterly", report_date, "net_profit", base_profit + idx, available_at, "screener", "b1", "2026-01-01"),
                        (symbol, "quarterly", report_date, "sales", 100 + idx, available_at, "screener", "b1", "2026-01-01"),
                        (symbol, "quarterly", report_date, "operating_profit", 20 + idx, available_at, "screener", "b1", "2026-01-01"),
                    ]
                )
            rows.append((symbol, "annual", "2025-03-31", "adjusted_equity_shares_cr", shares, "2025-06-30", "screener", "b1", "2026-01-01"))
        rows.extend(
            [
                ("CCC", "annual", "2025-03-31", "net_profit", 40.0, "2025-06-30", "screener", "b1", "2026-01-01"),
                ("CCC", "annual", "2025-03-31", "sales", 400.0, "2025-06-30", "screener", "b1", "2026-01-01"),
                ("CCC", "annual", "2025-03-31", "operating_profit", 70.0, "2025-06-30", "screener", "b1", "2026-01-01"),
                ("CCC", "annual", "2025-03-31", "adjusted_equity_shares_cr", 5.0, "2025-06-30", "screener", "b1", "2026-01-01"),
                ("AAA", "quarterly", "2026-03-31", "net_profit", 999.0, "2026-05-15", "screener", "future", "2026-05-01"),
            ]
        )
        conn.executemany("INSERT INTO screener_financials VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    finally:
        conn.close()


def _create_master(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE symbols (symbol_id TEXT, exchange TEXT, sector TEXT, industry TEXT)")
        conn.execute("CREATE TABLE sector_mapping (industry TEXT, system_sector TEXT)")
        conn.executemany(
            "INSERT INTO symbols VALUES (?, 'NSE', ?, ?)",
            [("AAA", "Tech", "Software"), ("BBB", "Tech", "Software"), ("CCC", "Bank", "Private Bank")],
        )
        conn.executemany("INSERT INTO sector_mapping VALUES (?, ?)", [("Tech", "IT"), ("Bank", "Financials")])
        conn.commit()
    finally:
        conn.close()


def test_fundamental_ttm_excludes_future_rows_and_uses_annual_fallback(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    screener = tmp_path / "screener.db"
    _create_ohlcv(ohlcv)
    _create_screener(screener)

    result = refresh_fundamental_ttm(
        ohlcv_db_path=ohlcv,
        screener_db_path=screener,
        valuation_dates=["2026-01-10"],
    )

    assert result.rows == 3
    conn = duckdb.connect(str(ohlcv), read_only=True)
    try:
        frame = conn.execute("SELECT * FROM fundamental_ttm ORDER BY symbol").df()
    finally:
        conn.close()
    aaa = frame.loc[frame["symbol"].eq("AAA")].iloc[0]
    ccc = frame.loc[frame["symbol"].eq("CCC")].iloc[0]
    assert aaa["earnings_source"] == "quarterly_ttm"
    assert aaa["ttm_net_profit_cr"] == 46.0
    assert ccc["earnings_source"] == "annual_fallback"
    assert ccc["ttm_net_profit_cr"] == 40.0


def test_stock_universe_and_sector_valuation_use_aggregate_earnings(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    screener = tmp_path / "screener.db"
    master = tmp_path / "master.db"
    _create_ohlcv(ohlcv)
    _create_screener(screener)
    _create_master(master)
    refresh_fundamental_ttm(ohlcv_db_path=ohlcv, screener_db_path=screener, valuation_dates=["2026-01-10", "2026-01-11"])

    result = refresh_valuation_index(
        ohlcv_db_path=ohlcv,
        master_db_path=master,
        universes=["UNIV_TOP2_MCAP"],
        from_date="2026-01-10",
        to_date="2026-01-11",
    )

    assert result.stock_rows == 4
    conn = duckdb.connect(str(ohlcv), read_only=True)
    try:
        stock = conn.execute("SELECT * FROM stock_valuation_daily WHERE date='2026-01-10' ORDER BY symbol").df()
        sector = conn.execute("SELECT * FROM sector_valuation_daily WHERE date='2026-01-10'").df()
    finally:
        conn.close()
    aaa = stock.loc[stock["symbol"].eq("AAA")].iloc[0]
    assert aaa["close"] == 110.0
    assert aaa["market_cap_cr"] == 1100.0
    assert round(float(aaa["pe_ttm"]), 4) == round(1100 / 46, 4)
    total_mcap = float(stock["market_cap_cr"].sum())
    total_profit = float(stock["ttm_net_profit_cr"].sum())
    assert round(float(sector["total_market_cap_cr"].sum()), 4) == round(total_mcap, 4)
    assert round(float(sector["total_ttm_profit_cr"].sum()), 4) == round(total_profit, 4)


def test_valuation_cycle_features_labels_extreme_percentiles(tmp_path: Path) -> None:
    db = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db))
    try:
        ensure_valuation_schema(conn)
        rows = []
        for idx, pe in enumerate([10, 11, 12, 13, 40], start=1):
            rows.append(("UNIV_TOP2_MCAP", "market_cap_weight", f"2026-01-0{idx}", 1000 + idx, 0.01, 2, 1000, 100, pe, 1 / pe))
        conn.executemany("INSERT INTO universe_index_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()

    result = refresh_valuation_cycle_features(ohlcv_db_path=db, min_history_days=2)

    assert result.rows == 5
    conn = duckdb.connect(str(db), read_only=True)
    try:
        latest = conn.execute("SELECT * FROM valuation_cycle_features ORDER BY date DESC LIMIT 1").fetchone()
    finally:
        conn.close()
    assert latest[11] in {"bubble", "expensive"}
    assert latest[12] in {"top_zone", "neutral"}
