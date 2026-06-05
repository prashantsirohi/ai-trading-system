from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.features.valuation_cycle import refresh_valuation_cycle_features
from ai_trading_system.domains.features.valuation_index import _build_universe_index, _load_prices, refresh_valuation_index
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


def test_fundamental_ttm_repairs_tiny_adjusted_share_count_from_raw_share_count(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    screener = tmp_path / "screener.db"
    _create_ohlcv(ohlcv)
    _create_screener(screener)
    conn = sqlite3.connect(str(screener))
    try:
        conn.executemany(
            "INSERT INTO screener_financials VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("AAA", "annual", "2025-03-31", "adjusted_equity_shares_cr", 0.01, "2025-06-30", "screener", "bad", "2026-01-01"),
                ("AAA", "annual", "2025-03-31", "equity_shares_outstanding", 100_000_000, "2025-06-30", "screener", "bad", "2026-01-01"),
                ("AAA", "annual", "2025-03-31", "equity_share_capital", 100.0, "2025-06-30", "screener", "bad", "2026-01-01"),
                ("AAA", "annual", "2025-03-31", "reserves", 900.0, "2025-06-30", "screener", "bad", "2026-01-01"),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    refresh_fundamental_ttm(
        ohlcv_db_path=ohlcv,
        screener_db_path=screener,
        valuation_dates=["2026-01-10"],
    )

    conn = duckdb.connect(str(ohlcv), read_only=True)
    try:
        row = conn.execute("SELECT adjusted_equity_shares_cr, book_value_cr FROM fundamental_ttm WHERE symbol='AAA'").fetchone()
    finally:
        conn.close()
    assert row[0] == 10.0
    assert row[1] == 1000.0


def test_fundamental_ttm_does_not_mix_other_symbol_quarters(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    screener = tmp_path / "screener.db"
    _create_ohlcv(ohlcv)
    _create_screener(screener)
    conn = sqlite3.connect(str(screener))
    try:
        rows = []
        for idx, report_date in enumerate(["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"]):
            available_at = ["2025-05-15", "2025-08-14", "2025-11-14", "2026-01-09"][idx]
            rows.extend(
                [
                    ("ZZZ", "quarterly", report_date, "net_profit", 10_000 + idx, available_at, "screener", "huge", "2026-01-01"),
                    ("ZZZ", "quarterly", report_date, "sales", 20_000 + idx, available_at, "screener", "huge", "2026-01-01"),
                    ("ZZZ", "quarterly", report_date, "operating_profit", 30_000 + idx, available_at, "screener", "huge", "2026-01-01"),
                ]
            )
        conn.executemany("INSERT INTO screener_financials VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    finally:
        conn.close()

    refresh_fundamental_ttm(
        ohlcv_db_path=ohlcv,
        screener_db_path=screener,
        valuation_dates=["2026-01-10"],
    )

    conn = duckdb.connect(str(ohlcv), read_only=True)
    try:
        row = conn.execute("SELECT ttm_sales_cr, ttm_net_profit_cr FROM fundamental_ttm WHERE symbol='AAA'").fetchone()
    finally:
        conn.close()
    assert row == (406.0, 46.0)


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


def test_incremental_valuation_index_refresh_carries_forward_prior_level(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    screener = tmp_path / "screener.db"
    master = tmp_path / "master.db"
    _create_ohlcv(ohlcv)
    _create_screener(screener)
    _create_master(master)
    refresh_fundamental_ttm(ohlcv_db_path=ohlcv, screener_db_path=screener, valuation_dates=["2026-01-10", "2026-01-11"])

    refresh_valuation_index(
        ohlcv_db_path=ohlcv,
        master_db_path=master,
        universes=["UNIV_TOP2_MCAP"],
        from_date="2026-01-10",
        to_date="2026-01-10",
    )
    refresh_valuation_index(
        ohlcv_db_path=ohlcv,
        master_db_path=master,
        universes=["UNIV_TOP2_MCAP"],
        from_date="2026-01-11",
        to_date="2026-01-11",
    )

    conn = duckdb.connect(str(ohlcv), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT date, level, return_1d
            FROM universe_index_daily
            WHERE universe_id = 'UNIV_TOP2_MCAP'
              AND index_type = 'market_cap_weight'
            ORDER BY date
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows[0][1] == 1000.0
    assert rows[0][2] is None
    assert rows[1][1] != 1000.0
    assert rows[1][2] is not None
    assert round(float(rows[1][1]), 4) == round(1000.0 * (1.0 + float(rows[1][2])), 4)


def test_load_prices_excludes_sparse_dates_and_tracks_previous_date(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    _create_ohlcv(ohlcv)
    conn = duckdb.connect(str(ohlcv))
    try:
        conn.execute("INSERT INTO _catalog VALUES ('AAA', 'NSE', '2026-01-12', 125.0, 125.0)")
        prices = _load_prices(conn, from_date="2026-01-10", to_date="2026-01-12")
    finally:
        conn.close()

    assert set(prices["date"].astype(str)) == {"2026-01-10", "2026-01-11"}
    aaa_latest = prices.loc[prices["symbol"].eq("AAA")].sort_values("date").iloc[-1]
    assert str(aaa_latest["previous_date"]) == "2026-01-10 00:00:00"


def test_universe_index_ignores_returns_after_long_symbol_absence() -> None:
    stock = pd.DataFrame(
        [
            {
                "universe_id": "UNIV_TOP1_MCAP",
                "symbol": "AAA",
                "date": pd.Timestamp("2022-01-03").date(),
                "close": 100.0,
                "previous_date": pd.Timestamp("2015-12-31").date(),
                "previous_close": 1.0,
                "market_cap_cr": 1000.0,
                "ttm_net_profit_cr": 100.0,
            }
        ]
    )

    index = _build_universe_index(stock)

    row = index.loc[index["index_type"].eq("market_cap_weight")].iloc[0]
    assert row["level"] == 1000.0
    assert pd.isna(row["return_1d"])


def test_requested_refresh_range_removes_stale_rows_before_first_price(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    screener = tmp_path / "screener.db"
    master = tmp_path / "master.db"
    _create_ohlcv(ohlcv)
    _create_screener(screener)
    _create_master(master)
    refresh_fundamental_ttm(ohlcv_db_path=ohlcv, screener_db_path=screener, valuation_dates=["2026-01-10", "2026-01-11"])
    conn = duckdb.connect(str(ohlcv))
    try:
        conn.execute(
            """
            INSERT INTO universe_index_daily
            VALUES ('UNIV_TOP2_MCAP', 'market_cap_weight', '2026-01-09', 999, NULL, 1, 999, 99, 10, .1)
            """
        )
    finally:
        conn.close()

    refresh_valuation_index(
        ohlcv_db_path=ohlcv,
        master_db_path=master,
        universes=["UNIV_TOP2_MCAP"],
        from_date="2026-01-09",
        to_date="2026-01-11",
    )

    conn = duckdb.connect(str(ohlcv), read_only=True)
    try:
        stale = conn.execute("SELECT COUNT(*) FROM universe_index_daily WHERE date = '2026-01-09'").fetchone()[0]
    finally:
        conn.close()
    assert stale == 0


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
        latest = conn.execute(
            "SELECT valuation_zone, cycle_signal, pe_median_5y, pe_avg_5y FROM valuation_cycle_features ORDER BY date DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert latest[0] in {"bubble", "expensive"}
    assert latest[1] in {"top_zone", "neutral"}
    assert latest[2] is not None
    assert latest[3] is not None


def test_valuation_cycle_features_insert_uses_column_names_for_legacy_table_order(tmp_path: Path) -> None:
    db = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db))
    try:
        ensure_valuation_schema(conn)
        conn.execute("DROP TABLE valuation_cycle_features")
        conn.execute(
            """
            CREATE TABLE valuation_cycle_features (
                entity_type VARCHAR NOT NULL,
                entity_id VARCHAR NOT NULL,
                date DATE NOT NULL,
                pe_ttm DOUBLE,
                earnings_yield DOUBLE,
                pe_pctile_3y DOUBLE,
                pe_pctile_5y DOUBLE,
                pe_pctile_10y DOUBLE,
                pe_zscore_3y DOUBLE,
                pe_zscore_5y DOUBLE,
                pe_zscore_10y DOUBLE,
                valuation_zone VARCHAR,
                cycle_signal VARCHAR,
                PRIMARY KEY (entity_type, entity_id, date)
            )
            """
        )
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
        latest = conn.execute(
            """
            SELECT valuation_zone, cycle_signal, pe_median_5y, pe_avg_5y
            FROM valuation_cycle_features
            ORDER BY date DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    assert latest[0] in {"bubble", "expensive"}
    assert latest[1] in {"top_zone", "neutral"}
    assert latest[2] is not None
    assert latest[3] is not None


def test_valuation_cycle_features_use_partial_history_when_full_window_unavailable(tmp_path: Path) -> None:
    db = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db))
    try:
        ensure_valuation_schema(conn)
        rows = []
        dates = pd.date_range("2025-01-01", periods=260, freq="B")
        for idx, day in enumerate(dates, start=1):
            pe = 10.0 + idx / 10.0
            rows.append(("UNIV_TOP2_MCAP", "market_cap_weight", day.date(), 1000 + idx, 1 / pe, 2, 1000, 100, pe, 1 / pe))
        conn.executemany("INSERT INTO universe_index_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()

    result = refresh_valuation_cycle_features(ohlcv_db_path=db)

    assert result.rows == 260
    conn = duckdb.connect(str(db), read_only=True)
    try:
        latest = conn.execute("SELECT pe_pctile_3y, valuation_zone FROM valuation_cycle_features ORDER BY date DESC LIMIT 1").fetchone()
    finally:
        conn.close()
    assert latest[0] is not None
    assert latest[1] != "unknown"
