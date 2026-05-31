from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path

import duckdb


def _load_readmodel():
    path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "ai_trading_system"
        / "ui"
        / "execution_api"
        / "services"
        / "readmodels"
        / "market_breadth.py"
    )
    spec = importlib.util.spec_from_file_location("market_breadth_readmodel", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_market_breadth_history_returns_percentages_and_dates(tmp_path: Path) -> None:
    root = tmp_path
    data_dir = root / "data"
    data_dir.mkdir()
    db_path = data_dir / "ohlcv.duckdb"

    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE _catalog (
                timestamp DATE,
                symbol_id VARCHAR,
                exchange VARCHAR,
                close DOUBLE
            )
            """
        )
        start = date(2025, 1, 1)
        rows = []
        for i in range(210):
            ts = start + timedelta(days=i)
            rows.extend(
                [
                    # Rising symbol is above every mature SMA.
                    (ts, "AAA", "NSE", 100.0 + i),
                    # Falling symbol is below every mature SMA.
                    (ts, "BBB", "NSE", 300.0 - i),
                    # Non-NSE rows must not affect NSE breadth.
                    (ts, "CCC", "BSE", 1000.0 + i),
                ]
            )
        con.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?)", rows)
    finally:
        con.close()

    readmodel = _load_readmodel()
    result = readmodel.get_market_breadth_history(root, limit=1)

    assert result["available"] is True
    assert result["unit"] == "percent"
    assert result["row_count"] == 1
    latest = result["rows"][0]
    assert latest["trade_date"] == "2025-07-29"
    assert latest["pct_above_sma20"] == 50.0
    assert latest["pct_above_sma50"] == 50.0
    assert latest["pct_above_sma200"] == 50.0
    assert latest["above_sma20"] == 1
    assert latest["above_sma50"] == 1
    assert latest["above_sma200"] == 1
    assert latest["symbols_sma20"] == 2
    assert latest["symbols_sma50"] == 2
    assert latest["symbols_sma200"] == 2
    assert latest["symbols_total"] == 2
    assert latest["new_52w_highs"] == 0
    assert latest["new_52w_lows"] == 0
    assert latest["advancers"] == 1
    assert latest["decliners"] == 1
    assert latest["index_level"] is None
    assert latest["pe_pctile_5y"] is None


def test_market_breadth_history_joins_market_context_and_requires_252_bars(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = data_dir / "ohlcv.duckdb"
    start = date(2019, 6, 1)
    final_date = start + timedelta(days=259)

    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE _catalog (
                timestamp DATE,
                symbol_id VARCHAR,
                exchange VARCHAR,
                close DOUBLE
            )
            """
        )
        rows = []
        for i in range(260):
            ts = start + timedelta(days=i)
            rows.extend([(ts, "AAA", "NSE", 100.0 + i), (ts, "BBB", "NSE", 400.0 - i)])
        con.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?)", rows)
        con.execute(
            """
            CREATE TABLE universe_index_daily (
                universe_id VARCHAR,
                index_type VARCHAR,
                date DATE,
                level DOUBLE
            )
            """
        )
        con.execute(
            "INSERT INTO universe_index_daily VALUES ('UNIV_TOP1000_MCAP', 'market_cap_weight', ?, 1234.5)",
            [final_date],
        )
        con.execute(
            """
            CREATE TABLE valuation_cycle_features (
                entity_type VARCHAR,
                entity_id VARCHAR,
                date DATE,
                pe_pctile_5y DOUBLE
            )
            """
        )
        con.execute(
            "INSERT INTO valuation_cycle_features VALUES ('universe', 'UNIV_TOP1000_MCAP', ?, 82.5)",
            [final_date],
        )
    finally:
        con.close()

    readmodel = _load_readmodel()
    frame = readmodel.load_operational_breadth_frame(tmp_path)
    result = readmodel.get_market_breadth_history(tmp_path, limit=1)

    assert frame.iloc[0]["trade_date"] >= "2020-01-01"
    assert frame.iloc[0]["ad_line"] == 0
    latest = result["rows"][0]
    assert latest["trade_date"] == str(final_date)
    assert latest["new_52w_highs"] == 1
    assert latest["new_52w_lows"] == 1
    assert latest["advancers"] == 1
    assert latest["decliners"] == 1
    assert latest["index_level"] == 1234.5
    assert latest["pe_pctile_5y"] == 82.5
