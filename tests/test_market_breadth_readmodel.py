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
