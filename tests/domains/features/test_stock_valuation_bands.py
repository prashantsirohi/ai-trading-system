from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.features.stock_valuation_bands import refresh_stock_valuation_bands
from ai_trading_system.domains.features.valuation_schema import ensure_valuation_schema


def _seed(path: Path, symbol: str, pe: list[float | None], ps: list[float], pb: list[float]) -> None:
    conn = duckdb.connect(str(path))
    try:
        ensure_valuation_schema(conn)
        rows = []
        dates = pd.date_range("2026-01-01", periods=len(ps), freq="D")
        for idx, day in enumerate(dates):
            rows.append(
                {
                    "universe_id": "UNIV_TEST",
                    "date": day.date(),
                    "symbol": symbol,
                    "sector_name": "IT",
                    "close": 100.0,
                    "adjusted_equity_shares_cr": 10.0,
                    "market_cap_cr": 1000.0,
                    "ttm_sales_cr": 100.0,
                    "ttm_net_profit_cr": 50.0 if pe[idx] is not None else -10.0,
                    "book_value_cr": 200.0,
                    "pe_ttm": pe[idx],
                    "ps_ttm": ps[idx],
                    "pb": pb[idx],
                    "earnings_yield": 0.05,
                    "valuation_warning": None,
                    "earnings_source": "quarterly_ttm",
                }
            )
        frame = pd.DataFrame(rows)
        conn.register("_rows", frame)
        conn.execute(
            """
            INSERT INTO stock_valuation_daily (
                universe_id, date, symbol, sector_name, close, adjusted_equity_shares_cr,
                market_cap_cr, ttm_sales_cr, ttm_net_profit_cr, book_value_cr, pe_ttm,
                ps_ttm, pb, earnings_yield, valuation_warning, earnings_source
            )
            SELECT
                universe_id, date, symbol, sector_name, close, adjusted_equity_shares_cr,
                market_cap_cr, ttm_sales_cr, ttm_net_profit_cr, book_value_cr, pe_ttm,
                ps_ttm, pb, earnings_yield, valuation_warning, earnings_source
            FROM _rows
            """
        )
    finally:
        conn.close()


def _latest(path: Path, symbol: str) -> pd.Series:
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return conn.execute(
            "SELECT * FROM stock_valuation_bands WHERE symbol = ? ORDER BY date DESC LIMIT 1",
            [symbol],
        ).df().iloc[0]
    finally:
        conn.close()


def test_stock_below_5y_median_bucket(tmp_path: Path) -> None:
    db = tmp_path / "ohlcv.duckdb"
    values = [10, 9, 8, 7, 6, 5, 4, 3, 2, 5.5]
    _seed(db, "AAA", values, values, values)

    refresh_stock_valuation_bands(ohlcv_db_path=db, universe_id="UNIV_TEST", min_history_days_3y=3, min_history_days_5y=3)

    assert _latest(db, "AAA")["valuation_history_bucket"] == "BELOW_OWN_MEDIAN"


def test_two_metrics_below_20th_percentile_bucket(tmp_path: Path) -> None:
    db = tmp_path / "ohlcv.duckdb"
    _seed(db, "AAA", list(range(10, 0, -1)), list(range(10, 0, -1)), [5] * 10)

    refresh_stock_valuation_bands(ohlcv_db_path=db, universe_id="UNIV_TEST", min_history_days_3y=3, min_history_days_5y=3)

    assert _latest(db, "AAA")["valuation_history_bucket"] == "DEEPLY_BELOW_HISTORY"


def test_two_metrics_above_80th_percentile_bucket(tmp_path: Path) -> None:
    db = tmp_path / "ohlcv.duckdb"
    _seed(db, "AAA", list(range(1, 11)), list(range(1, 11)), [5] * 10)

    refresh_stock_valuation_bands(ohlcv_db_path=db, universe_id="UNIV_TEST", min_history_days_3y=3, min_history_days_5y=3)

    assert _latest(db, "AAA")["valuation_history_bucket"] == "EXPENSIVE_VS_HISTORY"


def test_insufficient_history_bucket(tmp_path: Path) -> None:
    db = tmp_path / "ohlcv.duckdb"
    _seed(db, "AAA", [10, 9], [10, 9], [10, 9])

    refresh_stock_valuation_bands(ohlcv_db_path=db, universe_id="UNIV_TEST", min_history_days_3y=10, min_history_days_5y=10)

    row = _latest(db, "AAA")
    assert row["valuation_history_bucket"] == "INSUFFICIENT_HISTORY"
    assert row["valuation_history_score"] == 50


def test_loss_making_stock_uses_ps_pb_weights(tmp_path: Path) -> None:
    db = tmp_path / "ohlcv.duckdb"
    _seed(db, "LOSS", [None] * 10, [10, 9, 8, 7, 6, 5, 4, 3, 2, 5.5], [10, 9, 8, 7, 6, 5, 4, 3, 2, 5.5])

    refresh_stock_valuation_bands(ohlcv_db_path=db, universe_id="UNIV_TEST", min_history_days_3y=3, min_history_days_5y=3)

    row = _latest(db, "LOSS")
    assert pd.isna(row["pe_ttm"])
    assert row["valuation_history_bucket"] == "BELOW_OWN_MEDIAN"
    assert row["valuation_history_score"] == 70
