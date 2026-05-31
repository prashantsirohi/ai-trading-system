from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.domains.features.phase1 import (
    compute_phase1_symbol_features,
    refresh_phase1_features,
)
from ai_trading_system.domains.ranking.input_loader import RankerInputLoader


def _seed_catalog(conn: duckdb.DuckDBPyConnection, *, include_instrument_type: bool = True) -> None:
    instrument_sql = ", instrument_type VARCHAR" if include_instrument_type else ""
    conn.execute(
        f"""
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            adjusted_open DOUBLE,
            adjusted_high DOUBLE,
            adjusted_low DOUBLE,
            adjusted_close DOUBLE
            {instrument_sql}
        )
        """
    )


def _insert_price_rows(
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    dates: pd.DatetimeIndex,
    *,
    start: float = 100.0,
    step: float = 1.0,
    exchange: str = "NSE",
    instrument_type: str | None = "equity",
    adjusted_multiplier: float = 1.0,
) -> None:
    rows = []
    for i, date in enumerate(dates):
        close = start + i * step
        adjusted_close = close * adjusted_multiplier
        base = [
            symbol,
            exchange,
            date.to_pydatetime(),
            close - 1,
            close + 1,
            close - 2,
            close,
            1000 + i,
            (close - 1) * adjusted_multiplier,
            (close + 1) * adjusted_multiplier,
            (close - 2) * adjusted_multiplier,
            adjusted_close,
        ]
        if instrument_type is not None:
            base.append(instrument_type)
        rows.append(tuple(base))
    placeholders = ", ".join(["?"] * len(rows[0]))
    conn.executemany(f"INSERT INTO _catalog VALUES ({placeholders})", rows)


def test_phase1_schema_adjusted_prices_min_guards_and_aligned_beta(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    _seed_catalog(conn)
    dates = pd.bdate_range("2026-01-01", periods=130)
    _insert_price_rows(conn, "NIFTY_500", dates, start=1000, step=2, instrument_type="index")
    _insert_price_rows(conn, "AAA", dates, start=100, step=1, adjusted_multiplier=2.0)
    _insert_price_rows(conn, "YOUNG", dates[-35:], start=50, step=1)

    try:
        refresh_phase1_features(ohlcv_db_path=db_path, as_of=str(dates[-1].date()))
        conn.close()
        conn = duckdb.connect(str(db_path))
        features = conn.execute("SELECT * FROM feat_phase1_symbol_features").fetchdf()
        conn.close()
        loader = RankerInputLoader(
            ohlcv_db_path=str(db_path),
            feature_store_dir=str(tmp_path / "features"),
            master_db_path=str(tmp_path / "master.db"),
        )
        breadth = loader.load_latest_market_breadth(str(dates[-1].date()))
        aaa = features.loc[features["symbol_id"] == "AAA"].iloc[0]
        young = features.loc[features["symbol_id"] == "YOUNG"].iloc[0]

        assert {
            "realized_vol_20",
            "realized_vol_60",
            "beta_to_nifty_60",
            "max_drawdown_63",
            "max_drawdown_126",
            "atr_pct",
            "avg_value_traded_20",
            "liquidity_score",
        }.issubset(features.columns)
        assert pd.notna(aaa["beta_to_nifty_60"])
        assert aaa["beta_to_nifty_60_obs"] >= 40
        assert pd.isna(young["realized_vol_60"])
        assert pd.isna(young["beta_to_nifty_60"])
        assert pd.isna(young["max_drawdown_63"])
        assert {"breadth_score", "breadth_velocity_bucket", "advance_decline_ratio"}.issubset(breadth.columns)
        # Adjusted close is doubled, so avg traded value should be based on adjusted prices.
        assert aaa["avg_value_traded_20"] > 2 * 100 * 1000
    finally:
        try:
            conn.close()
        except Exception:
            pass


def test_phase1_excludes_index_like_symbols_when_is_benchmark_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    _seed_catalog(conn, include_instrument_type=False)
    dates = pd.bdate_range("2026-01-01", periods=70)
    _insert_price_rows(conn, "NIFTY50", dates, start=1000, step=1, instrument_type=None)
    _insert_price_rows(conn, "AAA", dates, start=100, step=1, instrument_type=None)
    try:
        refresh_phase1_features(ohlcv_db_path=db_path, as_of=str(dates[-1].date()))
        symbols = set(conn.execute("SELECT DISTINCT symbol_id FROM feat_phase1_symbol_features").fetchdf()["symbol_id"])
        assert "AAA" in symbols
        assert "NIFTY50" not in symbols
    finally:
        conn.close()


def test_phase1_delivery_trend_supports_timestamp_and_date_columns(tmp_path: Path) -> None:
    for column_name in ("timestamp", "date"):
        db_path = tmp_path / f"{column_name}.duckdb"
        conn = duckdb.connect(str(db_path))
        _seed_catalog(conn)
        dates = pd.bdate_range("2026-01-01", periods=70)
        _insert_price_rows(conn, "NIFTY_500", dates, start=1000, step=1, instrument_type="index")
        _insert_price_rows(conn, "AAA", dates, start=100, step=1)
        conn.execute(
            f"""
            CREATE TABLE _delivery (
                symbol_id VARCHAR,
                exchange VARCHAR,
                {column_name} TIMESTAMP,
                delivery_pct DOUBLE
            )
            """
        )
        conn.executemany(
            "INSERT INTO _delivery VALUES (?, ?, ?, ?)",
            [("AAA", "NSE", d.to_pydatetime(), 30.0 + i) for i, d in enumerate(dates[-25:])],
        )
        try:
            features = compute_phase1_symbol_features(conn, as_of=str(dates[-1].date())).set_index("symbol_id")
            assert features.loc["AAA", "delivery_pct_latest"] == pytest.approx(54.0)
            assert pd.notna(features.loc["AAA", "delivery_pct_20d_avg"])
            assert pd.notna(features.loc["AAA", "delivery_trend_score"])
        finally:
            conn.close()
