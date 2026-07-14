from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.analytics.feature_reader import FeatureReader
from ai_trading_system.domains.ranking.input_loader import RankerInputLoader


def _build_reader(tmp_path: Path) -> FeatureReader:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog AS
            SELECT * FROM (VALUES
                ('SAFE', 'NSE', TIMESTAMP '2026-01-01', 99.0, 101.0, 98.0, 100.0, 1000),
                ('OTHER', 'NSE', TIMESTAMP '2026-01-02', 199.0, 201.0, 198.0, 200.0, 2000)
            ) AS t(symbol_id, exchange, timestamp, open, high, low, close, volume)
            """
        )
    finally:
        conn.close()

    partition = tmp_path / "features" / "momentum" / "NSE"
    partition.mkdir(parents=True)
    pd.DataFrame(
        [
            {"symbol_id": "SAFE", "timestamp": pd.Timestamp("2026-01-01"), "value": 1.0},
            {"symbol_id": "SAFE", "timestamp": pd.Timestamp("2026-01-02"), "value": 2.0},
            {"symbol_id": "OTHER", "timestamp": pd.Timestamp("2026-01-02"), "value": 3.0},
        ]
    ).to_parquet(partition / "part.parquet", index=False)
    return FeatureReader(str(tmp_path / "features"), str(db_path))


def test_feature_reader_binds_symbols_dates_and_limits(tmp_path: Path) -> None:
    reader = _build_reader(tmp_path)

    rows = reader.read_feature(
        "momentum",
        symbols=["SAFE' OR 1=1 --"],
        date="2026-01-02",
        limit=5,
    )
    latest = reader.read_latest(
        "momentum",
        symbols=["SAFE"],
        cutoff_date="2026-01-01",
    )

    assert rows.empty
    assert latest[["symbol_id", "value"]].to_dict(orient="records") == [
        {"symbol_id": "SAFE", "value": 1.0}
    ]


def test_feature_reader_binds_catalog_and_fallback_values(tmp_path: Path) -> None:
    reader = _build_reader(tmp_path)

    injected = reader.read_ohlcv(
        exchange="NSE' OR 1=1 --",
        symbols=["SAFE' OR 1=1 --"],
        date="2026-01-02",
    )
    fallback = reader.read_per_symbol("momentum", "SAFE' OR 1=1 --")

    assert injected.empty
    assert fallback.empty


def test_feature_reader_rejects_path_escape_and_unbounded_limit(tmp_path: Path) -> None:
    reader = _build_reader(tmp_path)

    with pytest.raises(ValueError, match="escapes"):
        reader.read_feature("../../outside")
    with pytest.raises(ValueError, match="limit"):
        reader.read_feature("momentum", limit=1_000_001)


def test_rank_input_loader_binds_exchange_and_bounds_windows(tmp_path: Path) -> None:
    reader = _build_reader(tmp_path)
    loader = RankerInputLoader(
        ohlcv_db_path=reader.ohlcv_db_path,
        feature_store_dir=reader.feature_store_dir,
        master_db_path=str(tmp_path / "masterdata.db"),
    )

    injected = loader.load_latest_market_data(
        as_of="2026-01-02",
        exchanges=["NSE' OR 1=1 --"],
    )

    assert injected.empty
    with pytest.raises(ValueError, match="history_bars"):
        loader.load_latest_stage2(
            date="2026-01-02",
            exchanges=["NSE"],
            history_bars=5_001,
        )
    with pytest.raises(ValueError, match="window"):
        loader.load_latest_highs(date="2026-01-02", window=5_001)
