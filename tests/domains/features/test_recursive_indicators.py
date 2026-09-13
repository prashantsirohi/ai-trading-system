from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from ai_trading_system.domains.features.feature_store import FeatureStore
from ai_trading_system.domains.features.compute_features_batch import batch_ema, batch_macd, batch_supertrend
from ai_trading_system.domains.features.recursive_indicators import compute_recursive_feature, supertrend_series


def _prices(closes, symbol="AAA", exchange="NSE"):
    closes = np.asarray(closes, dtype=float)
    return pd.DataFrame({"symbol_id": symbol, "exchange": exchange,
        "timestamp": pd.date_range("2026-01-01", periods=len(closes)),
        "open": closes, "high": closes + 1, "low": closes - 1,
        "close": closes, "volume": 1000})


def test_ema_and_macd_match_hand_calculated_recurrences():
    prices = _prices([10, 12, 11, 15, 14])
    ema = compute_recursive_feature(prices, "ema", periods=[3])
    assert ema.ema_3.tolist() == [10, 11, 11, 13, 13.5]
    macd = compute_recursive_feature(prices, "macd", fast=2, slow=3, signal=2)
    assert macd.macd_line.tolist() == [0.1111, 0.7037, 0.4012]
    assert macd.macd_signal_2.tolist() == [0.1481, 0.5185, 0.4403]
    assert macd.macd_histogram.tolist() == [-0.037, 0.1852, -0.0391]


def test_supertrend_gap_atr_trailing_bands_and_both_reversals():
    prices = _prices([10, 10, 10, 15, 18, 17, 9, 8, 15])
    frame = compute_recursive_feature(prices, "supertrend", period=3, multiplier=1)
    assert frame.supertrend_3_1.iloc[:2].isna().all()
    assert frame.supertrend_dir_3_1.iloc[:2].isna().all()
    assert frame.supertrend_3_1.iloc[2:].tolist() == [12, 11.6667, 14, 14, 14, 12.3333, 8.6667]
    assert frame.supertrend_dir_3_1.iloc[2:].tolist() == [-1, 1, 1, 1, -1, -1, 1]


def test_flat_supertrend_initializes_after_atr_warmup():
    prices = _prices([10] * 8)
    line, direction = supertrend_series(prices.high, prices.low, prices.close, 3, 1)
    assert line.iloc[:2].isna().all()
    assert line.iloc[2:].tolist() == [12] * 6
    assert direction.iloc[2:].tolist() == [-1] * 6


@pytest.fixture
def store(tmp_path):
    prices = pd.concat([_prices([10, 10, 10, 15, 18, 17, 9, 8, 15]),
        _prices([100, 102, 99, 110, 95, 93, 90, 100, 98], "BBB"),
        _prices([500] * 9, "AAA", "BSE")], ignore_index=True).sample(frac=1, random_state=3)
    db = tmp_path / "ohlcv.duckdb"
    with duckdb.connect(str(db)) as conn:
        conn.register("prices", prices)
        conn.execute("CREATE TABLE _catalog AS SELECT *, open AS adjusted_open, high AS adjusted_high, low AS adjusted_low, close AS adjusted_close FROM prices")
        # The adjusted source must win over raw prices in both entry points.
        conn.execute("UPDATE _catalog SET close = close * 10, high = high * 10, low = low * 10")
    return FeatureStore(ohlcv_db_path=str(db), feature_store_dir=str(tmp_path / "features"), data_domain="operational")


CASES = [("ema", {"windows": [2, 3]}, batch_ema, {"periods": [2, 3]}),
         ("macd", {"fast": 2, "slow": 3, "signal": 2}, batch_macd, {"fast": 2, "slow": 3, "signal": 2}),
         ("supertrend", {"period": 3, "multiplier": 1}, batch_supertrend, {"period": 3, "multiplier": 1})]


@pytest.mark.parametrize("feature,params,batch,batch_params", CASES)
def test_batch_matches_per_symbol_and_isolates_exchange(store, tmp_path, feature, params, batch, batch_params):
    conn = store._get_conn()
    output = tmp_path / "batch"
    try:
        rows = batch(conn, exchange="NSE", feature_store_dir=output, **batch_params)
    finally:
        conn.close()
    count = 0
    for symbol in ("AAA", "BBB"):
        actual = pd.read_parquet(output / feature / "NSE" / f"{symbol}.parquet")
        expected = getattr(store, f"compute_{feature}")(symbol, "NSE", **params)
        expected = expected[expected.timestamp.isin(actual.timestamp)].reset_index(drop=True)
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected, check_dtype=False)
        assert actual.close.max() < 200
        count += len(actual)
    assert rows == count


@pytest.mark.parametrize("feature,params,batch,batch_params", CASES)
def test_tail_and_asof_reads_match_full_history_without_cross_listing_state(store, feature, params, batch, batch_params):
    method = getattr(store, f"compute_{feature}")
    combined = method(exchange=None, **params)
    for symbol, exchange in (("AAA", "NSE"), ("BBB", "NSE"), ("AAA", "BSE")):
        full = method(symbol, exchange, **params)
        grouped = combined[(combined.symbol_id == symbol) & (combined.exchange == exchange)].reset_index(drop=True)
        pd.testing.assert_frame_equal(full, grouped)
        tail = method(symbol, exchange, start_date="2026-01-05", end_date="2026-01-08", **params)
        expected = full[(full.timestamp > "2026-01-05") & (full.timestamp <= "2026-01-08")].reset_index(drop=True)
        pd.testing.assert_frame_equal(tail, expected)
    assert method("AAA' OR '1'='1", **params).empty


@pytest.mark.parametrize("feature,params,batch,batch_params", CASES)
def test_empty_exchange_does_not_write_features(store, tmp_path, feature, params, batch, batch_params):
    conn = store._get_conn()
    try:
        assert batch(conn, exchange="MISSING", feature_store_dir=tmp_path / "empty", **batch_params) == 0
    finally:
        conn.close()
    assert not (tmp_path / "empty").exists()


@pytest.mark.parametrize("feature", ["ema", "macd", "supertrend"])
def test_persisted_incremental_tail_matches_full_rebuild(tmp_path, feature):
    prices = _prices([100 + i / 3 + (i % 7) * 2 for i in range(70)])
    db = tmp_path / "prices.duckdb"
    initial = prices.iloc[:-1]
    with duckdb.connect(str(db)) as conn:
        conn.register("initial", initial)
        conn.execute("CREATE TABLE _catalog AS SELECT * FROM initial")
    feature_dir = tmp_path / "features"
    store = FeatureStore(ohlcv_db_path=str(db), feature_store_dir=str(feature_dir), data_domain="operational")
    store.compute_and_store_features(symbols=["AAA"], exchanges=["NSE"], feature_types=[feature], full_rebuild=True)
    with duckdb.connect(str(db)) as conn:
        latest = prices.iloc[-1:]
        conn.register("latest", latest)
        conn.execute("INSERT INTO _catalog SELECT * FROM latest")
    store.compute_and_store_features(symbols=["AAA"], exchanges=["NSE"], feature_types=[feature],
                               incremental=True, tail_bars=2, warmup_bars=1)
    actual = pd.read_parquet(feature_dir / feature / "NSE" / "AAA.parquet")
    expected = getattr(store, f"compute_{feature}")("AAA", "NSE")
    pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected, check_dtype=False)


def test_feature_engine_uses_single_atr_warmup():
    from ai_trading_system.domains.features.indicators import FeatureEngine
    prices = _prices([10, 10, 10, 15, 18, 17, 9, 8, 15])
    line, direction = FeatureEngine()._calculate_supertrend(prices.high, prices.low, prices.close, 3, 1)
    assert line.iloc[2] == 12
    assert direction.iloc[3] == 1
    assert direction.iloc[6] == -1
