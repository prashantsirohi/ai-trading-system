from __future__ import annotations

from pathlib import Path

import duckdb

from ai_trading_system.domains.features.feature_store import FeatureStore


POISON_SYMBOL = "RELIANCE' OR '1'='1"


def _seed_catalog(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog VALUES
                ('RELIANCE', 'NSE', TIMESTAMP '2024-01-01 09:15:00', 100, 101, 99, 100, 1000),
                ('INFY', 'NSE', TIMESTAMP '2024-01-02 09:15:00', 200, 201, 199, 200, 2000)
            """
        )
    finally:
        conn.close()


def _seed_feature_table(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE feat_rsi (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                date DATE,
                rsi_14 DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO feat_rsi VALUES
                ('RELIANCE', 'NSE', TIMESTAMP '2024-01-01 09:15:00', DATE '2024-01-01', 55.0),
                ('INFY', 'NSE', TIMESTAMP '2024-01-02 09:15:00', DATE '2024-01-02', 60.0)
            """
        )
    finally:
        conn.close()


def test_compute_rsi_treats_poison_symbol_as_literal_value(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "ohlcv.duckdb"
    feature_dir = tmp_path / "data" / "feature_store"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _seed_catalog(db_path)

    store = FeatureStore(
        ohlcv_db_path=str(db_path),
        feature_store_dir=str(feature_dir),
        data_domain="operational",
    )

    frame = store.compute_rsi(symbol_id=POISON_SYMBOL, exchange="NSE")

    assert frame.empty


def test_load_features_duckdb_treats_poison_symbol_as_literal_value(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "ohlcv.duckdb"
    feature_dir = tmp_path / "data" / "feature_store"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _seed_catalog(db_path)
    _seed_feature_table(db_path)

    store = FeatureStore(
        ohlcv_db_path=str(db_path),
        feature_store_dir=str(feature_dir),
        data_domain="operational",
    )

    frame = store.load_features_duckdb("rsi", symbol_id=POISON_SYMBOL, exchange="NSE")

    assert frame.empty


def test_create_snapshot_returns_id_and_registers_current_features(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "ohlcv.duckdb"
    feature_dir = tmp_path / "data" / "feature_store"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _seed_catalog(db_path)

    store = FeatureStore(
        ohlcv_db_path=str(db_path),
        feature_store_dir=str(feature_dir),
        data_domain="operational",
    )
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO _feature_registry
                (feature_id, feature_name, exchange, rows_computed, status)
            VALUES (1, 'rsi', 'NSE', 2, 'completed')
            """
        )
    finally:
        conn.close()

    snapshot_id = store.create_snapshot("quality baseline")

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        snapshot = conn.execute(
            "SELECT snapshot_id, symbols_processed, rows_written, note FROM _snapshots"
        ).fetchone()
        feature_snapshot_id = conn.execute(
            "SELECT snapshot_id FROM _feature_registry WHERE feature_id = 1"
        ).fetchone()[0]
    finally:
        conn.close()

    assert snapshot_id == 1
    assert snapshot == (1, 2, 1, "quality baseline")
    assert feature_snapshot_id == snapshot_id
