from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.domains.features.compute_features_batch import register_features, run_batch_feature_computation
from ai_trading_system.domains.features.feature_store import FeatureStore
from ai_trading_system.pipeline.stages.features import FeaturesStage


def _seed_catalog(db_path: Path, periods: int) -> pd.DatetimeIndex:
    dates = pd.bdate_range("2023-01-02", periods=periods)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _catalog (
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
        conn.execute("DELETE FROM _catalog")
        for idx, ts in enumerate(dates, start=1):
            close = 100.0 + idx
            conn.execute(
                "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "ABC",
                    "NSE",
                    ts.to_pydatetime(),
                    close - 1,
                    close + 1,
                    close - 2,
                    close,
                    1_000 + idx,
                ],
            )
    finally:
        conn.close()
    return dates


def _seed_catalog_multi_symbol(db_path: Path, periods: int = 260) -> None:
    dates = pd.bdate_range("2023-01-02", periods=periods)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _catalog (
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
        conn.execute("DELETE FROM _catalog")
        for symbol_offset, symbol in enumerate(["ABC", "XYZ"], start=1):
            for idx, ts in enumerate(dates, start=1):
                close = 100.0 + symbol_offset * 10 + idx * 0.5
                conn.execute(
                    "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        symbol,
                        "NSE",
                        ts.to_pydatetime(),
                        close - 1,
                        close + 1,
                        close - 2,
                        close,
                        10_000 + idx,
                    ],
                )
    finally:
        conn.close()


def test_batch_feature_registry_recovers_from_sequence_drift(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE SEQUENCE IF NOT EXISTS _feat_id_seq START 1")
        conn.execute(
            """
            CREATE TABLE _feature_registry (
                feature_id BIGINT PRIMARY KEY DEFAULT nextval('_feat_id_seq'),
                feature_name TEXT NOT NULL,
                exchange TEXT,
                computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rows_computed BIGINT,
                status TEXT DEFAULT 'completed'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _feature_registry
                (feature_id, feature_name, exchange, rows_computed, status)
            VALUES (1209425, 'existing', 'NSE', 1, 'completed')
            """
        )

        register_features(conn, "rsi", "NSE", 100)

        rows = conn.execute(
            """
            SELECT feature_id, feature_name, exchange, rows_computed, status
            FROM _feature_registry
            ORDER BY feature_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows[-1] == (1209426, "rsi", "NSE", 100, "completed")


def test_feature_store_incremental_recomputes_only_tail(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "ohlcv.duckdb"
    feature_dir = tmp_path / "data" / "feature_store"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    initial_dates = _seed_catalog(db_path, periods=600)
    fs = FeatureStore(
        ohlcv_db_path=str(db_path),
        feature_store_dir=str(feature_dir),
        data_domain="operational",
    )

    full = fs.compute_and_store_features(
        symbols=["ABC"],
        exchanges=["NSE"],
        feature_types=["rsi"],
        full_rebuild=True,
    )
    assert full["rsi"] > 252

    next_ts = initial_dates[-1] + pd.offsets.BDay(1)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "ABC",
                "NSE",
                next_ts.to_pydatetime(),
                800.0,
                802.0,
                799.0,
                801.0,
                5_000,
            ],
        )
    finally:
        conn.close()

    incremental = fs.compute_and_store_features(
        symbols=["ABC"],
        exchanges=["NSE"],
        feature_types=["rsi"],
        incremental=True,
        tail_bars=252,
    )
    assert 0 < incremental["rsi"] <= 252

    stored = pd.read_parquet(feature_dir / "rsi" / "NSE" / "ABC.parquet")
    assert pd.to_datetime(stored["timestamp"]).max() == pd.Timestamp(next_ts)


def test_features_stage_uses_ingest_updated_symbols_for_incremental_runs(
    tmp_path: Path, monkeypatch
) -> None:
    ingest_dir = tmp_path / "data" / "pipeline_runs" / "run-1" / "ingest" / "attempt_1"
    ingest_dir.mkdir(parents=True, exist_ok=True)
    ingest_summary = ingest_dir / "ingest_summary.json"
    ingest_summary.write_text(
        '{"updated_symbols": ["ABC"], "downstream_changed_symbols": ["ABC", "XYZ"]}',
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_run(**kwargs):
        captured.update(kwargs)
        return None

    monkeypatch.setattr("ai_trading_system.domains.ingest.daily_update_runner.run", fake_run)
    monkeypatch.setattr(FeaturesStage, "_record_snapshot", lambda self, context: (1, 2, 3))

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-1",
        run_date=datetime.now().date().isoformat(),
        stage_name="features",
        attempt_number=1,
        params={"data_domain": "operational", "feature_tail_bars": 252},
        artifacts={
            "ingest": {
                "ingest_summary": StageArtifact(
                    artifact_type="ingest_summary",
                    uri=str(ingest_summary),
                )
            }
        },
    )

    result = FeaturesStage().run(context)

    assert captured["features_only"] is True
    assert captured["symbols"] == ["ABC", "XYZ"]
    assert captured["full_rebuild"] is False
    assert captured["feature_tail_bars"] == 252
    assert result.metadata["feature_mode"] == "incremental"
    assert result.metadata["feature_compute_engine"] == "legacy"
    assert result.metadata["feature_parallelism"] == "none"
    assert result.metadata["feature_rows_by_type"] == {}


def test_features_stage_duckdb_batch_engine_records_metadata(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_batch_run(**kwargs):
        captured.update(kwargs)
        return {
            "mode": "duckdb_batch",
            "symbols_targeted": 2,
            "feature_result": {"rsi": 20, "adx": 10},
            "rows_written_total": 30,
        }

    monkeypatch.setattr(
        "ai_trading_system.domains.features.compute_features_batch.run_batch_feature_computation",
        fake_batch_run,
    )
    monkeypatch.setattr(
        "ai_trading_system.domains.features.sector_rs.compute_all_symbols_rs",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "ai_trading_system.domains.features.service.load_data_trust_summary",
        lambda *_args, **_kwargs: {"status": "trusted"},
    )
    monkeypatch.setattr(FeaturesStage, "_record_snapshot", lambda self, context: (1, 30, 2))

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-batch",
        run_date="2026-06-19",
        stage_name="features",
        attempt_number=1,
        params={
            "data_domain": "operational",
            "full_rebuild": True,
            "feature_compute_engine": "duckdb_batch",
            "enable_valuation_features": False,
            "enable_sector_earnings_features": False,
            "enable_phase1_features": False,
        },
    )

    result = FeaturesStage().run(context)

    assert captured["full_rebuild"] is True
    assert captured["symbols"] is None
    assert result.metadata["feature_compute_engine"] == "duckdb_batch"
    assert result.metadata["feature_parallelism"] == "duckdb_internal"
    assert result.metadata["feature_rows_by_type"] == {"rsi": 20, "adx": 10}
    assert result.metadata["target_symbol_count"] == 2


def test_duckdb_batch_features_write_compatible_symbol_parquet(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "ohlcv.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _seed_catalog_multi_symbol(db_path, periods=260)

    legacy_dir = tmp_path / "data" / "legacy_feature_store"
    legacy = FeatureStore(
        ohlcv_db_path=str(db_path),
        feature_store_dir=str(legacy_dir),
        data_domain="operational",
    ).compute_and_store_features(
        symbols=["ABC", "XYZ"],
        exchanges=["NSE"],
        feature_types=["rsi", "sma", "macd", "atr"],
        full_rebuild=True,
    )

    batch = run_batch_feature_computation(
        project_root=tmp_path,
        data_domain="operational",
        symbols=["ABC", "XYZ"],
        exchanges=["NSE"],
        feature_types=["rsi", "sma", "macd", "atr"],
        full_rebuild=True,
    )

    for feature_name in ["rsi", "sma", "macd", "atr"]:
        assert legacy[feature_name] > 0
        assert batch["feature_rows_by_type"][feature_name] > 0

    feature_dir = tmp_path / "data" / "feature_store"
    assert (feature_dir / "rsi" / "NSE" / "ABC.parquet").exists()
    assert (feature_dir / "sma" / "NSE" / "XYZ.parquet").exists()

    rsi = pd.read_parquet(feature_dir / "rsi" / "NSE" / "ABC.parquet")
    sma = pd.read_parquet(feature_dir / "sma" / "NSE" / "ABC.parquet")
    macd = pd.read_parquet(feature_dir / "macd" / "NSE" / "ABC.parquet")
    atr = pd.read_parquet(feature_dir / "atr" / "NSE" / "ABC.parquet")

    assert {"symbol_id", "exchange", "timestamp", "rsi_14"}.issubset(rsi.columns)
    assert {"sma_20", "sma_50", "sma_200"}.issubset(sma.columns)
    assert {"macd_line", "macd_signal_9", "macd_histogram"}.issubset(macd.columns)
    assert {"atr_14"}.issubset(atr.columns)
