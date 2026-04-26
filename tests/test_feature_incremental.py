from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
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
        '{"updated_symbols": ["ABC", "XYZ"]}',
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
