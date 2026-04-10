from __future__ import annotations

from pathlib import Path

import pandas as pd

from analytics.lightgbm_engine import LightGBMAlphaEngine


def _sample_training_frame(rows: int = 80) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "symbol_id": ["AAA"] * rows,
            "exchange": ["NSE"] * rows,
            "timestamp": pd.date_range("2024-01-01", periods=rows, freq="D"),
            "close": [100 + i for i in range(rows)],
            "open": [99 + i for i in range(rows)],
            "high": [101 + i for i in range(rows)],
            "low": [98 + i for i in range(rows)],
            "volume": [10_000 + (i * 10) for i in range(rows)],
            "rsi": [(i % 70) + 10 for i in range(rows)],
            "adx_value": [15 + (i % 20) for i in range(rows)],
            "atr_value": [1.0 + (i % 5) * 0.1 for i in range(rows)],
            "bb_upper": [105 + i for i in range(rows)],
            "bb_middle": [100 + i for i in range(rows)],
            "bb_lower": [95 + i for i in range(rows)],
            "st_upper": [104 + i for i in range(rows)],
            "st_lower": [96 + i for i in range(rows)],
            "st_signal": [1 if i % 2 == 0 else -1 for i in range(rows)],
            "return_5d": [0.05 if i % 3 == 0 else -0.01 for i in range(rows)],
            "target_5d": [1 if i % 3 == 0 else 0 for i in range(rows)],
        }
    )
    return frame


def test_lightgbm_engine_trains_and_saves(tmp_path: Path) -> None:
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    engine = LightGBMAlphaEngine(
        ohlcv_db_path=str(tmp_path / "ohlcv.duckdb"),
        feature_store_dir=str(tmp_path / "feature_store"),
        model_dir=str(model_dir),
        data_domain="research",
    )

    train_df = _sample_training_frame()
    model, metadata = engine.train(train_df, horizon=5)

    assert "importance" in metadata
    path = Path(engine.save_model(model, horizon=5))
    assert path.exists()

    loaded = engine.load_model(horizon=5)
    feature_cols = engine._feature_cols(train_df)
    preds = loaded.predict(train_df[feature_cols].fillna(0))
    assert len(preds) == len(train_df)
