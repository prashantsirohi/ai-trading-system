from __future__ import annotations

from pathlib import Path

import pandas as pd

from analytics.training_dataset import TrainingDatasetBuilder


class StubEngine:
    engine_name = "lightgbm"

    def prepare_training_data(self, **kwargs) -> pd.DataFrame:
        rows = 12
        return pd.DataFrame(
            {
                "symbol_id": ["AAA", "BBB"] * 6,
                "exchange": ["NSE"] * rows,
                "timestamp": pd.date_range("2024-01-01", periods=rows, freq="D"),
                "close": [100 + i for i in range(rows)],
                "open": [99 + i for i in range(rows)],
                "high": [101 + i for i in range(rows)],
                "low": [98 + i for i in range(rows)],
                "volume": [10_000 + i for i in range(rows)],
                "rsi": [40 + i for i in range(rows)],
                "adx_value": [20 + (i % 5) for i in range(rows)],
                "target_5d": [1 if i % 3 == 0 else 0 for i in range(rows)],
            }
        )

    def _feature_cols(self, df: pd.DataFrame):
        return ["rsi", "adx_value"]


def test_prepare_training_dataset_writes_parquet_and_metadata(tmp_path: Path) -> None:
    builder = TrainingDatasetBuilder(project_root=tmp_path, data_domain="research")
    prepared = builder.prepare(
        engine=StubEngine(),
        dataset_name="sample_lightgbm",
        from_date="2024-01-01",
        to_date="2024-01-12",
        horizon=5,
        validation_fraction=0.25,
    )

    assert prepared.dataset_path.exists()
    assert prepared.metadata_path.exists()

    df, metadata = TrainingDatasetBuilder.load_prepared_dataset(prepared.dataset_path)
    assert len(df) == 12
    assert metadata["dataset_ref"] == "research:training:sample_lightgbm"
    assert metadata["feature_columns"] == ["rsi", "adx_value"]
    assert metadata["target_column"] == "target_5d"
