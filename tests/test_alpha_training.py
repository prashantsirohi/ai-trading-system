from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from analytics.alpha.training import train_and_register_model, walk_forward_compare
from analytics.registry import RegistryStore


class _DummyModel:
    pass


class _DummyEngine:
    engine_name = "lightgbm"

    def __init__(self, model_dir: Path):
        self.model_dir = model_dir

    def train(self, train_df, horizon=5, **kwargs):
        return _DummyModel(), {"best_iteration": 7}

    def evaluate(self, dataset_df, *, model, horizon=5, **kwargs):
        return {
            "validation_auc": 0.66,
            "precision_at_10pct": 0.42,
            "avg_return_top_10pct": 0.031,
            "baseline_positive_rate": 0.3,
        }

    def evaluate_frame(self, dataset_df, *, model, horizon: int):
        return {"validation_auc": 0.61}

    def score_frame(self, dataset_df, *, model, horizon: int):
        scored = dataset_df.copy()
        scored["probability"] = scored["signal"]
        scored["prediction"] = (scored["probability"] >= 0.5).astype(int)
        scored["horizon"] = horizon
        return scored

    def save_model(self, model, horizon: int = 5):
        path = self.model_dir / f"dummy_h{horizon}.txt"
        path.write_text("dummy-model", encoding="utf-8")
        return str(path)

    def _feature_cols(self, df: pd.DataFrame):
        return ["signal"]


def _sample_training_df() -> pd.DataFrame:
    rows = []
    for year in range(2015, 2023):
        for idx in range(10):
            rows.append(
                {
                    "symbol_id": f"SYM{idx}",
                    "exchange": "NSE",
                    "timestamp": pd.Timestamp(f"{year}-01-{(idx % 9) + 1:02d}"),
                    "signal": 0.9 if idx < 3 else 0.1,
                    "target_5d": 1 if idx < 3 else 0,
                    "return_5d": 0.05 if idx < 3 else -0.01,
                    "rel_strength_pct": 90 - idx,
                    "vol_intensity_pct": 50 + idx,
                    "trend_score_pct": 55 + idx,
                    "prox_high_pct": 60 + idx,
                    "delivery_pct_pct": 45 + idx,
                    "sector_rs_pct": 50 + idx,
                    "stock_vs_sector_pct": 48 + idx,
                }
            )
    return pd.DataFrame(rows)


def test_walk_forward_compare_returns_summary_with_dummy_engine():
    result = walk_forward_compare(
        _sample_training_df(),
        engine=_DummyEngine(Path(".")),
        horizon=5,
        min_train_years=5,
    )

    assert result["summary"]["fold_count"] >= 1
    assert "avg_validation_auc" in result["summary"]


def test_train_and_register_model_writes_metadata_and_registry(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    engine = _DummyEngine(model_dir)
    dataset_meta = {
        "dataset_ref": "research:training:dummy_set",
        "dataset_uri": str(tmp_path / "datasets" / "dummy.parquet"),
        "validation_fraction": 0.2,
        "validation_start": "2021-01-01",
        "feature_schema_hash": "schema-hash",
    }

    trained = train_and_register_model(
        engine=engine,
        registry=registry,
        training_df=_sample_training_df(),
        dataset_meta=dataset_meta,
        horizon=5,
        model_name="dummy_model",
        model_version="v1",
        progress_interval=1,
        min_train_years=5,
    )

    metadata = json.loads(Path(trained["metadata_uri"]).read_text(encoding="utf-8"))
    model_record = registry.get_model_record(trained["model_id"])
    evals = registry.get_model_evals(trained["model_id"])

    assert metadata["model_id"] == trained["model_id"]
    assert metadata["walkforward"]["summary"]["fold_count"] >= 1
    assert model_record["train_snapshot_ref"] == "research:training:dummy_set"
    assert any(row["metric_name"] == "walkforward_avg_validation_auc" for row in evals)


def test_prediction_log_and_shadow_eval_roundtrip(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)

    inserted = registry.replace_prediction_log(
        "2026-03-31",
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "model_id": "model-1",
                "model_name": "alpha",
                "model_version": "v1",
                "score": 0.82,
                "probability": 0.82,
                "prediction": 1,
                "rank": 1,
            }
        ],
        deployment_mode="shadow_ml",
        horizon=5,
        model_id="model-1",
    )
    pending = registry.get_unscored_prediction_logs(5, deployment_mode="shadow_ml", model_id="model-1")
    evaluated = registry.replace_shadow_eval(
        [
            {
                "prediction_log_id": pending[0]["prediction_log_id"],
                "prediction_date": pending[0]["prediction_date"],
                "model_id": "model-1",
                "deployment_mode": "shadow_ml",
                "horizon": 5,
                "symbol_id": "AAA",
                "exchange": "NSE",
                "future_date": "2026-04-07",
                "realized_return": 0.04,
                "hit": True,
            }
        ]
    )
    still_pending = registry.get_unscored_prediction_logs(5, deployment_mode="shadow_ml", model_id="model-1")

    assert inserted == 1
    assert len(pending) == 1
    assert evaluated == 1
    assert still_pending == []
