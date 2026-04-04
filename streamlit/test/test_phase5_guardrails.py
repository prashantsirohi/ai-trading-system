from __future__ import annotations

from pathlib import Path

from analytics.alpha.drift import score_drift_rows
from analytics.alpha.policy import evaluate_promotion_candidate
from analytics.registry import RegistryStore


def _seed_prediction_logs_and_outcomes(registry: RegistryStore, *, model_id: str, horizon: int) -> None:
    registry.replace_prediction_log(
        "2026-03-01",
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "model_id": model_id,
                "model_name": "alpha",
                "model_version": "v1",
                "probability": 0.9,
                "prediction": 1,
                "rank": 1,
            },
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "model_id": model_id,
                "model_name": "alpha",
                "model_version": "v1",
                "probability": 0.2,
                "prediction": 0,
                "rank": 2,
            },
        ],
        deployment_mode="shadow_ml",
        horizon=horizon,
        model_id=model_id,
    )
    pending = registry.get_unscored_prediction_logs(horizon, deployment_mode="shadow_ml", model_id=model_id)
    rows = []
    for item in pending:
        realized_return = 0.05 if item["symbol_id"] == "AAA" else -0.01
        rows.append(
            {
                "prediction_log_id": item["prediction_log_id"],
                "prediction_date": item["prediction_date"],
                "model_id": model_id,
                "deployment_mode": "shadow_ml",
                "horizon": horizon,
                "symbol_id": item["symbol_id"],
                "exchange": item["exchange"],
                "future_date": "2026-03-06",
                "realized_return": realized_return,
                "hit": realized_return > 0,
            }
        )
    registry.replace_shadow_eval(rows)


def test_prediction_monitor_summary_uses_top_decile_logic(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    _seed_prediction_logs_and_outcomes(registry, model_id="model-1", horizon=5)

    summary = registry.get_prediction_monitor_summary(
        model_id="model-1",
        horizon=5,
        deployment_mode="shadow_ml",
        lookback_days=60,
        as_of_date="2026-03-10",
    )

    assert summary["prediction_rows"] == 2
    assert summary["matured_rows"] == 2
    assert summary["top_decile_rows"] == 1
    assert round(summary["top_decile_hit_rate"], 4) == 1.0
    assert round(summary["top_decile_avg_return"], 4) == 0.05


def test_score_drift_rows_handles_insufficient_reference_data() -> None:
    rows = score_drift_rows(
        model_id="model-1",
        deployment_mode="shadow_ml",
        horizon=5,
        prediction_date="2026-03-31",
        current_scores=[0.7, 0.8, 0.9],
        reference_scores=[],
    )

    assert len(rows) == 1
    assert rows[0]["status"] == "insufficient_data"


def test_evaluate_promotion_candidate_records_gate_failures_and_passes(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    model_id = registry.register_model(
        model_name="alpha",
        model_version="v1",
        artifact_uri="models/alpha_v1.txt",
        feature_schema_hash="schema-hash",
        train_snapshot_ref="research:training:alpha_v1",
        approval_status="pending",
    )
    registry.record_model_eval(
        model_id,
        {
            "validation_auc": 0.62,
            "walkforward_avg_validation_auc": 0.60,
        },
        dataset_ref="research:training:alpha_v1",
    )
    _seed_prediction_logs_and_outcomes(registry, model_id=model_id, horizon=5)
    registry.record_drift_metrics(
        score_drift_rows(
            model_id=model_id,
            deployment_mode="shadow_ml",
            horizon=5,
            prediction_date="2026-03-01",
            current_scores=[0.9, 0.2],
            reference_scores=[0.85, 0.25, 0.8, 0.3],
        )
    )

    result = evaluate_promotion_candidate(
        registry=registry,
        model_id=model_id,
        horizon=5,
        deployment_mode="shadow_ml",
        lookback_days=60,
        as_of_date="2026-03-10",
    )
    inserted = registry.record_promotion_gate_results(model_id, result["gate_results"])
    stored = registry.get_promotion_gate_results(model_id)

    assert result["overall_status"] == "fail"
    assert inserted == len(result["gate_results"])
    assert any(row["gate_name"] == "shadow_matured_rows" and row["status"] == "fail" for row in stored)
    assert any(row["gate_name"] == "validation_auc" and row["status"] == "pass" for row in stored)
