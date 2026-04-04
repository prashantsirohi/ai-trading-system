from __future__ import annotations

from pathlib import Path

from analytics.registry import RegistryStore
from ui.services.ml_workbench import (
    approve_workbench_model,
    deploy_workbench_model,
    load_model_workbench_detail,
    load_workbench_datasets,
    load_workbench_deployments,
    load_workbench_models,
    rollback_workbench_deployment,
)


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
                "probability": 0.1,
                "prediction": 0,
                "rank": 2,
            },
        ],
        deployment_mode="shadow_ml",
        horizon=horizon,
        model_id=model_id,
    )
    pending = registry.get_unscored_prediction_logs(horizon, deployment_mode="shadow_ml", model_id=model_id)
    registry.replace_shadow_eval(
        [
            {
                "prediction_log_id": row["prediction_log_id"],
                "prediction_date": row["prediction_date"],
                "model_id": model_id,
                "deployment_mode": "shadow_ml",
                "horizon": horizon,
                "symbol_id": row["symbol_id"],
                "exchange": row["exchange"],
                "future_date": "2026-03-06",
                "realized_return": 0.04 if row["symbol_id"] == "AAA" else -0.01,
                "hit": row["symbol_id"] == "AAA",
            }
            for row in pending
        ]
    )


def test_ml_workbench_service_loaders_return_registry_views(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    dataset_id = registry.register_dataset(
        dataset_ref="research:training:test_h5",
        dataset_uri=str(tmp_path / "datasets" / "test_h5.parquet"),
        data_domain="research",
        engine_name="lightgbm",
        feature_schema_version="alpha_v1",
        label_version="forward_return_v1",
        target_column="target_5d",
        from_date="2018-01-01",
        to_date="2025-12-31",
        horizon=5,
        row_count=1200,
        symbol_count=100,
        metadata={"validation_start": "2024-01-01", "validation_fraction": 0.2},
    )
    assert dataset_id

    model_id = registry.register_model(
        model_name="alpha",
        model_version="v1",
        artifact_uri=str(tmp_path / "models" / "alpha_v1.txt"),
        feature_schema_hash="schema-hash",
        train_snapshot_ref="research:training:test_h5",
        approval_status="pending",
        metadata={
            "engine": "lightgbm",
            "horizon": 5,
            "evaluation": {"validation_auc": 0.64, "precision_at_10pct": 0.41},
            "walkforward_summary": {"avg_validation_auc": 0.61},
        },
    )
    registry.record_model_eval(
        model_id,
        {"validation_auc": 0.64, "walkforward_avg_validation_auc": 0.61},
        dataset_ref="research:training:test_h5",
    )
    registry.approve_model(model_id)
    registry.deploy_model(model_id, environment="operational_shadow_5d", approved_by="test")
    registry.record_drift_metrics(
        [
            {
                "prediction_date": "2026-03-01",
                "model_id": model_id,
                "deployment_mode": "shadow_ml",
                "horizon": 5,
                "metric_name": "score_psi",
                "metric_value": 0.08,
                "threshold_value": 0.2,
                "status": "pass",
            }
        ]
    )
    registry.record_promotion_gate_results(
        model_id,
        [
            {
                "gate_name": "validation_auc",
                "status": "pass",
                "metric_value": 0.64,
                "threshold_value": 0.58,
            }
        ],
    )
    _seed_prediction_logs_and_outcomes(registry, model_id=model_id, horizon=5)

    datasets = load_workbench_datasets(tmp_path)
    models = load_workbench_models(tmp_path)
    deployments = load_workbench_deployments(tmp_path)
    detail = load_model_workbench_detail(model_id, tmp_path)

    assert len(datasets) == 1
    assert datasets.iloc[0]["dataset_ref"] == "research:training:test_h5"
    assert float(datasets.iloc[0]["validation_fraction"]) == 0.2

    assert len(models) == 1
    assert models.iloc[0]["model_id"] == model_id
    assert float(models.iloc[0]["validation_auc"]) == 0.64

    assert len(deployments) == 1
    assert deployments.iloc[0]["environment"] == "operational_shadow_5d"
    assert deployments.iloc[0]["model_name"] == "alpha"

    assert detail["model"]["model_id"] == model_id
    assert len(detail["evaluations"]) == 2
    assert len(detail["drift_metrics"]) == 1
    assert len(detail["promotion_gates"]) == 1
    assert detail["monitor_summary"]["matured_rows"] == 2


def test_ml_workbench_approve_deploy_and_rollback_helpers_update_registry(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    model_id = registry.register_model(
        model_name="alpha",
        model_version="v2",
        artifact_uri=str(tmp_path / "models" / "alpha_v2.txt"),
        feature_schema_hash="schema-hash",
        train_snapshot_ref="research:training:test_h20",
        approval_status="pending",
        metadata={"engine": "lightgbm", "horizon": 20},
    )
    prior_model_id = registry.register_model(
        model_name="alpha",
        model_version="v1",
        artifact_uri=str(tmp_path / "models" / "alpha_v1.txt"),
        feature_schema_hash="schema-hash",
        train_snapshot_ref="research:training:test_h20_prev",
        approval_status="approved",
        metadata={"engine": "lightgbm", "horizon": 20},
    )
    registry.deploy_model(prior_model_id, environment="operational_shadow_20d", approved_by="seed")

    approval = approve_workbench_model(model_id, tmp_path)
    deployment = deploy_workbench_model(
        model_id,
        environment="operational_shadow_20d",
        approved_by="ui-test",
        notes="shadow rollout",
        project_root=tmp_path,
    )
    rollback = rollback_workbench_deployment(
        environment="operational_shadow_20d",
        approved_by="ui-test",
        notes="rollback to prior",
        project_root=tmp_path,
    )

    assert approval["before"]["approval_status"] == "pending"
    assert approval["after"]["approval_status"] == "approved"
    assert deployment["active_deployment"]["model_id"] == model_id
    assert deployment["active_deployment"]["environment"] == "operational_shadow_20d"
    assert rollback["active_deployment"]["model_id"] == prior_model_id
