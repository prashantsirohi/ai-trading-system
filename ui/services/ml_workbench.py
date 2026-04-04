"""Shared helpers for the standalone ML workbench UI."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from analytics.registry import RegistryStore


def _project_root(project_root: str | Path | None = None) -> Path:
    return Path(project_root) if project_root else Path(__file__).resolve().parents[2]


def load_workbench_datasets(
    project_root: str | Path | None = None,
    *,
    limit: int = 100,
) -> pd.DataFrame:
    registry = RegistryStore(_project_root(project_root))
    rows = registry.list_datasets(limit=limit, data_domain="research")
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    metadata = frame.pop("metadata").apply(lambda value: value or {})
    frame["validation_start"] = metadata.apply(lambda item: item.get("validation_start"))
    frame["validation_fraction"] = metadata.apply(lambda item: item.get("validation_fraction"))
    return frame


def approve_workbench_model(
    model_id: str,
    project_root: str | Path | None = None,
) -> Dict[str, Any]:
    registry = RegistryStore(_project_root(project_root))
    before = registry.get_model_record(model_id)
    registry.approve_model(model_id)
    after = registry.get_model_record(model_id)
    return {"before": before, "after": after}


def load_workbench_models(
    project_root: str | Path | None = None,
    *,
    limit: int = 100,
) -> pd.DataFrame:
    registry = RegistryStore(_project_root(project_root))
    rows = registry.list_models(limit=limit)
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    metadata = frame.pop("metadata").apply(lambda value: value or {})
    frame["engine"] = metadata.apply(lambda item: item.get("engine"))
    frame["horizon"] = metadata.apply(lambda item: item.get("horizon"))
    frame["validation_auc"] = metadata.apply(lambda item: (item.get("evaluation") or {}).get("validation_auc"))
    frame["precision_at_10pct"] = metadata.apply(lambda item: (item.get("evaluation") or {}).get("precision_at_10pct"))
    frame["walkforward_avg_validation_auc"] = metadata.apply(
        lambda item: (item.get("walkforward_summary") or {}).get("avg_validation_auc")
    )
    return frame


def load_workbench_deployments(
    project_root: str | Path | None = None,
    *,
    limit: int = 50,
) -> pd.DataFrame:
    registry = RegistryStore(_project_root(project_root))
    deployments = pd.DataFrame(registry.list_deployments(limit=limit))
    if deployments.empty:
        return deployments

    models = pd.DataFrame(registry.list_models(limit=500))
    if models.empty:
        return deployments

    model_cols = ["model_id", "model_name", "model_version", "approval_status"]
    merged = deployments.merge(models[model_cols], on="model_id", how="left")
    return merged


def deploy_workbench_model(
    model_id: str,
    *,
    environment: str,
    approved_by: str,
    notes: str | None = None,
    project_root: str | Path | None = None,
) -> Dict[str, Any]:
    registry = RegistryStore(_project_root(project_root))
    deployment_id = registry.deploy_model(
        model_id=model_id,
        environment=environment,
        approved_by=approved_by,
        notes=notes,
    )
    return {
        "deployment_id": deployment_id,
        "active_deployment": registry.get_active_deployment(environment),
    }


def rollback_workbench_deployment(
    *,
    environment: str,
    approved_by: str,
    notes: str | None = None,
    project_root: str | Path | None = None,
) -> Dict[str, Any]:
    registry = RegistryStore(_project_root(project_root))
    deployment_id = registry.rollback_model_deployment(
        environment=environment,
        approved_by=approved_by,
        notes=notes,
    )
    return {
        "deployment_id": deployment_id,
        "active_deployment": registry.get_active_deployment(environment),
    }


def load_model_workbench_detail(
    model_id: str,
    project_root: str | Path | None = None,
    *,
    lookback_days: int = 60,
) -> Dict[str, Any]:
    registry = RegistryStore(_project_root(project_root))
    model_record = registry.get_model_record(model_id)
    metadata = model_record.get("metadata", {}) or {}
    horizon = metadata.get("horizon")

    detail: Dict[str, Any] = {
        "model": model_record,
        "metadata": metadata,
        "evaluations": pd.DataFrame(registry.get_model_evals(model_id)),
        "drift_metrics": pd.DataFrame(registry.get_latest_drift_metrics(model_id=model_id)),
        "promotion_gates": pd.DataFrame(registry.get_promotion_gate_results(model_id)),
        "deployments": pd.DataFrame(
            [row for row in registry.list_deployments(limit=100) if row.get("model_id") == model_id]
        ),
        "monitor_summary": {},
    }
    if horizon is not None:
        detail["monitor_summary"] = registry.get_prediction_monitor_summary(
            model_id=model_id,
            horizon=int(horizon),
            deployment_mode="shadow_ml",
            lookback_days=lookback_days,
        )
    return detail
