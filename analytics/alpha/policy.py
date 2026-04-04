"""Promotion guardrails for moving models beyond shadow mode."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from analytics.alpha.monitoring import summarize_model_shadow_performance


@dataclass(frozen=True)
class PromotionThresholds:
    min_validation_auc: float = 0.55
    min_walkforward_auc: float = 0.55
    min_top_decile_hit_rate: float = 0.50
    min_top_decile_avg_return: float = 0.0
    min_matured_rows: int = 20
    max_prediction_score_psi: float = 0.25


def _latest_metric(evals: List[Dict[str, Any]], metric_name: str) -> float | None:
    value = None
    for row in evals:
        if row["metric_name"] == metric_name:
            value = float(row["metric_value"])
    return value


def evaluate_promotion_candidate(
    *,
    registry,
    model_id: str,
    horizon: int,
    deployment_mode: str = "shadow_ml",
    lookback_days: int = 60,
    thresholds: PromotionThresholds | None = None,
    as_of_date: str | None = None,
) -> Dict[str, Any]:
    thresholds = thresholds or PromotionThresholds()
    evals = registry.get_model_evals(model_id)
    shadow_summary = summarize_model_shadow_performance(
        registry=registry,
        model_id=model_id,
        horizon=horizon,
        deployment_mode=deployment_mode,
        lookback_days=lookback_days,
        as_of_date=as_of_date,
    )
    drift_metrics = registry.get_latest_drift_metrics(
        model_id=model_id,
        deployment_mode=deployment_mode,
        horizon=horizon,
        prediction_date=as_of_date,
    )
    psi_value = None
    for row in drift_metrics:
        if row["metric_name"] == "prediction_score_psi":
            if row.get("status") != "insufficient_data":
                psi_value = float(row["metric_value"])
            break

    gates = [
        {
            "gate_name": "validation_auc",
            "metric_value": _latest_metric(evals, "validation_auc"),
            "threshold_value": thresholds.min_validation_auc,
        },
        {
            "gate_name": "walkforward_avg_validation_auc",
            "metric_value": _latest_metric(evals, "walkforward_avg_validation_auc"),
            "threshold_value": thresholds.min_walkforward_auc,
        },
        {
            "gate_name": "shadow_top_decile_hit_rate",
            "metric_value": shadow_summary.get("top_decile_hit_rate"),
            "threshold_value": thresholds.min_top_decile_hit_rate,
        },
        {
            "gate_name": "shadow_top_decile_avg_return",
            "metric_value": shadow_summary.get("top_decile_avg_return"),
            "threshold_value": thresholds.min_top_decile_avg_return,
        },
        {
            "gate_name": "shadow_matured_rows",
            "metric_value": shadow_summary.get("matured_rows"),
            "threshold_value": thresholds.min_matured_rows,
        },
        {
            "gate_name": "prediction_score_psi",
            "metric_value": psi_value,
            "threshold_value": thresholds.max_prediction_score_psi,
            "comparison": "max",
        },
    ]

    results: List[dict] = []
    for gate in gates:
        metric_value = gate.get("metric_value")
        threshold_value = gate.get("threshold_value")
        comparison = gate.get("comparison", "min")
        if metric_value is None:
            status = "insufficient_data"
        elif comparison == "max":
            status = "pass" if float(metric_value) <= float(threshold_value) else "fail"
        else:
            status = "pass" if float(metric_value) >= float(threshold_value) else "fail"
        results.append(
            {
                "gate_name": gate["gate_name"],
                "status": status,
                "metric_value": float(metric_value) if metric_value is not None else None,
                "threshold_value": float(threshold_value) if threshold_value is not None else None,
                "metadata": {
                    "horizon": int(horizon),
                    "deployment_mode": deployment_mode,
                    "lookback_days": int(lookback_days),
                },
            }
        )

    overall_status = "pass" if results and all(row["status"] == "pass" for row in results) else "fail"
    if any(row["status"] == "insufficient_data" for row in results):
        overall_status = "insufficient_data"
    return {
        "model_id": model_id,
        "horizon": int(horizon),
        "deployment_mode": deployment_mode,
        "shadow_summary": shadow_summary,
        "drift_metrics": drift_metrics,
        "gate_results": results,
        "overall_status": overall_status,
    }
