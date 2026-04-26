"""Drift metrics for prediction-score and feature monitoring."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

import numpy as np


@dataclass(frozen=True)
class DriftThresholds:
    psi_warn: float = 0.10
    psi_fail: float = 0.25


def population_stability_index(
    reference_values: Iterable[float],
    current_values: Iterable[float],
    *,
    bins: int = 10,
    epsilon: float = 1e-6,
) -> float:
    reference = np.asarray(list(reference_values), dtype=float)
    current = np.asarray(list(current_values), dtype=float)
    if reference.size == 0 or current.size == 0:
        return 0.0

    quantiles = np.linspace(0.0, 1.0, bins + 1)
    breakpoints = np.quantile(reference, quantiles)
    breakpoints = np.unique(breakpoints)
    if breakpoints.size <= 2:
        return 0.0

    reference_hist, _ = np.histogram(reference, bins=breakpoints)
    current_hist, _ = np.histogram(current, bins=breakpoints)
    reference_pct = reference_hist / max(reference_hist.sum(), 1)
    current_pct = current_hist / max(current_hist.sum(), 1)
    reference_pct = np.clip(reference_pct, epsilon, None)
    current_pct = np.clip(current_pct, epsilon, None)
    psi = np.sum((current_pct - reference_pct) * np.log(current_pct / reference_pct))
    return float(psi)


def score_drift_rows(
    *,
    model_id: str,
    deployment_mode: str,
    horizon: int,
    prediction_date: str,
    current_scores: Iterable[float],
    reference_scores: Iterable[float],
    thresholds: DriftThresholds | None = None,
) -> List[dict]:
    thresholds = thresholds or DriftThresholds()
    current_list = list(current_scores)
    reference_list = list(reference_scores)
    if not current_list or not reference_list:
        psi = 0.0
        status = "insufficient_data"
    else:
        psi = population_stability_index(reference_list, current_list)
        if psi >= thresholds.psi_fail:
            status = "fail"
        elif psi >= thresholds.psi_warn:
            status = "warn"
        else:
            status = "pass"
    return [
        {
            "prediction_date": prediction_date,
            "model_id": model_id,
            "deployment_mode": deployment_mode,
            "horizon": int(horizon),
            "metric_name": "prediction_score_psi",
            "metric_value": float(psi),
            "threshold_value": float(thresholds.psi_fail),
            "status": status,
            "metadata": {
                "psi_warn": float(thresholds.psi_warn),
                "psi_fail": float(thresholds.psi_fail),
                "current_count": len(current_list),
                "reference_count": len(reference_list),
            },
        }
    ]
