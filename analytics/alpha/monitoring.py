"""Monitoring helpers for shadow predictions and matured outcomes."""

from __future__ import annotations

from typing import Any, Dict


def summarize_model_shadow_performance(
    *,
    registry,
    model_id: str,
    horizon: int,
    deployment_mode: str = "shadow_ml",
    lookback_days: int = 60,
    as_of_date: str | None = None,
) -> Dict[str, Any]:
    """Return a compact model-specific shadow performance summary."""
    summary = registry.get_prediction_monitor_summary(
        model_id=model_id,
        horizon=horizon,
        deployment_mode=deployment_mode,
        lookback_days=lookback_days,
        as_of_date=as_of_date,
    )
    return {
        "model_id": model_id,
        "deployment_mode": deployment_mode,
        "horizon": horizon,
        "lookback_days": lookback_days,
        **summary,
    }
