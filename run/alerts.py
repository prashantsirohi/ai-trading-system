"""Simple alert manager for degraded pipeline conditions."""

from __future__ import annotations

from typing import Optional

from core.logging import logger


class AlertManager:
    """Persists and logs pipeline alerts for operator follow-up."""

    def __init__(self, registry):
        self.registry = registry

    def emit(
        self,
        run_id: str,
        alert_type: str,
        severity: str,
        message: str,
        stage_name: Optional[str] = None,
    ) -> None:
        log_fn = logger.error if severity == "critical" else logger.warning
        log_fn("pipeline_alert type=%s run_id=%s stage=%s message=%s", alert_type, run_id, stage_name, message)
        self.registry.record_alert(
            run_id=run_id,
            alert_type=alert_type,
            severity=severity,
            stage_name=stage_name,
            message=message,
        )
