"""Simple alert manager for degraded pipeline conditions."""

from __future__ import annotations

import os
import json
from typing import Optional

from ai_trading_system.platform.logging.logger import logger
import requests


# Severity rank used to gate telegram fan-out. Higher = more severe.
_SEVERITY_RANK: dict[str, int] = {"info": 0, "warning": 1, "critical": 2}

# Sentinel for "never fan out to telegram". Default behavior — keeps test runs
# and noisy DQ days from spamming the chat. Set ALERT_TELEGRAM_MIN_SEVERITY
# in env to "critical" / "warning" / "info" to opt back in.
_DISABLED = "disabled"


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
        self._fan_out_alert(
            run_id=run_id,
            alert_type=alert_type,
            severity=severity,
            message=message,
            stage_name=stage_name,
        )

    def emit_incident(
        self,
        *,
        run_id: str,
        alert_type: str,
        severity: str,
        message: str,
        dedupe_key: str,
        payload: dict,
        stage_name: str | None = None,
    ) -> str:
        outcome = self.registry.open_alert_incident(
            run_id=run_id,
            alert_type=alert_type,
            severity=severity,
            stage_name=stage_name,
            dedupe_key=dedupe_key,
            payload=payload,
        )
        if outcome != "DEDUPLICATED":
            self.emit(
                run_id=run_id,
                alert_type=alert_type,
                severity=severity,
                message=f"{message} payload={json.dumps(payload, sort_keys=True, default=str)}",
                stage_name=stage_name,
            )
        return outcome

    def resolve_incidents(
        self,
        *,
        run_id: str,
        alert_type: str,
        position_cycle_id: str,
        resolution: dict,
    ) -> int:
        return self.registry.resolve_alert_incidents(
            alert_type=alert_type,
            position_cycle_id=position_cycle_id,
            run_id=run_id,
            resolution=resolution,
        )

    def _fan_out_alert(
        self,
        *,
        run_id: str,
        alert_type: str,
        severity: str,
        message: str,
        stage_name: Optional[str] = None,
    ) -> None:
        text = (
            f"[{severity.upper()}] pipeline alert\n"
            f"run_id={run_id}\n"
            f"type={alert_type}\n"
            f"stage={stage_name or 'unknown'}\n"
            f"message={message}"
        )
        self.send_telegram_alert(text, severity=severity)

    @staticmethod
    def send_telegram_alert(message: str, severity: str = "warning") -> None:
        """Forward an alert to Telegram, gated by ALERT_TELEGRAM_MIN_SEVERITY.

        Default (env unset or set to ``disabled``): no telegram is sent — the
        alert is still logged and persisted to the registry. The publish
        stage's success digest is a separate code path and is not affected.

        To opt in: set ``ALERT_TELEGRAM_MIN_SEVERITY`` to one of
        ``critical``, ``warning``, or ``info``. Anything below the threshold
        is dropped silently.
        """
        min_severity = os.getenv("ALERT_TELEGRAM_MIN_SEVERITY", _DISABLED).lower().strip()
        if min_severity == _DISABLED or min_severity not in _SEVERITY_RANK:
            return
        if _SEVERITY_RANK.get(severity.lower(), 1) < _SEVERITY_RANK[min_severity]:
            return

        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not telegram_token or not telegram_chat_id:
            return
        url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        try:
            requests.post(
                url,
                json={"chat_id": telegram_chat_id, "text": message},
                timeout=10,
            )
        except requests.RequestException as exc:
            logger.warning("Telegram alert fan-out failed: %s", exc)
