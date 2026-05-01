"""Publisher retry, idempotency, and delivery-log helpers."""

from __future__ import annotations

import hashlib
import time
from typing import Any, Callable, Dict, Optional

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext


class PublisherDeliveryManager:
    """Handles retry-safe, idempotent delivery attempts for publish channels."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay_seconds: float = 1.0,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ):
        self.max_attempts = max_attempts
        self.base_delay_seconds = base_delay_seconds
        self.sleep_fn = sleep_fn or time.sleep

    def deliver(
        self,
        context: StageContext,
        channel: str,
        artifact: StageArtifact,
        sender: Callable[[], Dict[str, Any] | bool | None],
    ) -> Dict[str, Any]:
        if context.registry is None:
            raise RuntimeError("StageContext.registry is required for publisher delivery logging")

        dedupe_key = self.build_dedupe_key(channel, artifact)
        bypass_channels = (context.params or {}).get("bypass_dedupe_channels") or []
        bypass_dedupe = channel in bypass_channels
        successful = None if bypass_dedupe else context.registry.get_successful_delivery(dedupe_key)
        if successful:
            attempt_number = context.registry.next_delivery_attempt(dedupe_key)
            context.registry.record_delivery_log(
                run_id=context.run_id,
                stage_name=context.stage_name,
                channel=channel,
                artifact_uri=artifact.uri,
                artifact_hash=artifact.content_hash,
                dedupe_key=dedupe_key,
                attempt_number=attempt_number,
                status="duplicate",
                external_message_id=successful.get("external_message_id"),
                external_report_id=successful.get("external_report_id"),
                metadata={"reason": "dedupe_skip"},
            )
            return {
                "channel": channel,
                "status": "duplicate",
                "dedupe_key": dedupe_key,
                "attempt_number": attempt_number,
                "external_message_id": successful.get("external_message_id"),
                "external_report_id": successful.get("external_report_id"),
            }

        for retry_index in range(self.max_attempts):
            attempt_number = context.registry.next_delivery_attempt(dedupe_key)
            try:
                payload = sender()
                payload = self._normalize_sender_payload(payload)
                context.registry.record_delivery_log(
                    run_id=context.run_id,
                    stage_name=context.stage_name,
                    channel=channel,
                    artifact_uri=artifact.uri,
                    artifact_hash=artifact.content_hash,
                    dedupe_key=dedupe_key,
                    attempt_number=attempt_number,
                    status="delivered",
                    external_message_id=payload.get("message_id"),
                    external_report_id=payload.get("report_id"),
                    metadata=payload,
                )
                return {
                    "channel": channel,
                    "status": "delivered",
                    "dedupe_key": dedupe_key,
                    "attempt_number": attempt_number,
                    "external_message_id": payload.get("message_id"),
                    "external_report_id": payload.get("report_id"),
                }
            except Exception as exc:
                final_attempt = retry_index == self.max_attempts - 1
                status = "failed" if final_attempt else "retrying"
                context.registry.record_delivery_log(
                    run_id=context.run_id,
                    stage_name=context.stage_name,
                    channel=channel,
                    artifact_uri=artifact.uri,
                    artifact_hash=artifact.content_hash,
                    dedupe_key=dedupe_key,
                    attempt_number=attempt_number,
                    status=status,
                    error_message=str(exc),
                    metadata={"retry_index": retry_index + 1},
                )
                if final_attempt:
                    return {
                        "channel": channel,
                        "status": "failed",
                        "dedupe_key": dedupe_key,
                        "attempt_number": attempt_number,
                        "error_message": str(exc),
                    }
                self.sleep_fn(self.base_delay_seconds * (2**retry_index))

        raise RuntimeError("Unreachable delivery retry state")

    def build_dedupe_key(self, channel: str, artifact: StageArtifact) -> str:
        seed = f"{channel}:{artifact.content_hash or artifact.uri}"
        return hashlib.sha256(seed.encode("utf-8")).hexdigest()

    def _normalize_sender_payload(self, payload: Dict[str, Any] | bool | None) -> Dict[str, Any]:
        if payload is True or payload is None:
            return {}
        if payload is False:
            raise RuntimeError("Sender returned False")
        return dict(payload)
