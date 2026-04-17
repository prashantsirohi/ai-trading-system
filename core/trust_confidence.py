"""Cross-layer trust/confidence contracts and audit helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class TrustConfidenceEnvelope:
    trust_status: str
    provider_confidence: float | None = None
    feature_confidence: float | None = None
    rank_confidence: float | None = None
    execution_weight: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def attach_audit_fields(
    row: dict,
    *,
    run_id: str | None,
    stage: str | None,
    artifact_path: str | None,
) -> dict:
    """Attach artifact/run lineage for traceability."""
    return {
        **row,
        "audit_run_id": run_id,
        "audit_stage": stage,
        "audit_artifact_path": artifact_path,
    }
