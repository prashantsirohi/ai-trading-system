"""Shared stage and artifact contracts used across runtime layers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

@dataclass(frozen=True)
class TrustConfidenceEnvelope:
    """Portable trust/confidence envelope for rank/execute/publish reporting."""

    trust_status: str
    active_quarantined_dates: list[str] = field(default_factory=list)
    active_quarantined_symbols: int = 0
    fallback_ratio_latest: float = 0.0
    primary_ratio_latest: float | None = None
    unknown_ratio_latest: float = 0.0
    latest_provider_stats: dict[str, Any] = field(default_factory=dict)
    latest_trade_date: str | None = None
    latest_validated_date: str | None = None
    notes: list[str] = field(default_factory=list)
    provider_confidence: float | None = None
    feature_confidence: float | None = None
    rank_confidence: float | None = None
    execution_weight: float | None = None

    @classmethod
    def from_trust_summary(
        cls,
        summary: dict[str, Any] | None,
        *,
        notes: list[str] | None = None,
        feature_confidence: float | None = None,
        rank_confidence: float | None = None,
        execution_weight: float | None = None,
    ) -> "TrustConfidenceEnvelope":
        payload = dict(summary or {})
        embedded = dict(payload.get("trust_confidence") or {})
        return cls(
            trust_status=str(payload.get("status") or embedded.get("trust_status") or "unknown"),
            active_quarantined_dates=list(payload.get("active_quarantined_dates") or embedded.get("active_quarantined_dates") or []),
            active_quarantined_symbols=int(payload.get("active_quarantined_symbols") or embedded.get("active_quarantined_symbols") or 0),
            fallback_ratio_latest=float(payload.get("fallback_ratio_latest") or embedded.get("fallback_ratio_latest") or 0.0),
            primary_ratio_latest=_maybe_float(payload.get("primary_ratio_latest", embedded.get("primary_ratio_latest"))),
            unknown_ratio_latest=float(payload.get("unknown_ratio_latest") or embedded.get("unknown_ratio_latest") or 0.0),
            latest_provider_stats=dict(payload.get("latest_provider_stats") or embedded.get("latest_provider_stats") or {}),
            latest_trade_date=_maybe_str(payload.get("latest_trade_date", embedded.get("latest_trade_date"))),
            latest_validated_date=_maybe_str(payload.get("latest_validated_date", embedded.get("latest_validated_date"))),
            notes=list(notes or embedded.get("notes") or payload.get("notes") or []),
            provider_confidence=_maybe_float(embedded.get("provider_confidence")),
            feature_confidence=feature_confidence if feature_confidence is not None else _maybe_float(embedded.get("feature_confidence")),
            rank_confidence=rank_confidence if rank_confidence is not None else _maybe_float(embedded.get("rank_confidence")),
            execution_weight=execution_weight if execution_weight is not None else _maybe_float(embedded.get("execution_weight")),
        )

    def to_dict(self) -> dict[str, Any]:
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


@dataclass
class StageArtifact:
    """Describes a persisted output from a pipeline stage."""

    artifact_type: str
    uri: str
    row_count: Optional[int] = None
    content_hash: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    attempt_number: Optional[int] = None

    @classmethod
    def from_file(
        cls,
        artifact_type: str,
        path: Path,
        row_count: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        attempt_number: Optional[int] = None,
    ) -> "StageArtifact":
        return cls(
            artifact_type=artifact_type,
            uri=str(path),
            row_count=row_count,
            content_hash=compute_file_hash(path),
            metadata=metadata or {},
            attempt_number=attempt_number,
        )


@dataclass
class StageResult:
    """Standard stage response used by the orchestrator."""

    artifacts: List[StageArtifact] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class PipelineStageError(RuntimeError):
    """Base exception for stage-level failures."""


class DataQualityCriticalError(PipelineStageError):
    """Raised when a critical DQ rule fails and downstream work must stop.

    Use for hard-floor rules that should never be relaxed (catalog empty,
    duplicate keys, OHLC inconsistency, required-field nulls).
    """


class DataQualityRepairableError(PipelineStageError):
    """Raised when a critical-but-repairable DQ rule fails.

    Triggered for issues with an external root cause (provider gap, NSE
    reclassification, stuck quarantine) where auto-repair or relaxation
    can recover. Caught by the orchestrator to invoke repair logic; if
    ``dq_mode=relaxed``, the run is allowed to continue with status
    ``completed_with_dq_relaxations``.
    """


class PublishStageError(PipelineStageError):
    """Raised when publish targets fail after upstream stages have succeeded."""


@dataclass
class StageContext:
    """Execution context shared with each stage."""

    project_root: Path
    db_path: Path
    run_id: str
    run_date: str
    stage_name: str
    attempt_number: int
    registry: Any = None
    params: Dict[str, Any] = field(default_factory=dict)
    artifacts: Dict[str, Dict[str, StageArtifact]] = field(default_factory=dict)
    task_reporter: Any = None

    def output_dir(self) -> Path:
        from ai_trading_system.platform.db.paths import ensure_domain_layout

        paths = ensure_domain_layout(
            project_root=self.project_root,
            data_domain=self.params.get("data_domain", "operational"),
        )
        path = paths.pipeline_runs_dir / self.run_id / self.stage_name / f"attempt_{self.attempt_number}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_json(self, filename: str, payload: Dict[str, Any]) -> Path:
        output_path = self.output_dir() / filename
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
        return output_path

    def artifact_for(self, stage_name: str, artifact_type: str) -> Optional[StageArtifact]:
        return self.artifacts.get(stage_name, {}).get(artifact_type)

    def require_artifact(self, stage_name: str, artifact_type: str) -> StageArtifact:
        artifact = self.artifact_for(stage_name, artifact_type)
        if artifact is None:
            raise FileNotFoundError(
                f"Missing required artifact '{artifact_type}' from stage '{stage_name}'"
            )
        return artifact

    def report_task(
        self,
        *,
        task_name: str,
        status: str,
        detail: str | None = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if callable(self.task_reporter):
            self.task_reporter(
                {
                    "run_id": self.run_id,
                    "stage_name": self.stage_name,
                    "attempt_number": self.attempt_number,
                    "task_name": task_name,
                    "status": status,
                    "detail": detail,
                    "metadata": metadata or {},
                }
            )


def compute_file_hash(path: Path) -> str:
    """Return a stable SHA256 hash for an artifact file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _maybe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _maybe_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
