"""Shared stage and artifact contracts used across runtime layers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.paths import ensure_domain_layout


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
    """Raised when a critical DQ rule fails and downstream work must stop."""


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

    def output_dir(self) -> Path:
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


def compute_file_hash(path: Path) -> str:
    """Return a stable SHA256 hash for an artifact file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
