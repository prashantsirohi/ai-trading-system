"""Canonical Phase 3C artifact registry and safe read-only locator."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .telemetry import ApiMetrics


@dataclass(frozen=True, slots=True)
class CanonicalArtifactSpec:
    artifact_key: str
    filenames: tuple[str, ...]
    artifact_family: str
    schema_version: str | None = None
    required_fields: tuple[str, ...] = ()
    optional: bool = True
    source_priority: int = 100


def _spec(key: str, family: str, fields: tuple[str, ...] = (), *, optional: bool = True) -> CanonicalArtifactSpec:
    suffix = ".json" if key.endswith(("summary", "manifest", "readiness", "comparison")) else ".csv"
    return CanonicalArtifactSpec(key, (f"{key}{suffix}",), family, required_fields=fields, optional=optional)


ARTIFACT_SPECS: dict[str, CanonicalArtifactSpec] = {
    spec.artifact_key: spec for spec in (
        _spec("active_position_coverage", "position_coverage", ("position_cycle_id", "symbol_id", "exchange"), optional=False),
        _spec("active_position_missing_data", "position_coverage", ("position_cycle_id", "symbol_id")),
        _spec("position_monitor_reconciliation", "position_coverage", ("position_cycle_id",)),
        _spec("position_episode_compatibility", "position_recovery", ("position_cycle_id", "compatibility_status")),
        _spec("position_recovery_proposals", "position_recovery", ("position_cycle_id",)),
        _spec("position_recovery_actions", "position_recovery", ("position_cycle_id",)),
        _spec("phase3c5_calibration_eligible", "calibration"),
        _spec("phase3c5_calibration_excluded", "calibration"),
        _spec("phase3c5_calibration_quarantined", "calibration"),
        _spec("phase3c5_calibration_manifest", "calibration", optional=False),
        CanonicalArtifactSpec("phase3c5_calibration_quality_summary", ("phase3c5_calibration_quality_summary.json",), "calibration"),
        _spec("phase3c5_sample_coverage", "calibration", ("dimension", "value", "count", "status")),
        _spec("phase3c5_exclusion_reasons", "calibration", ("exclusion_reason", "count")),
        _spec("phase3c5_readiness_checks", "readiness", ("check_id", "status")),
        CanonicalArtifactSpec("phase3c5_phase4_readiness", ("phase3c5_phase4_readiness.json",), "readiness", required_fields=("verdict", "limitations")),
        CanonicalArtifactSpec("phase3c5_phase4_readiness_markdown", ("phase3c5_phase4_readiness.md",), "readiness"),
        CanonicalArtifactSpec("phase3c4_performance_summary", ("phase3c4_performance_summary.json",), "performance", required_fields=("run_id", "performance_status"), optional=False),
        _spec("phase3c4_performance_metrics", "performance", ("run_id", "stage_name", "operation_name")),
        _spec("phase3c4_artifact_metrics", "performance", ("artifact_name",)),
        _spec("phase3c4_database_metrics", "performance", ("stage_name", "operation_name")),
        CanonicalArtifactSpec("phase3c4_replay_comparison", ("phase3c4_replay_comparison.json",), "performance"),
        _spec("scan_routing", "routing", ("symbol_id", "exchange")),
        _spec("routing_conflicts", "routing_conflicts"),
        _spec("weekly_stock_stage_universe", "stages", ("symbol_id", "effective_stage")),
        _spec("weekly_sector_stage_universe", "stages", ("sector_id", "effective_stage")),
        _spec("registry_conflicts", "governance_conflicts"),
    )
}


@dataclass(frozen=True, slots=True)
class LocatedArtifact:
    artifact_key: str
    path: Path
    run_id: str | None
    source_as_of: datetime | None
    available_at: datetime | None
    content_hash: str
    schema_version: str | None
    policy_version: str | None
    artifact_status: str

    def lineage(self) -> dict[str, Any]:
        return {
            "source_type": "canonical_artifact", "source_id": self.artifact_key,
            "run_id": self.run_id, "content_hash": self.content_hash,
            "schema_version": self.schema_version, "policy_version": self.policy_version,
            "source_as_of": self.source_as_of, "available_at": self.available_at,
        }


class CanonicalArtifactLocator:
    """Locate promoted artifacts first, then safe-root evidence deterministically."""

    def __init__(self, access: Any, roots: Iterable[Path], metrics: ApiMetrics):
        self.access = access
        self.metrics = metrics
        self.roots = tuple(self._safe_root(root) for root in roots if root is not None and root.exists())

    @staticmethod
    def _safe_root(root: Path) -> Path:
        if root.is_symlink():
            raise ValueError("artifact root must not be a symlink")
        return root.resolve(strict=True)

    def locate_latest_successful(self, artifact_key: str, *, as_of: datetime | None = None) -> LocatedArtifact | None:
        spec = ARTIFACT_SPECS[artifact_key]
        with self.metrics.source_read(spec.artifact_family):
            registered = self.access.registered_artifacts(artifact_key, spec.filenames, as_of=as_of)
            for row in registered:
                located = self._from_registered(spec, row)
                if located is not None:
                    return located
            candidates: list[LocatedArtifact] = []
            for root in self.roots:
                for filename in spec.filenames:
                    for path in root.rglob(filename):
                        located = self._from_path(spec, path)
                        if located is not None and (as_of is None or located.source_as_of is None or located.source_as_of <= as_of):
                            candidates.append(located)
            candidates.sort(key=lambda item: (item.source_as_of or datetime.min.replace(tzinfo=timezone.utc), item.run_id or "", str(item.path)), reverse=True)
            return candidates[0] if candidates else None

    def locate_runs(self, anchor_key: str) -> list[dict[str, LocatedArtifact]]:
        anchors: list[LocatedArtifact] = []
        spec = ARTIFACT_SPECS[anchor_key]
        for root in self.roots:
            for filename in spec.filenames:
                for path in root.rglob(filename):
                    located = self._from_path(spec, path)
                    if located is not None:
                        anchors.append(located)
        registered = [self._from_registered(spec, row) for row in self.access.registered_artifacts(anchor_key, spec.filenames)]
        anchors.extend(item for item in registered if item is not None)
        unique: dict[tuple[str | None, Path], LocatedArtifact] = {(item.run_id, item.path.resolve()): item for item in anchors}
        runs: list[dict[str, LocatedArtifact]] = []
        for anchor in unique.values():
            family = {anchor_key: anchor}
            for key, candidate_spec in ARTIFACT_SPECS.items():
                if candidate_spec.artifact_family != spec.artifact_family or key == anchor_key:
                    continue
                for filename in candidate_spec.filenames:
                    sibling = anchor.path.parent / filename
                    located = self._from_path(candidate_spec, sibling)
                    if located is not None:
                        family[key] = located
                        break
            runs.append(family)
        runs.sort(key=lambda family: (family[anchor_key].source_as_of or datetime.min.replace(tzinfo=timezone.utc), family[anchor_key].run_id or ""), reverse=True)
        return runs

    def read(self, located: LocatedArtifact) -> dict[str, Any] | list[dict[str, Any]]:
        spec = ARTIFACT_SPECS[located.artifact_key]
        with self.metrics.source_read(spec.artifact_family) as observation:
            if located.path.suffix.lower() == ".json":
                payload = json.loads(located.path.read_text(encoding="utf-8"))
                rows = 1
            else:
                with located.path.open("r", encoding="utf-8", newline="") as handle:
                    payload = list(csv.DictReader(handle))
                rows = len(payload)
            observation["rows"] = rows
        fields = set(payload if isinstance(payload, dict) else (payload[0] if payload else {}))
        missing = set(spec.required_fields) - fields
        if missing:
            raise ValueError(f"canonical artifact {located.artifact_key} is missing required fields")
        return payload

    def _from_registered(self, spec: CanonicalArtifactSpec, row: dict[str, Any]) -> LocatedArtifact | None:
        path = Path(str(row["uri"]))
        located = self._from_path(spec, path, expected_hash=row.get("content_hash"))
        if located is None:
            return None
        metadata = _json(row.get("metadata_json"))
        return LocatedArtifact(
            located.artifact_key, located.path, str(row.get("run_id") or located.run_id) or None,
            _dt(row.get("run_date")) or located.source_as_of,
            _dt(row.get("promoted_at") or row.get("run_ended_at") or row.get("created_at")) or located.available_at,
            located.content_hash, metadata.get("schema_version") or located.schema_version,
            metadata.get("policy_version") or located.policy_version, "promoted",
        )

    def _from_path(self, spec: CanonicalArtifactSpec, path: Path, expected_hash: str | None = None) -> LocatedArtifact | None:
        if not path.is_file() or path.is_symlink():
            return None
        resolved = path.resolve(strict=True)
        if self.roots and not any(resolved.is_relative_to(root) for root in self.roots):
            return None
        content_hash = hashlib.sha256(resolved.read_bytes()).hexdigest()
        if expected_hash and content_hash != expected_hash:
            return None
        metadata = _peek_metadata(resolved)
        run_id = metadata.get("run_id") or _run_id_from_path(resolved)
        return LocatedArtifact(
            spec.artifact_key, resolved, str(run_id) if run_id else None,
            _dt(metadata.get("as_of") or metadata.get("source_as_of") or metadata.get("decision_time_max")),
            _dt(metadata.get("generated_at") or metadata.get("completed_at") or metadata.get("created_at")),
            content_hash, metadata.get("schema_version") or spec.schema_version,
            metadata.get("policy_version"), "immutable",
        )


def _peek_metadata(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".csv":
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                row = next(csv.DictReader(handle), {})
            return {key: row.get(key) for key in ("run_id", "as_of", "source_as_of", "available_at", "generated_at", "policy_version", "schema_version") if row.get(key)}
        except OSError:
            return {}
    if path.suffix.lower() != ".json":
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _run_id_from_path(path: Path) -> str | None:
    parts = path.parts
    if "pipeline_runs" in parts:
        index = parts.index("pipeline_runs")
        return parts[index + 1] if index + 1 < len(parts) else None
    return path.parent.name


def _json(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{value}T23:59:59+00:00")
        except ValueError:
            return None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)
