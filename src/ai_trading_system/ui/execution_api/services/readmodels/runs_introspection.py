"""Read models for run-level introspection (DQ results + artifacts).

Backs the ``/api/execution/runs/{run_id}/dq`` and
``/api/execution/runs/{run_id}/artifacts`` endpoints, plus the security-gated
``/api/execution/artifacts/{run_id}/{stage}/{name}`` artifact download.

All functions take a project root and read directly from the control-plane
DuckDB database. They never raise on a missing database — instead they return
``{"available": False}``-style payloads so the UI can render a degraded state.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import duckdb

from ai_trading_system.ui.execution_api.services.readmodels.latest_operational_snapshot import (
    ExecutionContext,
    get_execution_context,
)


# Path-segment validator: alphanumerics, dot, dash, underscore. Blocks any
# attempt to inject "..", "/", or absolute paths through URL params.
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9._-]+$")


def _control_plane_path(ctx: ExecutionContext) -> Path:
    return ctx.project_root / "data" / "control_plane.duckdb"


# ---------------------------------------------------------------------------
# DQ results
# ---------------------------------------------------------------------------


def get_dq_results_for_run(
    project_root: str | Path | None,
    run_id: str,
    *,
    severity: Optional[str] = None,
    stage: Optional[str] = None,
) -> dict[str, Any]:
    """Return DQ results for ``run_id`` plus per-severity aggregates.

    Optional filters:
      * ``severity`` — restricts to one severity tier (e.g. "warn", "error").
      * ``stage`` — restricts to a single pipeline stage (e.g. "ingest").
    """

    ctx = get_execution_context(project_root)
    cp_path = _control_plane_path(ctx)
    if not cp_path.exists():
        return {"available": False, "run_id": run_id, "results": []}

    where_clauses = ["run_id = ?"]
    params: list[Any] = [run_id]
    if severity:
        where_clauses.append("severity = ?")
        params.append(severity)
    if stage:
        where_clauses.append("stage_name = ?")
        params.append(stage)
    where_sql = " AND ".join(where_clauses)

    conn = duckdb.connect(str(cp_path), read_only=True)
    try:
        rows = conn.execute(
            f"""
            SELECT
                result_id, run_id, stage_name, rule_id, severity, status,
                COALESCE(failed_count, 0) AS failed_count,
                message, sample_uri, created_at
            FROM dq_result
            WHERE {where_sql}
            ORDER BY
                CASE severity WHEN 'error' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END,
                stage_name,
                rule_id
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    results: list[dict[str, Any]] = []
    counts_by_severity: dict[str, dict[str, int]] = {}
    total_failed = 0
    total_passed = 0

    for row in rows:
        result_id, rid, stage_name, rule_id, sev, status, failed_count, message, sample_uri, created_at = row
        results.append(
            {
                "result_id": result_id,
                "run_id": rid,
                "stage_name": stage_name,
                "rule_id": rule_id,
                "severity": sev,
                "status": status,
                "failed_count": int(failed_count or 0),
                "message": message,
                "sample_uri": sample_uri,
                "created_at": str(created_at) if created_at is not None else None,
            }
        )
        bucket = counts_by_severity.setdefault(
            sev or "unknown", {"failed": 0, "passed": 0}
        )
        if (status or "").lower() == "passed":
            bucket["passed"] += 1
            total_passed += 1
        else:
            bucket["failed"] += 1
            total_failed += 1

    return {
        "available": True,
        "run_id": run_id,
        "results": results,
        "summary": {
            "total": len(results),
            "total_failed": total_failed,
            "total_passed": total_passed,
            "counts_by_severity": counts_by_severity,
        },
        "filters": {"severity": severity, "stage": stage},
    }


# ---------------------------------------------------------------------------
# Artifact registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArtifactRecord:
    """Single ``pipeline_artifact`` row, normalised for the read model."""

    artifact_id: str
    run_id: str
    stage_name: str
    attempt_number: int
    artifact_type: str
    uri: str
    name: str  # basename of uri, used for the download URL
    content_hash: Optional[str]
    row_count: Optional[int]
    created_at: Optional[str]


def _row_to_artifact(row: tuple) -> ArtifactRecord:
    artifact_id, run_id, stage_name, attempt_number, artifact_type, uri, content_hash, row_count, created_at = row
    return ArtifactRecord(
        artifact_id=artifact_id,
        run_id=run_id,
        stage_name=stage_name,
        attempt_number=int(attempt_number),
        artifact_type=artifact_type,
        uri=uri,
        name=Path(uri).name,
        content_hash=content_hash,
        row_count=int(row_count) if row_count is not None else None,
        created_at=str(created_at) if created_at is not None else None,
    )


def get_artifacts_for_run(
    project_root: str | Path | None,
    run_id: str,
) -> dict[str, Any]:
    """Return the artifact list for ``run_id``, grouped by stage."""

    ctx = get_execution_context(project_root)
    cp_path = _control_plane_path(ctx)
    if not cp_path.exists():
        return {"available": False, "run_id": run_id, "artifacts": []}

    conn = duckdb.connect(str(cp_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT
                artifact_id, run_id, stage_name, attempt_number,
                artifact_type, uri, content_hash, row_count, created_at
            FROM pipeline_artifact
            WHERE run_id = ?
            ORDER BY stage_name, attempt_number, uri
            """,
            [run_id],
        ).fetchall()
    finally:
        conn.close()

    records = [_row_to_artifact(r) for r in rows]

    artifacts: list[dict[str, Any]] = []
    for record in records:
        artifacts.append(
            {
                "artifact_id": record.artifact_id,
                "run_id": record.run_id,
                "stage_name": record.stage_name,
                "attempt_number": record.attempt_number,
                "artifact_type": record.artifact_type,
                "uri": record.uri,
                "name": record.name,
                "content_hash": record.content_hash,
                "row_count": record.row_count,
                "created_at": record.created_at,
                "download_url": (
                    f"/api/execution/artifacts/{record.run_id}/{record.stage_name}/{record.name}"
                ),
            }
        )

    counts_by_stage: dict[str, int] = {}
    for record in records:
        counts_by_stage[record.stage_name] = counts_by_stage.get(record.stage_name, 0) + 1

    return {
        "available": True,
        "run_id": run_id,
        "artifacts": artifacts,
        "counts_by_stage": counts_by_stage,
        "total": len(artifacts),
    }


# ---------------------------------------------------------------------------
# Gated download path resolution
# ---------------------------------------------------------------------------


class ArtifactNotFoundError(LookupError):
    """Raised when the requested (run_id, stage, name) tuple has no record."""


class UnsafeArtifactPathError(PermissionError):
    """Raised when the resolved artifact path escapes the runs directory."""


def resolve_artifact_path(
    project_root: str | Path | None,
    run_id: str,
    stage: str,
    name: str,
) -> Path:
    """Resolve (run_id, stage, name) to an absolute path under pipeline_runs/.

    Performs three independent safety checks before returning a path:

    1. Each URL segment must match a strict allow-list (alphanumerics + ``._-``).
       Blocks ``..``, slashes, and any traversal attempt at the URL layer.
    2. The (run_id, stage_name) tuple must have a registered ``pipeline_artifact``
       row whose URI's basename equals ``name``. We trust the registry, not
       the raw filesystem walk.
    3. The fully resolved path must remain under ``pipeline_runs_dir``.

    Raises :class:`ArtifactNotFoundError` for missing records and
    :class:`UnsafeArtifactPathError` for path-escape attempts.
    """

    for segment in (run_id, stage, name):
        if not _SAFE_SEGMENT.fullmatch(segment):
            raise ArtifactNotFoundError(
                f"unsafe path segment in artifact request: {segment!r}"
            )

    ctx = get_execution_context(project_root)
    cp_path = _control_plane_path(ctx)
    if not cp_path.exists():
        raise ArtifactNotFoundError(
            "control_plane.duckdb is not present; cannot resolve artifact"
        )

    conn = duckdb.connect(str(cp_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT artifact_id, uri
            FROM pipeline_artifact
            WHERE run_id = ? AND stage_name = ?
            """,
            [run_id, stage],
        ).fetchall()
    finally:
        conn.close()

    matched_uri: Optional[str] = None
    for _artifact_id, uri in rows:
        if Path(uri).name == name:
            matched_uri = uri
            break
    if matched_uri is None:
        raise ArtifactNotFoundError(
            f"no artifact registered for run_id={run_id} stage={stage} name={name}"
        )

    candidate = Path(matched_uri)
    if not candidate.is_absolute():
        candidate = (ctx.project_root / candidate).resolve()
    else:
        candidate = candidate.resolve()

    runs_root = ctx.pipeline_runs_dir.resolve()
    try:
        candidate.relative_to(runs_root)
    except ValueError as exc:
        raise UnsafeArtifactPathError(
            f"resolved artifact path {candidate} is outside {runs_root}"
        ) from exc

    if not candidate.exists():
        raise ArtifactNotFoundError(
            f"artifact registered but file is missing on disk: {candidate}"
        )

    return candidate


__all__ = [
    "ArtifactNotFoundError",
    "ArtifactRecord",
    "UnsafeArtifactPathError",
    "get_artifacts_for_run",
    "get_dq_results_for_run",
    "resolve_artifact_path",
]
