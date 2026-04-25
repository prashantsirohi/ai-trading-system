"""Gated artifact download endpoint.

Sits at ``/api/execution/artifacts/{run_id}/{stage}/{name}`` and serves files
referenced by ``pipeline_artifact`` rows in the control-plane registry.

The actual safety logic (URL-segment regex check, registry lookup by basename,
resolved-path containment under ``pipeline_runs_dir``) lives in
:func:`resolve_artifact_path` — this router only translates the typed
exceptions into HTTP status codes.
"""

from __future__ import annotations

import mimetypes

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.runs_introspection import (
    ArtifactNotFoundError,
    UnsafeArtifactPathError,
    resolve_artifact_path,
)


router = APIRouter(prefix="/api/execution/artifacts", tags=["artifacts"])


@router.get("/{run_id}/{stage}/{name}")
def download_artifact(run_id: str, stage: str, name: str) -> FileResponse:
    """Serve an artifact file, gated by the registry + path-containment check."""

    try:
        path = resolve_artifact_path(project_root(), run_id, stage, name)
    except ArtifactNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except UnsafeArtifactPathError as exc:
        # 400 (not 404): the request was syntactically well-formed and the
        # registry knew about it — but the resolved location escaped the
        # pipeline_runs_dir sandbox. Surface the gate decision explicitly.
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    media_type, _ = mimetypes.guess_type(path.name)
    return FileResponse(
        path,
        media_type=media_type or "application/octet-stream",
        filename=path.name,
    )


__all__ = ["router"]
