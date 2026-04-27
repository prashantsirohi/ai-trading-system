"""Pydantic request models for the execution API.

These were previously defined inline in ``app.py``. They live here so route
modules can depend on a stable schema package without pulling the FastAPI
bootstrap into their import graph.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PipelineRunRequest(BaseModel):
    """Body for ``POST /api/execution/pipeline/run``."""

    label: str = "Execution API pipeline run"
    stages: list[str] = Field(
        default_factory=lambda: ["ingest", "features", "rank", "publish"]
    )
    params: dict[str, Any] = Field(default_factory=dict)
    run_id: str | None = None
    run_date: str | None = None


class PublishRetryRequest(BaseModel):
    """Body for ``POST /api/execution/pipeline/publish-retry``."""

    local_publish: bool = False
    run_id: str | None = None


class ShadowRunRequest(BaseModel):
    """Body for ``POST /api/execution/shadow/run``."""

    label: str = "Shadow refresh"
    backfill_days: int = 0
    prediction_date: str | None = None


__all__ = [
    "PipelineRunRequest",
    "PublishRetryRequest",
    "ShadowRunRequest",
]
