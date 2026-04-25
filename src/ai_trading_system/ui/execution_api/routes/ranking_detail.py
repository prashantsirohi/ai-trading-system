"""Per-symbol ranking detail + history endpoints.

  * ``GET /api/execution/ranking/{symbol}?run_id=`` — full ranked row +
    lifecycle + decision + curated factor block, optionally pinned to a
    specific run.
  * ``GET /api/execution/ranking/{symbol}/history?limit=N`` — historical
    rank position across the most recent N runs.

The list endpoint ``GET /api/execution/ranking`` (no path param) lives in
:mod:`routes.snapshots` and is matched first by FastAPI because it has no
path parameter — the new prefixed router below only declares paths with a
``{symbol}`` segment, so there is no collision.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Query

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.ranking_detail import (
    get_ranking_detail,
    get_ranking_history,
)


router = APIRouter(prefix="/api/execution/ranking", tags=["ranking-detail"])


@router.get("/{symbol}")
def execution_ranking_detail(
    symbol: str,
    run_id: Optional[str] = Query(
        default=None,
        description="Pin the response to a specific run; defaults to the latest.",
    ),
) -> dict[str, Any]:
    return get_ranking_detail(project_root(), symbol, run_id=run_id)


@router.get("/{symbol}/history")
def execution_ranking_history(
    symbol: str,
    limit: int = Query(
        default=20,
        ge=1,
        le=200,
        description="Maximum number of historical runs to return (newest first).",
    ),
) -> dict[str, Any]:
    return get_ranking_history(project_root(), symbol, limit=limit)


__all__ = ["router"]
