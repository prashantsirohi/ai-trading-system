"""Per-stock detail endpoints.

Backs the Stock Detail Workspace described in
``docs/EXECUTION_CONSOLE_PLAN.md`` (Phase 2b PR #12). Two endpoints:

  * ``GET /api/execution/stocks/{symbol}`` — fundamentals, latest quote,
    ranking position, lifecycle.
  * ``GET /api/execution/stocks/{symbol}/ohlcv`` — daily candles + delivery.

Both delegate to :mod:`ai_trading_system.ui.execution_api.services.readmodels.stock_detail`,
which is fastapi-free and continues to satisfy the layer-boundary lint.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Query

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.stock_detail import (
    get_stock_detail,
    get_stock_ohlcv,
)


router = APIRouter(prefix="/api/execution/stocks", tags=["stocks"])


@router.get("/{symbol}")
def execution_stock_detail(symbol: str) -> dict[str, Any]:
    """Return the consolidated detail payload for ``symbol``.

    Always returns 200 even when sources are missing — the UI inspects
    ``available`` and the per-block ``None``-ness to decide what to render.
    """

    return get_stock_detail(project_root(), symbol)


@router.get("/{symbol}/ohlcv")
def execution_stock_ohlcv(
    symbol: str,
    from_date: Optional[str] = Query(
        default=None,
        alias="from",
        description="Inclusive lower bound (ISO date, ``YYYY-MM-DD``).",
    ),
    to_date: Optional[str] = Query(
        default=None,
        alias="to",
        description="Inclusive upper bound (ISO date, ``YYYY-MM-DD``).",
    ),
    interval: str = Query(
        default="daily",
        description="Candle interval; only ``daily`` is supported today.",
    ),
    limit: Optional[int] = Query(
        default=None,
        ge=1,
        le=2000,
        description="Maximum candles to return; keeps the most recent rows.",
    ),
) -> dict[str, Any]:
    """Return OHLCV + delivery candles for ``symbol`` in ascending time order."""

    return get_stock_ohlcv(
        project_root(),
        symbol,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
        limit=limit,
    )


__all__ = ["router"]
