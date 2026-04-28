"""Sector-level endpoints.

  * ``GET /api/execution/sectors`` — list of all sectors with stage breadth.
  * ``GET /api/execution/sectors/{sector}/constituents`` — ALL stocks in a
    sector with latest price, technicals, and weekly Weinstein stage label.
"""

from __future__ import annotations

from fastapi import APIRouter

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.sector_detail import (
    get_sector_constituents,
    get_sectors_with_stage,
)

router = APIRouter(prefix="/api/execution/sectors", tags=["sectors"])


@router.get("")
def list_sectors():
    """All sectors with RS, momentum, quadrant, and stage distribution."""
    root = project_root()
    return get_sectors_with_stage(root)


@router.get("/{sector}/constituents")
def sector_constituents(sector: str):
    """All NSE stocks in *sector* with latest price, technicals, and stage label."""
    root = project_root()
    return get_sector_constituents(root, sector)
