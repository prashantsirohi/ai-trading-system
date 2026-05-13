"""Engine-driven backtest endpoints."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.backtest_service import (
    list_risk_profiles,
    run_backtest,
    run_winner_capture_backtest,
)


router = APIRouter(prefix="/api/execution/backtest", tags=["backtest"])


def _parse_iso(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid date: {value}") from exc


@router.get("/profiles")
def backtest_profiles() -> dict[str, Any]:
    """List all risk profiles available under ``config/risk_profiles/*.yaml``."""
    return {"profiles": list_risk_profiles()}


class BacktestRunRequest(BaseModel):
    profile: str = Field(..., description="Risk profile name")
    data_source: str = Field(default="pipeline_replay", description="pipeline_replay or research_dynamic")
    from_date: Optional[str] = Field(default=None, description="ISO date (inclusive)")
    to_date: Optional[str] = Field(default=None, description="ISO date (inclusive)")
    equity: float = Field(default=1_000_000.0, ge=0.0)
    persist: bool = Field(default=True)
    custom_config: Optional[dict[str, Any]] = Field(default=None)


class WinnerCaptureRequest(BaseModel):
    year: int = Field(..., ge=1990, le=2100)
    exchange: str = Field(default="NSE")
    top_gainers: int = Field(default=50, ge=1, le=500)
    rank_cutoff: int = Field(default=50, ge=1, le=500)
    persist: bool = Field(default=True)


@router.post("/run")
def backtest_run(req: BacktestRunRequest) -> dict[str, Any]:
    """Run an engine-driven backtest and return summary + trade rows inline."""
    return run_backtest(
        project_root(),
        profile_name=req.profile,
        data_source=req.data_source,
        from_date=_parse_iso(req.from_date),
        to_date=_parse_iso(req.to_date),
        equity=req.equity,
        persist=req.persist,
        custom_config=req.custom_config,
    )


@router.post("/winner-capture")
def winner_capture(req: WinnerCaptureRequest) -> dict[str, Any]:
    """Run yearly top-gainer capture analysis against research dynamic rankings."""
    return run_winner_capture_backtest(
        project_root(),
        year=req.year,
        exchange=req.exchange,
        top_gainers=req.top_gainers,
        rank_cutoff=req.rank_cutoff,
        persist=req.persist,
    )
