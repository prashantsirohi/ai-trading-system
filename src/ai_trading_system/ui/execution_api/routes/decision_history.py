"""Persisted decision-history and read-source diagnostics."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from ai_trading_system.ui.execution_api.routes._deps import project_root
from ai_trading_system.ui.execution_api.services.readmodels.decision_reads import (
    DecisionOperatorReadService,
    PatternHistoryReadRepository,
    RankHistoryReadRepository,
    Stage1AnalyticsReadRepository,
    Stage1LifecycleReadRepository,
    StageHistoryReadRepository,
)

router = APIRouter(prefix="/api/stocks", tags=["decision-history"])
diagnostics_router = APIRouter(prefix="/api/health", tags=["health"])


def _filters(from_date: str | None, to_date: str | None, exchange: str, model_version: str | None, config_hash: str | None, limit: int, offset: int) -> dict[str, Any]:
    return {"start_date": from_date, "end_date": to_date, "exchange": exchange, "model_version": model_version, "config_hash": config_hash, "limit": limit, "offset": offset}


@router.get("/{symbol}/rank-history")
def rank_history(symbol: str, from_date: str | None = Query(None, alias="from"), to_date: str | None = Query(None, alias="to"), exchange: str = "NSE", universe_id: str | None = None, model_version: str | None = None, config_hash: str | None = None, limit: int = Query(500, ge=1, le=2000), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    extra, params = (("universe_id = ?",), (universe_id,)) if universe_id else ((), ())
    return RankHistoryReadRepository(project_root()).history(symbol, **_filters(from_date, to_date, exchange, model_version, config_hash, limit, offset), extra_clauses=extra, extra_params=params)


@router.get("/{symbol}/stage-history")
def stage_history(symbol: str, from_date: str | None = Query(None, alias="from"), to_date: str | None = Query(None, alias="to"), exchange: str = "NSE", model_version: str | None = None, config_hash: str | None = None, limit: int = Query(500, ge=1, le=2000), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    return StageHistoryReadRepository(project_root()).history(symbol, **_filters(from_date, to_date, exchange, model_version, config_hash, limit, offset))


@router.get("/{symbol}/stage1-history")
def stage1_history(symbol: str, from_date: str | None = Query(None, alias="from"), to_date: str | None = Query(None, alias="to"), exchange: str = "NSE", model_version: str | None = None, config_hash: str | None = None, limit: int = Query(500, ge=1, le=2000), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    return Stage1AnalyticsReadRepository(project_root()).history(symbol, **_filters(from_date, to_date, exchange, model_version, config_hash, limit, offset))


@router.get("/{symbol}/pattern-history")
def pattern_history(symbol: str, from_date: str | None = Query(None, alias="from"), to_date: str | None = Query(None, alias="to"), exchange: str = "NSE", pattern_family: str | None = None, model_version: str | None = None, config_hash: str | None = None, limit: int = Query(500, ge=1, le=2000), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    extra, params = (("pattern_family = ?",), (pattern_family,)) if pattern_family else ((), ())
    return PatternHistoryReadRepository(project_root()).history(symbol, **_filters(from_date, to_date, exchange, model_version, config_hash, limit, offset), extra_clauses=extra, extra_params=params)


@router.get("/{symbol}/transitions")
def transitions(symbol: str, from_date: str | None = Query(None, alias="from"), to_date: str | None = Query(None, alias="to"), exchange: str = "NSE", limit: int = Query(500, ge=1, le=2000)) -> dict[str, Any]:
    frame = Stage1LifecycleReadRepository(project_root()).get_candidate_transitions(symbol, from_date, to_date, exchange, limit)
    return {"symbol_id": symbol.upper(), "rows": frame.astype(object).where(frame.notna(), None).to_dict(orient="records")}


@router.get("/{symbol}/decision-history")
def decision_history(symbol: str, from_date: str | None = Query(None, alias="from"), to_date: str | None = Query(None, alias="to"), exchange: str = "NSE", model_version: str | None = None, config_hash: str | None = None, limit: int = Query(500, ge=1, le=2000), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    return DecisionOperatorReadService(project_root()).decision_history(symbol, **_filters(from_date, to_date, exchange, model_version, config_hash, limit, offset))


@diagnostics_router.get("/decision-read-sources")
def decision_read_source_summary() -> dict[str, Any]:
    payload = DecisionOperatorReadService(project_root()).current(limit=500)
    return {"decision_read_source_summary": list(payload["sources"].values()), "errors": payload["errors"]}


__all__ = ["router", "diagnostics_router"]
