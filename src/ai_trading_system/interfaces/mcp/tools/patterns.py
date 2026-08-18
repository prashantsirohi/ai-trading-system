"""Point-in-time operational pattern history reads."""

from __future__ import annotations

from datetime import date
from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT, AS_OF_LATEST, AS_OF_NO_DATA, clamp_limit, coerce_date,
    envelope, json_safe,
)
from ai_trading_system.interfaces.mcp.readers import decisions

DEFAULT_LIMIT = 250
MAX_LIMIT = 2000
DATE_FIELDS = ("trade_date",)


def _row(record: dict[str, Any]) -> dict[str, Any]:
    observed = coerce_date(record.get("trade_date"))
    return {
        "symbol_id": json_safe(record.get("symbol_id")),
        "exchange": json_safe(record.get("exchange")),
        "trade_date": observed.isoformat() if observed else None,
        "pattern_family": json_safe(record.get("pattern_family")),
        "pattern_state": json_safe(record.get("pattern_state")),
        "pattern_score": json_safe(record.get("pattern_score")),
        "setup_quality": json_safe(record.get("setup_quality")),
        "pattern_promotion_state": json_safe(record.get("pattern_promotion_state")),
        "pivot_price": json_safe(record.get("pivot_price")),
        "distance_to_pivot_pct": json_safe(record.get("distance_to_pivot_pct")),
        "breakout_status": json_safe(record.get("breakout_status")),
        "breakout_attempt_flag": json_safe(record.get("breakout_attempt_flag")),
        "pattern_model_version": json_safe(record.get("pattern_model_version")),
        "pattern_config_hash": json_safe(record.get("pattern_config_hash")),
        "pipeline_run_id": json_safe(record.get("pipeline_run_id")),
    }


def get_pattern_detail(
    ctx: McpContext, symbol: str, *, exchange: str = "NSE",
    as_of: str | date | None = None,
) -> dict[str, Any]:
    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    records: list[dict[str, Any]] = []
    notes: list[str] = []
    with ctx.control_plane() as conn:
        if decisions.table_exists(conn, decisions.PATTERN_TABLE):
            try:
                records = decisions.latest_rows(
                    conn, decisions.PATTERN_TABLE, symbol_id=symbol_id,
                    exchange=exchange_code, as_of=as_of, limit=100,
                )
            except decisions.DecisionVersionUnavailable as exc:
                notes.append(str(exc))
    rows = [_row(record) for record in records]
    effective = max((coerce_date(row["trade_date"]) for row in rows), default=None)
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if rows else AS_OF_NO_DATA)
    return envelope(
        rows, source="control_plane.duckdb:pattern_history",
        as_of_status=status, as_of_requested=as_of, as_of_effective=effective,
        date_fields=DATE_FIELDS, notes=notes, symbol=symbol_id,
        exchange=exchange_code, actionable=True,
        shadow_pattern_lane_included=False, data_domain=ctx.paths.domain,
    )


def get_pattern_history(
    ctx: McpContext, symbol: str, *, exchange: str = "NSE",
    from_date: str | date | None = None, to_date: str | date | None = None,
    as_of: str | date | None = None, limit: int | None = None,
) -> dict[str, Any]:
    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    row_limit = clamp_limit(limit, default=DEFAULT_LIMIT, maximum=MAX_LIMIT)
    records: list[dict[str, Any]] = []
    notes: list[str] = []
    with ctx.control_plane() as conn:
        if decisions.table_exists(conn, decisions.PATTERN_TABLE):
            try:
                records = decisions.history_rows(
                    conn, decisions.PATTERN_TABLE, symbol_id=symbol_id,
                    exchange=exchange_code, from_date=from_date, to_date=to_date,
                    as_of=as_of, limit=row_limit,
                )
            except decisions.DecisionVersionUnavailable as exc:
                notes.append(str(exc))
    rows = [_row(record) for record in records]
    effective = max((coerce_date(row["trade_date"]) for row in rows), default=None)
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if rows else AS_OF_NO_DATA)
    return envelope(
        rows, source="control_plane.duckdb:pattern_history",
        as_of_status=status, as_of_requested=as_of, as_of_effective=effective,
        date_fields=DATE_FIELDS, notes=notes, symbol=symbol_id,
        exchange=exchange_code, actionable=True,
        shadow_pattern_lane_included=False, truncated=len(records) >= row_limit,
        data_domain=ctx.paths.domain,
    )


__all__ = ["get_pattern_detail", "get_pattern_history"]
