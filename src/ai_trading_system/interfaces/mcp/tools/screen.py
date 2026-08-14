"""Cross-sectional screening over the ranked universe.

Joins the version-pinned rank cross-section at a date with governed stage
structure and master sector classification, so an agent can ask "Stage 2 names
in Capital Goods above composite 70" in one call instead of pulling every
symbol and filtering client-side.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from ai_trading_system.domains.opportunities.contracts import (
    is_transition,
    legacy_code_for,
    normalize_stage,
    stage_family,
)
from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
    clamp_limit,
    coerce_date,
    envelope,
    json_safe,
)
from ai_trading_system.interfaces.mcp.readers import decisions, governed_stage, master

DEFAULT_UNIVERSE_ID = "NSE_OPERATIONAL"
DEFAULT_LIMIT = 50
MAX_LIMIT = 500

DATE_FIELDS = ("trade_date", "stage_as_of")

SORT_FIELDS = (
    "rank_position",
    "composite_score",
    "symbol_id",
)

_GOVERNED_TABLE = "weekly_stock_stage_history"


def _governed_stage_at(
    conn: Any, exchange: str, cutoff: date | None
) -> dict[str, dict[str, Any]]:
    """Latest governed stage observation per symbol at or before ``cutoff``."""

    result: dict[str, dict[str, Any]] = {}
    for record in governed_stage.snapshot(conn, exchange=exchange, cutoff=cutoff):
        symbol = str(record.get("symbol_id", "")).upper()
        observed = coerce_date(record.get("as_of"))
        stage = normalize_stage(record.get("effective_stage"))
        result[symbol] = {
            "stage_as_of": observed.isoformat() if observed else None,
            "stage_label": stage.value,
            "stage_label_legacy": legacy_code_for(stage),
            "stage_family": stage_family(stage),
            "is_transition": is_transition(stage),
            "stage_status": json_safe(record.get("stage_status")),
            "sector_name": json_safe(record.get("sector_name")),
        }
    return result


def screen_universe(
    ctx: McpContext,
    *,
    exchange: str = "NSE",
    universe_id: str = DEFAULT_UNIVERSE_ID,
    stage_label: str | None = None,
    stage_family_filter: str | None = None,
    sector: str | None = None,
    min_composite_score: float | None = None,
    max_rank_position: int | None = None,
    as_of: str | date | None = None,
    sort_by: str = "rank_position",
    limit: int | None = None,
) -> dict[str, Any]:
    """Filter the ranked universe cross-section at a point in time.

    ``stage_label`` accepts either vocabulary (``S2`` or ``stage_2_advancing``)
    and matches exactly; ``stage_family_filter`` matches the structural family,
    so ``stage_2`` also admits ``transition_2_to_3``.
    """

    exchange_code = ctx.resolve_exchange(exchange)
    row_limit = clamp_limit(limit, default=DEFAULT_LIMIT, maximum=MAX_LIMIT)
    cutoff = coerce_date(as_of)

    sort_key = str(sort_by or "rank_position").strip().lower()
    if sort_key not in SORT_FIELDS:
        raise ValueError(
            f"Unknown sort_by: {sort_by!r} (expected one of {list(SORT_FIELDS)})"
        )

    wanted_stage = normalize_stage(stage_label).value if stage_label else None
    wanted_family = (
        str(stage_family_filter).strip().lower() if stage_family_filter else None
    )

    notes: list[str] = []
    records: list[dict[str, Any]] = []
    stages: dict[str, dict[str, Any]] = {}
    effective: date | None = None

    with ctx.control_plane() as conn:
        if decisions.table_exists(conn, decisions.RANK_TABLE):
            effective = decisions.latest_trade_date(
                conn, decisions.RANK_TABLE, as_of=cutoff
            )
            try:
                records = decisions.latest_rows(
                    conn,
                    decisions.RANK_TABLE,
                    exchange=exchange_code,
                    as_of=cutoff,
                    extra_clauses=["universe_id = ?"],
                    extra_params=[universe_id],
                    limit=MAX_LIMIT * 20,
                )
            except decisions.DecisionVersionUnavailable as exc:
                notes.append(
                    f"Rank cross-section could not be version-pinned: {exc} "
                    "Returning no rows rather than mixing rank model versions."
                )
        stages = _governed_stage_at(conn, exchange_code, cutoff)

    # The symbol master is current-state only. It may fill missing sector names
    # for a latest query, but must never be projected into a historical answer.
    sector_by_symbol = (
        master.sector_by_symbol(ctx, exchange=exchange_code) if as_of is None else {}
    )

    rows: list[dict[str, Any]] = []
    for record in records:
        symbol = str(record.get("symbol_id", "")).upper()
        trade_date = coerce_date(record.get("trade_date"))
        stage = stages.get(symbol, {})
        row: dict[str, Any] = {
            "symbol_id": symbol,
            "exchange": json_safe(record.get("exchange")),
            "trade_date": trade_date.isoformat() if trade_date else None,
            "rank_position": json_safe(record.get("rank_position")),
            "composite_score": json_safe(record.get("composite_score")),
            "sector_name": stage.get("sector_name") or sector_by_symbol.get(symbol),
            "stage_as_of": stage.get("stage_as_of"),
            "stage_label": stage.get("stage_label"),
            "stage_label_legacy": stage.get("stage_label_legacy"),
            "stage_family": stage.get("stage_family"),
            "is_transition": stage.get("is_transition"),
        }

        if wanted_stage and row["stage_label"] != wanted_stage:
            continue
        if wanted_family and row["stage_family"] != wanted_family:
            continue
        if sector and str(row["sector_name"] or "").lower() != sector.strip().lower():
            continue
        if min_composite_score is not None and (
            row["composite_score"] is None
            or row["composite_score"] < float(min_composite_score)
        ):
            continue
        if max_rank_position is not None and (
            row["rank_position"] is None
            or row["rank_position"] > int(max_rank_position)
        ):
            continue
        rows.append(row)

    if sort_key == "composite_score":
        rows.sort(
            key=lambda item: (
                item.get(sort_key) is None,
                -(float(item[sort_key])) if item.get(sort_key) is not None else 0.0,
            )
        )
    else:
        rows.sort(
            key=lambda item: (
                item.get(sort_key) is None,
                item.get(sort_key) if item.get(sort_key) is not None else 0,
            )
        )

    matched = len(rows)
    truncated = matched > row_limit
    if truncated:
        rows = rows[:row_limit]
        notes.append(
            f"{matched} symbols matched; showing {row_limit}. Tighten the "
            "filters or raise 'limit' (max 500)."
        )

    if not records and not notes:
        notes.append(
            f"No ranked rows for universe {universe_id!r} on {exchange_code} at "
            "the requested date."
        )
    if records and not stages:
        notes.append(
            "No governed stage observations were available, so stage fields are "
            "null and stage filters would exclude everything."
        )

    if as_of is None:
        status = AS_OF_LATEST
    elif rows:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    return envelope(
        rows,
        source=(
            f"control_plane.duckdb:{decisions.RANK_TABLE} + "
            f"control_plane.duckdb:{_GOVERNED_TABLE}"
        ),
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        date_fields=DATE_FIELDS,
        notes=notes,
        exchange=exchange_code,
        universe_id=universe_id,
        stage_vocabulary="WeinsteinStage",
        filters={
            "stage_label": wanted_stage,
            "stage_family": wanted_family,
            "sector": sector,
            "min_composite_score": min_composite_score,
            "max_rank_position": max_rank_position,
        },
        sort_by=sort_key,
        matched_count=matched,
        truncated=truncated,
        universe_size=len(records),
        data_domain=ctx.paths.domain,
    )


__all__ = ["DEFAULT_UNIVERSE_ID", "SORT_FIELDS", "screen_universe"]
