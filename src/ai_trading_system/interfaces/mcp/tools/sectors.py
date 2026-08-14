"""Sector structure, derived point-in-time from the governed stage store.

``sector_detail.py``'s read models are not reused here: they lead with the
``sector_dashboard.csv`` rank artifact, which is latest-only, and they read the
symbol master over read-write SQLite. Both would break the point-in-time and
read-only contracts.

Instead, sector structure is aggregated from
``weekly_stock_stage_history``, which carries ``sector_id``/``sector_name`` on
every governed observation and is effective-dated — so a sector's stage
distribution can be answered honestly for a past date.
"""

from __future__ import annotations

from collections import Counter, defaultdict
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

_GOVERNED_TABLE = "weekly_stock_stage_history"

DATE_FIELDS = ("stage_as_of",)

_FAMILIES = ("stage_1", "stage_2", "stage_3", "stage_4", "unknown")


def _latest_observations(
    conn: Any, exchange: str, cutoff: date | None
) -> list[dict[str, Any]]:
    """Newest governed observation per symbol at or before ``cutoff``."""

    return governed_stage.snapshot(conn, exchange=exchange, cutoff=cutoff)


def get_sector_overview(
    ctx: McpContext,
    *,
    exchange: str = "NSE",
    as_of: str | date | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Stage distribution per sector at a point in time."""

    exchange_code = ctx.resolve_exchange(exchange)
    cutoff = coerce_date(as_of)
    row_limit = clamp_limit(limit, default=100, maximum=500)

    with ctx.control_plane() as conn:
        observations = _latest_observations(conn, exchange_code, cutoff)

    by_sector: dict[str, Counter] = defaultdict(Counter)
    dates: dict[str, list[date]] = defaultdict(list)
    for record in observations:
        sector = str(record.get("sector_name") or "Unclassified")
        stage = normalize_stage(record.get("effective_stage"))
        by_sector[sector][stage_family(stage)] += 1
        by_sector[sector]["total"] += 1
        if is_transition(stage):
            by_sector[sector]["in_transition"] += 1
        observed = coerce_date(record.get("as_of"))
        if observed:
            dates[sector].append(observed)

    rows: list[dict[str, Any]] = []
    for sector, counts in by_sector.items():
        total = counts["total"] or 1
        latest = max(dates[sector]) if dates[sector] else None
        row: dict[str, Any] = {
            "sector_name": sector,
            "constituents_observed": counts["total"],
            "in_transition": counts["in_transition"],
            "stage_as_of": latest.isoformat() if latest else None,
        }
        for family in _FAMILIES:
            row[f"{family}_count"] = counts[family]
            row[f"{family}_pct"] = round(100.0 * counts[family] / total, 2)
        # A simple, explainable health proxy: share of the sector that is
        # advancing rather than declining.
        row["stage_2_share_pct"] = row["stage_2_pct"]
        rows.append(row)

    rows.sort(key=lambda item: (-item["stage_2_pct"], item["sector_name"]))
    truncated = len(rows) > row_limit
    if truncated:
        rows = rows[:row_limit]

    notes: list[str] = []
    if not observations:
        notes.append(
            f"No governed stage observations exist for {exchange_code} at the "
            "requested date, so sector structure cannot be derived."
        )
    else:
        notes.append(
            "Sector structure is aggregated from governed weekly stage "
            "observations; it does not include rank-artifact sector RS or "
            "rotation quadrant, which are latest-only."
        )

    effective = max(
        (value for values in dates.values() for value in values), default=None
    )

    if as_of is None:
        status = AS_OF_LATEST
    elif rows:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    return envelope(
        rows,
        source=ctx.store_label(ctx.control_plane_db, _GOVERNED_TABLE),
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        date_fields=DATE_FIELDS,
        notes=notes,
        exchange=exchange_code,
        stage_vocabulary="WeinsteinStage",
        truncated=truncated,
        data_domain=ctx.paths.domain,
    )


def get_sector_constituents(
    ctx: McpContext,
    sector: str,
    *,
    exchange: str = "NSE",
    as_of: str | date | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """List the symbols in a sector with their stage and rank at ``as_of``."""

    exchange_code = ctx.resolve_exchange(exchange)
    sector_name = str(sector or "").strip()
    cutoff = coerce_date(as_of)
    row_limit = clamp_limit(limit, default=200, maximum=500)

    if not sector_name:
        raise ValueError("sector is required")

    with ctx.control_plane() as conn:
        observations = _latest_observations(conn, exchange_code, cutoff)
        rank_rows: list[dict[str, Any]] = []
        if decisions.table_exists(conn, decisions.RANK_TABLE):
            try:
                rank_rows = decisions.latest_rows(
                    conn,
                    decisions.RANK_TABLE,
                    exchange=exchange_code,
                    as_of=cutoff,
                    limit=10_000,
                )
            except decisions.DecisionVersionUnavailable:
                rank_rows = []

    rank_by_symbol = {
        str(record.get("symbol_id", "")).upper(): record for record in rank_rows
    }

    # Governed observations carry sector membership point-in-time. The current
    # master may fill gaps only for a latest query; using it for ``as_of`` would
    # leak later listings, classifications, market caps and industries.
    stage_members = {
        str(record.get("symbol_id", "")).upper(): record
        for record in observations
        if str(record.get("sector_name") or "").lower() == sector_name.lower()
    }
    master_members = (
        {
            str(row["symbol_id"]).upper(): row
            for row in master.sector_members(ctx, sector_name, exchange=exchange_code)
        }
        if as_of is None
        else {}
    )

    rows: list[dict[str, Any]] = []
    for symbol in sorted(set(stage_members) | set(master_members)):
        observation = stage_members.get(symbol)
        stage = normalize_stage(
            observation.get("effective_stage") if observation else None
        )
        observed = coerce_date(observation.get("as_of")) if observation else None
        rank_row = rank_by_symbol.get(symbol, {})
        master_row = master_members.get(symbol, {})
        rows.append(
            {
                "symbol_id": symbol,
                "exchange": exchange_code,
                "symbol_name": json_safe(master_row.get("symbol_name")),
                "mcap": json_safe(master_row.get("mcap")),
                "industry": json_safe(master_row.get("industry")),
                "stage_as_of": observed.isoformat() if observed else None,
                "stage_label": stage.value if observation else None,
                "stage_label_legacy": legacy_code_for(stage) if observation else None,
                "stage_family": stage_family(stage) if observation else None,
                "is_transition": is_transition(stage) if observation else None,
                "rank_position": json_safe(rank_row.get("rank_position")),
                "composite_score": json_safe(rank_row.get("composite_score")),
            }
        )

    rows.sort(
        key=lambda item: (
            item["rank_position"] is None,
            item["rank_position"] if item["rank_position"] is not None else 0,
            item["symbol_id"],
        )
    )
    truncated = len(rows) > row_limit
    if truncated:
        rows = rows[:row_limit]

    notes: list[str] = []
    if not rows:
        if as_of is None:
            known = master.list_sectors(ctx, exchange=exchange_code)
            notes.append(
                f"No constituents found for sector {sector_name!r} on "
                f"{exchange_code}. Known sectors: {known}."
            )
        else:
            notes.append(
                f"No governed constituents found for sector {sector_name!r} on "
                f"{exchange_code} at or before the requested date. The current "
                "symbol master was not used as a historical fallback."
            )

    effective = max(
        (coerce_date(row["stage_as_of"]) for row in rows if row.get("stage_as_of")),
        default=None,
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
            ctx.store_label(ctx.control_plane_db, _GOVERNED_TABLE)
            if as_of is not None
            else (
                f"control_plane.duckdb:{_GOVERNED_TABLE} + "
                f"masterdata.db:{master.TABLE}"
            )
        ),
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        date_fields=DATE_FIELDS,
        notes=notes,
        sector=sector_name,
        exchange=exchange_code,
        stage_vocabulary="WeinsteinStage",
        truncated=truncated,
        data_domain=ctx.paths.domain,
    )


__all__ = ["get_sector_constituents", "get_sector_overview"]
