"""Ranking position, factor breakdown, and rank history.

Reads ``control_plane.duckdb::rank_history`` rather than the
``ranked_signals.csv`` artifact. The history table only carries rows from
completed stage attempts, which sidesteps the "is this artifact promoted?"
question entirely, and it is version-pinned so scores from different rank model
versions are never mixed into one answer.
"""

from __future__ import annotations

from datetime import date
from typing import Any

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
from ai_trading_system.interfaces.mcp.readers import decisions

DATE_FIELDS = ("trade_date",)

# Columns grouped into the factor block, in the order the ranker applies them.
_FACTOR_COLUMNS = (
    "rs_score",
    "volume_score",
    "trend_score",
    "proximity_score",
    "sector_score",
    "momentum_score",
    "delivery_score",
)

_IDENTITY_COLUMNS = ("symbol_id", "exchange", "trade_date", "universe_id")

_POSITION_COLUMNS = (
    "rank_position",
    "rank_percentile",
    "composite_score",
    "composite_score_adjusted",
)

_PROVENANCE_COLUMNS = (
    "rank_model_version",
    "rank_formula_name",
    "rank_config_hash",
    "pipeline_run_id",
)


def _normalize_dates(row: dict[str, Any]) -> dict[str, Any]:
    for column in ("trade_date",):
        if column in row:
            observed = coerce_date(row[column])
            row[column] = observed.isoformat() if observed else None
    return row


def _split_blocks(record: dict[str, Any]) -> dict[str, Any]:
    """Group a raw rank row into identity / position / factors / provenance."""

    safe = {key: json_safe(value) for key, value in record.items()}
    _normalize_dates(safe)

    grouped: dict[str, Any] = {
        "identity": {k: safe[k] for k in _IDENTITY_COLUMNS if k in safe},
        "position": {k: safe[k] for k in _POSITION_COLUMNS if k in safe},
        "factors": {k: safe[k] for k in _FACTOR_COLUMNS if k in safe},
        "provenance": {k: safe[k] for k in _PROVENANCE_COLUMNS if k in safe},
    }
    accounted = set().union(*(set(block) for block in grouped.values()))
    # Anything the schema gains later still reaches the agent.
    grouped["other"] = {k: v for k, v in safe.items() if k not in accounted}
    return grouped


def get_rank_detail(
    ctx: McpContext,
    symbol: str,
    *,
    exchange: str = "NSE",
    as_of: str | date | None = None,
) -> dict[str, Any]:
    """Return the newest ranked row for a symbol at or before ``as_of``."""

    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)

    notes: list[str] = []
    record: dict[str, Any] | None = None
    coverage: dict[str, str | None] = {"first": None, "last": None}

    with ctx.control_plane() as conn:
        if decisions.table_exists(conn, decisions.RANK_TABLE):
            bounds = conn.execute(
                f"SELECT MIN(trade_date), MAX(trade_date) FROM {decisions.RANK_TABLE} "
                "WHERE UPPER(symbol_id) = ? AND exchange = ?",
                [symbol_id, exchange_code],
            ).fetchone()
            first = coerce_date(bounds[0]) if bounds else None
            last = coerce_date(bounds[1]) if bounds else None
            coverage = {
                "first": first.isoformat() if first else None,
                "last": last.isoformat() if last else None,
            }
            try:
                record = decisions.latest_row(
                    conn,
                    decisions.RANK_TABLE,
                    symbol_id=symbol_id,
                    exchange=exchange_code,
                    as_of=as_of,
                )
            except decisions.DecisionVersionUnavailable as exc:
                notes.append(
                    f"Rank history could not be version-pinned: {exc} Returning "
                    "no row rather than mixing rank model versions."
                )

    data = _split_blocks(record) if record else None
    effective = data["identity"].get("trade_date") if data else None

    if as_of is None:
        status = AS_OF_LATEST
    elif data:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    if data is None and not notes:
        if coverage["last"] is None:
            notes.append(
                f"{symbol_id} ({exchange_code}) has never been ranked in "
                "rank_history."
            )
        else:
            notes.append(
                f"Rank history for {symbol_id} covers {coverage['first']} to "
                f"{coverage['last']}, which does not include the requested date."
            )

    return envelope(
        data,
        source=ctx.store_label(ctx.control_plane_db, decisions.RANK_TABLE),
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        notes=notes,
        symbol=symbol_id,
        exchange=exchange_code,
        coverage=coverage,
        data_domain=ctx.paths.domain,
    )


def get_rank_history(
    ctx: McpContext,
    symbol: str,
    *,
    exchange: str = "NSE",
    from_date: str | date | None = None,
    to_date: str | date | None = None,
    as_of: str | date | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Return the rank position series for one symbol, oldest first."""

    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    row_limit = clamp_limit(limit)

    notes: list[str] = []
    records: list[dict[str, Any]] = []

    with ctx.control_plane() as conn:
        if decisions.table_exists(conn, decisions.RANK_TABLE):
            try:
                records = decisions.history_rows(
                    conn,
                    decisions.RANK_TABLE,
                    symbol_id=symbol_id,
                    exchange=exchange_code,
                    from_date=from_date,
                    to_date=to_date,
                    as_of=as_of,
                    limit=row_limit,
                )
            except decisions.DecisionVersionUnavailable as exc:
                notes.append(
                    f"Rank history could not be version-pinned: {exc} Returning "
                    "no rows rather than mixing rank model versions."
                )

    keep = (
        *_IDENTITY_COLUMNS,
        *_POSITION_COLUMNS,
        *_FACTOR_COLUMNS,
        *_PROVENANCE_COLUMNS,
    )
    rows = [
        _normalize_dates(
            {key: json_safe(value) for key, value in record.items() if key in keep}
        )
        for record in records
    ]

    if as_of is None:
        status = AS_OF_LATEST
    elif rows:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    if not rows and not notes:
        notes.append(
            f"No rank history for {symbol_id} ({exchange_code}) in the requested "
            "window."
        )

    return envelope(
        rows,
        source=ctx.store_label(ctx.control_plane_db, decisions.RANK_TABLE),
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=rows[-1]["trade_date"] if rows else None,
        date_fields=DATE_FIELDS,
        notes=notes,
        symbol=symbol_id,
        exchange=exchange_code,
        data_domain=ctx.paths.domain,
    )


__all__ = ["get_rank_detail", "get_rank_history"]
