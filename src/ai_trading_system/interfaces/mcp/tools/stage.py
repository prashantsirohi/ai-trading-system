"""Weinstein stage history across the three stores that hold it.

The system keeps stage state in three places with materially different
coverage, grain, and spelling:

``weekly_governed``  ``control_plane.duckdb::weekly_stock_stage_history`` —
    the Phase 3B governed weekly store, exchange-aware, canonical
    ``WeinsteinStage`` vocabulary including transition states. The default.
``weekly_legacy``    ``ohlcv.duckdb::weekly_stage_snapshot`` — the original
    weekly store. Carries no ``exchange`` column and in production its coverage
    stops well before the governed store begins, so reading it by default would
    report "no stage data" for recent dates.
``daily``            ``control_plane.duckdb::stage_history`` — daily,
    version-pinned, legacy ``S1..S4`` spelling.

Every row is emitted with both spellings so neither a legacy nor a canonical
filter silently misses rows, and ``meta`` always names the store it read plus
that store's coverage window.
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
from ai_trading_system.interfaces.mcp.readers import decisions

GRANULARITY_WEEKLY_GOVERNED = "weekly_governed"
GRANULARITY_WEEKLY_LEGACY = "weekly_legacy"
GRANULARITY_DAILY = "daily"

GRANULARITIES = (
    GRANULARITY_WEEKLY_GOVERNED,
    GRANULARITY_WEEKLY_LEGACY,
    GRANULARITY_DAILY,
)

_GOVERNED_TABLE = "weekly_stock_stage_history"
_LEGACY_TABLE = "weekly_stage_snapshot"

DATE_FIELDS = ("observation_date",)

# Stored as DATE; DuckDB hands them back through pandas as timestamps, so they
# are re-normalized to plain ISO dates for the wire.
_DATE_COLUMNS = (
    "observation_date",
    "source_week_start",
    "source_week_end",
    "stage_entry_date",
)


def _decorate(row: dict[str, Any], raw_stage: Any) -> dict[str, Any]:
    """Attach the four canonical stage fields and normalize date columns."""

    for column in _DATE_COLUMNS:
        if column in row:
            observed = coerce_date(row[column])
            row[column] = observed.isoformat() if observed else None

    stage = normalize_stage(raw_stage)
    row["stage_label"] = stage.value
    row["stage_label_legacy"] = legacy_code_for(stage)
    row["stage_family"] = stage_family(stage)
    row["is_transition"] = is_transition(stage)
    return row


def _bounds(
    to_date: str | date | None, as_of: str | date | None
) -> date | None:
    candidates = [value for value in (coerce_date(to_date), coerce_date(as_of)) if value]
    return min(candidates) if candidates else None


def _governed(
    ctx: McpContext,
    symbol_id: str,
    exchange: str,
    from_date: str | date | None,
    upper: date | None,
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, str | None]]:
    with ctx.control_plane() as conn:
        if not decisions.table_exists(conn, _GOVERNED_TABLE):
            return [], {"first": None, "last": None}

        coverage_row = conn.execute(
            f"SELECT MIN(as_of), MAX(as_of) FROM {_GOVERNED_TABLE} "
            "WHERE UPPER(symbol_id) = ? AND exchange = ?",
            [symbol_id, exchange],
        ).fetchone()

        clauses = ["UPPER(symbol_id) = ?", "exchange = ?"]
        params: list[Any] = [symbol_id, exchange]
        start = coerce_date(from_date)
        if start is not None:
            clauses.append("as_of >= CAST(? AS DATE)")
            params.append(start.isoformat())
        if upper is not None:
            clauses.append("as_of <= CAST(? AS DATE)")
            params.append(upper.isoformat())

        frame = conn.execute(
            f"""
            SELECT * FROM (
                SELECT
                    as_of AS observation_date,
                    source_week_start, source_week_end,
                    effective_stage, stage_status,
                    sector_id, sector_name,
                    classifier_version, run_id
                FROM {_GOVERNED_TABLE}
                WHERE {' AND '.join(clauses)}
                ORDER BY as_of DESC LIMIT ?
            ) ordered ORDER BY observation_date
            """,
            [*params, limit],
        ).fetchdf()

    rows = [
        _decorate(
            {key: json_safe(value) for key, value in record.items()},
            record.get("effective_stage"),
        )
        for record in frame.to_dict(orient="records")
    ]
    first = coerce_date(coverage_row[0]) if coverage_row else None
    last = coerce_date(coverage_row[1]) if coverage_row else None
    coverage = {
        "first": first.isoformat() if first else None,
        "last": last.isoformat() if last else None,
    }
    return rows, coverage


def _legacy(
    ctx: McpContext,
    symbol_id: str,
    from_date: str | date | None,
    upper: date | None,
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, str | None]]:
    with ctx.ohlcv() as conn:
        exists = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [_LEGACY_TABLE],
        ).fetchone()
        if not exists:
            return [], {"first": None, "last": None}

        coverage_row = conn.execute(
            f"SELECT MIN(week_end_date), MAX(week_end_date) FROM {_LEGACY_TABLE} "
            "WHERE UPPER(symbol) = ?",
            [symbol_id],
        ).fetchone()

        clauses = ["UPPER(symbol) = ?"]
        params: list[Any] = [symbol_id]
        start = coerce_date(from_date)
        if start is not None:
            clauses.append("week_end_date >= CAST(? AS DATE)")
            params.append(start.isoformat())
        if upper is not None:
            clauses.append("week_end_date <= CAST(? AS DATE)")
            params.append(upper.isoformat())

        frame = conn.execute(
            f"""
            SELECT * FROM (
                SELECT
                    week_end_date AS observation_date,
                    stage_label AS legacy_stage, stage_confidence,
                    stage_transition, bars_in_stage, stage_entry_date,
                    ma10w, ma30w, ma40w, ma30w_slope_4w,
                    weekly_rs_score, weekly_volume_ratio,
                    support_level, resistance_level, run_id
                FROM {_LEGACY_TABLE}
                WHERE {' AND '.join(clauses)}
                ORDER BY week_end_date DESC LIMIT ?
            ) ordered ORDER BY observation_date
            """,
            [*params, limit],
        ).fetchdf()

    rows = []
    for record in frame.to_dict(orient="records"):
        raw = record.pop("legacy_stage", None)
        rows.append(
            _decorate({key: json_safe(value) for key, value in record.items()}, raw)
        )
    first = coerce_date(coverage_row[0]) if coverage_row else None
    last = coerce_date(coverage_row[1]) if coverage_row else None
    coverage = {
        "first": first.isoformat() if first else None,
        "last": last.isoformat() if last else None,
    }
    return rows, coverage


def _daily(
    ctx: McpContext,
    symbol_id: str,
    exchange: str,
    from_date: str | date | None,
    upper: date | None,
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, str | None], list[str]]:
    notes: list[str] = []
    with ctx.control_plane() as conn:
        if not decisions.table_exists(conn, decisions.STAGE_TABLE):
            return [], {"first": None, "last": None}, notes

        coverage_row = conn.execute(
            f"SELECT MIN(trade_date), MAX(trade_date) FROM {decisions.STAGE_TABLE} "
            "WHERE UPPER(symbol_id) = ? AND exchange = ?",
            [symbol_id, exchange],
        ).fetchone()

        try:
            records = decisions.history_rows(
                conn,
                decisions.STAGE_TABLE,
                symbol_id=symbol_id,
                exchange=exchange,
                from_date=from_date,
                to_date=upper,
                limit=limit,
            )
        except decisions.DecisionVersionUnavailable as exc:
            records = []
            notes.append(
                f"Daily stage history could not be version-pinned: {exc} "
                "Returning no rows rather than mixing model versions."
            )

    rows = []
    for record in records:
        raw = record.get("stage_label")
        row = {key: json_safe(value) for key, value in record.items()}
        row["observation_date"] = row.pop("trade_date", None)
        rows.append(_decorate(row, raw))

    first = coerce_date(coverage_row[0]) if coverage_row else None
    last = coerce_date(coverage_row[1]) if coverage_row else None
    coverage = {
        "first": first.isoformat() if first else None,
        "last": last.isoformat() if last else None,
    }
    return rows, coverage, notes


def get_stage_history(
    ctx: McpContext,
    symbol: str,
    *,
    exchange: str = "NSE",
    granularity: str = GRANULARITY_WEEKLY_GOVERNED,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
    as_of: str | date | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Return stage observations for one symbol from the chosen store."""

    chosen = str(granularity or "").strip().lower()
    if chosen not in GRANULARITIES:
        raise ValueError(
            f"Unknown granularity: {granularity!r} (expected one of {list(GRANULARITIES)})"
        )

    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    row_limit = clamp_limit(limit)
    upper = _bounds(to_date, as_of)

    notes: list[str] = []
    if chosen == GRANULARITY_WEEKLY_GOVERNED:
        rows, coverage = _governed(
            ctx, symbol_id, exchange_code, from_date, upper, row_limit
        )
        source = ctx.store_label(ctx.control_plane_db, _GOVERNED_TABLE)
    elif chosen == GRANULARITY_WEEKLY_LEGACY:
        rows, coverage = _legacy(ctx, symbol_id, from_date, upper, row_limit)
        source = ctx.store_label(ctx.ohlcv_db, _LEGACY_TABLE)
        notes.append(
            "weekly_stage_snapshot has no exchange column, so rows are keyed by "
            "symbol alone; its coverage typically ends well before the governed "
            "store begins."
        )
    else:
        rows, coverage, daily_notes = _daily(
            ctx, symbol_id, exchange_code, from_date, upper, row_limit
        )
        source = ctx.store_label(ctx.control_plane_db, decisions.STAGE_TABLE)
        notes.extend(daily_notes)

    if as_of is None:
        status = AS_OF_LATEST
    elif rows:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    if not rows and coverage["last"] is None:
        notes.append(
            f"The {chosen} store holds no stage rows for {symbol_id} "
            f"({exchange_code}). Another granularity may have coverage."
        )
    elif not rows and coverage["last"] is not None:
        notes.append(
            f"The {chosen} store covers {coverage['first']} to {coverage['last']} "
            f"for {symbol_id}, which does not include the requested window."
        )

    return envelope(
        rows,
        source=source,
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=rows[-1]["observation_date"] if rows else None,
        date_fields=DATE_FIELDS,
        notes=notes,
        symbol=symbol_id,
        exchange=exchange_code,
        granularity=chosen,
        stage_vocabulary="WeinsteinStage",
        coverage=coverage,
        data_domain=ctx.paths.domain,
    )


__all__ = [
    "GRANULARITIES",
    "GRANULARITY_DAILY",
    "GRANULARITY_WEEKLY_GOVERNED",
    "GRANULARITY_WEEKLY_LEGACY",
    "get_stage_history",
]
