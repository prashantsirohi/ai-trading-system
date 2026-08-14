"""Fundamental evidence, cut off on publication date rather than fiscal period.

A quarter ending 2025-12-31 is not knowable on 2026-01-05. Every block below is
therefore filtered on the date the figure became *available*, not the period it
describes:

``financials``  ``screener_financials.available_at`` — bitemporal, part of the
    primary key.
``growth``      ``company_growth_features.available_at``.
``valuation``   ``screener_market_valuation.date`` — a price date, so it is
    already point-in-time.
``scores``      ``fundamental_scores.snapshot_date`` — the export date, used as
    a publication proxy and declared as such in ``meta.as_of_basis``.
``snapshot``    ``fundamental_snapshot.snapshot_date``, same basis.

Standalone and consolidated rows live under separate keys and are never
blended; the basis actually used is echoed back.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext, StoreUnavailableError
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
    coerce_date,
    envelope,
    json_safe,
)
from ai_trading_system.interfaces.mcp.readers import screener

SCORES_TABLE = "fundamental_scores"
SNAPSHOT_TABLE = "fundamental_snapshot"
GROWTH_TABLE = "company_growth_features"

_AS_OF_BASIS = {
    "financials": "screener_financials.available_at",
    "growth": "company_growth_features.available_at",
    "valuation": "screener_market_valuation.date",
    "scores": "fundamental_scores.snapshot_date (export date)",
    "snapshot": "fundamental_snapshot.snapshot_date (export date)",
}


def _table_exists(conn: Any, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table]
        ).fetchone()
        is not None
    )


def _latest_by_snapshot(
    conn: Any, table: str, symbol_id: str, cutoff: date | None
) -> dict[str, Any] | None:
    """Newest row from a snapshot-dated analytical table."""

    if not _table_exists(conn, table):
        return None
    clauses = ["UPPER(symbol) = ?"]
    params: list[Any] = [symbol_id]
    if cutoff is not None:
        # snapshot_date is DATE in some stores and VARCHAR in others, so the
        # column is cast as well as the parameter.
        clauses.append("CAST(snapshot_date AS DATE) <= CAST(? AS DATE)")
        params.append(cutoff.isoformat())
    frame = conn.execute(
        f"SELECT * FROM {table} WHERE {' AND '.join(clauses)} "
        "ORDER BY CAST(snapshot_date AS DATE) DESC LIMIT 1",
        params,
    ).fetchdf()
    if frame.empty:
        return None
    record = frame.to_dict(orient="records")[0]
    safe = {key: json_safe(value) for key, value in record.items()}
    observed = coerce_date(record.get("snapshot_date"))
    safe["snapshot_date"] = observed.isoformat() if observed else None
    return safe


def _growth_rows(
    conn: Any, symbol_id: str, basis: str, cutoff: date | None, limit: int = 12
) -> list[dict[str, Any]]:
    if not _table_exists(conn, GROWTH_TABLE):
        return []
    clauses = ["UPPER(symbol) = ?"]
    params: list[Any] = [symbol_id]

    columns = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [GROWTH_TABLE],
        ).fetchall()
    }
    if "statement_basis" in columns:
        clauses.append("statement_basis = ?")
        params.append(basis)
    if cutoff is not None and "available_at" in columns:
        clauses.append("CAST(available_at AS DATE) <= CAST(? AS DATE)")
        params.append(cutoff.isoformat())

    frame = conn.execute(
        f"SELECT * FROM {GROWTH_TABLE} WHERE {' AND '.join(clauses)} "
        "ORDER BY CAST(report_date AS DATE) DESC LIMIT ?",
        [*params, limit],
    ).fetchdf()

    rows: list[dict[str, Any]] = []
    for record in reversed(frame.to_dict(orient="records")):
        safe = {key: json_safe(value) for key, value in record.items()}
        for column in ("report_date", "available_at"):
            if column in safe:
                observed = coerce_date(record.get(column))
                safe[column] = observed.isoformat() if observed else None
        rows.append(safe)
    return rows


def get_fundamentals(
    ctx: McpContext,
    symbol: str,
    *,
    statement_basis: str = screener.DEFAULT_STATEMENT_BASIS,
    as_of: str | date | None = None,
) -> dict[str, Any]:
    """Return the five fundamental blocks for one symbol."""

    symbol_id = ctx.normalize_symbol(symbol)
    basis = screener.normalize_statement_basis(statement_basis)
    cutoff = coerce_date(as_of)

    notes: list[str] = []

    financial_rows = screener.financials(
        ctx, symbol_id, statement_basis=basis, as_of=cutoff
    )
    valuation = screener.market_valuation(
        ctx, symbol_id, statement_basis=basis, as_of=cutoff
    )
    company = screener.company_snapshot(ctx, symbol_id, as_of=cutoff)

    scores: dict[str, Any] | None = None
    snapshot: dict[str, Any] | None = None
    growth: list[dict[str, Any]] = []
    try:
        with ctx.fundamentals() as conn:
            scores = _latest_by_snapshot(conn, SCORES_TABLE, symbol_id, cutoff)
            snapshot = _latest_by_snapshot(conn, SNAPSHOT_TABLE, symbol_id, cutoff)
            growth = _growth_rows(conn, symbol_id, basis, cutoff)
    except StoreUnavailableError:
        notes.append(
            "fundamentals.duckdb is not present, so the scores, snapshot and "
            "growth blocks are unavailable."
        )

    available = screener.available_bases(ctx, symbol_id)
    if available and basis not in available:
        notes.append(
            f"No {basis} rows are stored for {symbol_id}; available bases: "
            f"{available}. Standalone and consolidated are never blended."
        )

    blocks: dict[str, Any] = {
        "company": company,
        "scores": scores,
        "snapshot": snapshot,
        "growth": growth,
        "financials": financial_rows,
        "valuation": valuation,
    }

    observed_dates = [
        value
        for value in (
            coerce_date(company.get("as_of_date")) if company else None,
            coerce_date(scores.get("snapshot_date")) if scores else None,
            coerce_date(snapshot.get("snapshot_date")) if snapshot else None,
            coerce_date(growth[-1].get("available_at")) if growth else None,
            coerce_date(financial_rows[-1].get("available_at"))
            if financial_rows
            else None,
            coerce_date(valuation.get("date")) if valuation else None,
        )
        if value is not None
    ]
    effective = max(observed_dates) if observed_dates else None
    populated = any(
        value for value in blocks.values() if value not in (None, [], {})
    )

    if as_of is None:
        status = AS_OF_LATEST
    elif populated:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    if not populated:
        notes.append(
            f"No fundamental evidence was published for {symbol_id} at or "
            "before the requested date."
        )

    return envelope(
        blocks,
        source=(
            f"{ctx.screener_db.name}:screener_financials + "
            f"{ctx.fundamentals_db.name}:{SCORES_TABLE}/{SNAPSHOT_TABLE}/{GROWTH_TABLE}"
        ),
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        notes=notes,
        symbol=symbol_id,
        statement_basis=basis,
        available_statement_bases=available,
        as_of_basis=_AS_OF_BASIS,
        data_domain=ctx.paths.domain,
    )


__all__ = ["GROWTH_TABLE", "SCORES_TABLE", "SNAPSHOT_TABLE", "get_fundamentals"]
