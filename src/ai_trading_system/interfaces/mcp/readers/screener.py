"""Read-only access to the Screener financials SQLite store.

``ScreenerFinancialsStore`` is not used here for two reasons:

* its constructor defaults to ``initialize=True``, which creates tables — a
  write to a live store — and ``connect()`` returns a read-write handle; and
* ``get_company_data`` takes the latest company snapshot and *every* financial
  and valuation row with no cutoff, so it cannot answer a historical question
  without leaking data published after the requested date.

This reader filters on ``available_at``, the publication timestamp that is part
of the ``screener_financials`` primary key. A fiscal period ending 2025-12-31
is not knowable on 2026-01-05, so ``report_date`` alone is never a valid cutoff.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext, StoreUnavailableError
from ai_trading_system.interfaces.mcp.envelope import coerce_date

DEFAULT_STATEMENT_BASIS = "standalone"
SUPPORTED_STATEMENT_BASES = ("standalone", "consolidated")


def normalize_statement_basis(value: str | None) -> str:
    """Validate the statement basis; standalone is the pipeline default."""

    basis = str(value or DEFAULT_STATEMENT_BASIS).strip().lower()
    if basis not in SUPPORTED_STATEMENT_BASES:
        raise ValueError(
            f"Unsupported statement_basis: {value!r} "
            f"(expected one of {list(SUPPORTED_STATEMENT_BASES)})"
        )
    return basis


def _table_exists(conn: Any, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", [table]
    ).fetchone()
    return row is not None


def company_snapshot(
    ctx: McpContext, symbol: str, *, as_of: str | date | None = None
) -> dict[str, Any] | None:
    """Latest company snapshot published at or before ``as_of``."""

    symbol_id = ctx.normalize_symbol(symbol)
    cutoff = coerce_date(as_of)
    try:
        store = ctx.sqlite(ctx.screener_db)
    except StoreUnavailableError:
        return None

    with store as conn:
        if not _table_exists(conn, "screener_company_snapshot"):
            return None
        clauses = ["symbol = ?"]
        params: list[Any] = [symbol_id]
        if cutoff is not None:
            clauses.append("as_of_date <= ?")
            params.append(cutoff.isoformat())
        row = conn.execute(
            "SELECT symbol, as_of_date, face_value, market_cap_cr, source "
            f"FROM screener_company_snapshot WHERE {' AND '.join(clauses)} "
            "ORDER BY as_of_date DESC LIMIT 1",
            params,
        ).fetchone()
        return dict(row) if row else None


def financials(
    ctx: McpContext,
    symbol: str,
    *,
    statement_basis: str = DEFAULT_STATEMENT_BASIS,
    as_of: str | date | None = None,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    """Financial line items published at or before ``as_of``.

    Rows carry both ``report_date`` (the fiscal period) and ``available_at``
    (when it became knowable). The cutoff applies to ``available_at``.
    """

    symbol_id = ctx.normalize_symbol(symbol)
    basis = normalize_statement_basis(statement_basis)
    cutoff = coerce_date(as_of)
    try:
        store = ctx.sqlite(ctx.screener_db)
    except StoreUnavailableError:
        return []

    with store as conn:
        if not _table_exists(conn, "screener_financials"):
            return []
        clauses = ["f.symbol = ?", "f.statement_basis = ?"]
        params: list[Any] = [symbol_id, basis]
        if cutoff is not None:
            clauses.append("f.available_at <= ?")
            params.append(cutoff.isoformat())

        joined = _table_exists(conn, "screener_metric_catalog")
        projection = (
            "f.metric_id, c.metric_name, c.statement_type, c.unit, "
            "f.period_type, f.report_date, f.available_at, f.value, f.statement_basis"
            if joined
            else "f.metric_id, f.metric_id AS metric_name, NULL AS statement_type, "
            "NULL AS unit, f.period_type, f.report_date, f.available_at, "
            "f.value, f.statement_basis"
        )
        join_sql = (
            "JOIN screener_metric_catalog c ON c.metric_id = f.metric_id"
            if joined
            else ""
        )
        rows = conn.execute(
            f"SELECT {projection} FROM screener_financials f {join_sql} "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY f.report_date, f.metric_id LIMIT ?",
            [*params, int(limit)],
        ).fetchall()
        return [dict(row) for row in rows]


def market_valuation(
    ctx: McpContext,
    symbol: str,
    *,
    statement_basis: str = DEFAULT_STATEMENT_BASIS,
    as_of: str | date | None = None,
) -> dict[str, Any] | None:
    """Latest market valuation row dated at or before ``as_of``."""

    symbol_id = ctx.normalize_symbol(symbol)
    basis = normalize_statement_basis(statement_basis)
    cutoff = coerce_date(as_of)
    try:
        store = ctx.sqlite(ctx.screener_db)
    except StoreUnavailableError:
        return None

    with store as conn:
        if not _table_exists(conn, "screener_market_valuation"):
            return None
        clauses = ["symbol = ?", "statement_basis = ?"]
        params: list[Any] = [symbol_id, basis]
        if cutoff is not None:
            clauses.append("date <= ?")
            params.append(cutoff.isoformat())
        row = conn.execute(
            "SELECT symbol, date, statement_basis, price, market_cap_cr, pe, pb, "
            "ev_ebitda, dividend_yield "
            f"FROM screener_market_valuation WHERE {' AND '.join(clauses)} "
            "ORDER BY date DESC LIMIT 1",
            params,
        ).fetchone()
        return dict(row) if row else None


def available_bases(ctx: McpContext, symbol: str) -> list[str]:
    """Statement bases actually stored for a symbol.

    Standalone and consolidated rows live under separate keys and must never be
    blended, so a caller can check which are present before choosing.
    """

    symbol_id = ctx.normalize_symbol(symbol)
    try:
        store = ctx.sqlite(ctx.screener_db)
    except StoreUnavailableError:
        return []

    with store as conn:
        if not _table_exists(conn, "screener_financials"):
            return []
        rows = conn.execute(
            "SELECT DISTINCT statement_basis FROM screener_financials WHERE symbol = ?",
            [symbol_id],
        ).fetchall()
        return sorted(str(row["statement_basis"]) for row in rows)


__all__ = [
    "DEFAULT_STATEMENT_BASIS",
    "SUPPORTED_STATEMENT_BASES",
    "available_bases",
    "company_snapshot",
    "financials",
    "market_valuation",
    "normalize_statement_basis",
]
