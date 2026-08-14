"""Read-only symbol master access.

Replaces ``SymbolMaster.from_masterdb`` and the SQLite reads inside
``stock_detail``/``sector_detail`` for MCP purposes. Two reasons:

* those helpers open read-write SQLite handles (invariant I1); and
* ``SymbolRecord`` carries no ``exchange``, ``security_id`` or ``mcap``, so it
  cannot distinguish a dual listing — which is exactly what symbol resolution
  has to get right.

``masterdata.py::search_symbols`` and ``get_symbols_by_sector`` are also
avoided: they build SQL by f-string interpolation, so agent input must never
reach them.
"""

from __future__ import annotations

import sqlite3
from contextlib import ExitStack
from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext, StoreUnavailableError

TABLE = "symbols"

# Ordered best-first. Resolution stops at the first tier that matches.
MATCH_EXACT_SYMBOL = "exact_symbol"
MATCH_EXACT_ISIN = "exact_isin"
MATCH_EXACT_SECURITY_ID = "exact_security_id"
MATCH_PREFIX = "prefix"
MATCH_NAME_CONTAINS = "name_contains"

_COLUMNS = (
    "symbol_id",
    "security_id",
    "symbol_name",
    "exchange",
    "instrument_type",
    "isin",
    "sector",
    "industry",
    "nse_symbol",
    "bse_symbol",
    "mcap",
)


def _available_columns(conn: sqlite3.Connection) -> list[str]:
    """Intersect the desired columns with the live schema.

    Older master schemas and fixtures omit optional columns; selecting a
    missing one would fail the whole read.
    """

    present = {row[1] for row in conn.execute(f"PRAGMA table_info({TABLE})")}
    return [column for column in _COLUMNS if column in present]


def _row_to_dict(row: sqlite3.Row, columns: list[str]) -> dict[str, Any]:
    return {column: row[column] for column in columns}


def search_symbols(
    ctx: McpContext, query: str, *, limit: int = 25
) -> list[dict[str, Any]]:
    """Return candidate master rows for ``query``, best match tier first.

    Ambiguity is preserved rather than resolved: a dual listing yields one row
    per exchange, and the caller decides.
    """

    text = str(query or "").strip()
    if not text:
        return []
    upper = text.upper()

    stack = ExitStack()
    try:
        conn = stack.enter_context(ctx.sqlite(ctx.master_db))
    except StoreUnavailableError:
        stack.close()
        return []

    with stack:
        columns = _available_columns(conn)
        if not columns:
            return []
        projection = ", ".join(columns)

        tiers: list[tuple[str, str, list[Any]]] = [
            (
                MATCH_EXACT_SYMBOL,
                f"SELECT {projection} FROM {TABLE} WHERE UPPER(symbol_id) = ?",
                [upper],
            )
        ]
        if "isin" in columns:
            tiers.append(
                (
                    MATCH_EXACT_ISIN,
                    f"SELECT {projection} FROM {TABLE} WHERE UPPER(isin) = ?",
                    [upper],
                )
            )
        if "security_id" in columns:
            tiers.append(
                (
                    MATCH_EXACT_SECURITY_ID,
                    f"SELECT {projection} FROM {TABLE} WHERE UPPER(security_id) = ?",
                    [upper],
                )
            )
        tiers.append(
            (
                MATCH_PREFIX,
                f"SELECT {projection} FROM {TABLE} WHERE UPPER(symbol_id) LIKE ? "
                "ORDER BY symbol_id LIMIT ?",
                [f"{upper}%", int(limit)],
            )
        )
        if "symbol_name" in columns:
            tiers.append(
                (
                    MATCH_NAME_CONTAINS,
                    f"SELECT {projection} FROM {TABLE} WHERE UPPER(symbol_name) LIKE ? "
                    "ORDER BY symbol_id LIMIT ?",
                    [f"%{upper}%", int(limit)],
                )
            )

        for match_type, sql, params in tiers:
            rows = conn.execute(sql, params).fetchall()
            if rows:
                return [
                    {**_row_to_dict(row, columns), "match_type": match_type}
                    for row in rows[: int(limit)]
                ]
    return []


def get_symbol_record(
    ctx: McpContext, symbol: str, exchange: str
) -> dict[str, Any] | None:
    """Return the master row for an exact ``(symbol_id, exchange)`` pair."""

    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    stack = ExitStack()
    try:
        conn = stack.enter_context(ctx.sqlite(ctx.master_db))
    except StoreUnavailableError:
        stack.close()
        return None

    with stack:
        columns = _available_columns(conn)
        if not columns:
            return None
        projection = ", ".join(columns)
        if "exchange" in columns:
            row = conn.execute(
                f"SELECT {projection} FROM {TABLE} "
                "WHERE UPPER(symbol_id) = ? AND UPPER(exchange) = ?",
                [symbol_id, exchange_code],
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT {projection} FROM {TABLE} WHERE UPPER(symbol_id) = ?",
                [symbol_id],
            ).fetchone()
        return _row_to_dict(row, columns) if row else None


def sector_members(
    ctx: McpContext, sector: str, *, exchange: str = "NSE", limit: int = 2000
) -> list[dict[str, Any]]:
    """Return master rows whose sector matches ``sector`` (case-insensitive)."""

    name = str(sector or "").strip()
    if not name:
        return []
    exchange_code = ctx.resolve_exchange(exchange)
    stack = ExitStack()
    try:
        conn = stack.enter_context(ctx.sqlite(ctx.master_db))
    except StoreUnavailableError:
        stack.close()
        return []

    with stack:
        columns = _available_columns(conn)
        if not columns or "sector" not in columns:
            return []
        projection = ", ".join(columns)
        clauses = ["LOWER(sector) = LOWER(?)"]
        params: list[Any] = [name]
        if "exchange" in columns:
            clauses.append("UPPER(exchange) = ?")
            params.append(exchange_code)
        rows = conn.execute(
            f"SELECT {projection} FROM {TABLE} WHERE {' AND '.join(clauses)} "
            "ORDER BY symbol_id LIMIT ?",
            [*params, int(limit)],
        ).fetchall()
        return [_row_to_dict(row, columns) for row in rows]


def sector_by_symbol(ctx: McpContext, *, exchange: str = "NSE") -> dict[str, str]:
    """Map ``symbol_id`` to sector name for one exchange."""

    exchange_code = ctx.resolve_exchange(exchange)
    stack = ExitStack()
    try:
        conn = stack.enter_context(ctx.sqlite(ctx.master_db))
    except StoreUnavailableError:
        stack.close()
        return {}

    with stack:
        columns = _available_columns(conn)
        if "sector" not in columns:
            return {}
        if "exchange" in columns:
            rows = conn.execute(
                f"SELECT symbol_id, sector FROM {TABLE} "
                "WHERE UPPER(exchange) = ? AND sector IS NOT NULL AND TRIM(sector) <> ''",
                [exchange_code],
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT symbol_id, sector FROM {TABLE} "
                "WHERE sector IS NOT NULL AND TRIM(sector) <> ''"
            ).fetchall()
        return {str(row["symbol_id"]).upper(): str(row["sector"]) for row in rows}


def list_sectors(ctx: McpContext, *, exchange: str = "NSE") -> list[str]:
    """Distinct sector names present in the master for one exchange."""

    return sorted({sector for sector in sector_by_symbol(ctx, exchange=exchange).values()})


__all__ = [
    "MATCH_EXACT_ISIN",
    "MATCH_EXACT_SECURITY_ID",
    "MATCH_EXACT_SYMBOL",
    "MATCH_NAME_CONTAINS",
    "MATCH_PREFIX",
    "get_symbol_record",
    "list_sectors",
    "search_symbols",
    "sector_by_symbol",
    "sector_members",
]
