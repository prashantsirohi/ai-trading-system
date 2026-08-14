"""Daily OHLCV candles with delivery, on an explicit price basis.

``_catalog.close`` is *unadjusted*. Every technical feature in the system is
computed on ``COALESCE(adjusted_*, raw)`` through the
``_catalog_feature_source`` view, so an agent that charts raw candles against
``sma_200`` from the feature store gets a broken picture across any split. This
tool therefore defaults to the adjusted basis and always reports which one it
returned.
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

# Table names are never parameterizable, so they come from trusted constants.
_ADJUSTED_TABLE = "_catalog_feature_source"
_RAW_TABLE = "_catalog"

DATE_FIELDS = ("date",)


def _effective_cutoff(
    to_date: str | date | None, as_of: str | date | None
) -> date | None:
    """The tighter of the display window end and the point-in-time cutoff."""

    bounds = [value for value in (coerce_date(to_date), coerce_date(as_of)) if value]
    return min(bounds) if bounds else None


def _coverage(
    ctx: McpContext, symbol: str, exchange: str
) -> dict[str, str | None]:
    """Full stored range for the symbol, independent of the query window.

    Lets an agent tell "nothing at that date" apart from "nothing ever".
    """

    with ctx.ohlcv() as conn:
        row = conn.execute(
            """
            SELECT MIN(CAST(timestamp AS DATE)), MAX(CAST(timestamp AS DATE))
            FROM _catalog
            WHERE symbol_id = ? AND exchange = ?
            """,
            [symbol, exchange],
        ).fetchone()
    first = coerce_date(row[0]) if row else None
    last = coerce_date(row[1]) if row else None
    return {
        "first": first.isoformat() if first else None,
        "last": last.isoformat() if last else None,
    }


def get_ohlcv(
    ctx: McpContext,
    symbol: str,
    *,
    exchange: str = "NSE",
    from_date: str | date | None = None,
    to_date: str | date | None = None,
    as_of: str | date | None = None,
    adjusted: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    """Return daily candles and delivery percentage for one symbol.

    ``adjusted`` selects the split/bonus-adjusted basis (the default, and the
    basis the feature store uses). ``as_of`` is a hard point-in-time cutoff;
    ``from_date``/``to_date`` are an ordinary display window. ``limit`` keeps
    the most recent rows while preserving ascending order.
    """

    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    row_limit = clamp_limit(limit)
    cutoff = _effective_cutoff(to_date, as_of)
    start = coerce_date(from_date)

    table = _ADJUSTED_TABLE if adjusted else _RAW_TABLE
    clauses = ["c.symbol_id = ?", "c.exchange = ?"]
    params: list[Any] = [symbol_id, exchange_code]
    if start is not None:
        clauses.append("CAST(c.timestamp AS DATE) >= CAST(? AS DATE)")
        params.append(start.isoformat())
    if cutoff is not None:
        clauses.append("CAST(c.timestamp AS DATE) <= CAST(? AS DATE)")
        params.append(cutoff.isoformat())

    sql = f"""
        SELECT
            CAST(c.timestamp AS DATE) AS date,
            c.open, c.high, c.low, c.close, c.volume,
            d.delivery_pct
        FROM {table} c
        LEFT JOIN _delivery d
          ON d.symbol_id = c.symbol_id
         AND d.exchange = c.exchange
         AND d.timestamp = CAST(c.timestamp AS DATE)
        WHERE {' AND '.join(clauses)}
        ORDER BY c.timestamp
    """

    with ctx.ohlcv(adjusted_view=adjusted) as conn:
        rows = conn.execute(sql, params).fetchall()

    candles = [
        {
            "date": json_safe(row[0]),
            "open": json_safe(row[1]),
            "high": json_safe(row[2]),
            "low": json_safe(row[3]),
            "close": json_safe(row[4]),
            "volume": json_safe(row[5]),
            "delivery_pct": json_safe(row[6]),
        }
        for row in rows
    ]

    truncated = len(candles) > row_limit
    if truncated:
        candles = candles[-row_limit:]

    notes: list[str] = []
    if truncated:
        notes.append(
            f"Truncated to the most recent {row_limit} sessions; raise 'limit' "
            "or narrow the date window for more."
        )

    if as_of is None:
        status = AS_OF_LATEST
    elif candles:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    effective = candles[-1]["date"] if candles else None
    coverage = _coverage(ctx, symbol_id, exchange_code)
    if not candles and coverage["last"] is None:
        notes.append(
            f"No OHLCV is stored for {symbol_id} on {exchange_code}; check the "
            "exchange or resolve the symbol first."
        )

    return envelope(
        candles,
        source=ctx.store_label(ctx.ohlcv_db, table),
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        date_fields=DATE_FIELDS,
        notes=notes,
        symbol=symbol_id,
        exchange=exchange_code,
        price_basis="adjusted" if adjusted else "raw",
        data_domain=ctx.paths.domain,
        truncated=truncated,
        coverage=coverage,
    )


__all__ = ["get_ohlcv"]
