"""Canonical point-in-time reads over governed weekly stock stage history."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any, Sequence

from ai_trading_system.domains.opportunities.stage_governance import (
    resolve_stage_observation_payloads,
)
from ai_trading_system.interfaces.mcp.envelope import coerce_date

HISTORY_TABLE = "weekly_stock_stage_history"
GOVERNANCE_TABLE = "stage_observation_governance"


def _table_exists(conn: Any, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table]
        ).fetchone()
        is not None
    )


def available(conn: Any) -> bool:
    """Whether the store can resolve corrections rather than reading raw rows."""

    return _table_exists(conn, HISTORY_TABLE) and _table_exists(conn, GOVERNANCE_TABLE)


def availability_cutoff(cutoff: date | None) -> datetime:
    """End-of-day UTC for a date cutoff, or current UTC for a latest query."""

    if cutoff is None:
        return datetime.now(timezone.utc)
    return datetime.combine(cutoff, time.max, tzinfo=timezone.utc)


def snapshot(
    conn: Any,
    *,
    exchange: str,
    cutoff: date | None,
    symbols: Sequence[str] | None = None,
    available_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Resolve one canonical terminal observation per symbol."""

    if not available(conn):
        return []
    effective = cutoff or datetime.now(timezone.utc).date()
    clauses = ["exchange = ?", "as_of <= CAST(? AS TIMESTAMP)"]
    params: list[Any] = [exchange, effective.isoformat()]
    normalized = [str(symbol).strip().upper() for symbol in symbols or ()]
    if normalized:
        placeholders = ",".join("?" for _ in normalized)
        clauses.append(f"symbol_id IN ({placeholders})")
        params.extend(normalized)
    return resolve_stage_observation_payloads(
        conn,
        scope="STOCK",
        table=HISTORY_TABLE,
        as_of=effective.isoformat(),
        available_at=available_at or availability_cutoff(cutoff),
        entity_columns=("exchange", "symbol_id"),
        clauses=clauses,
        params=params,
    )


def history(
    conn: Any,
    *,
    symbol_id: str,
    exchange: str,
    from_date: str | date | None,
    through: date | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Resolve canonical weekly history as known by the query cutoff."""

    if not available(conn):
        return []
    availability = availability_cutoff(through)
    clauses = ["UPPER(symbol_id) = ?", "exchange = ?", "created_at <= ?"]
    params: list[Any] = [symbol_id, exchange, availability.replace(tzinfo=None)]
    start = coerce_date(from_date)
    if start is not None:
        clauses.append("CAST(as_of AS DATE) >= CAST(? AS DATE)")
        params.append(start.isoformat())
    if through is not None:
        clauses.append("CAST(as_of AS DATE) <= CAST(? AS DATE)")
        params.append(through.isoformat())

    observed_rows = conn.execute(
        f"SELECT DISTINCT CAST(as_of AS DATE) AS observed_on FROM {HISTORY_TABLE} "
        f"WHERE {' AND '.join(clauses)} ORDER BY observed_on DESC LIMIT ?",
        [*params, int(limit)],
    ).fetchall()

    output: list[dict[str, Any]] = []
    for (value,) in reversed(observed_rows):
        observed_on = coerce_date(value)
        if observed_on is None:
            continue
        rows = snapshot(
            conn,
            exchange=exchange,
            cutoff=observed_on,
            symbols=[symbol_id],
            available_at=availability,
        )
        if rows:
            output.append(rows[0])
    return output


__all__ = [
    "GOVERNANCE_TABLE",
    "HISTORY_TABLE",
    "availability_cutoff",
    "available",
    "history",
    "snapshot",
]
