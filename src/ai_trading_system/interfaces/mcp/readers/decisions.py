"""Point-in-time reads over the versioned decision-history tables.

``rank_history``, ``stage_history``, ``stage1_history`` and ``pattern_history``
in ``control_plane.duckdb`` are all ``trade_date``-keyed and pinned to an
approved ``(model_version, config_hash)`` recorded in
``decision_model_deployment``. Mixing model versions in one answer would
compare incomparable scores, so every read here resolves the approved version
for the requested date first.

``decision_reads.py`` already implements this pinning, and its connections are
read-only, so it is reused directly wherever its semantics fit —
``RankHistoryReadRepository.history`` returns an ascending date range, which is
exactly a history query. It is *not* used for "the single latest row at or
before a date": that method orders ascending and applies ``LIMIT``, so on a
symbol with a long history it would return the oldest rows rather than the
newest. Those reads use the pinned ``QUALIFY`` query below instead.
"""

from __future__ import annotations

from datetime import date
from typing import Any, NamedTuple, Sequence

import duckdb

from ai_trading_system.interfaces.mcp.envelope import coerce_date

# Trusted internal constants: table names cannot be parameterized.
RANK_TABLE = "rank_history"
RANK_UNIVERSE_TABLE = "rank_universe_history"
STAGE_TABLE = "stage_history"
STAGE1_TABLE = "stage1_history"
PATTERN_TABLE = "pattern_history"

_VERSION_COLUMNS: dict[str, tuple[str, str, str]] = {
    # table: (decision_domain, version_column, config_column)
    RANK_TABLE: ("rank", "rank_model_version", "rank_config_hash"),
    RANK_UNIVERSE_TABLE: ("rank", "rank_model_version", "rank_config_hash"),
    STAGE_TABLE: ("stage", "stage_model_version", "stage_config_hash"),
    STAGE1_TABLE: ("stage1", "stage1_model_version", "stage1_config_hash"),
    PATTERN_TABLE: ("pattern", "pattern_model_version", "pattern_config_hash"),
}


class DecisionVersion(NamedTuple):
    model_version: str
    config_hash: str


class DecisionVersionUnavailable(RuntimeError):
    """No single approved model version covers the requested date."""


def table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()
    return row is not None


def latest_trade_date(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    as_of: str | date | None = None,
    date_column: str = "trade_date",
) -> date | None:
    """Newest ``date_column`` in ``table`` at or before ``as_of``."""

    if table not in _VERSION_COLUMNS and table != STAGE_TABLE:
        raise ValueError(f"Untrusted table name: {table!r}")
    cutoff = coerce_date(as_of)
    if cutoff is None:
        row = conn.execute(f"SELECT MAX({date_column}) FROM {table}").fetchone()
    else:
        row = conn.execute(
            f"SELECT MAX({date_column}) FROM {table} "
            f"WHERE {date_column} <= CAST(? AS DATE)",
            [cutoff.isoformat()],
        ).fetchone()
    return coerce_date(row[0]) if row else None


def approved_version(
    conn: duckdb.DuckDBPyConnection, table: str, as_of: str | date
) -> DecisionVersion:
    """Resolve the production-approved model version effective at ``as_of``."""

    if table not in _VERSION_COLUMNS:
        raise ValueError(f"Untrusted table name: {table!r}")
    domain, _, _ = _VERSION_COLUMNS[table]

    if not table_exists(conn, "decision_model_deployment"):
        raise DecisionVersionUnavailable(
            "decision_model_deployment is unavailable; decision history cannot "
            "be version-pinned."
        )

    cutoff = coerce_date(as_of)
    assert cutoff is not None
    rows = conn.execute(
        """
        SELECT model_version, config_hash
        FROM decision_model_deployment
        WHERE decision_domain = ?
          AND environment = 'production'
          AND status = 'approved'
          AND effective_from <= CAST(? AS DATE)
          AND (effective_to IS NULL OR effective_to >= CAST(? AS DATE))
        QUALIFY effective_from = MAX(effective_from) OVER ()
        """,
        [domain, cutoff.isoformat(), cutoff.isoformat()],
    ).fetchall()
    if len(rows) != 1:
        raise DecisionVersionUnavailable(
            f"Expected exactly one approved {domain} model version for "
            f"{cutoff.isoformat()}; found {len(rows)}."
        )
    return DecisionVersion(str(rows[0][0]), str(rows[0][1]))


def latest_row(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    symbol_id: str,
    exchange: str,
    as_of: str | date | None = None,
    extra_clauses: Sequence[str] = (),
    extra_params: Sequence[Any] = (),
) -> dict[str, Any] | None:
    """The newest version-pinned row for one symbol at or before ``as_of``.

    The effective date is resolved **per symbol**, not from the table's global
    maximum. The ranked universe is a rotating top-N cross-section, so a symbol
    that was ranked yesterday but not today is ordinary; pinning to the global
    latest date would report it as never ranked. ``as_of_effective`` in the
    response then tells the caller how old the row actually is.
    """

    if table not in _VERSION_COLUMNS:
        raise ValueError(f"Untrusted table name: {table!r}")
    _, version_column, config_column = _VERSION_COLUMNS[table]

    if not table_exists(conn, table):
        return None

    cutoff = coerce_date(as_of)
    clauses = ["UPPER(symbol_id) = ?", "exchange = ?"]
    params: list[Any] = [symbol_id, exchange]
    if cutoff is not None:
        clauses.append("trade_date <= CAST(? AS DATE)")
        params.append(cutoff.isoformat())
    clauses.extend(extra_clauses)
    params.extend(extra_params)

    effective_row = conn.execute(
        f"SELECT MAX(trade_date) FROM {table} WHERE {' AND '.join(clauses)}",
        params,
    ).fetchone()
    effective = coerce_date(effective_row[0]) if effective_row else None
    if effective is None:
        return None

    version = approved_version(conn, table, effective)

    frame = conn.execute(
        f"SELECT * FROM {table} "
        f"WHERE {' AND '.join(clauses)} "
        f"  AND trade_date = CAST(? AS DATE) "
        f"  AND {version_column} = ? AND {config_column} = ? "
        f"LIMIT 1",
        [*params, effective.isoformat(), version.model_version, version.config_hash],
    ).fetchdf()
    records = frame.to_dict(orient="records")
    return records[0] if records else None


def latest_rows(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    symbol_id: str | None = None,
    exchange: str | None = None,
    as_of: str | date | None = None,
    extra_clauses: Sequence[str] = (),
    extra_params: Sequence[Any] = (),
    limit: int = 2000,
) -> list[dict[str, Any]]:
    """Rows from the newest available ``trade_date`` at or before ``as_of``.

    Returns ``[]`` when the table is missing or holds nothing at that date, so
    a caller can distinguish "no data" from an error without catching.
    """

    if table not in _VERSION_COLUMNS:
        raise ValueError(f"Untrusted table name: {table!r}")
    _, version_column, config_column = _VERSION_COLUMNS[table]

    if not table_exists(conn, table):
        return []

    effective = latest_trade_date(conn, table, as_of=as_of)
    if effective is None:
        return []

    version = approved_version(conn, table, effective)

    clauses = [
        "trade_date = CAST(? AS DATE)",
        f"{version_column} = ?",
        f"{config_column} = ?",
    ]
    params: list[Any] = [
        effective.isoformat(),
        version.model_version,
        version.config_hash,
    ]
    if symbol_id:
        clauses.append("UPPER(symbol_id) = ?")
        params.append(symbol_id)
    if exchange:
        clauses.append("exchange = ?")
        params.append(exchange)
    clauses.extend(extra_clauses)
    params.extend(extra_params)

    frame = conn.execute(
        f"SELECT * FROM {table} WHERE {' AND '.join(clauses)} LIMIT ?",
        [*params, int(limit)],
    ).fetchdf()
    return frame.to_dict(orient="records")


def history_rows(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    symbol_id: str,
    exchange: str,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
    as_of: str | date | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Version-pinned rows for one symbol across a date range, oldest first.

    The upper bound is the tighter of ``to_date`` and ``as_of``; ``limit``
    keeps the most recent rows so a long history does not hide the present.
    """

    if table not in _VERSION_COLUMNS:
        raise ValueError(f"Untrusted table name: {table!r}")
    _, version_column, config_column = _VERSION_COLUMNS[table]

    if not table_exists(conn, table):
        return []

    upper_bounds = [value for value in (coerce_date(to_date), coerce_date(as_of)) if value]
    upper = min(upper_bounds) if upper_bounds else None

    effective = latest_trade_date(conn, table, as_of=upper)
    if effective is None:
        return []
    version = approved_version(conn, table, effective)

    clauses = [
        "UPPER(symbol_id) = ?",
        "exchange = ?",
        f"{version_column} = ?",
        f"{config_column} = ?",
    ]
    params: list[Any] = [
        symbol_id,
        exchange,
        version.model_version,
        version.config_hash,
    ]
    start = coerce_date(from_date)
    if start is not None:
        clauses.append("trade_date >= CAST(? AS DATE)")
        params.append(start.isoformat())
    if upper is not None:
        clauses.append("trade_date <= CAST(? AS DATE)")
        params.append(upper.isoformat())

    frame = conn.execute(
        f"SELECT * FROM ("
        f"  SELECT * FROM {table} WHERE {' AND '.join(clauses)} "
        f"  ORDER BY trade_date DESC LIMIT ?"
        f") ordered ORDER BY trade_date",
        [*params, int(limit)],
    ).fetchdf()
    return frame.to_dict(orient="records")


__all__ = [
    "PATTERN_TABLE",
    "RANK_TABLE",
    "RANK_UNIVERSE_TABLE",
    "STAGE1_TABLE",
    "STAGE_TABLE",
    "DecisionVersion",
    "DecisionVersionUnavailable",
    "approved_version",
    "history_rows",
    "latest_row",
    "latest_rows",
    "latest_trade_date",
    "table_exists",
]
