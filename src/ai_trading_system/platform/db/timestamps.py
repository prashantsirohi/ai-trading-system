"""Timestamp helpers for DuckDB control-plane tables.

The control-plane schema stores naive ``TIMESTAMP`` values. Treat those values
as UTC everywhere so duration and freshness math is independent of the host
timezone.
"""

from __future__ import annotations

from datetime import datetime, timezone


DUCKDB_UTC_NOW = "(current_timestamp AT TIME ZONE 'UTC')"


def utc_naive_now() -> datetime:
    """Return current UTC time as a timezone-naive ``datetime``."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_naive_now_string() -> str:
    """String form accepted by DuckDB ``CAST(? AS TIMESTAMP)``."""
    return utc_naive_now().strftime("%Y-%m-%d %H:%M:%S")
