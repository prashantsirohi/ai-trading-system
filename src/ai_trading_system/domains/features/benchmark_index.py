"""Governed loader for the system benchmark index.

The system benchmark is the equal-weight liquid-1000 universe index. Its
levels are stored in ``ohlcv.duckdb::universe_index_daily`` under universe id
``UNIV_TOP1000_MCAP`` (the id names the top-1000-by-market-cap membership;
the weighting is carried by ``index_type``). ``_index_catalog`` does not
contain a matching code, so all consumers must load the benchmark through
this module rather than assuming a catalog table.

``benchmark_source`` strings are ``table:universe_id:index_type`` and are the
serialized policy form used by pattern-lane calibration manifests.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

BENCHMARK_SYMBOL = "UNIV_TOP1000_EW"
BENCHMARK_SOURCE = "universe_index_daily:UNIV_TOP1000_MCAP:equal_weight"


def load_benchmark_levels(
    ohlcv_db: Path | str,
    *,
    source: str = BENCHMARK_SOURCE,
    through_date: str | None = None,
) -> pd.DataFrame:
    """Return the benchmark daily series as ``date``/``close`` columns.

    Raises when the source table is missing or returns no rows — a silent
    empty benchmark must never propagate into outcome computation.
    """
    table, universe_id, index_type = source.split(":")
    conn = duckdb.connect(str(ohlcv_db), read_only=True)
    try:
        exists = bool(conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
        ).fetchone()[0])
        if not exists:
            raise RuntimeError(f"benchmark source table {table} is missing from {ohlcv_db}")
        clause = "AND date <= CAST(? AS DATE)" if through_date else ""
        params: list[object] = [universe_id, index_type]
        if through_date:
            params.append(through_date)
        frame = conn.execute(
            f"""
            SELECT CAST(date AS DATE) AS date, level AS close
            FROM {table}
            WHERE universe_id = ? AND index_type = ? {clause}
            ORDER BY date
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        raise RuntimeError(f"benchmark source {source} returned no rows from {ohlcv_db}")
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    return frame


def load_benchmark_as_market_rows(
    ohlcv_db: Path | str,
    *,
    exchange: str,
    through_date: str,
    symbol: str = BENCHMARK_SYMBOL,
    source: str = BENCHMARK_SOURCE,
) -> pd.DataFrame:
    """Return the benchmark shaped as OHLCV market-frame rows for one exchange."""
    levels = load_benchmark_levels(ohlcv_db, source=source, through_date=through_date)
    return pd.DataFrame({
        "symbol_id": symbol,
        "exchange": exchange,
        "timestamp": levels["date"],
        "open": levels["close"],
        "high": levels["close"],
        "low": levels["close"],
        "close": levels["close"],
        "volume": 0,
    })
