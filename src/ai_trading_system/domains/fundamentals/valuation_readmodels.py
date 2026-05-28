"""Read helpers for fundamentals-side valuation cycle tables."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.analytical_store import default_fundamentals_duckdb_path


def load_latest_universe_valuation(
    *,
    fundamentals_db_path: str | Path | None = None,
    universe_id: str = "UNIV_TOP500_MCAP",
) -> pd.DataFrame:
    path = Path(fundamentals_db_path) if fundamentals_db_path is not None else default_fundamentals_duckdb_path()
    if not path.exists():
        return pd.DataFrame()
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return conn.execute(
            """
            WITH latest AS (
                SELECT MAX(date) AS date
                FROM universe_valuation_daily
                WHERE universe_id = ?
            )
            SELECT uv.*
            FROM universe_valuation_daily uv
            JOIN latest l ON uv.date = l.date
            WHERE uv.universe_id = ?
            """,
            [universe_id, universe_id],
        ).df()
    finally:
        conn.close()


def load_universe_valuation_history(
    *,
    fundamentals_db_path: str | Path | None = None,
    universe_id: str = "UNIV_TOP500_MCAP",
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    path = Path(fundamentals_db_path) if fundamentals_db_path is not None else default_fundamentals_duckdb_path()
    if not path.exists():
        return pd.DataFrame()
    filters = ["universe_id = ?"]
    params: list[str] = [universe_id]
    if from_date:
        filters.append("date >= CAST(? AS DATE)")
        params.append(str(from_date)[:10])
    if to_date:
        filters.append("date <= CAST(? AS DATE)")
        params.append(str(to_date)[:10])
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return conn.execute(
            f"""
            SELECT *
            FROM universe_valuation_daily
            WHERE {' AND '.join(filters)}
            ORDER BY date
            """,
            params,
        ).df()
    finally:
        conn.close()


__all__ = ["load_latest_universe_valuation", "load_universe_valuation_history"]
