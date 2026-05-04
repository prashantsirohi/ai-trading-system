"""Price-driven daily gainer scan over the OHLCV DuckDB catalog."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

_COLUMNS = ["symbol_id", "trade_date", "close", "prev_close", "pct_change", "volume"]


def compute_gainers(
    ohlcv_db_path: Path,
    as_of: date | None,
    threshold_pct: float = 5.0,
) -> pd.DataFrame:
    """Return NSE equity symbols whose latest close rose more than threshold_pct."""

    with duckdb.connect(str(ohlcv_db_path), read_only=True) as conn:
        resolved_as_of = _resolve_as_of(conn, as_of)
        if resolved_as_of is None:
            out = pd.DataFrame(columns=_COLUMNS)
            out.attrs["as_of"] = None
            return out

        sql = """
            WITH last_two AS (
              SELECT
                symbol_id,
                timestamp::DATE AS d,
                close,
                volume,
                ROW_NUMBER() OVER (
                  PARTITION BY symbol_id
                  ORDER BY timestamp DESC
                ) AS rn
              FROM _catalog
              WHERE exchange = 'NSE'
                AND COALESCE(is_benchmark, FALSE) = FALSE
                AND lower(COALESCE(instrument_type, 'equity')) IN ('equity', 'eq')
                AND timestamp::DATE <= ?
            )
            SELECT
              a.symbol_id,
              a.d AS trade_date,
              a.close,
              b.close AS prev_close,
              (a.close / NULLIF(b.close, 0) - 1.0) * 100.0 AS pct_change,
              a.volume
            FROM last_two a
            JOIN last_two b
              ON a.symbol_id = b.symbol_id
             AND a.rn = 1
             AND b.rn = 2
            WHERE b.close > 0
              AND (a.close - b.close) > (? / 100.0) * b.close
            ORDER BY pct_change DESC, a.symbol_id ASC
        """
        out = conn.execute(sql, [resolved_as_of, float(threshold_pct)]).fetchdf()

    if "trade_date" in out.columns:
        out = out.assign(trade_date=pd.to_datetime(out["trade_date"]).dt.date)
    out.attrs["as_of"] = resolved_as_of
    return out


def _resolve_as_of(conn: duckdb.DuckDBPyConnection, as_of: date | None) -> date | None:
    if as_of is not None:
        return as_of
    row = conn.execute(
        """
        SELECT max(timestamp::DATE)
        FROM _catalog
        WHERE exchange = 'NSE'
          AND COALESCE(is_benchmark, FALSE) = FALSE
          AND lower(COALESCE(instrument_type, 'equity')) IN ('equity', 'eq')
        """
    ).fetchone()
    return row[0] if row else None
