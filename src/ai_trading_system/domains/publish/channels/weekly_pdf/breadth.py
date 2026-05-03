"""Compute market breadth time series from the OHLCV DuckDB.

Produces, for each trade date in the window:
  - pct_above_sma20 / pct_above_sma50 / pct_above_sma200
  - new_52w_highs / new_52w_lows
  - advancers / decliners / unchanged

Symbols with insufficient bars for a given SMA window are excluded from
that ratio's denominator.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


_BREADTH_SQL = """
WITH base AS (
    SELECT
        CAST(timestamp AS DATE) AS trade_date,
        symbol_id,
        close,
        AVG(close) OVER w20  AS sma_20,
        AVG(close) OVER w50  AS sma_50,
        AVG(close) OVER w200 AS sma_200,
        COUNT(close) OVER w20  AS obs_20,
        COUNT(close) OVER w50  AS obs_50,
        COUNT(close) OVER w200 AS obs_200,
        MAX(close) OVER w252   AS hi_252,
        MIN(close) OVER w252   AS lo_252,
        LAG(close) OVER (PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE)) AS prev_close
    FROM _catalog
    WHERE exchange = 'NSE'
      AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
    WINDOW
        w20  AS (PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE) ROWS BETWEEN  19 PRECEDING AND CURRENT ROW),
        w50  AS (PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE) ROWS BETWEEN  49 PRECEDING AND CURRENT ROW),
        w200 AS (PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE) ROWS BETWEEN 199 PRECEDING AND CURRENT ROW),
        w252 AS (PARTITION BY symbol_id ORDER BY CAST(timestamp AS DATE) ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
)
SELECT
    trade_date,
    ROUND(100.0 * SUM(CASE WHEN obs_20  >=  20 AND close > sma_20  THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN obs_20  >=  20 THEN 1 ELSE 0 END), 0), 2) AS pct_above_sma20,
    ROUND(100.0 * SUM(CASE WHEN obs_50  >=  50 AND close > sma_50  THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN obs_50  >=  50 THEN 1 ELSE 0 END), 0), 2) AS pct_above_sma50,
    ROUND(100.0 * SUM(CASE WHEN obs_200 >= 200 AND close > sma_200 THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN obs_200 >= 200 THEN 1 ELSE 0 END), 0), 2) AS pct_above_sma200,
    SUM(CASE WHEN close >= hi_252 THEN 1 ELSE 0 END)              AS new_52w_highs,
    SUM(CASE WHEN close <= lo_252 THEN 1 ELSE 0 END)              AS new_52w_lows,
    SUM(CASE WHEN prev_close IS NOT NULL AND close > prev_close THEN 1 ELSE 0 END) AS advancers,
    SUM(CASE WHEN prev_close IS NOT NULL AND close < prev_close THEN 1 ELSE 0 END) AS decliners,
    SUM(CASE WHEN prev_close IS NOT NULL AND close = prev_close THEN 1 ELSE 0 END) AS unchanged,
    COUNT(DISTINCT symbol_id)                                                       AS universe_count
FROM base
GROUP BY trade_date
ORDER BY trade_date
"""


def compute_market_breadth(
    ohlcv_db_path: Path,
    end_date: date,
    weeks: int = 26,
) -> pd.DataFrame:
    """Run the breadth SQL over a rolling window ending on `end_date`.

    Returns an empty DataFrame on any failure (missing DB, query error, etc.) —
    breadth is informational and should never abort the report.
    """
    if not ohlcv_db_path.exists():
        logger.warning("ohlcv db missing at %s, skipping market breadth", ohlcv_db_path)
        return pd.DataFrame()

    try:
        import duckdb  # type: ignore
    except ImportError:
        logger.warning("duckdb not installed, skipping market breadth")
        return pd.DataFrame()

    # SMA200 needs ~250 prior trading days; pad the read window accordingly.
    pad_days = 252 + 30
    read_start = end_date - timedelta(weeks=weeks) - timedelta(days=pad_days)
    display_start = end_date - timedelta(weeks=weeks)

    try:
        con = duckdb.connect(str(ohlcv_db_path), read_only=True)
        try:
            df = con.execute(
                _BREADTH_SQL, [read_start.isoformat(), end_date.isoformat()]
            ).fetchdf()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("market breadth query failed: %s", exc)
        return pd.DataFrame()

    if df.empty:
        return df
    df = df.assign(trade_date=pd.to_datetime(df["trade_date"]).dt.date)
    return df.loc[df["trade_date"] >= display_start].reset_index(drop=True)


def latest_breadth_row(breadth: pd.DataFrame) -> Optional[dict]:
    if breadth is None or breadth.empty:
        return None
    return breadth.iloc[-1].to_dict()
