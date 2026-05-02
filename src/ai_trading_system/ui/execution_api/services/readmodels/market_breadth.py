"""Operational market breadth history for the execution console."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

LOG = logging.getLogger(__name__)


_BREADTH_SQL = """
WITH base AS (
    SELECT
        CAST(timestamp AS DATE) AS trade_date,
        symbol_id,
        close,
        AVG(close) OVER w20 AS sma_20,
        AVG(close) OVER w50 AS sma_50,
        AVG(close) OVER w200 AS sma_200,
        COUNT(close) OVER w20 AS obs_20,
        COUNT(close) OVER w50 AS obs_50,
        COUNT(close) OVER w200 AS obs_200
    FROM _catalog
    WHERE exchange = 'NSE'
    WINDOW
        w20 AS (
            PARTITION BY symbol_id
            ORDER BY CAST(timestamp AS DATE)
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ),
        w50 AS (
            PARTITION BY symbol_id
            ORDER BY CAST(timestamp AS DATE)
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ),
        w200 AS (
            PARTITION BY symbol_id
            ORDER BY CAST(timestamp AS DATE)
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        )
)
SELECT
    trade_date,
    SUM(CASE WHEN obs_20 >= 20 AND close > sma_20 THEN 1 ELSE 0 END) AS above_sma20,
    SUM(CASE WHEN obs_50 >= 50 AND close > sma_50 THEN 1 ELSE 0 END) AS above_sma50,
    SUM(CASE WHEN obs_200 >= 200 AND close > sma_200 THEN 1 ELSE 0 END) AS above_sma200,
    SUM(CASE WHEN obs_20 >= 20 THEN 1 ELSE 0 END) AS symbols_sma20,
    SUM(CASE WHEN obs_50 >= 50 THEN 1 ELSE 0 END) AS symbols_sma50,
    SUM(CASE WHEN obs_200 >= 200 THEN 1 ELSE 0 END) AS symbols_sma200,
    COUNT(DISTINCT symbol_id) AS symbols_total,
    ROUND(
        SUM(CASE WHEN obs_20 >= 20 AND close > sma_20 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN obs_20 >= 20 THEN 1 ELSE 0 END), 0),
        2
    ) AS pct_above_sma20,
    ROUND(
        SUM(CASE WHEN obs_50 >= 50 AND close > sma_50 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN obs_50 >= 50 THEN 1 ELSE 0 END), 0),
        2
    ) AS pct_above_sma50,
    ROUND(
        SUM(CASE WHEN obs_200 >= 200 AND close > sma_200 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN obs_200 >= 200 THEN 1 ELSE 0 END), 0),
        2
    ) AS pct_above_sma200
FROM base
GROUP BY trade_date
HAVING pct_above_sma20 IS NOT NULL
    OR pct_above_sma50 IS NOT NULL
    OR pct_above_sma200 IS NOT NULL
ORDER BY trade_date
"""


def get_market_breadth_history(
    project_root: str | Path,
    *,
    limit: int = 0,
) -> dict[str, Any]:
    """Return maximum available operational breadth history.

    ``limit=0`` means all available operational rows. A positive limit keeps
    the most recent N rows for clients that need a lighter payload.
    """
    root = Path(project_root)
    db_path = root / "data" / "ohlcv.duckdb"
    if not db_path.exists():
        return {"available": False, "rows": [], "unit": "percent", "source": str(db_path)}

    try:
        import duckdb  # type: ignore
    except ImportError:
        LOG.warning("duckdb not installed; market breadth history unavailable")
        return {"available": False, "rows": [], "unit": "percent", "source": str(db_path)}

    try:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            df = con.execute(_BREADTH_SQL).fetchdf()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        LOG.warning("market breadth history query failed: %s", exc)
        return {"available": False, "rows": [], "unit": "percent", "source": str(db_path)}

    if df.empty:
        return {"available": False, "rows": [], "unit": "percent", "source": str(db_path)}

    df = df.assign(
        trade_date=pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d"),
        pct_above_sma20=pd.to_numeric(df["pct_above_sma20"], errors="coerce"),
        pct_above_sma50=pd.to_numeric(df["pct_above_sma50"], errors="coerce"),
        pct_above_sma200=pd.to_numeric(df["pct_above_sma200"], errors="coerce"),
    ).dropna(subset=["trade_date"])

    if limit > 0:
        df = df.tail(limit)

    rows = [
        {
            "trade_date": row.trade_date,
            "pct_above_sma20": None if pd.isna(row.pct_above_sma20) else float(row.pct_above_sma20),
            "pct_above_sma50": None if pd.isna(row.pct_above_sma50) else float(row.pct_above_sma50),
            "pct_above_sma200": None if pd.isna(row.pct_above_sma200) else float(row.pct_above_sma200),
            "above_sma20": int(row.above_sma20),
            "above_sma50": int(row.above_sma50),
            "above_sma200": int(row.above_sma200),
            "symbols_sma20": int(row.symbols_sma20),
            "symbols_sma50": int(row.symbols_sma50),
            "symbols_sma200": int(row.symbols_sma200),
            "symbols_total": int(row.symbols_total),
        }
        for row in df.itertuples(index=False)
    ]
    return {
        "available": bool(rows),
        "rows": rows,
        "unit": "percent",
        "source": str(db_path),
        "row_count": len(rows),
    }
