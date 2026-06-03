"""Operational market breadth history for dashboards and publishing."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths

LOG = logging.getLogger(__name__)
LONG_TERM_BREADTH_START_DATE = "2020-01-01"
MIN_SOURCE_DATE_COVERAGE_RATIO = 0.5
MAX_RETURN_GAP_DAYS = 7

_BASE_BREADTH_SQL = """
WITH daily_raw AS (
    SELECT
        CAST(timestamp AS DATE) AS trade_date,
        symbol_id,
        {close_expr} AS close
    FROM _catalog
    WHERE exchange = 'NSE'
      AND {close_expr} IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol_id, CAST(timestamp AS DATE)
        ORDER BY timestamp DESC
    ) = 1
),
eligible_dates AS (
    SELECT trade_date
    FROM daily_raw
    GROUP BY trade_date
    HAVING COUNT(DISTINCT symbol_id) >= (
        SELECT CASE
            WHEN MAX(symbol_count) <= 2 THEN MAX(symbol_count)
            ELSE CEIL(MAX(symbol_count) * {min_source_date_coverage_ratio})
        END
        FROM (
            SELECT COUNT(DISTINCT symbol_id) AS symbol_count
            FROM daily_raw
            GROUP BY trade_date
        )
    )
),
eligible_daily AS (
    SELECT daily_raw.*
    FROM daily_raw
    INNER JOIN eligible_dates USING (trade_date)
),
base AS (
    SELECT
        trade_date,
        symbol_id,
        close,
        AVG(close) OVER w20 AS sma_20,
        AVG(close) OVER w50 AS sma_50,
        AVG(close) OVER w200 AS sma_200,
        COUNT(close) OVER w20 AS obs_20,
        COUNT(close) OVER w50 AS obs_50,
        COUNT(close) OVER w200 AS obs_200,
        COUNT(close) OVER w252 AS obs_252,
        MAX(close) OVER w252 AS hi_252,
        MIN(close) OVER w252 AS lo_252,
        LAG(close) OVER (
            PARTITION BY symbol_id
            ORDER BY trade_date
        ) AS prev_close,
        LAG(trade_date) OVER (
            PARTITION BY symbol_id
            ORDER BY trade_date
        ) AS prev_trade_date
    FROM eligible_daily
    WINDOW
        w20 AS (
            PARTITION BY symbol_id
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ),
        w50 AS (
            PARTITION BY symbol_id
            ORDER BY trade_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ),
        w200 AS (
            PARTITION BY symbol_id
            ORDER BY trade_date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ),
        w252 AS (
            PARTITION BY symbol_id
            ORDER BY trade_date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        )
),
daily AS (
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
        ) AS pct_above_sma200,
        SUM(CASE WHEN obs_252 >= 252 AND close >= hi_252 THEN 1 ELSE 0 END) AS new_52w_highs,
        SUM(CASE WHEN obs_252 >= 252 AND close <= lo_252 THEN 1 ELSE 0 END) AS new_52w_lows,
        SUM(CASE WHEN prev_close IS NOT NULL AND trade_date - prev_trade_date <= {max_return_gap_days} AND close > prev_close THEN 1 ELSE 0 END) AS advancers,
        SUM(CASE WHEN prev_close IS NOT NULL AND trade_date - prev_trade_date <= {max_return_gap_days} AND close < prev_close THEN 1 ELSE 0 END) AS decliners,
        SUM(CASE WHEN prev_close IS NOT NULL AND trade_date - prev_trade_date <= {max_return_gap_days} AND close = prev_close THEN 1 ELSE 0 END) AS unchanged
    FROM base
    GROUP BY trade_date
)
SELECT
    daily.*,
    {index_level_expr} AS index_level,
    {pe_pctile_expr} AS pe_pctile_5y
FROM daily
{index_level_join}
{pe_pctile_join}
WHERE trade_date >= CAST(? AS DATE)
  AND (
      pct_above_sma20 IS NOT NULL
      OR pct_above_sma50 IS NOT NULL
      OR pct_above_sma200 IS NOT NULL
  )
ORDER BY trade_date
"""


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "trade_date",
            "pct_above_sma20",
            "pct_above_sma50",
            "pct_above_sma200",
            "above_sma20",
            "above_sma50",
            "above_sma200",
            "symbols_sma20",
            "symbols_sma50",
            "symbols_sma200",
            "symbols_total",
            "new_52w_highs",
            "new_52w_lows",
            "advancers",
            "decliners",
            "unchanged",
            "ad_net",
            "ad_pct",
            "ad_pct_sma10",
            "ad_pct_sma20",
            "ad_pct_sum63",
            "ad_z252",
            "index_level",
            "pe_pctile_5y",
            "pe_pctile_5y_sma20",
            "net_new_highs",
            "net_new_highs_pct",
            "high_low_ratio",
            "high_low_ratio_sma10",
            "ad_line",
        ]
    )


def _table_exists(con: Any, table_name: str) -> bool:
    return bool(
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
    )


def _column_exists(con: Any, table_name: str, column_name: str) -> bool:
    return bool(
        con.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = ? AND column_name = ?",
            [table_name, column_name],
        ).fetchone()[0]
    )


def _breadth_sql(con: Any) -> str:
    has_index = _table_exists(con, "universe_index_daily")
    has_cycle = _table_exists(con, "valuation_cycle_features")
    close_expr = "COALESCE(adjusted_close, close)" if _column_exists(con, "_catalog", "adjusted_close") else "close"
    return _BASE_BREADTH_SQL.format(
        close_expr=close_expr,
        min_source_date_coverage_ratio=MIN_SOURCE_DATE_COVERAGE_RATIO,
        max_return_gap_days=MAX_RETURN_GAP_DAYS,
        index_level_expr="uid.level" if has_index else "NULL::DOUBLE",
        index_level_join=(
            """
LEFT JOIN universe_index_daily uid
  ON uid.date = daily.trade_date
 AND uid.universe_id = 'UNIV_TOP1000_MCAP'
 AND uid.index_type = 'market_cap_weight'
"""
            if has_index
            else ""
        ),
        pe_pctile_expr="vcf.pe_pctile_5y" if has_cycle else "NULL::DOUBLE",
        pe_pctile_join=(
            """
LEFT JOIN valuation_cycle_features vcf
  ON vcf.date = daily.trade_date
 AND vcf.entity_type = 'universe'
 AND vcf.entity_id = 'UNIV_TOP1000_MCAP'
"""
            if has_cycle
            else ""
        ),
    )


def load_operational_breadth_frame(project_root: str | Path) -> pd.DataFrame:
    """Load 2020-onward breadth with shared derived dashboard metrics."""
    root = Path(project_root)
    db_path = get_domain_paths(project_root=root, data_domain="operational").ohlcv_db_path
    if not db_path.exists():
        return _empty_frame()

    try:
        import duckdb  # type: ignore
    except ImportError:
        LOG.warning("duckdb not installed; market breadth history unavailable")
        return _empty_frame()

    try:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            df = con.execute(_breadth_sql(con), [LONG_TERM_BREADTH_START_DATE]).fetchdf()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        LOG.warning("market breadth history query failed: %s", exc)
        return _empty_frame()

    if df.empty:
        return _empty_frame()

    numeric_columns = [column for column in df.columns if column != "trade_date"]
    df = df.assign(
        trade_date=pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d"),
        **{column: pd.to_numeric(df[column], errors="coerce") for column in numeric_columns},
    ).dropna(subset=["trade_date"])
    df.loc[:, "net_new_highs"] = df["new_52w_highs"].fillna(0) - df["new_52w_lows"].fillna(0)
    df.loc[:, "net_new_highs_pct"] = (
        df["net_new_highs"] / df["symbols_sma200"].where(df["symbols_sma200"].ne(0))
    ).fillna(0.0)
    df.loc[:, "high_low_ratio"] = df["new_52w_highs"].fillna(0) / df["new_52w_lows"].fillna(0).clip(lower=1)
    df.loc[:, "high_low_ratio_sma10"] = df["high_low_ratio"].rolling(10, min_periods=1).mean()
    df.loc[:, "pe_pctile_5y_sma20"] = df["pe_pctile_5y"].rolling(20, min_periods=1).mean()
    daily_ad = df["advancers"].fillna(0) - df["decliners"].fillna(0)
    ad_denom = (df["advancers"].fillna(0) + df["decliners"].fillna(0)).replace(0, float("nan"))
    df.loc[:, "ad_net"] = daily_ad
    df.loc[:, "ad_pct"] = (daily_ad / ad_denom).fillna(0.0)
    df.loc[:, "ad_pct_sma10"] = df["ad_pct"].rolling(10, min_periods=1).mean()
    df.loc[:, "ad_pct_sma20"] = df["ad_pct"].rolling(20, min_periods=1).mean()
    df.loc[:, "ad_pct_sum63"] = df["ad_pct"].rolling(63, min_periods=1).sum()
    rolling_mean252 = df["ad_pct"].rolling(252, min_periods=2).mean()
    rolling_std252 = df["ad_pct"].rolling(252, min_periods=2).std(ddof=0)
    df.loc[:, "ad_z252"] = (
        (df["ad_pct"] - rolling_mean252) / rolling_std252.replace(0, float("nan"))
    ).fillna(0.0)
    df.loc[:, "ad_line"] = daily_ad.cumsum() - daily_ad.iloc[0]
    return df.reset_index(drop=True)


def _value(value: Any, *, integer: bool = False) -> int | float | None:
    if pd.isna(value):
        return None
    return int(value) if integer else float(value)


def get_market_breadth_history(
    project_root: str | Path,
    *,
    limit: int = 0,
) -> dict[str, Any]:
    """Return 2020-onward operational breadth history."""
    root = Path(project_root)
    db_path = get_domain_paths(project_root=root, data_domain="operational").ohlcv_db_path
    df = load_operational_breadth_frame(root)
    if df.empty:
        return {"available": False, "rows": [], "unit": "percent", "source": str(db_path)}

    if limit > 0:
        df = df.tail(limit)

    rows = [
        {
            "trade_date": row.trade_date,
            "pct_above_sma20": _value(row.pct_above_sma20),
            "pct_above_sma50": _value(row.pct_above_sma50),
            "pct_above_sma200": _value(row.pct_above_sma200),
            "above_sma20": _value(row.above_sma20, integer=True),
            "above_sma50": _value(row.above_sma50, integer=True),
            "above_sma200": _value(row.above_sma200, integer=True),
            "symbols_sma20": _value(row.symbols_sma20, integer=True),
            "symbols_sma50": _value(row.symbols_sma50, integer=True),
            "symbols_sma200": _value(row.symbols_sma200, integer=True),
            "symbols_total": _value(row.symbols_total, integer=True),
            "new_52w_highs": _value(row.new_52w_highs, integer=True),
            "new_52w_lows": _value(row.new_52w_lows, integer=True),
            "advancers": _value(row.advancers, integer=True),
            "decliners": _value(row.decliners, integer=True),
            "unchanged": _value(row.unchanged, integer=True),
            "ad_net": _value(row.ad_net, integer=True),
            "ad_pct": _value(row.ad_pct),
            "ad_pct_sma10": _value(row.ad_pct_sma10),
            "ad_pct_sma20": _value(row.ad_pct_sma20),
            "ad_pct_sum63": _value(row.ad_pct_sum63),
            "ad_z252": _value(row.ad_z252),
            "net_new_highs": _value(row.net_new_highs, integer=True),
            "net_new_highs_pct": _value(row.net_new_highs_pct),
            "high_low_ratio": _value(row.high_low_ratio),
            "high_low_ratio_sma10": _value(row.high_low_ratio_sma10),
            "index_level": _value(row.index_level),
            "pe_pctile_5y": _value(row.pe_pctile_5y),
            "pe_pctile_5y_sma20": _value(row.pe_pctile_5y_sma20),
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


__all__ = [
    "LONG_TERM_BREADTH_START_DATE",
    "get_market_breadth_history",
    "load_operational_breadth_frame",
]
