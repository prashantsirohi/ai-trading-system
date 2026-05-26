"""Company and sector earnings growth features."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.features.fundamental_period_facts import ensure_sector_earnings_schema


@dataclass(frozen=True)
class FundamentalGrowthResult:
    company_rows: int
    sector_rows: int
    start_date: str | None
    end_date: str | None


def refresh_fundamental_growth(
    *,
    ohlcv_db_path: str | Path,
    from_date: str | None = None,
    to_date: str | None = None,
) -> FundamentalGrowthResult:
    conn = duckdb.connect(str(ohlcv_db_path))
    try:
        ensure_sector_earnings_schema(conn)
        facts = conn.execute(
            """
            SELECT *
            FROM fundamental_period_facts_enriched
            WHERE period_type = 'quarterly'
              AND report_date <= COALESCE(CAST(? AS DATE), DATE '9999-12-31')
            ORDER BY symbol, report_date
            """,
            [str(to_date)[:10] if to_date else None],
        ).df()
        company_all = _company_growth(facts)
        sector_all = _sector_growth(company_all)
        company = _filter_dates(company_all, from_date, to_date)
        sector = _filter_dates(sector_all, from_date, to_date)
        if not company.empty:
            start, end = str(company["report_date"].min())[:10], str(company["report_date"].max())[:10]
            _replace_range(conn, "company_fundamental_growth", company, start, end)
        elif from_date or to_date:
            start = str(from_date or to_date)[:10]
            end = str(to_date or from_date)[:10]
            _delete_range(conn, "company_fundamental_growth", start, end)
        else:
            start = end = None
        if not sector.empty:
            sector_start, sector_end = str(sector["report_date"].min())[:10], str(sector["report_date"].max())[:10]
            _replace_range(conn, "sector_fundamental_growth", sector, sector_start, sector_end)
        elif from_date or to_date:
            _delete_range(conn, "sector_fundamental_growth", str(from_date or to_date)[:10], str(to_date or from_date)[:10])
    finally:
        conn.close()
    return FundamentalGrowthResult(
        company_rows=int(len(company)),
        sector_rows=int(len(sector)),
        start_date=start,
        end_date=end,
    )


def _company_growth(facts: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "symbol",
        "sector_name",
        "industry_group",
        "report_date",
        "available_at",
        "sales_cr",
        "net_profit_cr",
        "operating_profit_cr",
        "opm_pct",
        "npm_pct",
        "sales_qoq_growth",
        "sales_yoy_growth",
        "profit_qoq_growth",
        "profit_yoy_growth",
        "opm_yoy_change",
    ]
    if facts.empty:
        return pd.DataFrame(columns=columns)
    q = facts.copy()
    q.loc[:, "report_date"] = pd.to_datetime(q["report_date"]).dt.date
    q = q.sort_values(["symbol", "report_date"], kind="stable")
    for col in ("sales_cr", "net_profit_cr", "operating_profit_cr", "opm_pct", "npm_pct"):
        q.loc[:, col] = pd.to_numeric(q[col], errors="coerce")
    grouped = q.groupby("symbol", sort=False)
    q.loc[:, "sales_prev_q"] = grouped["sales_cr"].shift(1)
    q.loc[:, "sales_same_q_ly"] = grouped["sales_cr"].shift(4)
    q.loc[:, "profit_prev_q"] = grouped["net_profit_cr"].shift(1)
    q.loc[:, "profit_same_q_ly"] = grouped["net_profit_cr"].shift(4)
    q.loc[:, "opm_same_q_ly"] = grouped["opm_pct"].shift(4)
    q.loc[:, "sales_qoq_growth"] = q["sales_cr"] / q["sales_prev_q"].where(q["sales_prev_q"].gt(0)) - 1.0
    q.loc[:, "sales_yoy_growth"] = q["sales_cr"] / q["sales_same_q_ly"].where(q["sales_same_q_ly"].gt(0)) - 1.0
    q.loc[:, "profit_qoq_growth"] = q["net_profit_cr"] / q["profit_prev_q"].where(q["profit_prev_q"].gt(0)) - 1.0
    q.loc[:, "profit_yoy_growth"] = q["net_profit_cr"] / q["profit_same_q_ly"].where(q["profit_same_q_ly"].gt(0)) - 1.0
    q.loc[:, "opm_yoy_change"] = q["opm_pct"] - q["opm_same_q_ly"]
    return q[columns].reset_index(drop=True)


def _sector_growth(company: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "sector_name",
        "report_date",
        "company_count",
        "sector_sales_cr",
        "sector_profit_cr",
        "sector_operating_profit_cr",
        "median_sales_yoy_growth",
        "median_profit_yoy_growth",
        "median_sales_qoq_growth",
        "median_profit_qoq_growth",
        "avg_opm_pct",
        "median_opm_yoy_change",
        "sales_yoy_positive_pct",
        "profit_yoy_positive_pct",
        "margin_expansion_pct",
        "sector_sales_qoq_growth",
        "sector_sales_yoy_growth",
        "sector_profit_qoq_growth",
        "sector_profit_yoy_growth",
    ]
    if company.empty:
        return pd.DataFrame(columns=columns)
    base = company.loc[company["sector_name"].notna() & company["sector_name"].astype(str).str.strip().ne("")].copy()
    if base.empty:
        return pd.DataFrame(columns=columns)
    grouped = base.groupby(["sector_name", "report_date"], sort=True)
    sector = grouped.agg(
        company_count=("symbol", "nunique"),
        sector_sales_cr=("sales_cr", "sum"),
        sector_profit_cr=("net_profit_cr", "sum"),
        sector_operating_profit_cr=("operating_profit_cr", "sum"),
        median_sales_yoy_growth=("sales_yoy_growth", "median"),
        median_profit_yoy_growth=("profit_yoy_growth", "median"),
        median_sales_qoq_growth=("sales_qoq_growth", "median"),
        median_profit_qoq_growth=("profit_qoq_growth", "median"),
        avg_opm_pct=("opm_pct", "mean"),
        median_opm_yoy_change=("opm_yoy_change", "median"),
        sales_yoy_positive_pct=("sales_yoy_growth", _positive_pct),
        profit_yoy_positive_pct=("profit_yoy_growth", _positive_pct),
        margin_expansion_pct=("opm_yoy_change", _positive_pct),
    ).reset_index()
    sector = sector.sort_values(["sector_name", "report_date"], kind="stable")
    sgroup = sector.groupby("sector_name", sort=False)
    sector.loc[:, "sector_sales_prev_q"] = sgroup["sector_sales_cr"].shift(1)
    sector.loc[:, "sector_sales_same_q_ly"] = sgroup["sector_sales_cr"].shift(4)
    sector.loc[:, "sector_profit_prev_q"] = sgroup["sector_profit_cr"].shift(1)
    sector.loc[:, "sector_profit_same_q_ly"] = sgroup["sector_profit_cr"].shift(4)
    sector.loc[:, "sector_sales_qoq_growth"] = sector["sector_sales_cr"] / sector["sector_sales_prev_q"].where(sector["sector_sales_prev_q"].gt(0)) - 1.0
    sector.loc[:, "sector_sales_yoy_growth"] = sector["sector_sales_cr"] / sector["sector_sales_same_q_ly"].where(sector["sector_sales_same_q_ly"].gt(0)) - 1.0
    sector.loc[:, "sector_profit_qoq_growth"] = sector["sector_profit_cr"] / sector["sector_profit_prev_q"].where(sector["sector_profit_prev_q"].gt(0)) - 1.0
    sector.loc[:, "sector_profit_yoy_growth"] = sector["sector_profit_cr"] / sector["sector_profit_same_q_ly"].where(sector["sector_profit_same_q_ly"].gt(0)) - 1.0
    return sector[columns].reset_index(drop=True)


def _positive_pct(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return float("nan")
    return float(clean.gt(0).mean() * 100.0)


def _filter_dates(frame: pd.DataFrame, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    if frame.empty:
        return frame
    output = frame.copy()
    dates = pd.to_datetime(output["report_date"]).dt.date
    if from_date:
        output = output.loc[dates >= pd.Timestamp(from_date).date()]
        dates = pd.to_datetime(output["report_date"]).dt.date
    if to_date:
        output = output.loc[dates <= pd.Timestamp(to_date).date()]
    return output.reset_index(drop=True)


def _replace_range(conn: duckdb.DuckDBPyConnection, table: str, frame: pd.DataFrame, start: str, end: str) -> None:
    _delete_range(conn, table, start, end)
    if frame.empty:
        return
    columns = list(frame.columns)
    conn.register("_fundamental_growth_frame", frame[columns])
    try:
        conn.execute(f"INSERT INTO {table} ({', '.join(columns)}) SELECT {', '.join(columns)} FROM _fundamental_growth_frame")
    finally:
        conn.unregister("_fundamental_growth_frame")


def _delete_range(conn: duckdb.DuckDBPyConnection, table: str, start: str, end: str) -> None:
    conn.execute(
        f"DELETE FROM {table} WHERE report_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
        [start, end],
    )


__all__ = ["FundamentalGrowthResult", "refresh_fundamental_growth"]
