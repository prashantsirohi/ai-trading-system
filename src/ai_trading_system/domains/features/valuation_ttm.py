"""Point-in-time TTM fundamentals from Screener SQLite."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

from ai_trading_system.domains.features.valuation_schema import ensure_valuation_schema

TTM_METRICS = ("sales", "net_profit", "operating_profit", "adjusted_equity_shares_cr")


@dataclass(frozen=True)
class TtmRefreshResult:
    rows: int
    symbols: int
    dates: int
    quarterly_rows: int
    annual_fallback_rows: int
    missing_rows: int


def refresh_fundamental_ttm(
    *,
    ohlcv_db_path: str | Path,
    screener_db_path: str | Path,
    valuation_dates: Iterable[str | date] | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
) -> TtmRefreshResult:
    """Build `fundamental_ttm` in DuckDB using only financials available by date."""

    dates = _resolve_valuation_dates(ohlcv_db_path, valuation_dates, from_date, to_date)
    if not dates:
        return TtmRefreshResult(0, 0, 0, 0, 0, 0)
    financials = _load_financials(screener_db_path)
    if financials.empty:
        return TtmRefreshResult(0, 0, len(dates), 0, 0, 0)

    rows: list[dict] = []
    for symbol, group in financials.groupby("symbol", sort=True):
        quarterly = group.loc[group["period_type"].eq("quarterly")].sort_values("available_at")
        annual = group.loc[group["period_type"].eq("annual")].sort_values("available_at")
        for as_of in dates:
            row = _ttm_for_symbol_date(symbol, as_of, quarterly, annual)
            if row is not None:
                rows.append(row)

    frame = pd.DataFrame(rows)
    conn = duckdb.connect(str(ohlcv_db_path))
    try:
        ensure_valuation_schema(conn)
        if not frame.empty:
            start, end = min(dates), max(dates)
            conn.execute(
                """
                DELETE FROM fundamental_ttm
                WHERE as_of_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                """,
                [start, end],
            )
            conn.register("_fundamental_ttm_frame", frame)
            try:
                conn.execute(
                    """
                    INSERT INTO fundamental_ttm
                    SELECT
                        symbol,
                        CAST(as_of_date AS DATE),
                        ttm_sales_cr,
                        ttm_net_profit_cr,
                        ttm_operating_profit_cr,
                        adjusted_equity_shares_cr,
                        earnings_source,
                        source_batch_id,
                        CURRENT_TIMESTAMP
                    FROM _fundamental_ttm_frame
                    """
                )
            finally:
                conn.unregister("_fundamental_ttm_frame")
    finally:
        conn.close()

    if frame.empty:
        return TtmRefreshResult(0, 0, len(dates), 0, 0, 0)
    return TtmRefreshResult(
        rows=len(frame),
        symbols=int(frame["symbol"].nunique()),
        dates=len(dates),
        quarterly_rows=int(frame["earnings_source"].eq("quarterly_ttm").sum()),
        annual_fallback_rows=int(frame["earnings_source"].eq("annual_fallback").sum()),
        missing_rows=int(frame["earnings_source"].eq("missing").sum()),
    )


def _resolve_valuation_dates(
    ohlcv_db_path: str | Path,
    valuation_dates: Iterable[str | date] | None,
    from_date: str | date | None,
    to_date: str | date | None,
) -> list[str]:
    if valuation_dates is not None:
        return sorted({_date_string(value) for value in valuation_dates})
    conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        filters = ["exchange = 'NSE'", "close IS NOT NULL"]
        params: list[str] = []
        if from_date is not None:
            filters.append("CAST(timestamp AS DATE) >= CAST(? AS DATE)")
            params.append(_date_string(from_date))
        if to_date is not None:
            filters.append("CAST(timestamp AS DATE) <= CAST(? AS DATE)")
            params.append(_date_string(to_date))
        query = f"""
            SELECT DISTINCT CAST(timestamp AS DATE) AS date
            FROM _catalog
            WHERE {' AND '.join(filters)}
            ORDER BY date
        """
        return [_date_string(row[0]) for row in conn.execute(query, params).fetchall()]
    finally:
        conn.close()


def _load_financials(screener_db_path: str | Path) -> pd.DataFrame:
    conn = sqlite3.connect(str(screener_db_path))
    try:
        frame = pd.read_sql_query(
            f"""
            SELECT symbol, period_type, report_date, metric_id, value, available_at, sync_batch_id
            FROM screener_financials
            WHERE metric_id IN ({','.join(['?'] * len(TTM_METRICS))})
              AND value IS NOT NULL
              AND available_at IS NOT NULL
            """,
            conn,
            params=list(TTM_METRICS),
        )
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame.loc[:, "symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame.loc[:, "report_date"] = pd.to_datetime(frame["report_date"]).dt.date
    frame.loc[:, "available_at"] = pd.to_datetime(frame["available_at"]).dt.date
    frame.loc[:, "value"] = pd.to_numeric(frame["value"], errors="coerce")
    return frame.dropna(subset=["symbol", "available_at", "value"])


def _ttm_for_symbol_date(
    symbol: str,
    as_of: str,
    quarterly: pd.DataFrame,
    annual: pd.DataFrame,
) -> dict | None:
    as_of_date = pd.Timestamp(as_of).date()
    q = quarterly.loc[quarterly["available_at"].le(as_of_date)]
    a = annual.loc[annual["available_at"].le(as_of_date)]
    shares = _latest_metric(pd.concat([q, a], ignore_index=True), "adjusted_equity_shares_cr")
    source_batch = _latest_batch(pd.concat([q, a], ignore_index=True))

    q_values = {
        metric: _last_n_metric_sum(q, metric, 4)
        for metric in ("sales", "net_profit", "operating_profit")
    }
    if all(value is not None for value in q_values.values()):
        return {
            "symbol": symbol,
            "as_of_date": as_of,
            "ttm_sales_cr": q_values["sales"],
            "ttm_net_profit_cr": q_values["net_profit"],
            "ttm_operating_profit_cr": q_values["operating_profit"],
            "adjusted_equity_shares_cr": shares,
            "earnings_source": "quarterly_ttm",
            "source_batch_id": source_batch,
        }

    a_values = {
        metric: _latest_metric(a, metric)
        for metric in ("sales", "net_profit", "operating_profit")
    }
    if any(value is not None for value in a_values.values()):
        return {
            "symbol": symbol,
            "as_of_date": as_of,
            "ttm_sales_cr": a_values["sales"],
            "ttm_net_profit_cr": a_values["net_profit"],
            "ttm_operating_profit_cr": a_values["operating_profit"],
            "adjusted_equity_shares_cr": shares,
            "earnings_source": "annual_fallback",
            "source_batch_id": source_batch,
        }
    return {
        "symbol": symbol,
        "as_of_date": as_of,
        "ttm_sales_cr": None,
        "ttm_net_profit_cr": None,
        "ttm_operating_profit_cr": None,
        "adjusted_equity_shares_cr": shares,
        "earnings_source": "missing",
        "source_batch_id": source_batch,
    }


def _last_n_metric_sum(frame: pd.DataFrame, metric: str, n: int) -> float | None:
    values = frame.loc[frame["metric_id"].eq(metric)].sort_values(["report_date", "available_at"]).tail(n)
    if len(values) < n:
        return None
    return float(values["value"].sum())


def _latest_metric(frame: pd.DataFrame, metric: str) -> float | None:
    values = frame.loc[frame["metric_id"].eq(metric)].sort_values(["available_at", "report_date"])
    if values.empty:
        return None
    return float(values.iloc[-1]["value"])


def _latest_batch(frame: pd.DataFrame) -> str | None:
    if frame.empty or "sync_batch_id" not in frame.columns:
        return None
    values = frame.dropna(subset=["sync_batch_id"]).sort_values("available_at")
    return None if values.empty else str(values.iloc[-1]["sync_batch_id"])


def _date_string(value: str | date) -> str:
    return str(value)[:10]


__all__ = ["TtmRefreshResult", "refresh_fundamental_ttm"]
