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

    frame = _build_ttm_frame(financials, dates)
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


def _build_ttm_frame(financials: pd.DataFrame, dates: list[str]) -> pd.DataFrame:
    financials = financials.copy()
    financials.loc[:, "report_date"] = pd.to_datetime(financials["report_date"])
    financials.loc[:, "available_at"] = pd.to_datetime(financials["available_at"])
    symbols = sorted(financials["symbol"].dropna().astype(str).unique())
    if not symbols:
        return pd.DataFrame(columns=_ttm_columns())

    date_frame = pd.DataFrame({"as_of_date": pd.to_datetime(dates)})
    grid = pd.MultiIndex.from_product(
        [symbols, date_frame["as_of_date"]],
        names=["symbol", "as_of_date"],
    ).to_frame(index=False)

    quarterly_events = _quarterly_ttm_events(financials)
    annual_events = _annual_fallback_events(financials)
    share_events = _share_events(financials)

    quarterly_asof = _merge_events_asof(grid, quarterly_events)
    annual_asof = _merge_events_asof(grid[["symbol", "as_of_date"]], annual_events)
    share_asof = _merge_events_asof(grid[["symbol", "as_of_date"]], share_events)

    frame = quarterly_asof[["symbol", "as_of_date"]].copy()
    has_quarterly = quarterly_asof["ttm_net_profit_cr"].notna()
    has_annual = annual_asof["ttm_net_profit_cr"].notna()

    for column in ("ttm_sales_cr", "ttm_net_profit_cr", "ttm_operating_profit_cr"):
        frame.loc[:, column] = quarterly_asof[column].where(has_quarterly, annual_asof[column])
    frame.loc[:, "adjusted_equity_shares_cr"] = share_asof["adjusted_equity_shares_cr"]
    frame.loc[:, "earnings_source"] = "missing"
    frame.loc[has_annual, "earnings_source"] = "annual_fallback"
    frame.loc[has_quarterly, "earnings_source"] = "quarterly_ttm"
    frame.loc[:, "source_batch_id"] = quarterly_asof["source_batch_id"].where(
        has_quarterly,
        annual_asof["source_batch_id"],
    )
    return frame[_ttm_columns()].sort_values(["symbol", "as_of_date"]).reset_index(drop=True)


def _quarterly_ttm_events(financials: pd.DataFrame) -> pd.DataFrame:
    facts = _wide_period_facts(financials, period_type="quarterly")
    if facts.empty:
        return pd.DataFrame(columns=_event_columns())
    facts = facts.sort_values(["symbol", "report_date", "available_at"], kind="stable")
    grouped = facts.groupby("symbol", sort=False)
    for metric, output in (
        ("sales", "ttm_sales_cr"),
        ("net_profit", "ttm_net_profit_cr"),
        ("operating_profit", "ttm_operating_profit_cr"),
    ):
        facts.loc[:, output] = grouped[metric].transform(lambda values: values.rolling(4, min_periods=4).sum())
    events = facts.loc[
        facts[["ttm_sales_cr", "ttm_net_profit_cr", "ttm_operating_profit_cr"]].notna().all(axis=1),
        ["symbol", "available_at", "ttm_sales_cr", "ttm_net_profit_cr", "ttm_operating_profit_cr", "source_batch_id"],
    ].copy()
    return _dedupe_events(events)


def _annual_fallback_events(financials: pd.DataFrame) -> pd.DataFrame:
    facts = _wide_period_facts(financials, period_type="annual")
    if facts.empty:
        return pd.DataFrame(columns=_event_columns())
    facts = facts.rename(
        columns={
            "sales": "ttm_sales_cr",
            "net_profit": "ttm_net_profit_cr",
            "operating_profit": "ttm_operating_profit_cr",
        }
    )
    events = facts[
        ["symbol", "available_at", "ttm_sales_cr", "ttm_net_profit_cr", "ttm_operating_profit_cr", "source_batch_id"]
    ].copy()
    return _dedupe_events(events)


def _share_events(financials: pd.DataFrame) -> pd.DataFrame:
    shares = financials.loc[financials["metric_id"].eq("adjusted_equity_shares_cr")].copy()
    if shares.empty:
        return pd.DataFrame(columns=["symbol", "available_at", "adjusted_equity_shares_cr"])
    shares = shares.sort_values(["symbol", "available_at", "report_date"], kind="stable")
    shares = shares.drop_duplicates(["symbol", "available_at"], keep="last")
    shares = shares.rename(columns={"value": "adjusted_equity_shares_cr"})
    shares.loc[:, "available_at"] = pd.to_datetime(shares["available_at"])
    return shares[["symbol", "available_at", "adjusted_equity_shares_cr"]].reset_index(drop=True)


def _wide_period_facts(financials: pd.DataFrame, *, period_type: str) -> pd.DataFrame:
    facts = financials.loc[
        financials["period_type"].eq(period_type)
        & financials["metric_id"].isin(["sales", "net_profit", "operating_profit"])
    ].copy()
    if facts.empty:
        return pd.DataFrame()
    wide = (
        facts.pivot_table(
            index=["symbol", "report_date", "available_at"],
            columns="metric_id",
            values="value",
            aggfunc="max",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    batch = (
        facts.groupby(["symbol", "report_date", "available_at"], as_index=False)["sync_batch_id"]
        .last()
        .rename(columns={"sync_batch_id": "source_batch_id"})
    )
    wide = wide.merge(batch, on=["symbol", "report_date", "available_at"], how="left")
    for column in ("sales", "net_profit", "operating_profit"):
        if column not in wide.columns:
            wide.loc[:, column] = pd.NA
    return wide


def _merge_events_asof(grid: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    output = []
    events = events.copy()
    if "available_at" in events.columns:
        events = events.assign(available_at=pd.to_datetime(events["available_at"]))
    event_columns = [column for column in events.columns if column not in {"symbol", "available_at"}]
    output_columns = ["symbol", "as_of_date", *event_columns]
    if not event_columns:
        return grid.reset_index(drop=True)
    for symbol, symbol_grid in grid.groupby("symbol", sort=True):
        left = symbol_grid.sort_values("as_of_date", kind="stable")
        right = events.loc[events["symbol"].eq(symbol)].sort_values("available_at", kind="stable")
        if right.empty:
            merged = left.copy()
            for column in event_columns:
                merged.loc[:, column] = pd.NA
        else:
            merged = pd.merge_asof(
                left,
                right,
                left_on="as_of_date",
                right_on="available_at",
                direction="backward",
            )
            if "symbol_x" in merged.columns:
                merged = merged.rename(columns={"symbol_x": "symbol"}).drop(columns=["symbol_y"], errors="ignore")
        output.append(merged[output_columns])
    if not output:
        return grid.reset_index(drop=True)
    normalized = [frame.dropna(axis=1, how="all") for frame in output]
    return pd.concat(normalized, ignore_index=True).reindex(columns=output_columns)


def _dedupe_events(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events
    return (
        events.sort_values(["symbol", "available_at"], kind="stable")
        .drop_duplicates(["symbol", "available_at"], keep="last")
        .reset_index(drop=True)
    )


def _event_columns() -> list[str]:
    return [
        "symbol",
        "available_at",
        "ttm_sales_cr",
        "ttm_net_profit_cr",
        "ttm_operating_profit_cr",
        "source_batch_id",
    ]


def _ttm_columns() -> list[str]:
    return [
        "symbol",
        "as_of_date",
        "ttm_sales_cr",
        "ttm_net_profit_cr",
        "ttm_operating_profit_cr",
        "adjusted_equity_shares_cr",
        "earnings_source",
        "source_batch_id",
    ]


def _date_string(value: str | date) -> str:
    return str(value)[:10]


__all__ = ["TtmRefreshResult", "refresh_fundamental_ttm"]
