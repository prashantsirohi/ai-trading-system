"""Universe-level valuation cycle features stored in fundamentals DuckDB."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.analytical_store import (
    connect_fundamentals_duckdb,
    ensure_fundamentals_analytical_schema,
)


@dataclass(frozen=True)
class UniverseValuationResult:
    rows: int
    universes: list[str]
    start_date: str | None
    end_date: str | None


def refresh_universe_valuation_daily(
    *,
    ohlcv_db_path: str | Path,
    fundamentals_db_path: str | Path | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> UniverseValuationResult:
    frame = load_universe_valuation_inputs(ohlcv_db_path=ohlcv_db_path, to_date=to_date)
    valuation_all = compute_universe_valuation_daily(frame)
    valuation = _filter_dates(valuation_all, from_date, to_date)
    conn = connect_fundamentals_duckdb(fundamentals_db_path)
    try:
        ensure_fundamentals_analytical_schema(conn)
        if not valuation.empty:
            start, end = str(valuation["date"].min())[:10], str(valuation["date"].max())[:10]
            conn.execute(
                "DELETE FROM universe_valuation_daily WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
            conn.register("_universe_valuation_frame", valuation)
            try:
                conn.execute("INSERT INTO universe_valuation_daily SELECT * FROM _universe_valuation_frame")
            finally:
                conn.unregister("_universe_valuation_frame")
        elif from_date or to_date:
            start = str(from_date or to_date)[:10]
            end = str(to_date or from_date)[:10]
            conn.execute(
                "DELETE FROM universe_valuation_daily WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
        else:
            start = end = None
    finally:
        conn.close()
    return UniverseValuationResult(
        rows=int(len(valuation)),
        universes=sorted(valuation["universe_id"].dropna().astype(str).unique()) if not valuation.empty else [],
        start_date=start,
        end_date=end,
    )


def load_universe_valuation_inputs(*, ohlcv_db_path: str | Path, to_date: str | None = None) -> pd.DataFrame:
    conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        params = [str(to_date)[:10] if to_date else None]
        return conn.execute(
            """
            WITH index_levels AS (
                SELECT
                    universe_id,
                    date,
                    MAX(CASE WHEN index_type = 'equal_weight' THEN level END) AS index_level_equal_weight,
                    MAX(CASE WHEN index_type = 'market_cap_weight' THEN level END) AS index_level_mcap_weight
                FROM universe_index_daily
                WHERE date <= COALESCE(CAST(? AS DATE), DATE '9999-12-31')
                GROUP BY universe_id, date
            )
            SELECT
                sv.universe_id,
                sv.date,
                il.index_level_equal_weight,
                il.index_level_mcap_weight,
                sv.symbol,
                sv.market_cap_cr,
                sv.ttm_net_profit_cr
            FROM stock_valuation_daily sv
            LEFT JOIN index_levels il
              ON il.universe_id = sv.universe_id
             AND il.date = sv.date
            WHERE sv.date <= COALESCE(CAST(? AS DATE), DATE '9999-12-31')
            """,
            [params[0], params[0]],
        ).df()
    finally:
        conn.close()


def compute_universe_valuation_daily(frame: pd.DataFrame) -> pd.DataFrame:
    columns = _columns()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    base = frame.copy()
    base.loc[:, "date"] = pd.to_datetime(base["date"]).dt.date
    for column in ("market_cap_cr", "ttm_net_profit_cr", "index_level_equal_weight", "index_level_mcap_weight"):
        base.loc[:, column] = pd.to_numeric(base[column], errors="coerce")
    grouped = base.groupby(["universe_id", "date"], sort=True)
    daily = grouped.agg(
        index_level_equal_weight=("index_level_equal_weight", "max"),
        index_level_mcap_weight=("index_level_mcap_weight", "max"),
        total_market_cap_cr=("market_cap_cr", "sum"),
        total_ttm_profit_cr=("ttm_net_profit_cr", "sum"),
    ).reset_index()
    profitable = base.loc[base["ttm_net_profit_cr"].gt(0)]
    profit_daily = profitable.groupby(["universe_id", "date"], sort=True).agg(
        positive_profit_market_cap_cr=("market_cap_cr", "sum"),
        positive_profit_ttm_net_profit_cr=("ttm_net_profit_cr", "sum"),
    ).reset_index()
    loss = base.loc[base["ttm_net_profit_cr"].le(0) | base["ttm_net_profit_cr"].isna()]
    loss_daily = loss.groupby(["universe_id", "date"], sort=True).agg(
        loss_making_market_cap_cr=("market_cap_cr", "sum"),
    ).reset_index()
    daily = daily.merge(profit_daily, on=["universe_id", "date"], how="left").merge(
        loss_daily, on=["universe_id", "date"], how="left"
    )
    daily.loc[:, "positive_profit_market_cap_cr"] = daily["positive_profit_market_cap_cr"].fillna(0.0)
    daily.loc[:, "positive_profit_ttm_net_profit_cr"] = daily["positive_profit_ttm_net_profit_cr"].fillna(0.0)
    daily.loc[:, "loss_making_market_cap_cr"] = daily["loss_making_market_cap_cr"].fillna(0.0)
    daily.loc[:, "pe_ttm"] = daily["positive_profit_market_cap_cr"] / daily["positive_profit_ttm_net_profit_cr"].where(
        daily["positive_profit_ttm_net_profit_cr"].gt(0)
    )
    daily.loc[:, "earnings_yield"] = daily["positive_profit_ttm_net_profit_cr"] / daily["positive_profit_market_cap_cr"].where(
        daily["positive_profit_market_cap_cr"].gt(0)
    )
    daily.loc[:, "loss_mcap_pct"] = daily["loss_making_market_cap_cr"] / daily["total_market_cap_cr"].where(
        daily["total_market_cap_cr"].gt(0)
    )
    daily = daily.sort_values(["universe_id", "date"], kind="stable")
    pieces = []
    for _, group in daily.groupby("universe_id", sort=True):
        group = group.copy()
        pe = pd.to_numeric(group["pe_ttm"], errors="coerce")
        group.loc[:, "pe_200dma"] = pe.rolling(200, min_periods=20).mean()
        group.loc[:, "pe_1y_median"] = pe.rolling(252, min_periods=20).median()
        group.loc[:, "pe_3y_median"] = pe.rolling(756, min_periods=60).median()
        group.loc[:, "pe_5y_median"] = pe.rolling(1260, min_periods=60).median()
        group.loc[:, "pe_zscore_3y"] = _zscore(pe, 756, 60)
        group.loc[:, "pe_zscore_5y"] = _zscore(pe, 1260, 60)
        group.loc[:, "pe_percentile_3y"] = pe.rolling(756, min_periods=60).apply(_last_percentile, raw=False)
        group.loc[:, "pe_percentile_5y"] = pe.rolling(1260, min_periods=60).apply(_last_percentile, raw=False)
        group.loc[:, "valuation_zone"] = group["pe_percentile_5y"].combine_first(group["pe_percentile_3y"]).map(valuation_zone)
        pieces.append(group)
    result = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    result.loc[:, "created_at"] = pd.Timestamp.utcnow()
    return result[columns].reset_index(drop=True)


def valuation_zone(percentile: float | None) -> str:
    if percentile is None or pd.isna(percentile):
        return "unknown"
    value = float(percentile)
    if value < 10:
        return "deep_value_panic"
    if value < 25:
        return "cheap"
    if value < 60:
        return "fair"
    if value < 80:
        return "expensive"
    if value < 90:
        return "late_bull"
    return "bubble_top_risk"


def _zscore(values: pd.Series, window: int, min_periods: int) -> pd.Series:
    mean = values.rolling(window, min_periods=min_periods).mean()
    std = values.rolling(window, min_periods=min_periods).std()
    return (values - mean) / std.where(std.ne(0))


def _last_percentile(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return float("nan")
    return float(clean.rank(pct=True).iloc[-1] * 100.0)


def _filter_dates(frame: pd.DataFrame, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    dates = pd.to_datetime(out["date"]).dt.date
    if from_date:
        out = out.loc[dates >= pd.Timestamp(from_date).date()]
        dates = pd.to_datetime(out["date"]).dt.date
    if to_date:
        out = out.loc[dates <= pd.Timestamp(to_date).date()]
    return out.reset_index(drop=True)


def _columns() -> list[str]:
    return [
        "universe_id",
        "date",
        "index_level_equal_weight",
        "index_level_mcap_weight",
        "total_market_cap_cr",
        "total_ttm_profit_cr",
        "positive_profit_market_cap_cr",
        "loss_making_market_cap_cr",
        "pe_ttm",
        "earnings_yield",
        "loss_mcap_pct",
        "pe_200dma",
        "pe_1y_median",
        "pe_3y_median",
        "pe_5y_median",
        "pe_zscore_3y",
        "pe_zscore_5y",
        "pe_percentile_3y",
        "pe_percentile_5y",
        "valuation_zone",
        "created_at",
    ]


__all__ = [
    "UniverseValuationResult",
    "compute_universe_valuation_daily",
    "load_universe_valuation_inputs",
    "refresh_universe_valuation_daily",
    "valuation_zone",
]
