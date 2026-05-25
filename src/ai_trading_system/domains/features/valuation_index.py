"""Universe membership, daily stock valuation, and valuation indexes."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.features.valuation_schema import ensure_valuation_schema

DEFAULT_UNIVERSES = ("UNIV_TOP500_MCAP", "UNIV_TOP1000_MCAP")


@dataclass(frozen=True)
class ValuationIndexResult:
    stock_rows: int
    membership_rows: int
    universe_index_rows: int
    sector_rows: int
    universes: list[str]
    start_date: str | None
    end_date: str | None
    missing_earnings_rows: int
    loss_mcap_pct_max: float | None


def refresh_valuation_index(
    *,
    ohlcv_db_path: str | Path,
    master_db_path: str | Path,
    universes: list[str] | tuple[str, ...] = DEFAULT_UNIVERSES,
    from_date: str | None = None,
    to_date: str | None = None,
) -> ValuationIndexResult:
    """Refresh stock, universe, sector, and index valuation tables."""

    universe_ids = [str(universe).strip().upper() for universe in universes if str(universe).strip()]
    if not universe_ids:
        universe_ids = list(DEFAULT_UNIVERSES)
    sector_map = _load_sector_map(master_db_path)
    conn = duckdb.connect(str(ohlcv_db_path))
    try:
        ensure_valuation_schema(conn)
        prices = _load_prices(conn, from_date=from_date, to_date=to_date)
        if prices.empty:
            return ValuationIndexResult(0, 0, 0, 0, universe_ids, None, None, 0, None)
        prices.loc[:, "date"] = pd.to_datetime(prices["date"]).dt.date
        ttm = conn.execute(
            """
            SELECT symbol, as_of_date AS date, adjusted_equity_shares_cr, ttm_net_profit_cr, earnings_source
            FROM fundamental_ttm
            WHERE as_of_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            [prices["date"].min(), prices["date"].max()],
        ).df()
        if ttm.empty:
            return ValuationIndexResult(0, 0, 0, 0, universe_ids, str(prices["date"].min()), str(prices["date"].max()), 0, None)
        ttm.loc[:, "date"] = pd.to_datetime(ttm["date"]).dt.date
        base = prices.merge(ttm, on=["symbol", "date"], how="inner")
        if base.empty:
            return ValuationIndexResult(0, 0, 0, 0, universe_ids, str(prices["date"].min()), str(prices["date"].max()), 0, None)
        base.loc[:, "sector_name"] = base["symbol"].map(lambda symbol: sector_map.get(str(symbol), {}).get("sector_name", "Other"))
        base.loc[:, "industry_group"] = base["symbol"].map(lambda symbol: sector_map.get(str(symbol), {}).get("industry_group", ""))
        base.loc[:, "market_cap_cr"] = pd.to_numeric(base["close"], errors="coerce") * pd.to_numeric(
            base["adjusted_equity_shares_cr"], errors="coerce"
        )
        base.loc[:, "ttm_net_profit_cr"] = pd.to_numeric(base["ttm_net_profit_cr"], errors="coerce")
        base.loc[:, "pe_ttm"] = base["market_cap_cr"] / base["ttm_net_profit_cr"].where(base["ttm_net_profit_cr"].ne(0))
        base.loc[:, "earnings_yield"] = base["ttm_net_profit_cr"] / base["market_cap_cr"].where(base["market_cap_cr"].ne(0))
        base = base.loc[base["market_cap_cr"].notna() & base["market_cap_cr"].gt(0)].copy()

        stock_frames = []
        membership_frames = []
        for universe_id in universe_ids:
            limit = _universe_limit(universe_id)
            ranked = base.sort_values(["date", "market_cap_cr"], ascending=[True, False], kind="stable").copy()
            ranked.loc[:, "market_cap_rank"] = ranked.groupby("date").cumcount() + 1
            members = ranked.loc[ranked["market_cap_rank"].le(limit)].copy()
            members.loc[:, "universe_id"] = universe_id
            stock_frames.append(members)
            membership_frames.append(
                members[
                    [
                        "universe_id",
                        "date",
                        "symbol",
                        "sector_name",
                        "industry_group",
                        "market_cap_rank",
                    ]
                ]
                .rename(columns={"date": "as_of_date"})
                .assign(included=True, reason=f"top_{limit}_market_cap")
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO universe_definition
                VALUES (?, ?, 'market_cap_rank', ?, 1000, CURRENT_TIMESTAMP)
                """,
                [universe_id, universe_id.replace("_", " "), str(base["date"].min())],
            )

        stock = pd.concat(stock_frames, ignore_index=True) if stock_frames else pd.DataFrame()
        membership = pd.concat(membership_frames, ignore_index=True) if membership_frames else pd.DataFrame()
        _replace_range(conn, "stock_valuation_daily", stock, start=str(base["date"].min()), end=str(base["date"].max()))
        _replace_range(conn, "universe_membership", membership, date_col="as_of_date", start=str(base["date"].min()), end=str(base["date"].max()))

        universe_index = _build_universe_index(stock)
        sector_valuation = _build_sector_valuation(stock)
        _replace_range(conn, "universe_index_daily", universe_index, start=str(base["date"].min()), end=str(base["date"].max()))
        _replace_range(conn, "sector_valuation_daily", sector_valuation, start=str(base["date"].min()), end=str(base["date"].max()))
    finally:
        conn.close()

    loss_mcap_pct_max = None
    if not sector_valuation.empty and "loss_mcap_pct" in sector_valuation.columns:
        loss_mcap_pct_max = float(pd.to_numeric(sector_valuation["loss_mcap_pct"], errors="coerce").max())
    return ValuationIndexResult(
        stock_rows=len(stock),
        membership_rows=len(membership),
        universe_index_rows=len(universe_index),
        sector_rows=len(sector_valuation),
        universes=universe_ids,
        start_date=str(base["date"].min()),
        end_date=str(base["date"].max()),
        missing_earnings_rows=int(stock["ttm_net_profit_cr"].isna().sum()) if not stock.empty else 0,
        loss_mcap_pct_max=loss_mcap_pct_max,
    )


def _load_prices(conn: duckdb.DuckDBPyConnection, *, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    filters = ["exchange = 'NSE'", "COALESCE(adjusted_close, close) IS NOT NULL"]
    params: list[str] = []
    if from_date:
        filters.append("CAST(timestamp AS DATE) >= CAST(? AS DATE)")
        params.append(str(from_date)[:10])
    if to_date:
        filters.append("CAST(timestamp AS DATE) <= CAST(? AS DATE)")
        params.append(str(to_date)[:10])
    return conn.execute(
        f"""
        SELECT
            symbol_id AS symbol,
            CAST(timestamp AS DATE) AS date,
            COALESCE(adjusted_close, close) AS close
        FROM _catalog
        WHERE {' AND '.join(filters)}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol_id, CAST(timestamp AS DATE)
            ORDER BY timestamp DESC
        ) = 1
        """,
        params,
    ).df()


def _load_sector_map(master_db_path: str | Path) -> dict[str, dict[str, str]]:
    if not Path(master_db_path).exists():
        return {}
    conn = sqlite3.connect(str(master_db_path))
    try:
        rows = conn.execute(
            """
            SELECT
                s.symbol_id,
                COALESCE(sm.system_sector, s.sector, 'Other') AS sector_name,
                COALESCE(s.industry, s.sector, '') AS industry_group
            FROM symbols s
            LEFT JOIN sector_mapping sm ON s.sector = sm.industry
            WHERE s.exchange = 'NSE'
            """
        ).fetchall()
    finally:
        conn.close()
    return {
        str(symbol).upper(): {"sector_name": str(sector or "Other"), "industry_group": str(industry or "")}
        for symbol, sector, industry in rows
    }


def _universe_limit(universe_id: str) -> int:
    digits = "".join(ch for ch in universe_id if ch.isdigit())
    return int(digits or 1000)


def _build_universe_index(stock: pd.DataFrame) -> pd.DataFrame:
    if stock.empty:
        return pd.DataFrame()
    ordered = stock.sort_values(["universe_id", "symbol", "date"], kind="stable").copy()
    ordered.loc[:, "stock_return_1d"] = ordered.groupby(["universe_id", "symbol"])["close"].pct_change(fill_method=None)
    rows = []
    for universe_id, group in ordered.groupby("universe_id", sort=True):
        daily = group.groupby("date", sort=True).agg(
            constituent_count=("symbol", "nunique"),
            total_market_cap_cr=("market_cap_cr", "sum"),
            total_ttm_profit_cr=("ttm_net_profit_cr", "sum"),
        )
        daily.loc[:, "pe_ttm"] = daily["total_market_cap_cr"] / daily["total_ttm_profit_cr"].where(daily["total_ttm_profit_cr"].ne(0))
        daily.loc[:, "earnings_yield"] = daily["total_ttm_profit_cr"] / daily["total_market_cap_cr"].where(daily["total_market_cap_cr"].ne(0))
        ew_ret = group.groupby("date", sort=True)["stock_return_1d"].mean()
        weighted = group.copy()
        weighted.loc[:, "weight"] = weighted["market_cap_cr"] / weighted.groupby("date")["market_cap_cr"].transform("sum")
        mw_ret = weighted.assign(weighted_return=weighted["weight"] * weighted["stock_return_1d"]).groupby("date", sort=True)["weighted_return"].sum(min_count=1)
        for index_type, returns in {"equal_weight": ew_ret, "market_cap_weight": mw_ret}.items():
            levels = (1 + returns.fillna(0)).cumprod() * 1000.0
            frame = daily.copy()
            frame.loc[:, "universe_id"] = universe_id
            frame.loc[:, "index_type"] = index_type
            frame.loc[:, "return_1d"] = returns
            frame.loc[:, "level"] = levels
            frame = frame.reset_index()
            rows.append(
                frame[
                    [
                        "universe_id",
                        "index_type",
                        "date",
                        "level",
                        "return_1d",
                        "constituent_count",
                        "total_market_cap_cr",
                        "total_ttm_profit_cr",
                        "pe_ttm",
                        "earnings_yield",
                    ]
                ]
            )
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _build_sector_valuation(stock: pd.DataFrame) -> pd.DataFrame:
    if stock.empty:
        return pd.DataFrame()
    rows = []
    for keys, group in stock.groupby(["universe_id", "date", "sector_name"], sort=True):
        universe_id, day, sector = keys
        positive = group.loc[group["ttm_net_profit_cr"].gt(0)]
        loss = group.loc[group["ttm_net_profit_cr"].le(0) | group["ttm_net_profit_cr"].isna()]
        total_mcap = float(group["market_cap_cr"].sum())
        total_profit = float(group["ttm_net_profit_cr"].sum())
        pe_values = pd.to_numeric(group["pe_ttm"].replace([float("inf"), float("-inf")], pd.NA), errors="coerce").dropna()
        rows.append(
            {
                "universe_id": universe_id,
                "date": day,
                "sector_name": sector or "Other",
                "constituent_count": int(group["symbol"].nunique()),
                "positive_earnings_count": int(len(positive)),
                "loss_making_count": int(len(loss)),
                "total_market_cap_cr": total_mcap,
                "total_ttm_profit_cr": total_profit,
                "pe_ttm": total_mcap / total_profit if total_profit else None,
                "pe_median": float(pe_values.median()) if not pe_values.empty else None,
                "pe_trimmed_avg": _trimmed_average(pe_values),
                "earnings_yield": total_profit / total_mcap if total_mcap else None,
                "loss_mcap_pct": float(loss["market_cap_cr"].sum()) / total_mcap if total_mcap else None,
            }
        )
    return pd.DataFrame(rows)


def _trimmed_average(values: pd.Series) -> float | None:
    if values.empty:
        return None
    if len(values) < 5:
        return float(values.mean())
    lower = values.quantile(0.1)
    upper = values.quantile(0.9)
    trimmed = values.loc[values.ge(lower) & values.le(upper)]
    return float(trimmed.mean()) if not trimmed.empty else float(values.mean())


def _replace_range(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    frame: pd.DataFrame,
    *,
    start: str,
    end: str,
    date_col: str = "date",
) -> None:
    conn.execute(
        f"DELETE FROM {table_name} WHERE {date_col} BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
        [start, end],
    )
    if frame.empty:
        return
    columns_by_table = {
        "stock_valuation_daily": [
            "universe_id",
            "date",
            "symbol",
            "sector_name",
            "close",
            "adjusted_equity_shares_cr",
            "market_cap_cr",
            "ttm_net_profit_cr",
            "pe_ttm",
            "earnings_yield",
            "earnings_source",
        ],
        "universe_membership": [
            "universe_id",
            "as_of_date",
            "symbol",
            "sector_name",
            "industry_group",
            "market_cap_rank",
            "included",
            "reason",
        ],
        "universe_index_daily": [
            "universe_id",
            "index_type",
            "date",
            "level",
            "return_1d",
            "constituent_count",
            "total_market_cap_cr",
            "total_ttm_profit_cr",
            "pe_ttm",
            "earnings_yield",
        ],
        "sector_valuation_daily": [
            "universe_id",
            "date",
            "sector_name",
            "constituent_count",
            "positive_earnings_count",
            "loss_making_count",
            "total_market_cap_cr",
            "total_ttm_profit_cr",
            "pe_ttm",
            "pe_median",
            "pe_trimmed_avg",
            "earnings_yield",
            "loss_mcap_pct",
        ],
    }
    columns = columns_by_table.get(table_name, list(frame.columns))
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{table_name} frame missing required columns: {missing}")
    conn.register("_valuation_frame", frame[columns])
    try:
        conn.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) SELECT {', '.join(columns)} FROM _valuation_frame")
    finally:
        conn.unregister("_valuation_frame")


__all__ = ["DEFAULT_UNIVERSES", "ValuationIndexResult", "refresh_valuation_index"]
