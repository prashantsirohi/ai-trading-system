"""Load OHLCV/metadata and build custom sector indices."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd


SYMBOL_COLUMNS = ("symbol_id", "symbol", "Symbol", "SYMBOL")
DATE_COLUMNS = ("date", "timestamp", "trade_date", "Date", "TIMESTAMP")
CLOSE_COLUMNS = ("close", "Close", "CLOSE")
VOLUME_COLUMNS = ("volume", "Volume", "VOLUME", "ttl_trd_qnty")
EXCHANGE_COLUMNS = ("exchange", "Exchange", "EXCHANGE")

MARKET_CAP_COLUMNS = (
    "market_cap",
    "market_cap_cr",
    "total_market_cap",
    "Market Cap",
    "Market Capitalization",
    "mcap",
)
COMPANY_COLUMNS = ("company_name", "Company Name", "Company", "name", "security_name")
SECTOR_COLUMNS = ("sector", "Sector", "system_sector", "broad_sector", "macro_sector")
INDUSTRY_COLUMNS = (
    "industry",
    "Industry",
    "industry_group",
    "sub_sector",
    "Sub Sector",
    "nse_industry",
    "business_group",
)
THEME_COLUMNS = ("theme", "Theme", "basket_theme", "custom_theme")


def load_ohlcv_catalog(
    ohlcv_db_path: str | Path,
    *,
    run_date: str | None = None,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """Load normalized OHLCV rows from ``_catalog``."""
    db_path = Path(ohlcv_db_path)
    if not db_path.exists():
        return _empty_ohlcv()

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        if not _table_exists(conn, "_catalog"):
            return _empty_ohlcv()
        columns = _duckdb_columns(conn, "_catalog")
        symbol_col = _first_existing(columns, SYMBOL_COLUMNS)
        date_col = _first_existing(columns, DATE_COLUMNS)
        close_col = _first_existing(columns, CLOSE_COLUMNS)
        volume_col = _first_existing(columns, VOLUME_COLUMNS)
        exchange_col = _first_existing(columns, EXCHANGE_COLUMNS)
        if not symbol_col or not date_col or not close_col:
            return _empty_ohlcv()

        select_parts = [
            f'"{symbol_col}" AS symbol',
            f'CAST("{date_col}" AS DATE) AS date',
            f'CAST("{close_col}" AS DOUBLE) AS close',
        ]
        if volume_col:
            select_parts.append(f'CAST("{volume_col}" AS DOUBLE) AS volume')
        else:
            select_parts.append("CAST(NULL AS DOUBLE) AS volume")
        if exchange_col:
            select_parts.append(f'CAST("{exchange_col}" AS VARCHAR) AS exchange')
        else:
            select_parts.append("'NSE' AS exchange")

        filters = []
        params: list[object] = []
        if run_date:
            filters.append(f'CAST("{date_col}" AS DATE) <= CAST(? AS DATE)')
            params.append(run_date)
        if exchange_col and exchange:
            filters.append(f'("{exchange_col}" IS NULL OR "{exchange_col}" = ?)')
            params.append(exchange)
        where = f" WHERE {' AND '.join(filters)}" if filters else ""
        query = f"SELECT {', '.join(select_parts)} FROM _catalog{where}"
        frame = conn.execute(query, params).fetchdf()
    finally:
        conn.close()

    if frame.empty:
        return _empty_ohlcv()
    frame.loc[:, "symbol"] = frame["symbol"].astype(str).str.strip()
    frame.loc[:, "date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame.loc[:, "close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame.loc[:, "volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "date", "close"])
    frame = frame.sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)
    return frame


def load_symbol_metadata(master_db_path: str | Path, *, exchange: str = "NSE") -> pd.DataFrame:
    """Load symbol metadata, preferring ``stock_details`` and falling back to ``symbols``."""
    db_path = Path(master_db_path)
    if not db_path.exists():
        return _empty_metadata()
    conn = sqlite3.connect(db_path)
    try:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "stock_details" in tables:
            stock_details = _load_stock_details(conn, exchange=exchange)
            if not stock_details.empty:
                return stock_details
        if "symbols" in tables:
            return _load_symbols_metadata(conn, exchange=exchange, has_sector_mapping="sector_mapping" in tables)
    finally:
        conn.close()
    return _empty_metadata()


def attach_metadata(ohlcv: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    """Attach metadata and fill missing sector/industry values with ``Other``."""
    frame = ohlcv.copy()
    if metadata is None or metadata.empty:
        frame.loc[:, "company_name"] = frame["symbol"]
        frame.loc[:, "sector"] = "Other"
        frame.loc[:, "industry"] = "Other"
        frame.loc[:, "market_cap"] = pd.NA
        return frame[["symbol", "date", "close", "volume", "exchange", "company_name", "sector", "industry", "market_cap"]]
    merged = frame.merge(metadata, on="symbol", how="left")
    merged.loc[:, "company_name"] = merged["company_name"].fillna(merged["symbol"])
    for column in ("sector", "industry"):
        if column not in merged.columns:
            merged.loc[:, column] = "Other"
        merged.loc[:, column] = _clean_group_label(merged[column])
    sector_missing = merged["sector"].eq("Other") & merged["industry"].ne("Other")
    industry_missing = merged["industry"].eq("Other") & merged["sector"].ne("Other")
    merged.loc[sector_missing, "sector"] = merged.loc[sector_missing, "industry"]
    merged.loc[industry_missing, "industry"] = merged.loc[industry_missing, "sector"]
    merged.loc[:, "market_cap"] = pd.to_numeric(merged["market_cap"], errors="coerce")
    columns = ["symbol", "date", "close", "volume", "exchange", "company_name", "sector", "industry", "market_cap"]
    if "theme" in merged.columns:
        merged.loc[:, "theme"] = _clean_group_label(merged["theme"])
        columns.append("theme")
    return merged[columns]


def build_rotation_indices(
    enriched_ohlcv: pd.DataFrame,
    *,
    group_col: str,
    group_type: str,
    parent_col: str | None = None,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Build 100-based custom indices for any rotation grouping level."""
    columns = [
        "date",
        "rotation_group_type",
        "rotation_group_name",
        "parent_sector",
        "rotation_index",
        "weighting_method",
        "constituent_count",
    ]
    if enriched_ohlcv is None or enriched_ohlcv.empty or group_col not in enriched_ohlcv.columns:
        return pd.DataFrame(columns=columns), {}

    data = enriched_ohlcv.copy()
    data.loc[:, "date"] = pd.to_datetime(data["date"], errors="coerce").astype("datetime64[ns]")
    data = data.dropna(subset=["date", "symbol", "close"])
    if data.empty:
        return pd.DataFrame(columns=columns), {}
    data.loc[:, group_col] = _clean_group_label(data[group_col])
    if parent_col and parent_col in data.columns:
        data.loc[:, parent_col] = _clean_group_label(data[parent_col])
    data.loc[:, "_date_order"] = data["date"].astype("int64")

    latest_idx = data.groupby("symbol", sort=False)["_date_order"].idxmax()
    latest_caps = pd.to_numeric(data.loc[latest_idx].set_index("symbol").get("market_cap"), errors="coerce")
    records: list[dict[str, object]] = []
    methods: dict[str, str] = {}
    for group_name, group_rows in data.groupby(group_col, dropna=False):
        group_text = str(group_name or "Other").strip() or "Other"
        pivot = group_rows.pivot_table(index="date", columns="symbol", values="close", aggfunc="last").sort_index()
        if pivot.empty:
            continue
        symbols = [str(symbol) for symbol in pivot.columns]
        caps = pd.to_numeric(latest_caps.reindex(symbols), errors="coerce") if latest_caps is not None else pd.Series(dtype=float)
        if caps.notna().any() and float(caps.fillna(0).sum()) > 0:
            weights = (caps.fillna(0) / caps.fillna(0).sum()).astype(float)
            method = "market_cap"
        else:
            weights = pd.Series(1.0 / len(symbols), index=symbols, dtype=float)
            method = "equal_weight"
        weighted_close = pivot.ffill().mul(weights, axis=1).sum(axis=1, min_count=1)
        base = _first_positive(weighted_close)
        if base is None:
            continue
        parent_sector = group_text
        if parent_col and parent_col in group_rows.columns:
            parents = _clean_group_label(group_rows[parent_col]).dropna()
            if not parents.empty:
                parent_sector = str(parents.mode().iloc[0] if not parents.mode().empty else parents.iloc[-1])
        index_values = 100.0 * weighted_close / base
        methods[group_text] = method
        for date_value, rotation_index in index_values.dropna().items():
            record = {
                "date": pd.Timestamp(date_value).date().isoformat(),
                "rotation_group_type": group_type,
                "rotation_group_name": group_text,
                "parent_sector": parent_sector,
                "rotation_index": float(rotation_index),
                "weighting_method": method,
                "constituent_count": len(symbols),
            }
            if group_type == "industry":
                record["industry"] = group_text
                record["sector_index"] = float(rotation_index)
            if group_type == "sector":
                record["sector"] = group_text
                record["sector_index"] = float(rotation_index)
            records.append(record)
    output = pd.DataFrame.from_records(records)
    return output.sort_values(["rotation_group_type", "rotation_group_name", "date"], kind="stable").reset_index(drop=True), methods


def build_sector_custom_indices(enriched_ohlcv: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    """Build one 100-based custom index per industry."""
    return build_rotation_indices(
        enriched_ohlcv,
        group_col="industry",
        group_type="industry",
        parent_col="sector",
    )


def build_benchmark_index(
    ohlcv: pd.DataFrame,
    custom_indices: pd.DataFrame,
    *,
    metadata: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, str]:
    """Resolve the benchmark using the requested priority order."""
    if ohlcv is None or ohlcv.empty:
        return pd.DataFrame(columns=["date", "benchmark_index"]), "missing"

    symbol_upper = ohlcv["symbol"].astype(str).str.upper()
    for preferred in ("NIFTY 500", "NIFTY 50"):
        mask = symbol_upper == preferred
        if mask.any():
            bench = _index_from_series(ohlcv.loc[mask, ["date", "close"]], "close")
            if not bench.empty:
                return bench, preferred

    univ = _universe_equal_weight_benchmark(ohlcv, metadata=metadata, max_symbols=1000)
    if not univ.empty:
        return univ, "UNIV_TOP1000"

    if custom_indices is not None and not custom_indices.empty:
        group_column = "rotation_group_name" if "rotation_group_name" in custom_indices.columns else "industry"
        value_column = "rotation_index" if "rotation_index" in custom_indices.columns else "sector_index"
        pivot = custom_indices.pivot_table(index="date", columns=group_column, values=value_column, aggfunc="last")
        series = pivot.sort_index().ffill().mean(axis=1)
        bench = pd.DataFrame({"date": pd.to_datetime(series.index), "benchmark_index": series.to_numpy(dtype=float)})
        bench.loc[:, "date"] = bench["date"].dt.date.astype(str)
        return bench, "custom_sector_equal_weight"
    return pd.DataFrame(columns=["date", "benchmark_index"]), "missing"


def _load_stock_details(conn: sqlite3.Connection, *, exchange: str) -> pd.DataFrame:
    columns = _sqlite_columns(conn, "stock_details")
    symbol_col = _first_existing(columns, SYMBOL_COLUMNS)
    sector_col = _first_existing(columns, SECTOR_COLUMNS)
    industry_col = _first_existing(columns, INDUSTRY_COLUMNS)
    company_col = _first_existing(columns, COMPANY_COLUMNS)
    market_cap_col = _first_existing(columns, MARKET_CAP_COLUMNS)
    theme_col = _first_existing(columns, THEME_COLUMNS)
    exchange_col = _first_existing(columns, EXCHANGE_COLUMNS)
    if not symbol_col:
        return _empty_metadata()
    select_parts = [
        f'"{symbol_col}" AS symbol',
        f'"{sector_col}" AS sector' if sector_col else "NULL AS sector",
        f'"{industry_col}" AS industry' if industry_col else "'Other' AS industry",
        f'"{company_col}" AS company_name' if company_col else f'"{symbol_col}" AS company_name',
        f'"{market_cap_col}" AS market_cap' if market_cap_col else "NULL AS market_cap",
    ]
    if theme_col:
        select_parts.append(f'"{theme_col}" AS theme')
    filters = []
    params: list[object] = []
    if exchange_col and exchange:
        filters.append(f'("{exchange_col}" IS NULL OR "{exchange_col}" = ?)')
        params.append(exchange)
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    return _normalize_metadata(pd.read_sql_query(f"SELECT {', '.join(select_parts)} FROM stock_details{where}", conn, params=params))


def _load_symbols_metadata(
    conn: sqlite3.Connection,
    *,
    exchange: str,
    has_sector_mapping: bool,
) -> pd.DataFrame:
    columns = _sqlite_columns(conn, "symbols")
    symbol_col = _first_existing(columns, SYMBOL_COLUMNS)
    sector_col = _first_existing(columns, SECTOR_COLUMNS)
    industry_col = _first_existing(columns, INDUSTRY_COLUMNS)
    legacy_group_col = _first_existing(columns, ("sector", "Sector", "industry", "Industry"))
    company_col = _first_existing(columns, COMPANY_COLUMNS)
    market_cap_col = _first_existing(columns, MARKET_CAP_COLUMNS)
    theme_col = _first_existing(columns, THEME_COLUMNS)
    exchange_col = _first_existing(columns, EXCHANGE_COLUMNS)
    if not symbol_col:
        return _empty_metadata()
    mapping_col = legacy_group_col or industry_col or sector_col
    if mapping_col and has_sector_mapping:
        sector_expr = f"COALESCE(sm.system_sector, s.\"{sector_col}\", s.\"{mapping_col}\", 'Other') AS sector" if sector_col else f"COALESCE(sm.system_sector, s.\"{mapping_col}\", 'Other') AS sector"
        industry_source = industry_col or legacy_group_col or sector_col
        industry_expr = f'COALESCE(s."{industry_source}", sm.system_sector, \'Other\') AS industry'
        join = f' LEFT JOIN sector_mapping sm ON s."{mapping_col}" = sm.industry'
    elif sector_col or industry_col or legacy_group_col:
        sector_source = sector_col or legacy_group_col or industry_col
        industry_source = industry_col or legacy_group_col or sector_col
        sector_expr = f'COALESCE(s."{sector_source}", \'Other\') AS sector'
        industry_expr = f'COALESCE(s."{industry_source}", \'Other\') AS industry'
        join = ""
    else:
        sector_expr = "'Other' AS sector"
        industry_expr = "'Other' AS industry"
        join = ""
    select_parts = [
        f's."{symbol_col}" AS symbol',
        sector_expr,
        industry_expr,
        f's."{company_col}" AS company_name' if company_col else f's."{symbol_col}" AS company_name',
        f's."{market_cap_col}" AS market_cap' if market_cap_col else "NULL AS market_cap",
    ]
    if theme_col:
        select_parts.append(f's."{theme_col}" AS theme')
    filters = []
    params: list[object] = []
    if exchange_col and exchange:
        filters.append(f'(s."{exchange_col}" IS NULL OR s."{exchange_col}" = ?)')
        params.append(exchange)
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    return _normalize_metadata(pd.read_sql_query(f"SELECT {', '.join(select_parts)} FROM symbols s{join}{where}", conn, params=params))


def _normalize_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return _empty_metadata()
    output = frame.copy()
    output.loc[:, "symbol"] = output["symbol"].astype(str).str.strip()
    output.loc[:, "company_name"] = output["company_name"].fillna(output["symbol"]).astype(str)
    if "sector" not in output.columns:
        output.loc[:, "sector"] = "Other"
    if "industry" not in output.columns:
        output.loc[:, "industry"] = "Other"
    output.loc[:, "sector"] = _clean_group_label(output["sector"])
    output.loc[:, "industry"] = _clean_group_label(output["industry"])
    sector_missing = output["sector"].eq("Other") & output["industry"].ne("Other")
    industry_missing = output["industry"].eq("Other") & output["sector"].ne("Other")
    output.loc[sector_missing, "sector"] = output.loc[sector_missing, "industry"]
    output.loc[industry_missing, "industry"] = output.loc[industry_missing, "sector"]
    output.loc[:, "market_cap"] = pd.to_numeric(output["market_cap"], errors="coerce")
    columns = ["symbol", "company_name", "sector", "industry", "market_cap"]
    if "theme" in output.columns:
        output.loc[:, "theme"] = _clean_group_label(output["theme"])
        columns.append("theme")
    return output.drop_duplicates(subset=["symbol"], keep="last")[columns]


def _clean_group_label(series: pd.Series) -> pd.Series:
    cleaned = series.fillna("Other").astype(str).str.strip()
    cleaned = cleaned.mask(cleaned.eq("") | cleaned.str.lower().isin({"nan", "none", "null"}), "Other")
    return cleaned


def _universe_equal_weight_benchmark(
    ohlcv: pd.DataFrame,
    *,
    metadata: pd.DataFrame | None,
    max_symbols: int,
) -> pd.DataFrame:
    ohlcv = ohlcv.copy()
    ohlcv.loc[:, "date"] = pd.to_datetime(ohlcv["date"], errors="coerce").astype("datetime64[ns]")
    ohlcv = ohlcv.dropna(subset=["date"])
    if ohlcv.empty:
        return pd.DataFrame(columns=["date", "benchmark_index"])
    ohlcv.loc[:, "_date_order"] = ohlcv["date"].astype("int64")
    latest_idx = ohlcv.groupby("symbol", sort=False)["_date_order"].idxmax()
    latest_symbols = ohlcv.loc[latest_idx].copy()
    if metadata is not None and not metadata.empty and "market_cap" in metadata.columns:
        cap_lookup = metadata.set_index("symbol")["market_cap"]
        latest_symbols = latest_symbols.assign(_market_cap=latest_symbols["symbol"].map(cap_lookup))
        latest_symbols = latest_symbols.sort_values(["_market_cap", "symbol"], ascending=[False, True], na_position="last")
    else:
        latest_symbols = latest_symbols.sort_values("symbol", kind="stable")
    universe = set(latest_symbols.head(max_symbols)["symbol"].astype(str))
    pivot = (
        ohlcv.loc[ohlcv["symbol"].astype(str).isin(universe)]
        .pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
        .sort_index()
    )
    if pivot.empty:
        return pd.DataFrame(columns=["date", "benchmark_index"])
    normalized = pivot.ffill().apply(lambda col: 100.0 * col / _first_positive(col) if _first_positive(col) else pd.NA)
    series = normalized.mean(axis=1)
    return pd.DataFrame({"date": pd.to_datetime(series.index).date.astype(str), "benchmark_index": series.to_numpy(dtype=float)})


def _index_from_series(frame: pd.DataFrame, value_column: str) -> pd.DataFrame:
    values = frame.sort_values("date", kind="stable").dropna(subset=[value_column])
    if values.empty:
        return pd.DataFrame(columns=["date", "benchmark_index"])
    base = _first_positive(values[value_column])
    if base is None:
        return pd.DataFrame(columns=["date", "benchmark_index"])
    output = values[["date", value_column]].copy()
    output.loc[:, "benchmark_index"] = 100.0 * pd.to_numeric(output[value_column], errors="coerce") / base
    output.loc[:, "date"] = pd.to_datetime(output["date"]).dt.date.astype(str)
    return output[["date", "benchmark_index"]]


def _first_positive(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce")
    positive = values[values > 0].dropna()
    if positive.empty:
        return None
    return float(positive.iloc[0])


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _duckdb_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()]


def _sqlite_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()]


def _first_existing(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    existing = {str(column).lower(): str(column) for column in columns}
    for candidate in candidates:
        found = existing.get(str(candidate).lower())
        if found:
            return found
    return None


def _empty_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(columns=["symbol", "date", "close", "volume", "exchange"])


def _empty_metadata() -> pd.DataFrame:
    return pd.DataFrame(columns=["symbol", "company_name", "sector", "industry", "market_cap"])
