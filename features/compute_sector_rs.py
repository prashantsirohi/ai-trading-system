import pandas as pd
import duckdb
import sqlite3
from pathlib import Path
from core.logging import logger


def load_all_symbols_with_sector(masterdb_path: str = "data/masterdata.db"):
    """Load all symbols from stock_details with sector mapping"""
    conn = sqlite3.connect(masterdb_path)

    rows = conn.execute("""
        SELECT Symbol, Sector
        FROM stock_details
        WHERE Symbol IS NOT NULL
    """).fetchall()

    conn.close()

    sector_map = {sym: sector for sym, sector in rows if sector}

    return sector_map


def load_liquidity_filtered_symbols(
    db_path: str,
    sector_map: dict[str, str],
    top_n: int = 800,
    min_recent_days: int = 180,
    lookback_days: int = 365,
) -> list[str]:
    """Select a broad but liquid sector-strength universe from recent OHLCV history.

    The production default is the top `top_n` symbols by median traded value over the
    recent lookback window, with a minimum observation threshold so thin/new listings
    do not distort the sector breadth signal.
    """
    conn_duck = duckdb.connect(db_path, read_only=True)
    max_ts = conn_duck.execute("SELECT MAX(timestamp) FROM _catalog").fetchone()[0]
    if max_ts is None:
        conn_duck.close()
        return []

    lookback_start = pd.Timestamp(max_ts).normalize() - pd.Timedelta(days=lookback_days)
    query = """
        SELECT
            symbol_id,
            MEDIAN(close * volume) AS median_turnover,
            AVG(close * volume) AS avg_turnover,
            COUNT(*) AS recent_days
        FROM _catalog
        WHERE timestamp >= ?
          AND close IS NOT NULL
          AND volume IS NOT NULL
          AND volume > 0
        GROUP BY symbol_id
        HAVING COUNT(*) >= ?
        ORDER BY median_turnover DESC, avg_turnover DESC
    """
    liq_df = conn_duck.execute(query, [lookback_start, min_recent_days]).df()
    conn_duck.close()
    if liq_df.empty:
        return []

    liq_df["symbol_id"] = liq_df["symbol_id"].astype(str)
    liq_df = liq_df[liq_df["symbol_id"].isin(sector_map)]
    liq_df = liq_df.head(top_n)

    logger.info(
        "Selected %s liquid symbols using %s-day lookback and >=%s recent days",
        len(liq_df),
        lookback_days,
        min_recent_days,
    )
    return liq_df["symbol_id"].tolist()


def _resolve_liquidity_universe(
    db_path: str,
    sector_map: dict[str, str],
    universe_top_n: int,
    min_recent_days: int,
    lookback_days: int,
) -> tuple[list[str], int]:
    """Return a liquidity-filtered universe, relaxing thresholds when history is shallow."""
    attempts = [
        (universe_top_n, min_recent_days, lookback_days),
        (universe_top_n, min(90, min_recent_days), min(180, lookback_days)),
        (universe_top_n, min(30, min_recent_days), min(90, lookback_days)),
        (min(400, universe_top_n), min(10, min_recent_days), min(30, lookback_days)),
    ]

    for top_n, min_days, lb_days in attempts:
        symbols = load_liquidity_filtered_symbols(
            db_path=db_path,
            sector_map=sector_map,
            top_n=top_n,
            min_recent_days=min_days,
            lookback_days=lb_days,
        )
        if symbols:
            return symbols, min_days

    logger.warning(
        "Liquidity filter returned no symbols across adaptive attempts; falling back to all mapped symbols."
    )
    return list(sector_map.keys()), 0


def _resolve_rs_lookbacks(available_days: int) -> list[int]:
    """Choose RS lookbacks that fit the currently available history."""
    preferred = [20, 50, 100]
    usable = [lb for lb in preferred if lb < available_days]
    if usable:
        return usable

    if available_days >= 8:
        return [3, 5, min(7, available_days - 1)]
    if available_days >= 5:
        return [2, 3, available_days - 1]
    if available_days >= 3:
        return [1, 2]
    return []


def compute_all_symbols_rs(
    db_path: str = "data/ohlcv.duckdb",
    feature_store_dir: str = "data/feature_store",
    masterdb_path: str = "data/masterdata.db",
    universe_top_n: int = 800,
    min_recent_days: int = 180,
    lookback_days: int = 365,
):
    logger.info("=" * 60)
    logger.info("Sector RS Analysis - Liquidity-Filtered Broad Universe")
    logger.info("=" * 60)

    logger.info("Step 1: Loading sector mapping from stock_details...")
    sector_map = load_all_symbols_with_sector(masterdb_path=masterdb_path)
    logger.info("Mapped %s stocks to sectors", len(sector_map))

    sector_counts = {}
    for sym, sector in sector_map.items():
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    logger.info("Sector distribution:")
    for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        logger.info("  %s: %s", sector, count)

    logger.info("Step 2: Selecting liquidity-filtered sector universe...")
    all_symbols, effective_min_recent_days = _resolve_liquidity_universe(
        db_path=db_path,
        sector_map=sector_map,
        universe_top_n=universe_top_n,
        min_recent_days=min_recent_days,
        lookback_days=lookback_days,
    )
    logger.info("Sector-strength universe contains %s symbols", len(all_symbols))
    if effective_min_recent_days and effective_min_recent_days != min_recent_days:
        logger.info(
            "Adaptive liquidity fallback used min_recent_days=%s instead of %s",
            effective_min_recent_days,
            min_recent_days,
        )

    logger.info("Step 3: Fetching OHLCV data from DuckDB...")
    conn_duck = duckdb.connect(db_path, read_only=True)
    query = f"""
        SELECT symbol_id, timestamp, close
        FROM _catalog
        WHERE symbol_id IN ({",".join([repr(s) for s in all_symbols])})
        ORDER BY symbol_id, timestamp
    """
    ohlcv = conn_duck.execute(query).df()
    conn_duck.close()
    logger.info("Fetched %s rows for %s symbols", len(ohlcv), len(all_symbols))
    if ohlcv.empty:
        logger.warning("No OHLCV rows available for sector RS; writing empty artifacts.")
        return _write_empty_outputs(feature_store_dir=feature_store_dir)

    logger.info("Step 4: Deduplicating by date and pivoting...")
    ohlcv["date"] = pd.to_datetime(ohlcv["timestamp"]).dt.normalize()
    ohlcv = ohlcv.drop_duplicates(subset=["date", "symbol_id"], keep="last")
    close_df = ohlcv.pivot(index="date", columns="symbol_id", values="close")
    close_df = close_df.sort_index()
    close_df = close_df.dropna(how="all", axis=1)
    logger.info("Wide format: %s", close_df.shape)
    if close_df.empty:
        logger.warning("Pivoted close matrix is empty; writing empty sector RS artifacts.")
        return _write_empty_outputs(feature_store_dir=feature_store_dir)

    logger.info("Step 5: Computing daily returns...")
    returns = close_df.pct_change(fill_method=None)
    logger.info("Returns shape: %s", returns.shape)

    logger.info("Step 6: Computing Equal-Weight Index returns...")
    ew_index_ret = returns.mean(axis=1)
    logger.info("EW Index returns: %s days", len(ew_index_ret))

    logger.info("Step 7: Converting to EW Index price (base=100)...")
    ew_index = (1 + ew_index_ret).cumprod() * 100
    ew_index.name = "ew_index"
    logger.info("EW Index range: %.2f to %.2f", ew_index.min(), ew_index.max())

    logger.info("Step 8: Computing RS features...")
    lookbacks = _resolve_rs_lookbacks(len(close_df.index))
    if not lookbacks:
        logger.warning(
            "Not enough history (%s days) to compute sector RS; writing empty artifacts.",
            len(close_df.index),
        )
        return _write_empty_outputs(feature_store_dir=feature_store_dir)
    logger.info("Using adaptive RS lookbacks: %s", lookbacks)
    rs_dict = {}

    for lb in lookbacks:
        stock_ret = close_df / close_df.shift(lb)
        index_ret = ew_index / ew_index.shift(lb)
        rs = stock_ret.div(index_ret, axis=0)
        rs_dict[lb] = rs
        logger.info("RS_%s: %s", lb, rs.shape)

    logger.info("Step 9: Computing RS ranks and combined...")
    rank_frames = [rs_dict[lb].rank(axis=1, pct=True) for lb in lookbacks]
    rs_combined = pd.concat(rank_frames, keys=lookbacks).groupby(level=1).mean()
    logger.info("Combined RS: %s", rs_combined.shape)

    logger.info("Step 10: Aggregating RS per sector...")
    rs_sector = rs_combined.T.groupby(sector_map).mean().T
    logger.info("Sector RS shape: %s", rs_sector.shape)
    if rs_sector.empty:
        logger.warning("Sector RS computation produced no rows; writing empty artifacts.")
        return _write_empty_outputs(feature_store_dir=feature_store_dir)

    logger.info("Step 11: Ranking sectors...")
    sector_rank = rs_sector.rank(axis=1, pct=True)
    logger.info("Strong sectors (>70th percentile):")
    latest_sector_rank = sector_rank.iloc[-1]
    strong_sectors = latest_sector_rank[latest_sector_rank > 0.7].sort_values(
        ascending=False
    )
    for sector, rank in strong_sectors.items():
        logger.info("  %s: %.2f%%", sector, rank * 100)
    logger.info("Weak sectors (<30th percentile):")
    weak_sectors = latest_sector_rank[latest_sector_rank < 0.3].sort_values()
    for sector, rank in weak_sectors.items():
        logger.info("  %s: %.2f%%", sector, rank * 100)

    logger.info("Step 12: Computing stock RS vs sector...")
    rs_vs_sector = pd.DataFrame(
        index=rs_combined.index, columns=rs_combined.columns, dtype=float
    )
    for sector in rs_sector.columns:
        sector_stocks = [s for s in rs_combined.columns if sector_map.get(s) == sector]
        if sector_stocks:
            rs_vs_sector[sector_stocks] = (
                rs_combined[sector_stocks].values - rs_sector[sector].values[:, None]
            )
    logger.info("RS vs Sector shape: %s", rs_vs_sector.shape)

    logger.info("Step 13: Applying triple confirmation filters...")
    logger.info("Filter 1: Strong sector (>70th percentile)")
    strong_sector_names = latest_sector_rank[latest_sector_rank > 0.7].index.tolist()
    sector_filter = pd.Series(
        [
            sector_map.get(c, "Other") in strong_sector_names
            for c in rs_combined.columns
        ],
        index=rs_combined.columns,
    )
    logger.info("Stocks in strong sectors: %s", sector_filter.sum())

    logger.info("Filter 2: Stock stronger than sector (>0)")
    stock_filter = rs_vs_sector.iloc[-1] > 0
    logger.info("Stocks outperforming sector: %s", stock_filter.sum())

    logger.info("Filter 3: Absolute strength (>0.5)")
    absolute_filter = rs_combined.iloc[-1] > 0.5
    logger.info("Stocks with absolute strength: %s", absolute_filter.sum())

    logger.info("Final signal (all 3 filters):")
    final_signal_mask = sector_filter & stock_filter & absolute_filter
    final_count = final_signal_mask.sum()
    logger.info("Stocks meeting all criteria: %s", final_count)

    logger.info("Step 14: Getting top stocks...")
    logger.info("Top stocks meeting all criteria:")

    final_stocks = []
    latest_scores = rs_combined.iloc[-1]
    latest_sector_ranks = sector_rank.iloc[-1]
    latest_vs_sector = rs_vs_sector.iloc[-1]

    for sym in rs_combined.columns:
        if final_signal_mask[sym]:
            final_stocks.append(
                {
                    "Symbol": sym,
                    "Sector": sector_map[sym],
                    "RS_Score": latest_scores[sym],
                    "Vs_Sector": latest_vs_sector[sym],
                    "Sector_Rank": latest_sector_ranks[sector_map[sym]],
                }
            )

    if final_stocks:
        result_df = pd.DataFrame(final_stocks).sort_values("RS_Score", ascending=False)
        result_df["RS_Score"] = result_df["RS_Score"].round(3)
        result_df["Vs_Sector"] = result_df["Vs_Sector"].round(3)
        result_df["Sector_Rank"] = result_df["Sector_Rank"].round(1)
        logger.info("\n%s", result_df.head(50).to_string(index=False))
    else:
        logger.info("No stocks meet all criteria.")
        result_df = pd.DataFrame(final_stocks)

    logger.info("Step 15: Saving features...")
    output_dir = Path(feature_store_dir) / "all_symbols"
    output_dir.mkdir(parents=True, exist_ok=True)

    rs_sector.index = pd.to_datetime(rs_sector.index).normalize()
    rs_sector = rs_sector[~rs_sector.index.duplicated(keep="last")]
    rs_sector = rs_sector.dropna(how="all")
    rs_sector.to_parquet(output_dir / "sector_rs.parquet", index=True)
    logger.info("Saved sector RS: %s", rs_sector.shape)

    rs_vs_sector.index = pd.to_datetime(rs_vs_sector.index).normalize()
    rs_vs_sector = rs_vs_sector[~rs_vs_sector.index.duplicated(keep="last")]
    rs_vs_sector = rs_vs_sector.dropna(how="all")
    rs_vs_sector.to_parquet(output_dir / "stock_vs_sector.parquet", index=True)
    logger.info("Saved stock vs sector RS: %s", rs_vs_sector.shape)

    ew_index_df = pd.DataFrame(
        {"timestamp": ew_index.index, "ew_index": ew_index.values}
    )
    ew_index_df.to_parquet(output_dir / "ew_index.parquet", index=False)
    logger.info("Saved EW index")

    logger.info("=" * 60)
    logger.info("Sector RS analysis complete for %s symbols!", len(all_symbols))
    logger.info("=" * 60)

    return result_df

def _write_empty_outputs(feature_store_dir: str = "data/feature_store"):
    """Persist empty but schema-valid outputs so downstream consumers can degrade safely."""
    output_dir = Path(feature_store_dir) / "all_symbols"
    output_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame().to_parquet(output_dir / "sector_rs.parquet", index=True)
    pd.DataFrame().to_parquet(output_dir / "stock_vs_sector.parquet", index=True)
    pd.DataFrame(columns=["timestamp", "ew_index"]).to_parquet(
        output_dir / "ew_index.parquet", index=False
    )
    return pd.DataFrame()


if __name__ == "__main__":
    compute_all_symbols_rs()
