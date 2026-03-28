import pandas as pd
import duckdb
import sqlite3
import numpy as np
from pathlib import Path


def load_all_symbols_with_sector():
    """Load all symbols from stock_details with sector mapping"""
    conn = sqlite3.connect("data/masterdata.db")

    rows = conn.execute("""
        SELECT Symbol, Sector
        FROM stock_details
        WHERE Symbol IS NOT NULL
    """).fetchall()

    conn.close()

    sector_map = {sym: sector for sym, sector in rows if sector}

    return sector_map


def compute_all_symbols_rs():
    print("=" * 60)
    print("Sector RS Analysis - ALL Symbols")
    print("=" * 60)

    print("\nStep 1: Loading sector mapping from stock_details...")
    sector_map = load_all_symbols_with_sector()
    print(f"  Mapped {len(sector_map)} stocks to sectors")

    sector_counts = {}
    for sym, sector in sector_map.items():
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    print("  Sector distribution:")
    for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        print(f"    {sector}: {count}")

    print("\nStep 2: Fetching OHLCV data from DuckDB...")
    all_symbols = list(sector_map.keys())

    conn_duck = duckdb.connect("data/ohlcv.duckdb", read_only=True)
    query = f"""
        SELECT symbol_id, timestamp, close
        FROM _catalog
        WHERE symbol_id IN ({",".join([repr(s) for s in all_symbols])})
        ORDER BY symbol_id, timestamp
    """
    ohlcv = conn_duck.execute(query).df()
    conn_duck.close()
    print(f"  Fetched {len(ohlcv)} rows for {len(all_symbols)} symbols")

    print("\nStep 3: Deduplicating by date and pivoting...")
    ohlcv["date"] = pd.to_datetime(ohlcv["timestamp"]).dt.normalize()
    ohlcv = ohlcv.drop_duplicates(subset=["date", "symbol_id"], keep="last")
    close_df = ohlcv.pivot(index="date", columns="symbol_id", values="close")
    close_df = close_df.sort_index()
    close_df = close_df.dropna(how="all", axis=1)
    print(f"  Wide format: {close_df.shape}")

    print("\nStep 4: Computing daily returns...")
    returns = close_df.pct_change(fill_method=None)
    print(f"  Returns shape: {returns.shape}")

    print("\nStep 5: Computing Equal-Weight Index returns...")
    ew_index_ret = returns.mean(axis=1)
    print(f"  EW Index returns: {len(ew_index_ret)} days")

    print("\nStep 6: Converting to EW Index price (base=100)...")
    ew_index = (1 + ew_index_ret).cumprod() * 100
    ew_index.name = "ew_index"
    print(f"  EW Index range: {ew_index.min():.2f} to {ew_index.max():.2f}")

    print("\nStep 7: Computing RS features...")
    lookbacks = [20, 50, 100]
    rs_dict = {}

    for lb in lookbacks:
        stock_ret = close_df / close_df.shift(lb)
        index_ret = ew_index / ew_index.shift(lb)
        rs = stock_ret.div(index_ret, axis=0)
        rs_dict[lb] = rs
        print(f"  RS_{lb}: {rs.shape}")

    print("\nStep 8: Computing RS ranks and combined...")
    rs_20_rank = rs_dict[20].rank(axis=1, pct=True)
    rs_50_rank = rs_dict[50].rank(axis=1, pct=True)
    rs_100_rank = rs_dict[100].rank(axis=1, pct=True)

    rs_combined = (rs_20_rank + rs_50_rank + rs_100_rank) / 3
    print(f"  Combined RS: {rs_combined.shape}")

    print("\nStep 9: Aggregating RS per sector...")
    rs_sector = rs_combined.T.groupby(sector_map).mean().T
    print(f"  Sector RS shape: {rs_sector.shape}")

    print("\nStep 10: Ranking sectors...")
    sector_rank = rs_sector.rank(axis=1, pct=True)
    print("Strong sectors (>70th percentile):")
    latest_sector_rank = sector_rank.iloc[-1]
    strong_sectors = latest_sector_rank[latest_sector_rank > 0.7].sort_values(
        ascending=False
    )
    for sector, rank in strong_sectors.items():
        print(f"  {sector}: {rank:.2%}")
    print("Weak sectors (<30th percentile):")
    weak_sectors = latest_sector_rank[latest_sector_rank < 0.3].sort_values()
    for sector, rank in weak_sectors.items():
        print(f"  {sector}: {rank:.2%}")

    print("\nStep 11: Computing stock RS vs sector...")
    rs_vs_sector = pd.DataFrame(
        index=rs_combined.index, columns=rs_combined.columns, dtype=float
    )
    for sector in rs_sector.columns:
        sector_stocks = [s for s in rs_combined.columns if sector_map.get(s) == sector]
        if sector_stocks:
            rs_vs_sector[sector_stocks] = (
                rs_combined[sector_stocks].values - rs_sector[sector].values[:, None]
            )
    print(f"  RS vs Sector shape: {rs_vs_sector.shape}")

    print("\nStep 12: Applying triple confirmation filters...")
    print("  Filter 1: Strong sector (>70th percentile)")
    strong_sector_names = latest_sector_rank[latest_sector_rank > 0.7].index.tolist()
    sector_filter = pd.Series(
        [
            sector_map.get(c, "Other") in strong_sector_names
            for c in rs_combined.columns
        ],
        index=rs_combined.columns,
    )
    print(f"    Stocks in strong sectors: {sector_filter.sum()}")

    print("  Filter 2: Stock stronger than sector (>0)")
    stock_filter = rs_vs_sector.iloc[-1] > 0
    print(f"    Stocks outperforming sector: {stock_filter.sum()}")

    print("  Filter 3: Absolute strength (>0.5)")
    absolute_filter = rs_combined.iloc[-1] > 0.5
    print(f"    Stocks with absolute strength: {absolute_filter.sum()}")

    print("\n  Final signal (all 3 filters):")
    final_signal_mask = sector_filter & stock_filter & absolute_filter
    final_count = final_signal_mask.sum()
    print(f"    Stocks meeting all criteria: {final_count}")

    print("\nStep 13: Getting top stocks...")
    print("\nTop stocks meeting all criteria:")

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
        print(result_df.head(50).to_string(index=False))
    else:
        print("No stocks meet all criteria.")
        result_df = pd.DataFrame(final_stocks)

    print("\nStep 14: Saving features...")
    output_dir = Path("data/feature_store/all_symbols")
    output_dir.mkdir(parents=True, exist_ok=True)

    rs_sector.index = pd.to_datetime(rs_sector.index).normalize()
    rs_sector = rs_sector[~rs_sector.index.duplicated(keep="last")]
    rs_sector = rs_sector.dropna(how="all")
    rs_sector.to_parquet(output_dir / "sector_rs.parquet", index=True)
    print(f"  Saved sector RS: {rs_sector.shape}")

    rs_vs_sector.index = pd.to_datetime(rs_vs_sector.index).normalize()
    rs_vs_sector = rs_vs_sector[~rs_vs_sector.index.duplicated(keep="last")]
    rs_vs_sector = rs_vs_sector.dropna(how="all")
    rs_vs_sector.to_parquet(output_dir / "stock_vs_sector.parquet", index=True)
    print(f"  Saved stock vs sector RS: {rs_vs_sector.shape}")

    ew_index_df = pd.DataFrame(
        {"timestamp": ew_index.index, "ew_index": ew_index.values}
    )
    ew_index_df.to_parquet(output_dir / "ew_index.parquet", index=False)
    print(f"  Saved EW index")

    print("\n" + "=" * 60)
    print(f"Sector RS analysis complete for {len(sector_map)} symbols!")
    print("=" * 60)

    return result_df


if __name__ == "__main__":
    compute_all_symbols_rs()
