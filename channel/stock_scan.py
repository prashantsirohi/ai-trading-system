"""
Stock Scanner Module
====================
Scans and filters stocks based on sector strength, momentum, and relative strength.

Usage:
    python -m channel.stock_scan
    python -m channel.stock_scan --local    # Skip Google Sheets update
"""

import os
import sys
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from dotenv import load_dotenv
from utils.logger import logger

load_dotenv(os.path.join(project_root, ".env"))

logger.disable("googleapiclient")
logger.disable("google.auth")


def load_sector_rs() -> pd.DataFrame:
    """Load sector RS from parquet, normalize dates."""
    df = pq.read_table("data/feature_store/all_symbols/sector_rs.parquet").to_pandas()
    df.index = pd.to_datetime(df.index).normalize()
    df = df[~df.index.duplicated(keep="last")]
    return df


def load_stock_vs_sector() -> pd.DataFrame:
    """Load stock vs sector RS from parquet, normalize dates."""
    df = pq.read_table(
        "data/feature_store/all_symbols/stock_vs_sector.parquet"
    ).to_pandas()
    df.index = pd.to_datetime(df.index).normalize()
    df = df[~df.index.duplicated(keep="last")]
    return df


def load_sector_mapping() -> pd.DataFrame:
    """Load symbol to sector mapping from SQLite."""
    import sqlite3

    conn = sqlite3.connect("data/masterdata.db")
    df = pd.read_sql("SELECT Symbol, Sector FROM stock_details", conn)
    conn.close()
    df.columns = ["symbol", "sector"]
    return df


def load_sector_map() -> dict:
    """Load symbol to sector mapping as dictionary."""
    import sqlite3

    conn = sqlite3.connect("data/masterdata.db")
    rows = conn.execute("SELECT Symbol, Sector FROM stock_details").fetchall()
    conn.close()
    return {sym: sector for sym, sector in rows if sector}


def compute_stock_rs_full(
    stock_vs_sector: pd.DataFrame, sector_rs: pd.DataFrame, sector_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Compute full stock RS = sector RS (for each stock's sector) + stock vs sector."""
    sector_map = dict(zip(sector_mapping["symbol"], sector_mapping["sector"]))

    stock_rs_full = stock_vs_sector.copy()

    for stock in stock_vs_sector.columns:
        sector = sector_map.get(stock)
        if sector and sector in sector_rs.columns:
            sector_rs_vals = sector_rs[sector].ffill()
            stock_vs_vals = stock_vs_sector[stock].ffill()
            stock_rs_full[stock] = sector_rs_vals + stock_vs_vals

    return stock_rs_full


def get_sector_ranks(sector_rs: pd.DataFrame) -> pd.DataFrame:
    """Get sector ranks by RS."""
    ranks = sector_rs.rank(axis=1, ascending=False)
    return ranks


def get_momentum_sectors(sector_rs: pd.DataFrame, days: int = 5) -> pd.Series:
    """Get sectors with improving momentum (RS increasing)."""
    filled = sector_rs.ffill()
    momentum = filled.diff(days)
    return momentum.iloc[-1]


def scan_stocks(
    sector_rs: pd.DataFrame,
    stock_vs_sector: pd.DataFrame,
    sector_mapping: pd.DataFrame,
    strong_sector_count: int = 3,
) -> pd.DataFrame:
    """
    Scan stocks based on sector strength and momentum.

    Steps:
    1. Filter strong & momentum sectors
    2. Get stocks in those sectors
    3. Build stock-level metrics
    4. Calculate RS vs sector
    5. Apply filters
    6. Rank final stocks
    """
    today = sector_rs.index[-1]
    sector_map = dict(zip(sector_mapping["symbol"], sector_mapping["sector"]))

    sector_ranks = get_sector_ranks(sector_rs)
    sector_rank_today = sector_ranks.loc[today]

    strong_sectors = sector_rank_today[sector_rank_today <= strong_sector_count]

    momentum = get_momentum_sectors(sector_rs, days=5)
    improving_sectors = momentum[momentum < 0].index

    selected_sectors = set(strong_sectors.index) | set(improving_sectors)
    logger.info(f"Selected sectors: {selected_sectors}")

    stock_rs_full = compute_stock_rs_full(stock_vs_sector, sector_rs, sector_mapping)
    stock_rs_filled = stock_rs_full.ffill()

    rs_today = stock_rs_filled.loc[today]
    rs_20 = (
        stock_rs_filled.iloc[-20]
        if len(stock_rs_filled) >= 20
        else stock_rs_filled.iloc[0]
    )
    rs_50 = (
        stock_rs_filled.iloc[-50]
        if len(stock_rs_filled) >= 50
        else stock_rs_filled.iloc[0]
    )

    stock_momentum_20 = stock_rs_filled.diff(20).iloc[-1]

    df = pd.DataFrame(
        {
            "rs": rs_today,
            "rs_20": rs_20,
            "rs_50": rs_50,
            "momentum": stock_momentum_20,
        }
    )

    df = df.dropna(subset=["rs"])

    df["sector"] = df.index.map(lambda s: sector_map.get(s, "Other"))

    sector_avg = df.groupby("sector")["rs"].transform("mean")
    df["rs_vs_sector"] = df["rs"] - sector_avg

    df = df[df["sector"].isin(selected_sectors)]

    if df.empty:
        logger.warning("No stocks after sector filter - returning empty result")
        return df

    df["acceleration"] = df["rs_20"] - df["rs_50"]

    def classify(row):
        if row["rs"] > 0.9 and row["rs_vs_sector"] > 0.2 and row["momentum"] > 0:
            return ("BUY", "Strong + Leader + Momentum")
        elif row["acceleration"] > 0.2 and row["rs_vs_sector"] > 0:
            return ("EARLY", "Accelerating + Sector breakout")
        elif row["rs"] > 0.7:
            return ("WATCH", "Strong but momentum weak")
        else:
            return ("REJECT", "Weak vs sector + falling")

    results = df.apply(classify, axis=1)
    df["category"] = results.apply(lambda x: x[0])
    df["why"] = results.apply(lambda x: x[1])

    df["score"] = 0.5 * df["rs"] + 0.3 * df["rs_20"] + 0.2 * (df["rs_vs_sector"] + 0.5)

    df = df.sort_values("score", ascending=False)

    buy_stocks = df[df["category"] == "BUY"]
    watch_stocks = df[df["category"] == "WATCH"].head(5)
    top = pd.concat([buy_stocks, watch_stocks])

    top = top.round(2)

    return top


def compute_sector_momentum_20(sector_rs: pd.DataFrame) -> pd.Series:
    """Compute 20-day momentum for sectors."""
    filled = sector_rs.ffill()
    return filled.diff(20).iloc[-1]


def update_google_sheets(stocks: pd.DataFrame):
    """Update Google Sheets with stock scan results."""
    try:
        from channel.google_sheets_manager import GoogleSheetsManager

        gs = GoogleSheetsManager()
        spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")

        if not spreadsheet_id:
            logger.warning("GOOGLE_SPREADSHEET_ID not set, skipping sheet update")
            return

        stocks_with_index = stocks.reset_index()
        stocks_with_index.rename(columns={"index": "Symbol"}, inplace=True)
        stocks_with_index["report_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

        sheet = gs.get_or_create_sheet("Stock Scan")

        if sheet:
            ws = gs.get_worksheet("Stock Scan")
            ws.clear()
            gs.append_rows(stocks_with_index, "Stock Scan", include_header=True)

        logger.info(f"Stock scan updated in Google Sheets ({len(stocks)} stocks)")

    except Exception as e:
        logger.error(f"Google Sheets update failed: {e}")


def run(local_only: bool = False):
    """Run stock scanner."""
    logger.info("=" * 60)
    logger.info("STOCK SCANNER")
    logger.info("=" * 60)

    logger.info("Loading data...")
    sector_rs = load_sector_rs()
    stock_vs_sector = load_stock_vs_sector()
    sector_mapping = load_sector_mapping()

    logger.info(
        f"Loaded {len(sector_rs.columns)} sectors, {len(sector_mapping)} stocks"
    )

    logger.info("Scanning stocks...")
    stocks = scan_stocks(sector_rs, stock_vs_sector, sector_mapping)

    logger.info(f"Found {len(stocks)} stocks meeting criteria")
    print("\n" + "=" * 60)
    print("TOP STOCKS:")
    print("=" * 60)
    print("\n" + "=" * 65)
    print(f"{'Symbol':<12} {'Category':<10} {'Why':<40}")
    print("-" * 65)
    for idx, row in stocks.iterrows():
        cat = row["category"]
        emoji = {
            "BUY": "BUY",
            "EARLY": "EARLY",
            "WATCH": "WATCH",
            "REJECT": "REJECT",
        }.get(cat, "")
        print(f"{idx:<12} {cat:<10} {row['why']:<40}")
    print("=" * 65 + "\n")
    print("=" * 60 + "\n")

    if not local_only:
        update_google_sheets(stocks)

    return stocks


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local", action="store_true", help="Skip Google Sheets update"
    )
    args = parser.parse_args()

    run(local_only=args.local)
