"""
Stock Scanner Module
====================
Scans and filters stocks based on sector strength, momentum, and relative strength.

Usage:
    python -m ai_trading_system.domains.ranking.stock_scan
    python -m ai_trading_system.domains.ranking.stock_scan --local    # Skip Google Sheets update
"""

from datetime import datetime

import pandas as pd
from core.logging import logger
from ai_trading_system.domains.ranking import _scan_data
from ai_trading_system.domains.ranking._script_env import bootstrap_script_environment

project_root = bootstrap_script_environment(__file__)


def load_sector_rs() -> pd.DataFrame:
    """Load sector RS from parquet, normalize dates."""
    return _scan_data.load_sector_rs()


def load_stock_vs_sector() -> pd.DataFrame:
    """Load stock vs sector RS from parquet, normalize dates."""
    return _scan_data.load_stock_vs_sector()


def load_sector_mapping() -> pd.DataFrame:
    """Load symbol to sector mapping from SQLite."""
    return _scan_data.load_sector_mapping()


def load_sector_map() -> dict:
    """Load symbol to sector mapping as dictionary."""
    return _scan_data.load_sector_map()


def compute_stock_rs_full(
    stock_vs_sector: pd.DataFrame, sector_rs: pd.DataFrame, sector_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Compute full stock RS = sector RS (for each stock's sector) + stock vs sector."""
    return _scan_data.compute_stock_rs_full(stock_vs_sector, sector_rs, sector_mapping)


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
    """Backward-compatible Google Sheets publish wrapper."""
    try:
        from ai_trading_system.domains.publish.channels.google_sheets import (
            publish_stock_scan,
        )

        publish_stock_scan(stocks)

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
