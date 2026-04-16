"""
Sector Rotation Dashboard
=========================
Computes sector strength, momentum, quadrants and top stocks per sector.
Updates Google Sheets with dashboard.

Usage:
    python -m channel.sector_dashboard
    python -m channel.sector_dashboard --local    # Skip Google Sheets update
"""

from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq

from core.bootstrap import ensure_project_root_on_path

project_root = str(ensure_project_root_on_path(__file__))

from utils.env import load_project_env
from core.logging import logger

load_project_env(__file__)

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


def compute_stock_rs(
    sector_rs: pd.DataFrame, stock_vs_sector: pd.DataFrame
) -> pd.DataFrame:
    """Compute full stock RS = sector RS + stock vs sector."""
    stock_rs = sector_rs.copy()

    for col in stock_vs_sector.columns:
        if col in stock_rs.columns:
            stock_rs[col] = sector_rs[col] + stock_vs_sector[col]

    return stock_rs


def compute_sector_momentum(sector_rs: pd.DataFrame, days: int = 20) -> pd.DataFrame:
    """Compute momentum as change in RS over specified days."""
    filled = sector_rs.ffill()
    return filled.diff(days)


def classify_quadrant(
    rs: float,
    momentum: float,
    rs_threshold: float = 0.5,
    momentum_threshold: float = 0.0,
) -> str:
    """Classify sector into quadrant."""
    if rs > rs_threshold and momentum > momentum_threshold:
        return "Leading"
    elif rs > rs_threshold:
        return "Weakening"
    elif momentum > momentum_threshold:
        return "Improving"
    else:
        return "Lagging"


def build_dashboard(
    sector_rs: pd.DataFrame, sector_momentum: pd.DataFrame
) -> pd.DataFrame:
    """Build sector dashboard with quadrants."""
    sector_rs_filled = sector_rs.ffill()

    dashboard = pd.DataFrame(
        {
            "RS": sector_rs.iloc[-1],
            "RS_20": sector_rs_filled.iloc[-20]
            if len(sector_rs_filled) >= 20
            else sector_rs_filled.iloc[0],
            "RS_50": sector_rs_filled.iloc[-50]
            if len(sector_rs_filled) >= 50
            else sector_rs_filled.iloc[0],
            "RS_100": sector_rs_filled.iloc[-100]
            if len(sector_rs_filled) >= 100
            else sector_rs_filled.iloc[0],
            "Momentum": sector_momentum.iloc[-1],
        }
    )

    dashboard = dashboard.dropna(subset=["RS", "Momentum"])

    dashboard["RS_rank"] = (
        dashboard["RS"].rank(ascending=False, na_option="keep").fillna(0).astype(int)
    )
    dashboard["RS_rank_pct"] = dashboard["RS"].rank(ascending=False, pct=True)
    dashboard["Momentum_rank"] = (
        dashboard["Momentum"]
        .rank(ascending=False, na_option="keep")
        .fillna(0)
        .astype(int)
    )
    dashboard["Momentum_rank_pct"] = dashboard["Momentum"].rank(
        ascending=False, pct=True
    )

    dashboard["Quadrant"] = dashboard.apply(
        lambda row: classify_quadrant(row["RS"], row["Momentum"]), axis=1
    )

    cols_to_round = [
        "RS",
        "RS_20",
        "RS_50",
        "RS_100",
        "Momentum",
        "RS_rank_pct",
        "Momentum_rank_pct",
    ]
    dashboard[cols_to_round] = dashboard[cols_to_round].round(2)

    dashboard = dashboard.sort_values("RS", ascending=False)
    dashboard.index.name = "Sector"

    return dashboard


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


def get_top_stocks_per_sector(
    stock_rs: pd.DataFrame, sector_mapping: pd.DataFrame, top_n: int = 5
) -> dict:
    """Get top stocks inside each sector."""
    sector_map = dict(zip(sector_mapping["symbol"], sector_mapping["sector"]))

    result = {}
    unique_sectors = sector_mapping["sector"].unique()

    for sector in unique_sectors:
        sector_stocks = [s for s, sec in sector_map.items() if sec == sector]

        if not sector_stocks:
            continue

        available_stocks = [s for s in sector_stocks if s in stock_rs.columns]
        if not available_stocks:
            continue

        scores = (
            stock_rs.iloc[-1][available_stocks].sort_values(ascending=False).head(top_n)
        )
        result[sector] = scores.to_dict()

    return result


def update_google_sheets(dashboard: pd.DataFrame):
    """Backward-compatible Google Sheets publish wrapper."""
    try:
        from publishers.google_sheets import publish_sector_dashboard

        publish_sector_dashboard(dashboard)

    except Exception as e:
        logger.error(f"Google Sheets update failed: {e}")


def run(local_only: bool = False):
    """Run sector dashboard."""
    logger.info("=" * 60)
    logger.info("SECTOR DASHBOARD")
    logger.info("=" * 60)

    logger.info("Loading data...")
    sector_rs = load_sector_rs()
    stock_vs_sector = load_stock_vs_sector()
    sector_mapping = load_sector_mapping()

    logger.info(
        f"Loaded {len(sector_rs.columns)} sectors, {len(sector_mapping)} stocks"
    )

    logger.info("Computing sector momentum (20-day)...")
    sector_momentum = compute_sector_momentum(sector_rs, days=20)

    logger.info("Building dashboard...")
    dashboard = build_dashboard(sector_rs, sector_momentum)

    today = sector_rs.index[-1]
    logger.info(f"Dashboard for {today}:")
    print("\n" + "=" * 50)
    print(dashboard[["RS", "Momentum", "Quadrant"]].to_string())
    print("=" * 50 + "\n")

    logger.info("Getting top stocks per sector...")
    stock_rs_full = compute_stock_rs_full(stock_vs_sector, sector_rs, sector_mapping)
    top_stocks = get_top_stocks_per_sector(stock_rs_full, sector_mapping, top_n=5)

    print("TOP STOCKS IN LEADING SECTORS:")
    print("-" * 40)
    leading_sectors = dashboard[dashboard["Quadrant"] == "Leading"].index.tolist()
    for sector in leading_sectors:
        if sector in top_stocks:
            stocks = list(top_stocks[sector].items())[:3]
            print(f"\n{sector}:")
            for stock, score in stocks:
                print(f"  {stock}: {score:.4f}")
    print("-" * 40)

    dashboard["report_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    if not local_only:
        update_google_sheets(dashboard)

    return dashboard, top_stocks


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local", action="store_true", help="Skip Google Sheets update"
    )
    args = parser.parse_args()

    run(local_only=args.local)
