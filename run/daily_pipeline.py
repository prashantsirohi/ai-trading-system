"""
Daily Pipeline
=============
Orchestrates daily trading system tasks:
1. Check trading holiday (exit if holiday)
2. Check weekday (exit if weekend)
3. Run daily update (OHLCV + features)
4. If Saturday: portfolio analysis + stock_scan + sector_dashboard
5. If weekday: stock_scan + sector_dashboard

Usage:
    python run/daily_pipeline.py
    python run/daily_pipeline.py --force   # Skip holiday/weekend checks
"""

import os
import sys
from datetime import datetime, timedelta

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from dotenv import load_dotenv
from utils.logger import logger
from utils.data_config import (
    should_truncate_data,
    truncate_old_data,
    DATA_RETENTION_YEARS,
    FEATURE_STORE_DIR,
)
import sqlite3

load_dotenv(os.path.join(project_root, ".env"))

logger.info(f"Environment: {os.getenv('ENV', 'local')}")
logger.info(
    f"Data retention: {DATA_RETENTION_YEARS if DATA_RETENTION_YEARS else 'All'} years"
)


def is_trading_holiday(date: datetime = None) -> bool:
    """Check if given date is a trading holiday."""
    if date is None:
        date = datetime.now()

    date_str = date.strftime("%Y-%m-%d")

    conn = sqlite3.connect(os.path.join(project_root, "data", "masterdata.db"))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM nse_holidays WHERE date = ?", (date_str,))
    count = cursor.fetchone()[0]
    conn.close()

    return count > 0


def is_weekend(date: datetime = None) -> bool:
    """Check if given date is Saturday (5) or Sunday (6)."""
    if date is None:
        date = datetime.now()
    return date.weekday() >= 5


def run_daily_update():
    """Run daily OHLCV update and feature computation."""
    from collectors.daily_update_runner import run as run_daily

    logger.info("Running daily OHLCV update...")
    run_daily(symbols_only=False, features_only=False, batch_size=500, bulk=False)
    logger.info("Daily update complete")

    # Create snapshot for reproducibility
    from features.feature_store import FeatureStore
    from datetime import datetime

    fs = FeatureStore()
    snapshot_id = fs.create_snapshot(f"Daily pipeline {datetime.now().date()}")
    logger.info(f"Created snapshot: {snapshot_id}")


def run_portfolio_analysis():
    """Run portfolio analysis from Google Sheets."""
    try:
        from channel.portfolio_analyzer import Portfolio, PortfolioManager
        from channel.google_sheets_manager import GoogleSheetsManager
        import sqlite3
        import duckdb
        from collectors.yfinance_collector import YFinanceCollector

        logger.info("Running portfolio analysis...")

        gs = GoogleSheetsManager()
        gs.open_spreadsheet()

        portfolio = Portfolio(name="My Portfolio", initial_cash=0)

        # Load sector map from stock_details (Sector column)
        conn = sqlite3.connect("data/masterdata.db")
        rows = conn.execute("SELECT Symbol, Sector FROM stock_details").fetchall()
        sector_map = {sym: sector for sym, sector in rows if sector}
        conn.close()

        # Load RS data
        portfolio._sector_map = sector_map
        portfolio.load_rs_data()

        # Read portfolio from Google Sheets
        try:
            ws = gs.get_worksheet("PORTFOLIO")
            if ws:
                # Use get_all_values to read raw data (first 3 columns only)
                values = ws.get_all_values()
                if values and len(values) > 1:
                    data = []
                    for row in values[1:]:
                        if row and len(row) >= 3:
                            try:
                                symbol = str(row[0]).strip()
                                qty = float(row[1]) if row[1] else 0
                                avg_price = float(row[2]) if row[2] else 0
                                # Skip summary rows
                                if (
                                    symbol.lower() in ["total", "summary", ""]
                                    or "positions" in symbol.lower()
                                ):
                                    continue
                                data.append(
                                    {
                                        "Symbol": symbol,
                                        "Qty": qty,
                                        "Avg Price": avg_price,
                                    }
                                )
                            except (ValueError, TypeError):
                                continue
                    logger.info(f"Loaded {len(data)} positions from PORTFOLIO sheet")

                    # Get current prices from Yahoo Finance (more reliable than Dhan)
                    symbols = [d["Symbol"] for d in data if d.get("Symbol")]
                    if symbols:
                        logger.info(f"Fetching current prices from Yahoo Finance...")
                        yfc = YFinanceCollector()
                        price_map = yfc.get_latest_prices(symbols)
                        logger.info(f"Got prices for {len(price_map)} symbols")

                # Add positions
                for d in data:
                    symbol = str(d.get("Symbol", "")).strip()
                    qty = float(d.get("Qty", 0) or 0)
                    avg_price = float(d.get("Avg Price", 0) or 0)
                    if symbol and qty > 0:
                        sector = sector_map.get(symbol, "Other")
                        portfolio.add_position(symbol, qty, avg_price, sector=sector)
                        if symbol in price_map:
                            portfolio.update_position_price(symbol, price_map[symbol])

                logger.info(f"Portfolio loaded: {len(portfolio.positions)} positions")

                # Save portfolio with current prices
                pm = PortfolioManager()
                if pm.sheets_client:
                    pm.save_portfolio_to_sheet(portfolio, "PORTFOLIO")
                    pm.save_swot_analysis(portfolio, "Portfolio Analysis")
                    logger.info("Portfolio and SWOT analysis saved to Google Sheets")
            else:
                logger.info("No PORTFOLIO sheet found, skipping analysis")
        except Exception as e:
            logger.warning(f"Could not read PORTFOLIO sheet: {e}")

        logger.info("Portfolio analysis complete")
    except Exception as e:
        logger.warning(f"Portfolio analysis skipped: {e}")


def run_stock_scan():
    """Run stock scanner."""
    from channel.stock_scan import run as run_scan

    logger.info("Running stock scan...")
    run_scan(local_only=False)
    logger.info("Stock scan complete")


def run_sector_dashboard():
    """Run sector dashboard."""
    from channel.sector_dashboard import run as run_dashboard

    logger.info("Running sector dashboard...")
    run_dashboard(local_only=False)
    logger.info("Sector dashboard complete")


def main(force: bool = False):
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    is_saturday = now.weekday() == 5

    logger.info("=" * 60)
    logger.info(f"DAILY PIPELINE - {today}")
    logger.info("=" * 60)

    # Truncate old data if running in github/prod mode
    if should_truncate_data():
        truncate_old_data()

    if not force:
        if is_trading_holiday(now):
            logger.info(f"⛔ {today} is a trading holiday. Exiting.")
            return

        if is_weekend(now):
            day_name = now.strftime("%A")
            logger.info(f"⛔ {today} is {day_name}. Weekend - exiting.")
            return

    run_daily_update()

    logger.info("Weekday detected - running daily tasks:")
    logger.info("  - Portfolio analysis")
    logger.info("  - Stock scan")
    logger.info("  - Sector dashboard")
    run_portfolio_analysis()
    run_stock_scan()
    run_sector_dashboard()

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force", action="store_true", help="Skip holiday/weekend checks"
    )
    args = parser.parse_args()

    main(force=args.force)
