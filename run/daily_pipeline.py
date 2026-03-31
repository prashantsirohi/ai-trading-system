"""
Daily Pipeline wrapper.

This keeps the historical entrypoint while delegating execution to the
resilient 4-stage orchestrator:
1. ingest
2. features
3. rank
4. publish

Usage:
    python run/daily_pipeline.py
    python run/daily_pipeline.py --force
    python run/daily_pipeline.py --local-publish
    python run/daily_pipeline.py --smoke --local-publish
"""

import os
import sys
from datetime import datetime

from core.bootstrap import ensure_project_root_on_path
project_root = str(ensure_project_root_on_path(__file__))
from core.env import load_project_env
from core.logging import logger
from run.orchestrator import PipelineOrchestrator
from utils.data_config import (
    should_truncate_data,
    truncate_old_data,
    data_retention_years,
)
import sqlite3

load_project_env(project_root)

logger.info(f"Environment: {os.getenv('ENV', 'local')}")
logger.info(
    f"Data retention: {data_retention_years() if data_retention_years() else 'All'} years"
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


def run_portfolio_analysis():
    """Run portfolio analysis from Google Sheets."""
    try:
        from channel.portfolio_analyzer import Portfolio, PortfolioManager
        from publishers.google_sheets import GoogleSheetsManager
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


def main(
    force: bool = False,
    local_publish: bool = False,
    smoke: bool = False,
    stages: str = "ingest,features,rank,publish",
    canary: bool = False,
    symbol_limit: int | None = None,
    skip_preflight: bool = False,
    data_domain: str = "operational",
):
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info(f"DAILY PIPELINE - {today}")
    logger.info("=" * 60)

    # Truncate old data if running in github/prod mode
    if should_truncate_data(data_domain):
        truncate_old_data(data_domain=data_domain)

    if not force:
        if is_trading_holiday(now):
            logger.info(f"⛔ {today} is a trading holiday. Exiting.")
            return

        if is_weekend(now):
            day_name = now.strftime("%A")
            logger.info(f"⛔ {today} is {day_name}. Weekend - exiting.")
            return

    orchestrator = PipelineOrchestrator(project_root)
    result = orchestrator.run_pipeline(
        stage_names=(
            ["ingest", "features", "rank"]
            if canary and stages == "ingest,features,rank,publish"
            else [stage.strip() for stage in stages.split(",") if stage.strip()]
        ),
        run_date=today,
        params={
            "force": force,
            "batch_size": 700,
            "bulk": False,
            "local_publish": local_publish,
            "smoke": smoke,
            "canary": canary,
            "symbol_limit": symbol_limit if symbol_limit is not None else (25 if canary else None),
            "preflight": not skip_preflight,
            "data_domain": data_domain,
        },
    )

    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE - run_id={result['run_id']} status={result['status']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force", action="store_true", help="Skip holiday/weekend checks"
    )
    parser.add_argument(
        "--local-publish",
        action="store_true",
        help="Skip networked Telegram/Google Sheets delivery and write a local publish summary instead",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Run a self-contained local smoke flow with synthetic data",
    )
    parser.add_argument(
        "--stages",
        default="ingest,features,rank,publish",
        help="Comma-separated stage list. Example: publish",
    )
    parser.add_argument(
        "--canary",
        action="store_true",
        help="Run a limited real canary flow with a smaller live symbol universe.",
    )
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=None,
        help="Limit live symbol universe size for canary runs.",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip local readiness checks before running live stages.",
    )
    parser.add_argument(
        "--data-domain",
        choices=["operational", "research"],
        default="operational",
        help="Select the storage domain for this wrapper.",
    )
    args = parser.parse_args()

    main(
        force=args.force,
        local_publish=args.local_publish,
        smoke=args.smoke,
        stages=args.stages,
        canary=args.canary,
        symbol_limit=args.symbol_limit,
        skip_preflight=args.skip_preflight,
        data_domain=args.data_domain,
    )
