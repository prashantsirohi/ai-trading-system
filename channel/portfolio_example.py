"""
Portfolio Analysis Examples

Usage:
    python portfolio_example.py

This module provides:
- Portfolio tracking with positions
- P&L calculations
- Sector exposure analysis
- Google Sheets integration
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from portfolio_analyzer import Portfolio, PortfolioManager, PositionType


def example_create_portfolio():
    print("=" * 50)
    print("Example 1: Create & Track Portfolio")
    print("=" * 50)

    portfolio = Portfolio(name="My Portfolio", initial_cash=100000)

    holdings = [
        {"symbol": "RELIANCE", "quantity": 50, "avg_price": 2500, "sector": "Energy"},
        {"symbol": "TCS", "quantity": 30, "avg_price": 3500, "sector": "IT"},
        {"symbol": "HDFCBANK", "quantity": 40, "avg_price": 1600, "sector": "Banks"},
        {"symbol": "INFY", "quantity": 25, "avg_price": 1500, "sector": "IT"},
        {"symbol": "SBIN", "quantity": 100, "avg_price": 600, "sector": "Banks"},
        {"symbol": "HINDUNILVR", "quantity": 20, "avg_price": 2500, "sector": "FMCG"},
        {"symbol": "SUNPHARMA", "quantity": 35, "avg_price": 1200, "sector": "Pharma"},
        {"symbol": "LT", "quantity": 25, "avg_price": 3200, "sector": "Industrial"},
    ]

    for h in holdings:
        portfolio.add_position(**h)

    prices = {
        "RELIANCE": 3544,
        "TCS": 3355,
        "HDFCBANK": 1700,
        "INFY": 1550,
        "SBIN": 650,
        "HINDUNILVR": 2600,
        "SUNPHARMA": 1300,
        "LT": 3400,
    }
    portfolio.update_prices(prices)

    metrics = portfolio.get_metrics()

    print(f"\nPortfolio Value: {metrics.total_value:,.2f}")
    print(f"Cash: {metrics.cash:,.2f}")
    print(f"Total P&L: {metrics.total_pnl:,.2f} ({metrics.total_pnl_pct:.2f}%)")
    print(f"Positions: {metrics.position_count}")

    print("\n--- Top Holdings ---")
    for h in metrics.top_holdings:
        print(f"  {h['symbol']}: {h['market_value']:,.2f} ({h['pnl_pct']:+.2f}%)")

    print("\n--- Sector Exposure ---")
    for sector, pct in sorted(metrics.sector_exposure.items(), key=lambda x: -x[1]):
        print(f"  {sector}: {pct:.1f}%")

    return portfolio


def example_google_sheets():
    print("\n" + "=" * 50)
    print("Example 2: Google Sheets Integration")
    print("=" * 50)

    pm = PortfolioManager()

    if not pm.sheets_client:
        print("Not connected to Google Sheets")
        return

    print("Connected!")

    portfolio = pm.create_sample_portfolio()

    print("\nSaving to Google Sheets...")
    if pm.save_portfolio_to_sheet(portfolio, "PORTFOLIO"):
        print("Saved!")

    print("\nLoading from Google Sheets...")
    loaded = pm.load_portfolio_from_sheet("PORTFOLIO")
    if loaded:
        print(f"Loaded: {loaded.name}")
        print(f"Positions: {len(loaded.positions)}")
        print(f"Value: {loaded.total_market_value:,.2f}")


def example_rs_based_portfolio():
    print("\n" + "=" * 50)
    print("Example 3: RS-Based Portfolio Selection")
    print("=" * 50)

    try:
        import pandas as pd

        sector_rs = pd.read_parquet(
            "../data/feature_store/all_symbols/sector_rs.parquet"
        )
        stock_vs_sector = pd.read_parquet(
            "../data/feature_store/all_symbols/stock_vs_sector.parquet"
        )

        latest_rank = sector_rs.iloc[-1].sort_values(ascending=False)
        strong_sectors = list(latest_rank[latest_rank > 0.6].index)

        latest_stock = stock_vs_sector.iloc[-1].sort_values(ascending=False)

        print(f"Strong Sectors: {strong_sectors}")

        print("\nTop Stocks in Strong Sectors:")
        selected = []
        for sym, rs_val in latest_stock.items():
            if rs_val > 0.5 and len(selected) < 10:
                selected.append({"symbol": sym, "rs_score": rs_val})

        for s in selected[:10]:
            print(f"  {s['symbol']}: {s['rs_score']:.3f}")

        return selected

    except Exception as e:
        print(f"Could not load RS data: {e}")
        return []


def example_export_to_csv():
    print("\n" + "=" * 50)
    print("Example 4: Export to CSV")
    print("=" * 50)

    portfolio = Portfolio(name="Export Demo")
    portfolio.add_position("RELIANCE", 10, 2500, sector="Energy")
    portfolio.add_position("TCS", 5, 3500, sector="IT")
    portfolio.update_prices({"RELIANCE": 3544, "TCS": 3355})

    df = portfolio.get_holdings_dataframe()
    print("\nHoldings DataFrame:")
    print(df.to_string(index=False))

    csv_path = Path("reports/portfolio_holdings.csv")
    csv_path.parent.mkdir(exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"\nExported to {csv_path}")


if __name__ == "__main__":
    p1 = example_create_portfolio()
    example_google_sheets()
    example_rs_based_portfolio()
    example_export_to_csv()

    print("\n" + "=" * 50)
    print("All examples complete!")
    print("=" * 50)
