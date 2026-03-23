"""
Google Sheets Integration Examples

Usage:
    python google_sheets_example.py

Setup:
    1. OAuth2: python oauth_flow.py (first time only)
    2. Credentials: ../client_secret.json (in project root)
    3. Token: ../token.json (generated after auth)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from google_sheets_manager import GoogleSheetsManager, SectorReportSheets


def example_basic_operations():
    print("=" * 50)
    print("Example 1: Basic Operations")
    print("=" * 50)

    sheets = GoogleSheetsManager(
        spreadsheet_id="1_hyFH_RpMXlKlCQZuDkkt-Eh0CxFtqV_zNtk-_W3KUQ"
    )

    if sheets.client:
        sheets.open_spreadsheet()
        if sheets.spreadsheet:
            print(f"Connected: {sheets.spreadsheet.title}")
            print(f"Sheets: {sheets.list_worksheets()}")
        else:
            print("Connected to Google but couldn't open spreadsheet")

        df = sheets.read_worksheet("Sheet1")
        if df is not None and not df.empty:
            print(f"\nData ({len(df)} rows):")
            print(df.head())
        else:
            print("\nSheet1 is empty")
    else:
        print("Not connected")


def example_sector_report():
    print("\n" + "=" * 50)
    print("Example 2: Sector Report to Google Sheets")
    print("=" * 50)

    sheets = SectorReportSheets(
        spreadsheet_id="1_hyFH_RpMXlKlCQZuDkkt-Eh0CxFtqV_zNtk-_W3KUQ"
    )

    if not sheets.client:
        print("Not connected. Creating sample data...")

        sample_data = pd.DataFrame(
            {
                "Sector": ["Power", "Pharma", "Metals", "IT", "Banks"],
                "RS_Score": [0.637, 0.616, 0.641, 0.293, 0.551],
                "Momentum_20d": [0.05, 0.03, 0.08, -0.02, 0.01],
                "Signal": ["BUY", "BUY", "BUY", "SELL", "HOLD"],
            }
        )

        print("\nSample Sector Report:")
        print(sample_data.to_string(index=False))
    else:
        sector_rs = pd.read_parquet(
            "../data/feature_store/all_symbols/sector_rs.parquet"
        )
        latest = sector_rs.iloc[-1].sort_values(ascending=False)

        sector_data = pd.DataFrame(
            {
                "Sector": latest.index[:10].tolist(),
                "RS_Score": latest.values[:10].round(3),
                "Momentum_20d": sector_rs.iloc[-20:]
                .mean()
                .reindex(latest.index[:10])
                .round(3)
                .values,
            }
        )
        sector_data["Signal"] = sector_data["RS_Score"].apply(
            lambda x: "BUY" if x > 0.6 else "HOLD" if x > 0.5 else "SELL"
        )

        print("Writing sector report...")
        sheets.write_sector_report(sector_data)
        print("Done!")


def example_write_stock_signals():
    print("\n" + "=" * 50)
    print("Example 3: Write Stock Signals")
    print("=" * 50)

    signals = [
        {"Symbol": "RELIANCE", "Sector": "Energy", "RS_Score": 0.75, "Signal": "BUY"},
        {"Symbol": "TCS", "Sector": "IT", "RS_Score": 0.45, "Signal": "HOLD"},
        {"Symbol": "HDFCBANK", "Sector": "Banks", "RS_Score": 0.62, "Signal": "BUY"},
    ]

    sheets = SectorReportSheets(
        spreadsheet_id="1_hyFH_RpMXlKlCQZuDkkt-Eh0CxFtqV_zNtk-_W3KUQ"
    )

    if sheets.client:
        print("Writing stock signals...")
        sheets.write_stock_signals(signals)
        print("Done!")
    else:
        print("Not connected. Sample signals:")
        for s in signals:
            print(f"  {s}")


if __name__ == "__main__":
    example_basic_operations()
    example_sector_report()
    example_write_stock_signals()
