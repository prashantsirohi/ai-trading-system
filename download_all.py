"""
Script to download historical data for all symbols.
Usage: python download_all.py [max_per_day]
Default: 1000 symbols per day (DhanHQ daily limit)
"""

import os
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
sys.path.insert(0, script_dir)

from dotenv import load_dotenv
load_dotenv(os.path.join(script_dir, ".env"))

from collectors.dhan_collector import DhanCollector

def main():
    client_id = os.getenv("DHAN_CLIENT_ID")
    access_token = os.getenv("DHAN_ACCESS_TOKEN")
    
    if not client_id or not access_token:
        print("Error: DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN not found in .env")
        return
    
    max_per_day = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    
    collector = DhanCollector(
        client_id=client_id,
        access_token=access_token,
        db_path="data/masterdata.db",
        data_dir="data/raw/NSE_EQ"
    )
    
    print(f"Starting download (max {max_per_day} per day)...")
    results = collector.fetch_pending_symbols(max_per_day=max_per_day)
    
    stats = collector.get_download_stats()
    print(f"\nDownload Status: {stats['downloaded']}/{stats['total']} symbols")
    print(f"Pending: {stats['pending']} symbols")

if __name__ == "__main__":
    main()
