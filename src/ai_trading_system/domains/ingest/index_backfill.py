"""Index backfill script - run historical index data backfill.

Usage:
    python -m ai_trading_system.domains.ingest.index_backfill --from 2021-01-01 --to 2026-04-18

This script backfills 5 years of NSE sectoral index OHLC data:
- NIFTY 50, NIFTY BANK, NIFTY AUTO, NIFTY IT, NIFTY PHARMA
- NIFTY FMCG, NIFTY METAL, NIFTY ENERGY, NIFTY REALTY, NIFTY INFRA
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime

import pandas as pd

from ai_trading_system.domains.ingest.index_ingest import get_index_collector
from ai_trading_system.platform.logging.logger import logger


def run_index_backfill(from_date: str, to_date: str, batch_size: int = 50) -> dict:
    """Run index backfill for given date range.
    
    Args:
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
        batch_size: Number of records per batch insert
        
    Returns:
        Summary dict with stats
    """
    start_time = time.time()
    
    # Get collector
    collector = get_index_collector()
    
    # Ensure tables exist
    collector._ensure_tables()
    collector._register_indices()
    
    # Get date range
    dates = pd.bdate_range(from_date, to_date)
    total_dates = len(dates)
    
    logger.info(f"Index backfill: {from_date} to {to_date}")
    logger.info(f"Trading days: {total_dates}")
    logger.info(f"Indices: {len(collector.config.indices)}")
    
    all_data = []
    total_fetched = 0
    total_ingested = 0
    errors = []
    
    for idx, date in enumerate(dates):
        date_str = date.strftime('%Y-%m-%d')
        
        for index_name, _, _, _ in collector.config.indices:
            try:
                df = collector.fetch_index_ohlc(index_name, date_str, date_str)
                if not df.empty:
                    all_data.append(df)
                    total_fetched += 1
            except Exception as e:
                errors.append(f"{index_name}:{date_str}: {e}")
        
        # Batch insert
        if len(all_data) >= batch_size or (idx == total_dates - 1 and all_data):
            result = pd.concat(all_data, ignore_index=True)
            result['provider'] = 'nseindia'
            result['ingest_run_id'] = 'backfill'
            
            count = collector.ingest(result)
            total_ingested += count
            
            logger.info(f"Progress: {idx+1}/{total_dates} dates, {total_ingested} records")
            
            all_data = []
            time.sleep(0.1)  # Rate limiting
    
    duration = time.time() - start_time
    
    summary = {
        "from_date": from_date,
        "to_date": to_date,
        "total_trading_days": total_dates,
        "total_indices": len(collector.config.indices),
        "records_fetched": total_fetched,
        "records_ingested": total_ingested,
        "duration_seconds": round(duration, 2),
        "errors": len(errors),
    }
    
    logger.info(f"Index backfill complete: {total_ingested} records in {duration:.1f}s")
    
    if errors:
        logger.warning(f"Errors encountered: {len(errors)}")
    
    return summary


def main():
    parser = argparse.ArgumentParser(description="Backfill NSE sectoral index OHLC data")
    parser.add_argument(
        "--from", 
        dest="from_date",
        default="2021-01-01",
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Batch size for inserts"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually backfilling"
    )
    
    args = parser.parse_args()
    
    if args.dry_run:
        dates = pd.bdate_range(args.from_date, args.to_date)
        collector = get_index_collector()
        logger.info(f"Dry run: Would backfill {len(dates)} days × {len(collector.config.indices)} indices")
        logger.info(f"Approximate records: {len(dates) * len(collector.config.indices)}")
        return
    
    summary = run_index_backfill(args.from_date, args.to_date, args.batch_size)
    
    print("\n" + "="*50)
    print("INDEX BACKFILL SUMMARY")
    print("="*50)
    print(f"Period: {summary['from_date']} to {summary['to_date']}")
    print(f"Trading days: {summary['total_trading_days']}")
    print(f"Indices: {summary['total_indices']}")
    print(f"Records fetched: {summary['records_fetched']}")
    print(f"Records ingested: {summary['records_ingested']}")
    print(f"Duration: {summary['duration_seconds']}s")
    print(f"Errors: {summary['errors']}")
    print("="*50)


if __name__ == "__main__":
    main()
