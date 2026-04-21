"""Stock backfill using yfinance as fallback when Dhan API unavailable.

Usage:
    python -m collectors.stock_backfill --from 2025-01-01 --to 2026-04-18
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import duckdb

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from ai_trading_system.platform.logging.logger import logger


def fetch_yfinance_ohlc(symbol: str, from_date: str, to_date: str) -> pd.DataFrame:
    """Fetch OHLC data from yfinance for a single symbol."""
    import yfinance as yf
    
    nse_symbol = f"{symbol}.NS"
    
    try:
        data = yf.download(nse_symbol, start=from_date, end=to_date, progress=False)
        
        if data.empty:
            return pd.DataFrame()
        
        # Handle multi-index columns from yfinance
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        
        # Reset index and rename
        df = data.reset_index()
        df = df.rename(columns={
            'Date': 'timestamp',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
        })
        
        # Keep only needed columns
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        
        # Add symbol info
        df['symbol_id'] = symbol
        df['exchange'] = 'NSE'
        
        return df
        
    except Exception as e:
        logger.warning(f"yfinance fetch failed for {symbol}: {e}")
        return pd.DataFrame()


def get_pending_symbols(ohlcv_db_path: str, masterdb_path: str) -> list:
    """Get symbols in masterdb but not in _catalog."""
    import sqlite3
    
    m_conn = sqlite3.connect(masterdb_path)
    d_conn = duckdb.connect(ohlcv_db_path, read_only=True)
    
    master_symbols = set([r[0] for r in m_conn.execute(
        "SELECT symbol_id FROM symbols WHERE exchange = 'NSE'"
    ).fetchall()])
    
    cataloged = set([r[0] for r in d_conn.execute(
        "SELECT DISTINCT symbol_id FROM _catalog WHERE exchange = 'NSE'"
    ).fetchall()])
    
    pending = list(master_symbols - cataloged)
    
    m_conn.close()
    d_conn.close()
    
    return pending


def run_stock_backfill(
    ohlcv_db_path: str,
    masterdb_path: str,
    from_date: str,
    to_date: str,
    batch_size: int = 50,
    limit: int = None,
) -> dict:
    """Run stock backfill using yfinance."""
    import sqlite3
    
    # Get pending symbols
    pending = get_pending_symbols(ohlcv_db_path, masterdb_path)
    
    if limit:
        pending = pending[:limit]
    
    logger.info(f"Starting stock backfill: {len(pending)} symbols")
    logger.info(f"Date range: {from_date} to {to_date}")
    
    total_fetched = 0
    total_ingested = 0
    failed = []
    
    # Process in batches
    for idx, symbol in enumerate(pending):
        try:
            df = fetch_yfinance_ohlc(symbol, from_date, to_date)
            
            if df.empty:
                failed.append((symbol, "No data"))
                continue
            
            # Write to DuckDB
            conn = duckdb.connect(ohlcv_db_path)
            try:
                conn.register('stock_data', df)
                conn.execute("""
                    INSERT INTO _catalog 
                    (symbol_id, exchange, timestamp, open, high, low, close, volume, 
                     provider, provider_priority, validation_status)
                    SELECT 
                        symbol_id, exchange, timestamp::TIMESTAMP, open, high, low, close, volume,
                        'yfinance', 2, 'trusted_fallback'
                    FROM stock_data
                """)
                conn.execute("DROP VIEW stock_data")
                total_ingested += len(df)
            finally:
                conn.close()
            
            total_fetched += 1
            
            # Progress update
            if (idx + 1) % 10 == 0:
                logger.info(f"Progress: {idx+1}/{len(pending)} symbols, {total_ingested} rows")
            
            # Rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            failed.append((symbol, str(e)[:50]))
            continue
    
    logger.info(f"Stock backfill complete: {total_fetched} symbols, {total_ingested} rows")
    
    return {
        "symbols_processed": total_fetched,
        "rows_ingested": total_ingested,
        "failed": len(failed),
        "failed_symbols": failed[:10],
    }


def main():
    import sqlite3
    
    parser = argparse.ArgumentParser(description="Backfill stock OHLC using yfinance")
    parser.add_argument("--from", dest="from_date", default="2025-01-01", help="Start date")
    parser.add_argument("--to", dest="to_date", default=datetime.now().strftime("%Y-%m-%d"), help="End date")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of symbols")
    parser.add_argument("--dry-run", action="store_true", help="Show pending count")
    
    args = parser.parse_args()
    
    ohlcv_db_path = str(project_root / "data" / "ohlcv.duckdb")
    masterdb_path = str(project_root / "data" / "masterdata.db")
    
    if args.dry_run:
        pending = get_pending_symbols(ohlcv_db_path, masterdb_path)
        logger.info(f"Dry run: {len(pending)} symbols need backfill")
        return
    
    result = run_stock_backfill(
        ohlcv_db_path=ohlcv_db_path,
        masterdb_path=masterdb_path,
        from_date=args.from_date,
        to_date=args.to_date,
        limit=args.limit,
    )
    
    print("\n" + "="*50)
    print("STOCK BACKFILL SUMMARY")
    print("="*50)
    print(f"Symbols processed: {result['symbols_processed']}")
    print(f"Rows ingested: {result['rows_ingested']}")
    print(f"Failed: {result['failed']}")
    if result['failed_symbols']:
        print(f"Sample failures: {result['failed_symbols'][:5]}")
    print("="*50)


if __name__ == "__main__":
    main()
