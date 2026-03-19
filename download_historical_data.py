import sys
import os
import time
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collectors.dhan_collector import DhanCollector


def clean_security_id(security_id):
    """Remove .0 suffix from security_id"""
    if security_id and security_id.endswith(".0"):
        return security_id[:-2]
    return security_id


def get_all_symbols(collector):
    """Get all symbols with security_id"""
    conn = sqlite3.connect(collector.db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT security_id
        FROM symbols 
        WHERE security_id IS NOT NULL 
        AND security_id != ''
        ORDER BY security_id
    """)

    security_ids = [str(row[0]) for row in cursor.fetchall()]
    conn.close()
    return security_ids


def mark_downloaded(collector, security_id, from_date, to_date, rows_count):
    """Mark symbol as downloaded in tracker"""
    conn = sqlite3.connect(collector.db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR REPLACE INTO download_tracker 
        (security_id, last_updated_date, starting_date, status, rows_count)
        VALUES (?, ?, ?, 'completed', ?)
    """,
        (security_id, to_date, from_date, rows_count),
    )
    conn.commit()
    conn.close()


def main():
    load_dotenv()

    client_id = os.getenv("DHAN_CLIENT_ID", "")
    access_token = os.getenv("DHAN_ACCESS_TOKEN", "")

    if not client_id or not access_token:
        logger.error("API credentials not found in .env")
        return

    collector = DhanCollector(client_id=client_id, access_token=access_token)

    # Create tracker table
    collector.init_download_tracker()

    # Get all symbols
    all_symbols = get_all_symbols(collector)
    logger.info(f"Total symbols to download: {len(all_symbols)}")

    # Calculate date range (10 years)
    from_date = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")

    # Download settings
    max_per_day = 4000  # Leave margin for API limit

    logger.info(f"Starting download from {from_date} to {to_date}")
    logger.info(f"Max downloads today: {max_per_day}")

    downloaded = 0
    skipped = 0
    failed = 0

    for i, security_id in enumerate(all_symbols[:max_per_day]):
        logger.info(
            f"[{i + 1}/{min(len(all_symbols), max_per_day)}] Processing {security_id}"
        )

        # Check if already downloaded
        if collector.parquet_file_exists(security_id):
            logger.info(f"  File exists, skipping")
            mark_downloaded(collector, security_id, from_date, to_date, 0)
            skipped += 1
            continue

        # Download data
        try:
            df = collector.get_historical_data(
                security_id=security_id, from_date=from_date, to_date=to_date
            )

            if df is not None and not df.empty:
                collector.save_to_parquet(df, security_id)
                mark_downloaded(collector, security_id, from_date, to_date, len(df))
                logger.info(f"  Downloaded {len(df)} rows -> {security_id}.parquet")
                downloaded += 1
            else:
                logger.warning(f"  No data returned")
                failed += 1
        except Exception as e:
            logger.error(f"  Error: {e}")
            failed += 1

        # Rate limiting (1 request per second)
        time.sleep(1.2)

    logger.info(f"\n=== Download Summary ===")
    logger.info(f"Downloaded: {downloaded}")
    logger.info(f"Skipped (exists): {skipped}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Remaining: {len(all_symbols) - downloaded - skipped - failed}")


if __name__ == "__main__":
    main()
