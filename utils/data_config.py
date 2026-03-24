"""
Data retention configuration.
Determines how much historical data to keep based on environment.
"""

import os
from pathlib import Path

ENV = os.getenv("ENV", "local").lower()

RETENTION_YEARS = {
    "local": None,  # Keep all data
    "github": 1,  # Keep only 1 year
    "prod": 1,
}

DATA_RETENTION_YEARS = RETENTION_YEARS.get(ENV, None)

FEATURE_STORE_DIR = Path("data/feature_store")
OHLCV_DB_PATH = Path("data/ohlcv.duckdb")
MASTER_DB_PATH = Path("data/masterdata.db")


def should_truncate_data() -> bool:
    """Check if data should be truncated."""
    return DATA_RETENTION_YEARS is not None


def get_cutoff_date():
    """Get cutoff date for data retention."""
    from datetime import datetime, timedelta

    if DATA_RETENTION_YEARS is None:
        return None

    return datetime.now() - timedelta(days=365 * DATA_RETENTION_YEARS)


def truncate_old_data():
    """
    Truncate old data to retain only specified years.
    Call this at start of GitHub Actions workflow.
    """
    if not should_truncate_data():
        return

    from utils.logger import logger
    import sqlite3
    import pyarrow.parquet as pq
    import pandas as pd

    cutoff = get_cutoff_date()
    if cutoff is None:
        return

    logger.info(
        f"Truncating data older than {cutoff.date()} (keeping {DATA_RETENTION_YEARS} year(s))"
    )

    # Truncate OHLCV DuckDB
    if OHLCV_DB_PATH.exists():
        conn = sqlite3.connect(OHLCV_DB_PATH)
        try:
            conn.execute(f"DELETE FROM ohlcv WHERE timestamp < '{cutoff.date()}'")
            conn.commit()
            logger.info("Truncated OHLCV data")
        except Exception as e:
            logger.warning(f"Could not truncate OHLCV: {e}")
        finally:
            conn.close()

    # Truncate feature store parquet files
    if FEATURE_STORE_DIR.exists():
        for parquet_file in FEATURE_STORE_DIR.rglob("*.parquet"):
            try:
                df = pq.read_table(str(parquet_file)).to_pandas()
                if "timestamp" in df.columns:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                    df = df[df["timestamp"] >= cutoff]
                    df.to_parquet(str(parquet_file), index=False)
            except Exception as e:
                logger.warning(f"Could not truncate {parquet_file}: {e}")

    logger.info("Data truncation complete")
