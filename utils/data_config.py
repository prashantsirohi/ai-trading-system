"""Data retention and path helpers for operational and research domains."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from utils.data_domains import DataDomainPaths, ensure_domain_layout, get_domain_paths
from utils.logger import logger

ENV = os.getenv("ENV", "local").lower()
DATA_DOMAIN = os.getenv("DATA_DOMAIN", "operational").lower()

RETENTION_YEARS = {
    "local": None,
    "github": 1,
    "prod": 1,
}


def get_active_domain_paths(project_root: Path | str | None = None) -> DataDomainPaths:
    """Return the currently configured domain layout."""
    return ensure_domain_layout(project_root=project_root, data_domain=DATA_DOMAIN)


ACTIVE_PATHS = get_active_domain_paths()
FEATURE_STORE_DIR = ACTIVE_PATHS.feature_store_dir
OHLCV_DB_PATH = ACTIVE_PATHS.ohlcv_db_path
MASTER_DB_PATH = ACTIVE_PATHS.master_db_path


def data_retention_years(data_domain: str | None = None) -> int | None:
    """Operational data uses rolling retention; research data remains static."""
    if (data_domain or DATA_DOMAIN) == "research":
        return None
    return RETENTION_YEARS.get(ENV, None)


def should_truncate_data(data_domain: str | None = None) -> bool:
    """Check whether retention pruning should run for the requested domain."""
    return data_retention_years(data_domain) is not None


def get_cutoff_date(data_domain: str | None = None):
    """Get cutoff date for operational data retention."""
    retention_years = data_retention_years(data_domain)
    if retention_years is None:
        return None
    return datetime.now() - timedelta(days=365 * retention_years)


def truncate_old_data(project_root: Path | str | None = None, data_domain: str | None = None) -> None:
    """Prune old operational data without touching the research store."""
    domain = data_domain or DATA_DOMAIN
    if not should_truncate_data(domain):
        return

    cutoff = get_cutoff_date(domain)
    if cutoff is None:
        return

    paths = get_domain_paths(project_root=project_root, data_domain=domain)
    logger.info(
        "Truncating %s data older than %s (keeping %s year(s))",
        domain,
        cutoff.date(),
        data_retention_years(domain),
    )

    if paths.ohlcv_db_path.exists():
        conn = duckdb.connect(str(paths.ohlcv_db_path))
        try:
            conn.execute("DELETE FROM _catalog WHERE timestamp < ?", [cutoff.date()])
            logger.info("Truncated OHLCV catalog for %s", domain)
        except Exception as exc:
            logger.warning(f"Could not truncate OHLCV for {domain}: {exc}")
        finally:
            conn.close()

    if paths.feature_store_dir.exists():
        for parquet_file in paths.feature_store_dir.rglob("*.parquet"):
            try:
                df = pq.read_table(str(parquet_file)).to_pandas()
                if "timestamp" in df.columns:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                    df = df[df["timestamp"] >= cutoff]
                    df.to_parquet(str(parquet_file), index=False)
            except Exception as exc:
                logger.warning(f"Could not truncate {parquet_file}: {exc}")

    logger.info("Data truncation complete for %s", domain)
