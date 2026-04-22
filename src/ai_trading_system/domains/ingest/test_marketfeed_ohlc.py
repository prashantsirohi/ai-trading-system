"""
Test Dhan Market Feed OHLC API for bulk daily updates.

This uses the v2/marketfeed/ohlc endpoint which accepts multiple
security IDs in a single request - much more efficient than
fetching one-by-one.

Usage:
    python -m collectors.test_marketfeed_ohlc
"""

import datetime
import logging
import os
from pathlib import Path

import pandas as pd
import requests

from core.bootstrap import ensure_project_root_on_path


def _resolve_project_root(anchor: str | Path) -> Path:
    env_root = os.getenv("AI_TRADING_PROJECT_ROOT")
    if env_root:
        root = Path(env_root).resolve()
        if root.exists():
            ensure_project_root_on_path(root)
            return root
    anchor_path = Path(anchor).resolve()
    for parent in anchor_path.parents:
        if parent.name == "src" and (parent / "ai_trading_system").exists():
            root = parent.parent
            ensure_project_root_on_path(root)
            return root
    return ensure_project_root_on_path(anchor_path)


project_root = _resolve_project_root(__file__)

from ai_trading_system.platform.utils.env import load_project_env

load_project_env(project_root)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


API_URL = "https://api.dhan.co/v2/marketfeed/ohlc"


def get_dhan_token() -> str:
    """Get current JWT access token from .env."""
    return os.getenv("DHAN_ACCESS_TOKEN", "")


def get_client_id() -> str:
    """Get client ID from .env."""
    return os.getenv("DHAN_CLIENT_ID", "")


def fetch_ohlc_bulk(security_ids: list, access_token: str, client_id: str) -> dict:
    """Fetch OHLC data for multiple securities in a single API call."""
    headers = {
        "access-token": access_token,
        "client-id": client_id,
        "Content-Type": "application/json",
    }

    payload = {"instruments": {"NSE_EQ": security_ids}}

    logger.info(f"Fetching OHLC for {len(security_ids)} securities...")
    response = requests.post(API_URL, headers=headers, json=payload, timeout=60)

    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Error {response.status_code}: {response.text}")
        return {}


def process_ohlc_response(data: dict) -> pd.DataFrame:
    """Convert API response to DataFrame."""
    if not data or "data" not in data:
        return pd.DataFrame()

    processed = []
    today = datetime.datetime.now()

    for sec_id, details in data.get("data", {}).items():
        ohlc = details.get("ohlc", {})
        if not ohlc:
            continue

        row = {
            "security_id": sec_id,
            "timestamp": today,
            "open": ohlc.get("open"),
            "high": ohlc.get("high"),
            "low": ohlc.get("low"),
            "close": ohlc.get("close"),
            "volume": details.get("volume"),
            "last_price": details.get("last_price"),
        }
        processed.append(row)

    return pd.DataFrame(processed)


def test_api():
    """Test the Market Feed OHLC API with a small batch."""
    access_token = get_dhan_token()
    client_id = get_client_id()

    if not access_token or not client_id:
        logger.error("Missing DHAN_ACCESS_TOKEN or DHAN_CLIENT_ID in .env")
        return

    import duckdb

    conn = duckdb.connect(
        os.path.join(str(project_root), "data", "ohlcv.duckdb"), read_only=True
    )
    try:
        symbols = conn.execute(
            "SELECT security_id FROM _catalog WHERE exchange = 'NSE' GROUP BY security_id LIMIT 10"
        ).fetchall()
        security_ids = [str(s[0]) for s in symbols]
    finally:
        conn.close()

    logger.info(f"Testing with {len(security_ids)} security IDs: {security_ids}")

    raw_data = fetch_ohlc_bulk(security_ids, access_token, client_id)

    if raw_data:
        df = process_ohlc_response(raw_data)
        logger.info(f"Received {len(df)} rows")
        print(df.head(10))
    else:
        logger.error("No data received from API")


def test_large_batch():
    """Test with larger batch (up to 1000)."""
    access_token = get_dhan_token()
    client_id = get_client_id()

    if not access_token or not client_id:
        logger.error("Missing DHAN_ACCESS_TOKEN or DHAN_CLIENT_ID in .env")
        return

    import duckdb

    conn = duckdb.connect(
        os.path.join(str(project_root), "data", "ohlcv.duckdb"), read_only=True
    )
    try:
        symbols = conn.execute(
            "SELECT security_id FROM _catalog WHERE exchange = 'NSE' GROUP BY security_id LIMIT 500"
        ).fetchall()
        security_ids = [str(s[0]) for s in symbols]
    finally:
        conn.close()

    logger.info(f"Testing with {len(security_ids)} security IDs...")

    import time

    t0 = time.time()
    raw_data = fetch_ohlc_bulk(security_ids, access_token, client_id)
    elapsed = time.time() - t0

    if raw_data:
        df = process_ohlc_response(raw_data)
        logger.info(f"Received {len(df)} rows in {elapsed:.1f}s")
        print(df.head(10))
    else:
        logger.error("No data received from API")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--large", action="store_true", help="Test with 500 symbols")
    args = parser.parse_args()

    if args.large:
        test_large_batch()
    else:
        test_api()
