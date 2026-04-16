"""
Full ingestion: Fetch real OHLCV data for all NSE symbols from Dhan API.
Handles 1000 API calls/day limit by batching across multiple days.

Usage:
    python ingest_full.py                    # Run one batch (respects daily limit)
    python ingest_full.py --dry-run         # Show what would be ingested
    python ingest_full.py --force           # Override daily limit check
"""

import os
import sys
import time
import asyncio
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any

from core.bootstrap import ensure_project_root_on_path

project_root = str(ensure_project_root_on_path(__file__))

from utils.env import load_project_env
from core.logging import logger

load_project_env(project_root)

from collectors.dhan_collector import DhanCollector
from collectors.ingest_validation import validate_ohlcv_frame

DB_PATH = os.path.join(project_root, "data", "ohlcv.duckdb")
DAILY_LIMIT = 1000
BATCH_SIZE = 100
MAX_CONCURRENT = 5
HISTORY_DAYS = 10000


async def _fetch_symbol(
    collector,
    symbol_info: Dict,
    from_date: str,
    to_date: str,
    semaphore: asyncio.Semaphore,
) -> tuple:
    """Fetch one symbol with semaphore rate limiting."""
    async with semaphore:
        security_id = symbol_info["security_id"]
        exchange = symbol_info["exchange"]

        clean_sid = str(security_id)
        if clean_sid.endswith(".0"):
            clean_sid = clean_sid[:-2]

        exchange_segment = "NSE_EQ" if exchange.upper() == "NSE" else "BSE_EQ"

        collector._rate_limit_wait()

        try:
            collector._ensure_valid_token()

            data = collector.dhan.historical_daily_data(
                security_id=clean_sid,
                exchange_segment=exchange_segment,
                instrument_type="EQUITY",
                from_date=from_date,
                to_date=to_date,
            )

            if not data or not isinstance(data, dict):
                return symbol_info["symbol_id"], None, "NO_DATA"

            if data.get("status") == "failure":
                remarks = data.get("remarks", {})
                if isinstance(remarks, dict):
                    return (
                        symbol_info["symbol_id"],
                        None,
                        remarks.get("error_code", "UNKNOWN"),
                    )
                return symbol_info["symbol_id"], None, str(remarks)

            inner = data.get("data", data)
            if not isinstance(inner, dict):
                return symbol_info["symbol_id"], None, "INVALID_FORMAT"

            open_arr = inner.get("open", [])
            if not open_arr or not isinstance(open_arr, list):
                return symbol_info["symbol_id"], None, "EMPTY_RESPONSE"

            import pandas as pd

            timestamps = inner.get("timestamp", [])
            if not timestamps:
                return symbol_info["symbol_id"], None, "NO_TIMESTAMP"

            df = pd.DataFrame(
                {
                    "open": inner.get("open", []),
                    "high": inner.get("high", []),
                    "low": inner.get("low", []),
                    "close": inner.get("close", []),
                    "volume": inner.get("volume", []),
                    "timestamp": pd.to_datetime(timestamps, unit="s", errors="coerce"),
                }
            )

            if df["timestamp"].isna().all():
                return symbol_info["symbol_id"], None, "INVALID_TIMESTAMP"

            df["symbol_id"] = symbol_info["symbol_id"]
            df["security_id"] = clean_sid
            df["exchange"] = exchange

            rows = len(df)
            return symbol_info["symbol_id"], df, None

        except Exception as e:
            return symbol_info["symbol_id"], None, str(e)


async def _fetch_batch_async(
    collector,
    symbols: List[Dict],
    from_date: str,
    to_date: str,
    max_concurrent: int,
) -> tuple:
    """Fetch a batch of symbols concurrently."""
    semaphore = asyncio.Semaphore(max_concurrent)

    tasks = [
        _fetch_symbol(collector, s, from_date, to_date, semaphore) for s in symbols
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    dfs = []
    errors = []
    for r in results:
        if isinstance(r, Exception):
            errors.append(("UNKNOWN", str(r)))
        else:
            sym_id, df, err = r
            if df is not None and not df.empty:
                dfs.append(df)
            elif err:
                errors.append((sym_id, err))

    return dfs, errors


def write_dfs_to_duckdb(conn, dfs: list, from_date: str, to_date: str) -> tuple:
    """Write DataFrames to DuckDB. Returns (rows_written, symbols_written)."""
    import pandas as pd

    if not dfs:
        return 0, 0

    all_rows = pd.concat(dfs, ignore_index=True)
    all_rows = validate_ohlcv_frame(all_rows, source_label="ingest_full.write_dfs_to_duckdb")

    rows_written = 0
    symbols_written = 0

    for sym_id in all_rows["symbol_id"].unique():
        sym_df = all_rows[all_rows["symbol_id"] == sym_id]
        exchange = sym_df["exchange"].iloc[0]

        conn.execute("CREATE TEMP VIEW temp_sym AS SELECT * FROM sym_df")
        try:
            conn.execute("""
                INSERT INTO _catalog
                    (symbol_id, security_id, exchange, timestamp,
                     open, high, low, close, volume)
                SELECT
                    symbol_id, security_id, exchange, timestamp,
                    open, high, low, close,
                    COALESCE(volume, 0) AS volume
                FROM temp_sym
                ON CONFLICT (symbol_id, exchange, timestamp) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """)
        finally:
            conn.execute("DROP VIEW temp_sym")

        rows_written += len(sym_df)
        symbols_written += 1

    return rows_written, symbols_written


def get_already_ingested(conn) -> set:
    """Return set of (symbol_id, exchange) already in catalog."""
    try:
        rows = conn.execute(
            "SELECT DISTINCT symbol_id, exchange FROM _catalog"
        ).fetchall()
        return {(r[0], r[1]) for r in rows}
    except Exception:
        return set()


def run_ingestion(
    collector,
    from_date: str,
    to_date: str,
    exchanges: List[str],
    batch_size: int,
    max_concurrent: int,
    force: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Main ingestion loop. Handles daily API limit by processing in batches.
    """
    symbols = collector.get_symbols_from_masterdb(exchanges=exchanges)
    if not symbols:
        return {"error": "No symbols found"}

    logger.info(f"Total symbols to ingest: {len(symbols)}")

    conn = collector._get_duckdb_conn()
    ingested = get_already_ingested(conn)
    conn.close()

    pending = [s for s in symbols if (s["symbol_id"], s["exchange"]) not in ingested]

    if not pending:
        logger.info("All symbols already ingested. Nothing to do.")
        return {"symbols_processed": 0, "status": "already_complete"}

    logger.info(f"Pending symbols: {len(pending)}")

    if dry_run:
        logger.info(
            f"[DRY RUN] Would ingest: {[s['symbol_id'] for s in pending[:20]]}..."
        )
        return {"dry_run": True, "pending_count": len(pending)}

    estimated_calls = len(pending)
    if collector.daily_request_count > 0:
        remaining = DAILY_LIMIT - collector.daily_request_count
        logger.info(
            f"Daily requests used: {collector.daily_request_count}/{DAILY_LIMIT}"
        )
        if remaining <= 0:
            logger.error("Daily API limit already reached.")
            if not force:
                return {
                    "error": "Daily limit reached",
                    "requests_used": collector.daily_request_count,
                }
        if estimated_calls > remaining and not force:
            logger.warning(
                f"Estimated {estimated_calls} calls needed but only {remaining} remaining. "
                f"Will process {remaining} symbols now. Run again tomorrow for the rest."
            )
            pending = pending[:remaining]

    t0 = time.time()
    total_rows = 0
    total_symbols = 0
    total_errors = 0
    all_errors = []

    for i in range(0, len(pending), batch_size):
        batch = pending[i : i + batch_size]
        batch_num = i // batch_size + 1
        n_batches = (len(pending) + batch_size - 1) // batch_size

        logger.info(
            f"[Batch {batch_num}/{n_batches}] Processing {len(batch)} symbols..."
        )

        dfs, errors = asyncio.run(
            _fetch_batch_async(collector, batch, from_date, to_date, max_concurrent)
        )

        if dfs:
            conn = collector._get_duckdb_conn()
            rows, syms = write_dfs_to_duckdb(conn, dfs, from_date, to_date)
            conn.close()
            total_rows += rows
            total_symbols += syms
            logger.info(f"  Wrote {rows} rows for {syms} symbols")
        else:
            logger.info("  No data fetched this batch")

        batch_errors = len(errors)
        total_errors += batch_errors
        all_errors.extend(errors)

        if errors:
            error_types = {}
            for sym, err in errors:
                error_types[err] = error_types.get(err, 0) + 1
            for err_type, count in sorted(error_types.items(), key=lambda x: -x[1]):
                logger.warning(f"  [{err_type}]: {count} symbols")

        remaining = len(pending) - (i + batch_size)
        if remaining > 0:
            wait_sec = (batch_size / max_concurrent) * 0.2 + 1
            logger.info(f"  Sleeping {wait_sec:.0f}s before next batch...")
            time.sleep(wait_sec)

    duration = time.time() - t0

    result = {
        "symbols_processed": total_symbols,
        "rows_written": total_rows,
        "errors": total_errors,
        "duration_sec": round(duration, 1),
        "from_date": from_date,
        "to_date": to_date,
    }

    logger.info(
        f"Done in {duration:.1f}s: {total_symbols} symbols, "
        f"{total_rows} rows, {total_errors} errors"
    )

    return result


def main():
    parser = argparse.ArgumentParser(description="Full ingestion from Dhan API")
    parser.add_argument(
        "--force", action="store_true", help="Override daily limit check"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be ingested"
    )
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--concurrent", type=int, default=MAX_CONCURRENT)
    parser.add_argument("--days-history", type=int, default=HISTORY_DAYS)
    parser.add_argument("--from-date", type=str, default=None)
    args = parser.parse_args()

    collector = DhanCollector(
        api_key=os.getenv("DHAN_API_KEY", ""),
        client_id=os.getenv("DHAN_CLIENT_ID", ""),
        access_token=os.getenv("DHAN_ACCESS_TOKEN", ""),
    )

    collector._ensure_valid_token()

    if args.from_date:
        from_date = args.from_date
    else:
        from_date = (datetime.now() - timedelta(days=args.days_history)).strftime(
            "%Y-%m-%d"
        )

    to_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Date range: {from_date} -> {to_date}")

    result = run_ingestion(
        collector,
        from_date=from_date,
        to_date=to_date,
        exchanges=["NSE"],
        batch_size=args.batch_size,
        max_concurrent=args.concurrent,
        force=args.force,
        dry_run=args.dry_run,
    )

    if result.get("error"):
        logger.error(f"Ingestion failed: {result['error']}")
        sys.exit(1)
    elif result.get("dry_run"):
        logger.info(f"Dry run complete. {result['pending_count']} symbols pending.")
    else:
        logger.info(f"Ingestion complete: {result}")


if __name__ == "__main__":
    main()
