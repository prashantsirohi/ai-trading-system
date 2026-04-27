"""Inspect and optionally repair swapped symbol/exchange rows in ingest tables."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

from ai_trading_system.analytics.data_trust import ensure_data_trust_schema
from ai_trading_system.platform.db.paths import canonicalize_project_root
from ai_trading_system.platform.logging.logger import logger

PROJECT_ROOT = canonicalize_project_root(os.getenv("AI_TRADING_PROJECT_ROOT") or Path.cwd())
DEFAULT_DB_PATH = str(PROJECT_ROOT / "data" / "ohlcv.duckdb")


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _swapped_row_count(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    if not _table_exists(conn, table_name):
        return 0
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE symbol_id IN ('NSE', 'BSE')
          AND exchange NOT IN ('NSE', 'BSE')
        """
    ).fetchone()
    return int(row[0]) if row else 0


def _repair_swapped_rows(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    if not _table_exists(conn, table_name):
        return 0
    before = _swapped_row_count(conn, table_name)
    if before <= 0:
        return 0
    conn.execute(
        f"""
        UPDATE {table_name}
        SET
            symbol_id = exchange,
            exchange = symbol_id
        WHERE symbol_id IN ('NSE', 'BSE')
          AND exchange NOT IN ('NSE', 'BSE')
        """
    )
    return before


def run(*, db_path: str, apply: bool, fail_on_drift: bool) -> int:
    ensure_data_trust_schema(db_path)
    conn = duckdb.connect(db_path)
    try:
        before_counts = {
            "_catalog": _swapped_row_count(conn, "_catalog"),
            "_delivery": _swapped_row_count(conn, "_delivery"),
        }
        logger.info("Swapped-row scan (before): %s", before_counts)

        repaired_counts = {"_catalog": 0, "_delivery": 0}
        if apply:
            repaired_counts["_catalog"] = _repair_swapped_rows(conn, "_catalog")
            repaired_counts["_delivery"] = _repair_swapped_rows(conn, "_delivery")
            logger.info("Rows repaired: %s", repaired_counts)

        after_counts = {
            "_catalog": _swapped_row_count(conn, "_catalog"),
            "_delivery": _swapped_row_count(conn, "_delivery"),
        }
        logger.info("Swapped-row scan (after): %s", after_counts)
    finally:
        conn.close()

    remaining = sum(after_counts.values())
    if fail_on_drift and remaining > 0:
        logger.error("Schema drift remains after scan/repair: %s", after_counts)
        return 2
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect and repair swapped symbol_id/exchange rows in ingest tables.",
    )
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="Path to ohlcv.duckdb")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply in-place repair for swapped rows.",
    )
    parser.add_argument(
        "--fail-on-drift",
        action="store_true",
        help="Return non-zero exit code when swapped rows remain.",
    )
    args = parser.parse_args()
    return run(db_path=args.db_path, apply=args.apply, fail_on_drift=args.fail_on_drift)


if __name__ == "__main__":
    raise SystemExit(main())
