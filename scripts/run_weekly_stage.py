"""Compute weekly stage labels for every symbol and persist snapshots.

Reads daily OHLCV from `_catalog` in ohlcv.duckdb, resamples to W-FRI bars,
classifies each symbol's latest weekly stage, and writes to:

    data/ohlcv.duckdb :: weekly_stage_snapshot
    data/stage_store/weekly_stage_snapshots/week_end_date=YYYY-MM-DD/<run_id>.parquet

Idempotent: re-running for the same week overwrites those rows.

Usage
-----
    python -m scripts.run_weekly_stage --exchange NSE
    python -m scripts.run_weekly_stage --exchange NSE --asof 2024-04-26
    python -m scripts.run_weekly_stage --symbols RELIANCE,INFY --asof 2024-04-26
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

import duckdb
import pandas as pd

from ai_trading_system.domains.ranking.stage_classifier import (
    StageResult,
    classify_latest,
)
from ai_trading_system.domains.ranking.stage_store import (
    get_prior_stage,
    write_snapshots,
)
from ai_trading_system.domains.ranking.weekly import to_weekly
from ai_trading_system.platform.db.paths import ensure_domain_layout

LOG = logging.getLogger("run_weekly_stage")


def _load_daily(
    ohlcv_db_path: Path,
    *,
    exchange: str,
    asof: Optional[str],
    symbols: Optional[Sequence[str]],
) -> pd.DataFrame:
    """Pull daily OHLCV bars for all (or listed) symbols up to `asof`."""
    conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        params: list[object] = [exchange]
        clauses = ["exchange = ?"]
        if asof:
            clauses.append("CAST(timestamp AS DATE) <= CAST(? AS DATE)")
            params.append(asof)
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            clauses.append(f"symbol_id IN ({placeholders})")
            params.extend(symbols)
        where = " AND ".join(clauses)
        return conn.execute(
            f"""
            SELECT symbol_id AS symbol, timestamp, open, high, low, close, volume
            FROM _catalog
            WHERE {where}
            ORDER BY symbol_id, timestamp
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()


def _classify_one(
    symbol: str,
    daily: pd.DataFrame,
    *,
    ohlcv_db_path: Path,
) -> Optional[StageResult]:
    if daily.empty:
        return None
    daily = daily.set_index("timestamp").sort_index()
    weekly = to_weekly(daily)
    if weekly.empty:
        return None
    week_end = weekly.index[-1].date().isoformat()
    prior = get_prior_stage(ohlcv_db_path, symbol=symbol, before_date=week_end)
    return classify_latest(weekly, symbol=symbol, prior_stage=prior)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--asof", default=None,
                        help="ISO date; defaults to today")
    parser.add_argument("--symbols", default=None,
                        help="Comma-separated symbol list; default = all")
    parser.add_argument("--data-domain", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--ohlcv-db", default=None,
                        help="Override path to ohlcv.duckdb")
    parser.add_argument("--parquet-root", default=None,
                        help="Override parquet output root")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    paths = ensure_domain_layout(data_domain=args.data_domain)
    ohlcv_db_path = Path(args.ohlcv_db) if args.ohlcv_db else ohlcv_db_path
    if not ohlcv_db_path.exists():
        LOG.error("ohlcv.duckdb not found at %s", ohlcv_db_path)
        return 2

    parquet_root = (
        Path(args.parquet_root) if args.parquet_root
        else paths.root_dir / "stage_store" / "weekly_stage_snapshots"
    )
    run_id = args.run_id or f"weekly-stage-{uuid.uuid4().hex[:8]}"
    symbols = [s.strip() for s in args.symbols.split(",")] if args.symbols else None

    LOG.info("loading daily OHLCV from %s", ohlcv_db_path)
    daily = _load_daily(
        ohlcv_db_path,
        exchange=args.exchange,
        asof=args.asof,
        symbols=symbols,
    )
    if daily.empty:
        LOG.warning("no daily rows returned; nothing to do")
        return 0

    results: list[StageResult] = []
    skipped = 0
    for symbol, sub in daily.groupby("symbol", sort=False):
        try:
            res = _classify_one(symbol, sub, ohlcv_db_path=ohlcv_db_path)
        except Exception:  # noqa: BLE001
            LOG.exception("classifier failed for %s", symbol)
            skipped += 1
            continue
        if res is None:
            skipped += 1
            continue
        results.append(res)

    LOG.info("classified %d symbols (%d skipped)", len(results), skipped)

    summary = write_snapshots(
        results,
        ohlcv_db_path=ohlcv_db_path,
        parquet_root=parquet_root,
        run_id=run_id,
        created_at=datetime.now(timezone.utc),
    )
    LOG.info(
        "wrote %d rows | duckdb=%s | parquet=%s",
        summary["rows"], summary["duckdb_path"], summary["parquet_path"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
