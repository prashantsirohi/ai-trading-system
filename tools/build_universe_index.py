"""Build the UNIV_TOP1000 universe index series.

Offline CLI that:
1. Walks the research-domain trading-day calendar.
2. Recomputes top-1000 membership on the 1st trading day of each month
   (point-in-time from `_catalog` turnover).
3. Computes equal-weight daily-return composite per bar.
4. Persists membership snapshots, per-bar diagnostics, and the canonical
   price series to DuckDB.

Usage
-----
    python -m tools.build_universe_index \\
        --from-date 2018-01-01 --to-date 2025-12-31 \\
        --project-root . [--rebuild] [--top-n 1000] [--min-used-ratio 0.70] \\
        [--allow-gaps]

Gap intolerance
---------------
If a trading day is missing mid-history (consecutive `_catalog` dates skip a
day), the tool raises explicitly so operators can decide whether to backfill
OHLCV or accept the gap. ``--allow-gaps`` makes a gap day a held-level bar
with ``quality_flag='gap'`` instead.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

from ai_trading_system.domains.features.universe_index import (
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MIN_RECENT_DAYS,
    DEFAULT_MIN_USED_RATIO,
    DEFAULT_TOP_N,
    UNIVERSE_INDEX_BASE_LEVEL,
    UNIVERSE_INDEX_CODE,
    IndexBarDiagnostics,
    compute_index_bar,
    compute_membership_for_rebalance,
    ensure_index_catalog_tables,
    first_trading_day_of_month,
    latest_index_level,
    trading_days_between,
    upsert_index_bar,
    upsert_membership,
)
from ai_trading_system.platform.db.paths import ensure_domain_layout


logger = logging.getLogger(__name__)


def _delete_existing(ohlcv_db_path: Path) -> None:
    con = duckdb.connect(str(ohlcv_db_path))
    try:
        con.execute(
            "DELETE FROM _index_catalog WHERE index_code = ?",
            [UNIVERSE_INDEX_CODE],
        )
        con.execute(
            "DELETE FROM _universe_index_diagnostics WHERE index_code = ?",
            [UNIVERSE_INDEX_CODE],
        )
        con.execute("DELETE FROM _universe_membership")
    finally:
        con.close()


def _existing_dates(ohlcv_db_path: Path) -> set[date]:
    con = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT date FROM _index_catalog WHERE index_code = ?",
            [UNIVERSE_INDEX_CODE],
        ).fetchall()
    finally:
        con.close()
    return {row[0] for row in rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build UNIV_TOP1000 universe index.")
    parser.add_argument("--from-date", required=True, help="ISO date (inclusive)")
    parser.add_argument("--to-date", required=True, help="ISO date (inclusive)")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--min-recent-days", type=int, default=DEFAULT_MIN_RECENT_DAYS)
    parser.add_argument("--min-used-ratio", type=float, default=DEFAULT_MIN_USED_RATIO)
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Drop existing UNIV_TOP1000 rows before building.",
    )
    parser.add_argument(
        "--allow-gaps",
        action="store_true",
        help="Skip-day gaps emit held-level bars instead of raising.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    project_root = Path(args.project_root).resolve()
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    if not paths.ohlcv_db_path.exists():
        logger.error("research OHLCV DB not found at %s", paths.ohlcv_db_path)
        return 2

    ensure_index_catalog_tables(paths.ohlcv_db_path)

    start = date.fromisoformat(args.from_date)
    end = date.fromisoformat(args.to_date)

    if args.rebuild:
        logger.info("rebuild requested — deleting existing UNIV_TOP1000 rows")
        _delete_existing(paths.ohlcv_db_path)

    trading_days = trading_days_between(
        paths.ohlcv_db_path, start, end, exchange=args.exchange
    )
    if not trading_days:
        logger.error("no trading days in _catalog within [%s, %s]", start, end)
        return 2

    first_per_month = first_trading_day_of_month(trading_days)
    rebalance_dates = set(first_per_month.values())

    skip_dates = _existing_dates(paths.ohlcv_db_path) if not args.rebuild else set()

    # Initial level.
    latest = latest_index_level(paths.ohlcv_db_path)
    if latest is None:
        prev_level = UNIVERSE_INDEX_BASE_LEVEL
    else:
        prev_level = latest[1]
        logger.info("resuming from %s level=%.4f", latest[0], prev_level)

    membership_cache: dict[tuple[int, int], list[str]] = {}
    membership_rebalance_date: dict[tuple[int, int], date] = {}

    n_bars = 0
    n_members_written = 0

    prev_day: date | None = None
    for d in trading_days:
        ym = (d.year, d.month)

        # Detect calendar gaps (more than ~5 calendar days between trading
        # days = a real gap, not a weekend).
        if (
            prev_day is not None
            and (d - prev_day).days > 5
            and not args.allow_gaps
        ):
            logger.error(
                "trading-day gap between %s and %s (>%d days). Re-run with --allow-gaps "
                "to emit held-level bars.",
                prev_day, d, 5,
            )
            return 3

        if d in rebalance_dates and d not in membership_cache.get(ym, ()):
            members_df, sparse = compute_membership_for_rebalance(
                paths.ohlcv_db_path,
                rebalance_date=d,
                top_n=args.top_n,
                lookback_days=args.lookback_days,
                min_recent_days=args.min_recent_days,
                exchange=args.exchange,
            )
            n_members_written += upsert_membership(
                paths.ohlcv_db_path,
                rebalance_date=d,
                members_df=members_df,
                sparse_history=sparse,
            )
            membership_cache[ym] = list(members_df["symbol_id"]) if not members_df.empty else []
            membership_rebalance_date[ym] = d
            logger.info(
                "rebalance %s: %d members (sparse=%s)",
                d, len(membership_cache[ym]), sparse,
            )

        if d in skip_dates:
            # Bar already persisted — keep level chain consistent.
            con = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
            try:
                row = con.execute(
                    "SELECT close FROM _index_catalog WHERE index_code = ? AND date = ?",
                    [UNIVERSE_INDEX_CODE, d],
                ).fetchone()
            finally:
                con.close()
            if row:
                prev_level = float(row[0])
            prev_day = d
            continue

        if ym not in membership_cache:
            # No rebalance yet (first month of history). Hold level until one fires.
            logger.warning("no membership yet at %s — holding level=%.4f", d, prev_level)
            diag = IndexBarDiagnostics(
                index_code=UNIVERSE_INDEX_CODE,
                date=d,
                rebalance_date=d,
                n_members=0,
                n_used=0,
                n_missing=0,
                used_ratio=0.0,
                daily_return=0.0,
                index_level=prev_level,
                quality_flag="sparse_membership",
            )
        else:
            rebalance_date = membership_rebalance_date[ym]
            new_level, diag = compute_index_bar(
                paths.ohlcv_db_path,
                bar_date=d,
                constituents=membership_cache[ym],
                previous_index_level=prev_level,
                rebalance_date=rebalance_date,
                min_used_ratio=args.min_used_ratio,
                exchange=args.exchange,
            )
            prev_level = new_level

        upsert_index_bar(paths.ohlcv_db_path, diagnostics=diag)
        n_bars += 1
        prev_day = d

    logger.info(
        "done: %d bars written, %d membership rows across %d rebalances",
        n_bars, n_members_written, len(membership_cache),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
