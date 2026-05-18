"""Backfill rank_cohort_performance with historical rank snapshots.

Re-scores 2018+ trading days via load_research_ranked_by_date and writes the
results into data/research.duckdb::rank_cohort_performance. Idempotent —
re-running for the same dates replaces those rows.

Usage:
    uv run python scripts/backfill_rank_cohort_historical.py \
        --from-date 2018-01-01 --to-date 2024-12-31 --frequency daily

For a quick smoke test on one quarter:
    uv run python scripts/backfill_rank_cohort_historical.py \
        --from-date 2024-01-01 --to-date 2024-03-31 --frequency daily
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

from ai_trading_system.research.perf_tracker.historical_backfill import (
    VALID_FREQUENCIES,
    run_historical_backfill,
)


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _print_coverage_summary(project_root: Path) -> None:
    db_path = project_root / "data" / "research.duckdb"
    if not db_path.exists():
        return
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        total = con.execute("SELECT COUNT(*) FROM rank_cohort_performance").fetchone()[0]
        date_range = con.execute(
            "SELECT MIN(run_date), MAX(run_date) FROM rank_cohort_performance"
        ).fetchone()
        per_year = con.execute(
            """
            SELECT extract(year FROM run_date) AS yr,
                   COUNT(*) AS rows,
                   COUNT(DISTINCT run_date) AS dates,
                   COUNT(DISTINCT symbol_id) AS symbols,
                   AVG(CASE WHEN fwd_20d_return IS NULL THEN 0 ELSE 1 END) AS fwd_20d_fill
            FROM rank_cohort_performance
            GROUP BY yr ORDER BY yr
            """
        ).fetchall()
    finally:
        con.close()
    print()
    print("Table coverage after backfill:")
    print(f"  total rows: {total}  date range: {date_range[0]} → {date_range[1]}")
    print(f"  {'year':<6}{'rows':>10}{'dates':>8}{'symbols':>10}{'fwd_20d_fill':>15}")
    for yr, rows, dates, syms, fill in per_year:
        print(f"  {int(yr):<6}{rows:>10}{dates:>8}{syms:>10}{fill:>15.1%}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-date", type=_parse_date, required=True)
    parser.add_argument("--to-date", type=_parse_date, required=True)
    parser.add_argument(
        "--frequency",
        choices=VALID_FREQUENCIES,
        default="daily",
        help="Cadence of dates to backfill (default: daily).",
    )
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress INFO-level progress logs (errors still print).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.to_date < args.from_date:
        print("ERROR: --to-date must be on or after --from-date", file=sys.stderr)
        return 1

    print(f"Historical backfill: {args.from_date} → {args.to_date} ({args.frequency})")
    print(f"Project root       : {args.project_root}")
    print()

    result = run_historical_backfill(
        from_date=args.from_date,
        to_date=args.to_date,
        project_root=args.project_root,
        frequency=args.frequency,
        exchange=args.exchange,
    )
    print()
    print(f"Done: dates_processed={result['dates_processed']}, rows_upserted={result['rows_upserted']}")
    _print_coverage_summary(args.project_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
