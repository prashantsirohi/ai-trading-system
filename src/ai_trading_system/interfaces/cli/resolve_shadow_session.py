"""Resolve a completed NSE session without consulting price availability."""

from __future__ import annotations

import argparse
from contextlib import closing
from datetime import date, datetime, time, timedelta
from pathlib import Path
import sqlite3
import sys
from zoneinfo import ZoneInfo

import duckdb

from ai_trading_system.platform.db.paths import (
    get_domain_paths,
    require_data_root_available,
)

IST = ZoneInfo("Asia/Kolkata")
NSE_CLOSE = time(15, 30)


def resolve_session(
    master_path: Path, *, requested: date | None = None, now: datetime | None = None
) -> date:
    """Use the expected calendar, never roll back around missing market bars."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        raise ValueError("Session resolution requires a timezone-aware clock")
    now = now.astimezone(IST)
    latest = now.date() if now.time() >= NSE_CLOSE else now.date() - timedelta(days=1)
    if requested is not None and requested > latest:
        raise ValueError("Requested session is future or has not closed at 15:30 IST")
    candidate = requested or latest
    # A missing store/table/year is an error, not permission to assume weekdays.
    with closing(
        sqlite3.connect(f"{master_path.resolve().as_uri()}?mode=ro", uri=True)
    ) as conn:
        holidays = {
            date.fromisoformat(str(row[0]))
            for row in conn.execute("SELECT date FROM nse_holidays")
        }
    covered_years = {day.year for day in holidays}
    while True:
        if candidate.year not in covered_years:
            raise ValueError(
                f"NSE holiday calendar has no coverage for {candidate.year}"
            )
        if candidate.weekday() < 5 and candidate not in holidays:
            return candidate
        if requested is not None:
            raise ValueError(
                f"Requested session {candidate} is an NSE holiday or weekend"
            )
        candidate -= timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-date", type=date.fromisoformat)
    parser.add_argument("--run-id", help="Validate an existing pinned run's session")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        paths = get_domain_paths(
            project_root=args.project_root, data_domain="operational"
        )
        require_data_root_available(paths)
        now = datetime.now(IST)
        session = resolve_session(
            paths.master_db_path, requested=args.run_date, now=now
        )
        if args.run_id:
            validate_run_session(
                paths.root_dir / "control_plane.duckdb", args.run_id, session
            )
    except (ValueError, RuntimeError, sqlite3.Error, duckdb.Error, OSError) as exc:
        parser.exit(2, f"[daily-shadow] calendar error: {exc}\n")
    print(
        f"[daily-shadow] maintenance_date={now.date()} decision_session={session} (NSE calendar, 15:30 IST close)",
        file=sys.stderr,
    )
    print(session)


def validate_run_session(control_plane_path: Path, run_id: str, session: date) -> None:
    """A holiday fallback must not resume a pinned run under another date."""
    if not control_plane_path.exists():
        return  # Pipeline schema verification owns the absent-store failure.
    with duckdb.connect(str(control_plane_path), read_only=True) as conn:
        row = conn.execute(
            "SELECT CAST(run_date AS DATE) FROM pipeline_run WHERE run_id = ?", [run_id]
        ).fetchone()
    if row is not None and row[0] != session:
        raise ValueError(
            f"Pinned run {run_id} belongs to {row[0]}, not resolved session {session}; use a matching completed RUN_DATE or a new RUN_ID"
        )


if __name__ == "__main__":
    main()
