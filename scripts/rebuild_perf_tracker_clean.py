"""Build and validate a clean performance-tracker database candidate."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from ai_trading_system.research.perf_tracker.backfill import run_backfill
from ai_trading_system.research.perf_tracker.health import build_tracker_health
from ai_trading_system.research.perf_tracker.historical_backfill import run_historical_backfill


def _parse_date(raw: str) -> date:
    return date.fromisoformat(raw)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-data-root", type=Path, required=True)
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--historical-from", type=_parse_date, default=date(2018, 1, 1))
    parser.add_argument("--historical-to", type=_parse_date, default=date(2026, 3, 27))
    parser.add_argument("--historical-frequency", choices=("daily", "weekly", "quarterly"), default="quarterly")
    return parser.parse_args()


def _link(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        return
    target.symlink_to(source, target_is_directory=source.is_dir())


def _validate(db_path: Path) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        weekend_rows = con.execute(
            "SELECT COUNT(*) FROM rank_cohort_performance WHERE DAYOFWEEK(run_date) IN (0, 6)"
        ).fetchone()[0]
        source_counts = con.execute(
            """
            SELECT COALESCE(source_type, '<null>') AS source_type,
                   COUNT(*) AS rows,
                   COUNT(DISTINCT run_date) AS dates,
                   MIN(run_date) AS first_date,
                   MAX(run_date) AS last_date
            FROM rank_cohort_performance
            GROUP BY source_type
            ORDER BY source_type
            """
        ).fetchall()
        factor_coverage = con.execute(
            """
            SELECT
                COUNT(factor_rs), COUNT(factor_vol), COUNT(factor_trend),
                COUNT(factor_prox), COUNT(factor_deliv), COUNT(factor_sector),
                COUNT(factor_momentum_accel), COUNT(factor_above_200dma),
                COUNT(factor_liquidity), COUNT(factor_delivery_trend)
            FROM rank_cohort_performance_trusted
            """
        ).fetchone()
        bucket_counts = con.execute(
            """
            SELECT COALESCE(watchlist_bucket, 'unassigned'), COUNT(*)
            FROM rank_cohort_performance_trusted
            GROUP BY 1 ORDER BY 2 DESC
            """
        ).fetchall()
    finally:
        con.close()
    return {
        "weekend_rows": int(weekend_rows or 0),
        "source_counts": [
            {
                "source_type": row[0],
                "rows": int(row[1]),
                "dates": int(row[2]),
                "first_date": row[3].isoformat() if row[3] else None,
                "last_date": row[4].isoformat() if row[4] else None,
            }
            for row in source_counts
        ],
        "factor_non_null_rows": {
            key: int(value or 0)
            for key, value in zip(
                (
                    "factor_rs", "factor_vol", "factor_trend", "factor_prox",
                    "factor_deliv", "factor_sector", "factor_momentum_accel",
                    "factor_above_200dma", "factor_liquidity",
                    "factor_delivery_trend",
                ),
                factor_coverage,
                strict=True,
            )
        },
        "bucket_counts": {row[0]: int(row[1]) for row in bucket_counts},
    }


def main() -> int:
    args = _parse_args()
    source_root = args.source_data_root.resolve()
    staging_root = args.staging_root.resolve()
    report_dir = args.report_dir.resolve()
    staging_root.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    candidate_db = staging_root / "research.duckdb"
    if candidate_db.exists():
        raise SystemExit(f"Candidate already exists: {candidate_db}")

    _link(source_root / "ohlcv.duckdb", staging_root / "ohlcv.duckdb")
    _link(source_root / "pipeline_runs", staging_root / "pipeline_runs")
    _link(
        source_root / "research" / "research_ohlcv.duckdb",
        staging_root / "research" / "research_ohlcv.duckdb",
    )

    os.environ["DATA_ROOT"] = str(staging_root)
    historical = run_historical_backfill(
        from_date=args.historical_from,
        to_date=args.historical_to,
        frequency=args.historical_frequency,
    )
    operational = run_backfill()
    health = build_tracker_health()
    validation = _validate(candidate_db)
    accepted = (
        health["fixture_rows"] == 0
        and health["duplicate_keys"] == 0
        and validation["weekend_rows"] == 0
        and health["latest_date"] == health["latest_pipeline_artifact_date"]
    )
    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "candidate_db": str(candidate_db),
        "historical_backfill": historical,
        "operational_backfill": operational,
        "health": health,
        "validation": validation,
        "accepted": accepted,
    }
    (report_dir / "rebuild_validation.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
