"""Standalone CLI for the screenshot-based fundamental opportunity report."""

from __future__ import annotations

import argparse
from pathlib import Path

from ai_trading_system.domains.fundamentals.analytical_store import default_fundamentals_duckdb_path
from ai_trading_system.domains.publish.channels.fundamental_opportunities.builder import (
    build_fundamental_opportunity_report,
)
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.platform.utils.env import load_project_env


def main() -> None:
    load_project_env(Path.cwd())
    paths = get_domain_paths()
    parser = argparse.ArgumentParser(description="Build a screenshot-style fundamental opportunity PDF report.")
    parser.add_argument("--as-of", default=None, help="As-of date as YYYY-MM-DD. Defaults to latest available.")
    parser.add_argument("--fundamentals-db-path", type=Path, default=default_fundamentals_duckdb_path())
    parser.add_argument("--ohlcv-db-path", type=Path, default=paths.ohlcv_db_path)
    parser.add_argument("--tracker-db-path", type=Path, default=paths.root_dir / "candidate_tracker.duckdb")
    parser.add_argument("--fundamental-scores-path", type=Path, default=paths.fundamentals_dir / "fundamental_scores_latest.csv")
    parser.add_argument("--universe-id", default="UNIV_TOP1000_MCAP")
    parser.add_argument("--limit-per-bucket", type=int, default=25)
    parser.add_argument("--output-dir", type=Path, default=paths.reports_dir / "fundamental_opportunities")
    parser.add_argument("--update-tracker", action="store_true", help="Update candidate_tracker.duckdb from the generated shortlist.")
    args = parser.parse_args()

    manifest = build_fundamental_opportunity_report(
        as_of=args.as_of,
        fundamentals_db_path=args.fundamentals_db_path,
        ohlcv_db_path=args.ohlcv_db_path,
        tracker_db_path=args.tracker_db_path,
        fundamental_scores_path=args.fundamental_scores_path,
        universe_id=args.universe_id,
        limit_per_bucket=args.limit_per_bucket,
        output_dir=args.output_dir,
        update_tracker=args.update_tracker,
    )
    print(f"HTML: {manifest['html_path']}")
    if manifest.get("pdf_path"):
        print(f"PDF: {manifest['pdf_path']}")
    if manifest.get("pdf_error"):
        print(f"PDF warning: {manifest['pdf_error']}")
    print(f"Shortlist: {manifest['shortlist_path']}")
    print(f"Manifest: {manifest['manifest_path']}")
    if manifest.get("tracker_update"):
        print(f"Tracker update: {manifest['tracker_update']}")
    warnings = manifest.get("warnings") or []
    for warning in warnings:
        print(f"Warning: {warning}")


if __name__ == "__main__":
    main()
