"""Populate the research domain with static historical OHLCV data."""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

from collectors.ingest_full import run_ingestion
from collectors.dhan_collector import DhanCollector
from core.paths import ensure_domain_layout, research_static_end_date
from core.logging import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill static research OHLCV data")
    parser.add_argument("--from-date", default="2000-01-01")
    parser.add_argument("--to-date", help="Defaults to Dec 31 of prior year")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    to_date = args.to_date or research_static_end_date(date.today())

    with log_context(run_id="research-backfill", stage_name="ingest"):
        logger.info(
            "Starting research backfill from=%s to=%s db=%s",
            args.from_date,
            to_date,
            paths.ohlcv_db_path,
        )
        collector = DhanCollector(
            api_key=os.getenv("DHAN_API_KEY", ""),
            client_id=os.getenv("DHAN_CLIENT_ID", ""),
            access_token=os.getenv("DHAN_ACCESS_TOKEN", ""),
            db_path=str(paths.ohlcv_db_path),
            masterdb_path=str(paths.master_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain="research",
        )
        if not collector.use_api:
            access_token = collector.token_manager.ensure_valid_token()
            collector.client_id = collector.client_id or collector.token_manager.client_id
            collector.api_key = collector.api_key or collector.token_manager.api_key
            collector.access_token = access_token or collector.access_token
            collector.use_api = bool(collector.client_id and collector.access_token)
            if collector.use_api:
                collector._init_dhan_client()
        result = run_ingestion(
            collector=collector,
            from_date=args.from_date,
            to_date=to_date,
            exchanges=["NSE"],
            batch_size=args.batch_size,
            max_concurrent=args.max_concurrent,
            force=args.force,
            dry_run=args.dry_run,
        )
        logger.info("Research backfill result: %s", result)


if __name__ == "__main__":
    main()
