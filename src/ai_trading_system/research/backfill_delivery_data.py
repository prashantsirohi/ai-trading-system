"""Populate the research domain with static historical delivery data."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from collectors.delivery_collector import DeliveryCollector
from ai_trading_system.platform.db.paths import ensure_domain_layout, research_static_end_date
from ai_trading_system.platform.logging.logger import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill static research delivery data"
    )
    parser.add_argument("--from-date", default="2000-01-01")
    parser.add_argument("--to-date", help="Defaults to Dec 31 of prior year")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--backend",
        choices=["mto", "nse_securitywise"],
        default="nse_securitywise",
        help="Delivery data source backend",
    )
    parser.add_argument("--symbol-limit", type=int, help="Limit symbols for canary backfills")
    parser.add_argument("--skip-features", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[3]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    to_date = args.to_date or research_static_end_date(date.today())

    with log_context(run_id="research-delivery-backfill", stage_name="delivery"):
        logger.info(
            "Starting research delivery backfill from=%s to=%s db=%s",
            args.from_date,
            to_date,
            paths.ohlcv_db_path,
        )
        collector = DeliveryCollector(
            ohlcv_db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain="research",
            source=args.backend,
        )
        symbols = None
        if args.symbol_limit:
            symbols = collector.security_scraper.get_nse_symbols(limit=args.symbol_limit)
        inserted = collector.fetch_range(
            from_date=args.from_date,
            to_date=to_date,
            n_workers=args.workers,
            symbols=symbols,
        )
        feature_rows = 0
        if not args.skip_features:
            feature_rows = collector.compute_delivery_features(exchange="NSE")
        logger.info(
            "Research delivery backfill complete inserted=%s feature_rows=%s",
            inserted,
            feature_rows,
        )


if __name__ == "__main__":
    main()
