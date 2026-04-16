"""Research backtest entrypoint using the static research data domain."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from analytics.backtester import EventBacktester
from analytics.rank_backtester import RankBacktester
from core.paths import ensure_domain_layout, research_static_end_date
from core.logging import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research backtesting pipeline")
    parser.add_argument("--mode", choices=["event", "rank"], default="rank")
    parser.add_argument("--from-date", help="Inclusive start date for research backtests")
    parser.add_argument("--to-date", help="Inclusive end date. Defaults to prior year end.")
    parser.add_argument("--event-type", default="BREAKOUT")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--rebalance-days", type=int, default=21)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    to_date = args.to_date or research_static_end_date()
    from_date = args.from_date or f"{max(date.fromisoformat(to_date).year - 5, 2000)}-01-01"

    with log_context(run_id="research-backtest", stage_name=args.mode):
        logger.info("Starting research backtest mode=%s from=%s to=%s", args.mode, from_date, to_date)
        if args.mode == "event":
            backtester = EventBacktester(
                ohlcv_db_path=str(paths.ohlcv_db_path),
                feature_store_dir=str(paths.feature_store_dir),
                data_domain="research",
            )
            result = backtester.run_event_backtest(
                event_type=args.event_type,
                from_date=from_date,
                to_date=to_date,
            )
            logger.info("Research event backtest complete: %s", result.get("metrics", {}))
            return

        rank_backtester = RankBacktester(
            ohlcv_db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            top_n=args.top_n,
            rebalance_days=args.rebalance_days,
            data_domain="research",
        )
        result = rank_backtester.quick_backtest(
            from_date=from_date,
            to_date=to_date,
        )
        logger.info("Research rank backtest complete: %s", result.get("metrics", {}))


if __name__ == "__main__":
    main()
