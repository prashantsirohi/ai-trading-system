"""Research entrypoint for rule-based cup-and-handle and round-bottom backtests."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from analytics.patterns import PatternBacktestConfig, run_pattern_backtest
from utils.data_domains import ensure_domain_layout, research_static_end_date
from utils.logger import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research backtest for cup-and-handle and round-bottom patterns")
    parser.add_argument("--from-date", help="Inclusive start date for research scan")
    parser.add_argument("--to-date", help="Inclusive end date. Defaults to the prior year end.")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--symbols", help="Optional comma-separated symbol list")
    parser.add_argument("--output-dir", help="Optional explicit output directory")
    parser.add_argument("--project-root", help="Optional project root override for testing or custom runs")
    parser.add_argument("--precompute-all-charts", action="store_true", help="Render HTML charts for every detected event.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(args.project_root) if args.project_root else Path(__file__).resolve().parents[1]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    to_date = args.to_date or research_static_end_date()
    from_date = args.from_date or f"{max(date.fromisoformat(to_date).year - 5, 2000)}-01-01"
    symbols = [item.strip().upper() for item in str(args.symbols or "").split(",") if item.strip()]
    config = PatternBacktestConfig(exchange=args.exchange, symbols=tuple(symbols))

    with log_context(run_id="research-pattern-backtest", stage_name="patterns"):
        result = run_pattern_backtest(
            project_root=project_root,
            from_date=from_date,
            to_date=to_date,
            exchange=args.exchange,
            symbols=symbols,
            config=config,
            output_dir=args.output_dir,
            precompute_all_charts=bool(args.precompute_all_charts),
        )
        logger.info(
            "Pattern research bundle written to %s",
            result["paths"]["bundle_dir"],
        )
        print(f"Pattern events: {result['paths']['pattern_events']}")
        print(f"Pattern trades: {result['paths']['pattern_trades']}")
        print(f"Summary CSV: {result['paths']['summary_csv']}")
        print(f"Summary JSON: {result['paths']['summary_json']}")


if __name__ == "__main__":
    main()
