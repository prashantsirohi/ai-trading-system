"""CLI for single-symbol diagnostic reports."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths, require_data_root_available

from .dataset import build_symbol_report
from .loaders import latest_ohlcv_date, normalize_symbol
from .renderer import render_symbol_report


def _parse_period_start(to_date: date, period: str) -> date:
    value = str(period or "1y").strip().lower()
    if value.endswith("y"):
        years = int(value[:-1] or "1")
        return (pd.Timestamp(to_date) - pd.DateOffset(years=years)).date()
    if value.endswith("m"):
        months = int(value[:-1] or "1")
        return (pd.Timestamp(to_date) - pd.DateOffset(months=months)).date()
    if value.endswith("d"):
        days = int(value[:-1] or "1")
        return (pd.Timestamp(to_date) - pd.Timedelta(days=days)).date()
    raise argparse.ArgumentTypeError("period must use a suffix like 1y, 6m, or 90d")


def _default_output(reports_dir: Path, *, symbol: str, from_date: date, to_date: date) -> Path:
    return reports_dir / "symbol_reports" / f"{symbol}_{from_date.isoformat()}_{to_date.isoformat()}.html"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a single-symbol system performance report.")
    parser.add_argument("--symbol", required=True, help="Symbol id, e.g. RELIANCE")
    parser.add_argument("--exchange", default="NSE", help="Exchange, default NSE")
    parser.add_argument("--from-date", help="Start date YYYY-MM-DD. Defaults from --period.")
    parser.add_argument("--to-date", help="End date YYYY-MM-DD. Defaults to latest OHLCV date for the symbol.")
    parser.add_argument("--period", default="1y", help="Lookback when --from-date is omitted, e.g. 1y, 6m, 90d.")
    parser.add_argument("--output", help="Output HTML path. Defaults under reports/symbol_reports.")
    parser.add_argument("--project-root", help="Repository root override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    symbol = normalize_symbol(args.symbol)
    exchange = str(args.exchange or "NSE").strip().upper()

    paths = get_domain_paths(project_root=args.project_root, data_domain="operational")
    require_data_root_available(paths)

    to_date = pd.Timestamp(args.to_date).date() if args.to_date else latest_ohlcv_date(
        paths.ohlcv_db_path,
        symbol=symbol,
        exchange=exchange,
    )
    if to_date is None:
        parser.error(f"No OHLCV history found for {symbol}/{exchange}")
    from_date = pd.Timestamp(args.from_date).date() if args.from_date else _parse_period_start(to_date, args.period)

    output = Path(args.output) if args.output else _default_output(
        paths.reports_dir,
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
    )

    try:
        data = build_symbol_report(
            paths,
            symbol=symbol,
            exchange=exchange,
            from_date=from_date,
            to_date=to_date,
        )
    except ValueError as exc:
        parser.error(str(exc))

    written = render_symbol_report(data, output)
    print(str(written))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
