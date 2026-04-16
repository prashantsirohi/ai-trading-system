"""Archive daily NSE bhavcopy CSVs locally for a date range."""

from __future__ import annotations

import argparse
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from collectors.nse_collector import NSECollector
from core.logging import logger


def iter_business_dates(from_date: str, to_date: str) -> list[str]:
    dates = pd.bdate_range(from_date, to_date)
    return [ts.date().isoformat() for ts in dates]


def bhavcopy_filename(trade_date: str) -> str:
    dt = date.fromisoformat(trade_date)
    return f"nse_{dt.strftime('%d%b%Y').upper()}.csv"


def archive_bhavcopy_range(
    *,
    project_root: Path,
    from_date: str,
    to_date: str,
    force: bool = False,
    delay_seconds: float = 0.25,
) -> dict:
    raw_dir = project_root / "data" / "raw" / "NSE_EQ"
    raw_dir.mkdir(parents=True, exist_ok=True)

    collector = NSECollector(data_dir=str(raw_dir))
    saved_files: list[str] = []
    skipped_files: list[str] = []
    missing_dates: list[str] = []

    for trade_date in iter_business_dates(from_date, to_date):
        out_path = raw_dir / bhavcopy_filename(trade_date)
        if out_path.exists() and not force:
            skipped_files.append(out_path.name)
            continue

        df = collector.get_bhavcopy(trade_date)
        if df.empty:
            missing_dates.append(trade_date)
            continue

        df.to_csv(out_path, index=False)
        saved_files.append(out_path.name)
        logger.info("Archived NSE bhavcopy %s -> %s rows", trade_date, len(df))
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    return {
        "from_date": from_date,
        "to_date": to_date,
        "raw_dir": str(raw_dir),
        "saved_count": len(saved_files),
        "skipped_count": len(skipped_files),
        "missing_count": len(missing_dates),
        "saved_files": saved_files,
        "skipped_files": skipped_files,
        "missing_dates": missing_dates,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive NSE bhavcopy CSVs locally.")
    parser.add_argument("--from-date", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--force", action="store_true", help="Overwrite existing local CSV files.")
    parser.add_argument("--delay-seconds", type=float, default=0.25, help="Pause between downloads.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    result = archive_bhavcopy_range(
        project_root=project_root,
        from_date=args.from_date,
        to_date=args.to_date,
        force=bool(args.force),
        delay_seconds=max(0.0, float(args.delay_seconds)),
    )
    logger.info("NSE bhavcopy archive result: %s", result)


if __name__ == "__main__":
    main()
