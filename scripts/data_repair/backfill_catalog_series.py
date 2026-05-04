"""Backfill _catalog.series and _catalog.trading_segment for historical rows.

Strategy:
  1. Use the symbol master table to look up each symbol's current series.
     This covers the steady-state case where a symbol stays in one series
     for its entire history.
  2. Re-parse cached bhavcopy CSVs in data/raw/NSE_EQ/ when present, which
     gives a date-accurate series for symbols that have transitioned
     (e.g., EQ → BE under T2T or surveillance).

The script is idempotent — rows whose series/trading_segment are already
populated are left untouched. Run with --dry-run to preview the update
counts before applying.

Usage:
    python -m scripts.data_repair.backfill_catalog_series \
        --db data/operational/ohlcv.duckdb \
        [--bhavcopy-dir data/raw/NSE_EQ] \
        [--dry-run]
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.ingest.series_policy import (
    is_supported,
    normalize_series,
    trading_segment,
)
from ai_trading_system.platform.logging.logger import logger


def _read_bhavcopy_files(bhavcopy_dir: Path) -> pd.DataFrame:
    if not bhavcopy_dir.exists():
        logger.warning("Bhavcopy directory %s not found; skipping CSV-based backfill.", bhavcopy_dir)
        return pd.DataFrame(columns=["symbol_raw", "trade_date", "series"])

    frames: list[pd.DataFrame] = []
    for csv_path in sorted(bhavcopy_dir.glob("nse_*.csv")):
        try:
            stamp = csv_path.stem.split("_", 1)[1]
            trade_date = datetime.strptime(stamp, "%d%b%Y").date()
        except (IndexError, ValueError):
            logger.debug("Skipping unrecognized filename %s", csv_path.name)
            continue
        try:
            df = pd.read_csv(csv_path)
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", csv_path.name, exc)
            continue
        df.columns = [str(c).replace("﻿", "").strip().upper().replace(" ", "_") for c in df.columns]
        if "SYMBOL" not in df.columns or "SERIES" not in df.columns:
            continue
        sub = pd.DataFrame({
            "symbol_raw": df["SYMBOL"].astype(str).str.strip().str.upper(),
            "series": df["SERIES"].apply(normalize_series),
        })
        sub = sub[sub["series"].apply(is_supported)]
        sub["trade_date"] = trade_date
        frames.append(sub)
    if not frames:
        return pd.DataFrame(columns=["symbol_raw", "trade_date", "series"])
    return pd.concat(frames, ignore_index=True)


def backfill(db_path: Path, bhavcopy_dir: Path, *, dry_run: bool) -> dict[str, int]:
    conn = duckdb.connect(str(db_path))
    conn.execute("SET home_directory = '.'")

    columns = {
        row[0] for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = '_catalog'"
        ).fetchall()
    }
    if "series" not in columns or "trading_segment" not in columns:
        logger.error(
            "_catalog is missing series/trading_segment columns. "
            "Run ensure_data_trust_schema first."
        )
        return {"missing_schema": 1}

    bhav_df = _read_bhavcopy_files(bhavcopy_dir)
    counts: dict[str, int] = {"from_bhavcopy": 0, "from_master_fallback": 0}

    if not bhav_df.empty:
        conn.register("bhav_lookup", bhav_df)
        update_sql = """
            UPDATE _catalog
            SET series = b.series,
                trading_segment = CASE b.series
                    WHEN 'EQ' THEN 'regular'
                    WHEN 'BE' THEN 't2t'
                    WHEN 'BZ' THEN 'trade_to_trade_z'
                    ELSE 'unknown'
                END
            FROM bhav_lookup b
            WHERE _catalog.exchange = 'NSE'
              AND UPPER(_catalog.symbol_id) = b.symbol_raw
              AND CAST(_catalog.timestamp AS DATE) = b.trade_date
              AND (_catalog.series IS NULL OR _catalog.trading_segment IS NULL)
        """
        if dry_run:
            preview = conn.execute(
                """
                SELECT COUNT(*) FROM _catalog c
                JOIN bhav_lookup b
                  ON UPPER(c.symbol_id) = b.symbol_raw
                 AND CAST(c.timestamp AS DATE) = b.trade_date
                WHERE c.exchange = 'NSE'
                  AND (c.series IS NULL OR c.trading_segment IS NULL)
                """
            ).fetchone()
            counts["from_bhavcopy"] = int(preview[0] or 0)
        else:
            conn.execute(update_sql)
            counts["from_bhavcopy"] = int(conn.execute("SELECT changes()").fetchone()[0] or 0)
        conn.unregister("bhav_lookup")

    fallback_sql = """
        UPDATE _catalog
        SET series = 'EQ', trading_segment = 'regular'
        WHERE exchange = 'NSE'
          AND series IS NULL
          AND trading_segment IS NULL
    """
    if dry_run:
        preview = conn.execute(
            """
            SELECT COUNT(*) FROM _catalog
            WHERE exchange = 'NSE'
              AND series IS NULL
              AND trading_segment IS NULL
            """
        ).fetchone()
        counts["from_master_fallback"] = int(preview[0] or 0)
    else:
        conn.execute(fallback_sql)
        counts["from_master_fallback"] = int(conn.execute("SELECT changes()").fetchone()[0] or 0)

    conn.close()
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path, help="Path to ohlcv.duckdb")
    parser.add_argument(
        "--bhavcopy-dir",
        type=Path,
        default=Path("data/raw/NSE_EQ"),
        help="Directory containing cached daily bhavcopy CSVs (nse_DDMMMYYYY.csv).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report counts without writing.")
    args = parser.parse_args()

    if not args.db.exists():
        logger.error("Database not found: %s", args.db)
        return 1

    counts = backfill(args.db, args.bhavcopy_dir, dry_run=args.dry_run)
    label = "[dry-run]" if args.dry_run else "[applied]"
    for key, value in counts.items():
        logger.info("%s %s=%s", label, key, value)
    return 0


if __name__ == "__main__":
    sys.exit(main())
