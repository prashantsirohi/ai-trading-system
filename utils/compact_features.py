"""
Monthly parquet compaction script.

Usage:
    python -m utils.compact_features --help
    python -m utils.compact_features --feature rsi --month 2026-03
    python -m utils.compact_features --all
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetWriter
from core.logging import logger


def compact_feature(feature_dir: str, month: str = None) -> str:
    """Compact all parquet files in feature directory."""
    feature_path = Path(feature_dir)
    if not feature_path.exists():
        logger.warning(f"Feature directory not found: {feature_dir}")
        return ""

    files = list(feature_path.glob("*.parquet"))
    if not files:
        logger.warning(f"No parquet files found in {feature_dir}")
        return ""

    logger.info(f"Compacting {len(files)} files from {feature_dir}")
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            logger.error(f"Failed to read {f}: {e}")

    if not dfs:
        logger.error("No tables read successfully")
        return ""

    logger.info(f"Concatenating {len(dfs)} dataframes...")
    combined_df = pd.concat(dfs, ignore_index=True)

    combined_table = pa.Table.from_pandas(combined_df)

    if not dfs:
        logger.error("No tables read successfully")
        return ""

    output_name = f"merged_{month or 'all'}.parquet" if month else "merged.parquet"
    output_path = feature_path / output_name

    pq_writer = ParquetWriter(output_path, combined_table.schema, compression="zstd")
    pq_writer.write_table(combined_table)
    pq_writer.close()

    original_size = sum(f.stat().st_size for f in files)
    new_size = output_path.stat().st_size
    reduction = (1 - new_size / original_size) * 100

    logger.info(
        f"Compacted to {output_path.name}: {combined_table.num_rows:,} rows, "
        f"{original_size / 1024 / 1024:.1f}MB → {new_size / 1024 / 1024:.1f}MB "
        f"(saved {reduction:.1f}%)"
    )
    return str(output_path)


def main():
    parser = argparse.ArgumentParser(description="Compact parquet feature files")
    parser.add_argument("--feature", help="Feature name (e.g., rsi, atr)")
    parser.add_argument("--month", help="Month partition (e.g., 2026-03)")
    parser.add_argument("--all", action="store_true", help="Compact all features")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be compacted without doing it",
    )

    args = parser.parse_args()
    base_path = Path(__file__).parent.parent / "data" / "feature_store"

    features = (
        ["rsi", "sma", "atr", "adx", "bb", "roc", "supertrend", "delivery"]
        if args.all
        else [args.feature]
    )

    for feature in features:
        feature_dir = base_path / feature / "NSE"
        if not feature_dir.exists():
            logger.warning(f"Feature directory not found: {feature_dir}")
            continue

        if args.dry_run:
            files = list(feature_dir.glob("*.parquet"))
            total_size = sum(f.stat().st_size for f in files)
            logger.info(
                f"Would compact {len(files)} files ({total_size / 1024 / 1024:.1f}MB): {feature_dir}"
            )
        else:
            compact_feature(str(feature_dir), args.month)

    if args.dry_run:
        sys.exit(0)

    logger.info("Compaction complete!")


if __name__ == "__main__":
    main()
