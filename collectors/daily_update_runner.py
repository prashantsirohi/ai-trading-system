"""
Daily EOD Update Runner
=======================
Usage:
    python daily_update_runner.py                          # Full update (OHLCV + Features)
    python daily_update_runner.py --symbols-only          # OHLCV only
    python daily_update_runner.py --features-only         # Features only
    python daily_update_runner.py --force                 # Force overwrite
    python daily_update_runner.py --batch-size 500       # Custom batch size

This script is designed to run after market close (3:30 PM IST).
It performs incremental updates - only fetching rows newer than
the last date already stored in DuckDB.
"""

import os
import sys
import argparse

from core.bootstrap import ensure_project_root_on_path

project_root = str(ensure_project_root_on_path(__file__))

from collectors.dhan_collector import DhanCollector
from features.feature_store import FeatureStore
from utils.data_domains import ensure_domain_layout
from utils.logger import logger


def run(
    symbols_only: bool,
    features_only: bool,
    batch_size: int,
    bulk: bool,
    symbol_limit: int | None = None,
    data_domain: str = "operational",
    symbols: list[str] | None = None,
    full_rebuild: bool = False,
    feature_tail_bars: int = 252,
):
    paths = ensure_domain_layout(project_root=project_root, data_domain=data_domain)
    incremental_features = data_domain == "operational" and not full_rebuild
    collector = DhanCollector(
        db_path=str(paths.ohlcv_db_path),
        masterdb_path=str(paths.master_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        data_domain=data_domain,
    )

    if features_only:
        logger.info("=" * 60)
        logger.info("MODE: Features Only - recomputing all features")
        logger.info("=" * 60)

        if symbols is None:
            import duckdb

            conn = duckdb.connect(collector.db_path, read_only=True)
            try:
                syms = conn.execute(
                    "SELECT DISTINCT symbol_id FROM _catalog WHERE exchange = 'NSE'"
                ).fetchall()
                symbols = [r[0] for r in syms]
                if symbol_limit is not None:
                    symbols = symbols[:symbol_limit]
            finally:
                conn.close()

        logger.info(f"Computing features for {len(symbols)} symbols...")
        fs = FeatureStore(
            ohlcv_db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain=data_domain,
        )
        result = fs.compute_and_store_features(
            symbols=symbols,
            exchanges=["NSE"],
            feature_types=[
                "rsi",
                "adx",
                "sma",
                "ema",
                "macd",
                "atr",
                "bb",
                "roc",
                "supertrend",
            ],
            incremental=incremental_features,
            tail_bars=feature_tail_bars,
            full_rebuild=full_rebuild,
        )
        logger.info(f"Feature computation complete: {result}")

        logger.info("Computing sector RS and relative strength...")
        from features.compute_sector_rs import compute_all_symbols_rs

        compute_all_symbols_rs(
            db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            masterdb_path=str(paths.master_db_path),
        )
        logger.info("Sector RS computation complete")

        return

    if bulk:
        logger.info("=" * 60)
        logger.info("MODE: Bulk OHLC - Fast single API call for today's data")
        logger.info("=" * 60)

        result = collector.run_daily_update_bulk(
            exchanges=["NSE"],
            symbol_limit=symbol_limit,
            compute_features=False,
        )
        logger.info(f"Bulk daily update result: {result}")
        return

    if symbols_only:
        logger.info("=" * 60)
        logger.info("MODE: Symbols Only - OHLCV fetch, no feature update")
        logger.info("=" * 60)

        result = collector.run_daily_update(
            exchanges=["NSE"],
            batch_size=batch_size,
            max_concurrent=10,
            symbol_limit=symbol_limit,
            compute_features=False,
        )
        logger.info(f"Daily update result: {result}")
        logger.info("")
        logger.info("TIP: Run features separately after OHLCV update:")
        logger.info("  python collectors/daily_update_runner.py --features-only")
        return

    logger.info("=" * 60)
    logger.info("MODE: Full Update - OHLCV + Features")
    logger.info(f"Batch size: {batch_size} (2 batches = {batch_size * 2} symbols)")
    logger.info("=" * 60)

    result = collector.run_daily_update(
        exchanges=["NSE"],
        batch_size=batch_size,
        max_concurrent=10,
        symbol_limit=symbol_limit,
        compute_features=False,
    )

    logger.info("=" * 60)
    logger.info("DAILY UPDATE COMPLETE")
    logger.info(f"  Symbols updated : {result.get('symbols_updated', 0)}")
    logger.info(f"  Symbols errors  : {result.get('symbols_errors', 0)}")
    logger.info(f"  Duration        : {result.get('duration_sec', 0):.1f}s")
    logger.info("=" * 60)
    updated_symbols = result.get("updated_symbols") or None
    if updated_symbols or full_rebuild:
        target_symbols = None if full_rebuild else updated_symbols
        logger.info(
            "Recomputing features for %s symbols (mode=%s, tail_bars=%s)",
            "all" if target_symbols is None else len(target_symbols),
            "incremental" if incremental_features else "full_rebuild",
            feature_tail_bars,
        )
        fs = FeatureStore(
            ohlcv_db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain=data_domain,
        )
        feat_result = fs.compute_and_store_features(
            symbols=target_symbols,
            exchanges=["NSE"],
            feature_types=[
                "rsi",
                "adx",
                "sma",
                "ema",
                "macd",
                "atr",
                "bb",
                "roc",
                "supertrend",
            ],
            incremental=incremental_features,
            tail_bars=feature_tail_bars,
            full_rebuild=full_rebuild,
        )
        logger.info(f"Feature computation complete: {feat_result}")

    logger.info("Computing sector RS and relative strength...")
    from features.compute_sector_rs import compute_all_symbols_rs

    compute_all_symbols_rs(
        db_path=str(paths.ohlcv_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        masterdb_path=str(paths.master_db_path),
    )
    logger.info("Sector RS computation complete")
    logger.info("")
    logger.info("TIP: Recompute features for updated symbols:")
    logger.info("  python collectors/daily_update_runner.py --features-only")


def main():
    parser = argparse.ArgumentParser(description="Daily EOD Update")
    parser.add_argument(
        "--batch-size", type=int, default=700, help="Symbols per batch (default: 700)"
    )
    parser.add_argument(
        "--symbols-only", action="store_true", help="Only fetch OHLCV, skip features"
    )
    parser.add_argument(
        "--features-only",
        action="store_true",
        help="Only recompute features, skip OHLCV fetch",
    )
    parser.add_argument(
        "--force", action="store_true", help="Force update even if today's data exists"
    )
    parser.add_argument(
        "--bulk",
        action="store_true",
        help="Use bulk OHLC API (fast, today only). Use for quick daily updates.",
    )
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=None,
        help="Limit the live symbol universe for canary/test runs.",
    )
    parser.add_argument(
        "--data-domain",
        choices=["operational", "research"],
        default="operational",
        help="Resolved storage domain for this run.",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Force full feature recomputation instead of incremental tail updates.",
    )
    parser.add_argument(
        "--feature-tail-bars",
        type=int,
        default=252,
        help="Tail window to recompute for incremental operational feature updates.",
    )
    args = parser.parse_args()

    run(
        symbols_only=args.symbols_only,
        features_only=args.features_only,
        batch_size=args.batch_size,
        bulk=args.bulk,
        symbol_limit=args.symbol_limit,
        data_domain=args.data_domain,
        full_rebuild=args.full_rebuild,
        feature_tail_bars=args.feature_tail_bars,
    )


if __name__ == "__main__":
    main()
