"""CLI for the stage-gate vs baseline backtest comparison.

Usage
-----
    uv run python scripts/run_stage_gate_backtest.py
    uv run python scripts/run_stage_gate_backtest.py --top-n 20 --start 2025-09-01
    uv run python scripts/run_stage_gate_backtest.py --out reports/stage_gate_bt.csv
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from ai_trading_system.platform.db.paths import ensure_domain_layout

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from analytics.stage_gate_backtest import (
    print_report,
    run_comparison,
    summarise,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ohlcv-db",  default=None)
    parser.add_argument("--top-n",     type=int, default=20)
    parser.add_argument("--min-score", type=float, default=50.0)
    parser.add_argument("--start",     default="2025-08-01")
    parser.add_argument("--end-4w",    default="2026-03-21")
    parser.add_argument("--end-12w",   default="2026-02-14")
    parser.add_argument("--exchange",  default="NSE")
    parser.add_argument("--out",       default=None,
                        help="Optional CSV output path for period-level results")
    parser.add_argument("--filter-by-market-stage", action="store_true",
                        help="Only include rebalance weeks where the market is in S2 (bull regime)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    paths = ensure_domain_layout()
    ohlcv_db = args.ohlcv_db or str(paths.ohlcv_db_path)
    # Derive feature_store from the DB's parent so explicit --ohlcv-db always works.
    feature_dir = str(Path(ohlcv_db).parent / "feature_store")

    results = run_comparison(
        ohlcv_db_path=ohlcv_db,
        feature_store_dir=feature_dir,
        top_n=args.top_n,
        min_score=args.min_score,
        start=args.start,
        end_4w=args.end_4w,
        end_12w=args.end_12w,
        exchange=args.exchange,
        filter_by_market_stage=args.filter_by_market_stage,
    )

    if results.empty:
        print("No results — check data range or DB path.")
        return 1

    summary = summarise(results)
    print_report(results, summary)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        results.to_csv(out_path, index=False)
        print(f"  Period-level results saved → {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
