#!/usr/bin/env python
"""Phase-7 follow-up: investigate alternate transforms of the breadth series.

Phase 7 found that breadth-LEVEL regime does not predict UNIV_TOP1000
forward returns. This script tests whether the same daily breadth series
carries signal under different transforms (momentum, transitions,
persistence). Prints a verdict per hypothesis and writes a JSON +
daily CSV for drill-in.

Example:
    python scripts/investigate_alternate_signals.py \\
        --from 2005-01-01 --to 2025-12-31 \\
        --out reports/alternate_signals_20yr
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ai_trading_system.research.backtesting.alternate_signals import (
    build_alternate_signals_report,
    write_alternate_signals_report,
)
from ai_trading_system.research.backtesting.regime_report import (
    DEFAULT_BENCHMARK,
    DEFAULT_HORIZONS,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Alternate-signal investigation")
    p.add_argument("--from", dest="from_date", required=True)
    p.add_argument("--to", dest="to_date", required=True)
    p.add_argument("--out", type=Path, default=Path("reports/alternate_signals"))
    p.add_argument("--benchmark", default=DEFAULT_BENCHMARK)
    p.add_argument("--db", type=Path, default=None)
    p.add_argument(
        "--horizons", default=",".join(str(h) for h in DEFAULT_HORIZONS)
    )
    p.add_argument(
        "--momentum-windows",
        default="5,20",
        help="Trading-day windows over which to compute breadth Δs",
    )
    p.add_argument("--project-root", type=Path, default=Path("."))
    return p.parse_args(argv)


def _format_verdict(passes: bool) -> str:
    return "PASS" if passes else "FAIL"


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)
    horizons = tuple(int(h) for h in args.horizons.split(",") if h.strip())
    windows = tuple(int(w) for w in args.momentum_windows.split(",") if w.strip())

    report = build_alternate_signals_report(
        project_root=args.project_root,
        from_date=args.from_date,
        to_date=args.to_date,
        horizons=horizons,
        benchmark=args.benchmark,
        db_path=args.db,
        momentum_windows=windows,
    )
    paths = write_alternate_signals_report(report, out_dir=args.out)
    print(f"wrote {paths['findings']}")
    print(f"wrote {paths['daily']}")

    print("\n=== alternate-signal hypothesis tests ===")
    for hypothesis_id, payload in report.findings["hypotheses"].items():
        print(f"\n[{hypothesis_id}] {payload['description']}")
        for horizon_key, result in payload["results"].items():
            if "monotone_non_decreasing" in result:
                verdict = _format_verdict(result["monotone_non_decreasing"])
                spread = result.get("q5_minus_q1_pct")
                spread_str = f"  Q5-Q1={spread:+.2f}%" if spread is not None else ""
                print(f"  {horizon_key:>4} ({verdict}):{spread_str}")
                for row in result.get("by_bucket", []):
                    mean = row["mean_return_pct"]
                    mean_str = f"{mean:+.2f}%" if mean is not None else "n/a"
                    print(
                        f"     {row['bucket']:14s} mean={mean_str:>8s} "
                        f"n={row['sample_size']}"
                    )
            else:
                # Binary hypothesis (transition vs steady-state)
                for key, sub in result.items():
                    if not isinstance(sub, dict):
                        continue
                    mean = sub.get("mean_return_pct")
                    mean_str = f"{mean:+.2f}%" if mean is not None else "n/a"
                    print(
                        f"  {horizon_key:>4} {key:14s} mean={mean_str:>8s} "
                        f"win_rate={sub.get('win_rate_pct')}% n={sub['sample_size']}"
                    )
                if result.get("true_minus_false_pct") is not None:
                    print(
                        f"       Δ(true - false) = "
                        f"{result['true_minus_false_pct']:+.2f}%"
                    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
