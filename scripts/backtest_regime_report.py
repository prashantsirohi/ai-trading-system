#!/usr/bin/env python
"""Phase-7 regime-stratified backtest validation CLI.

Tags every trading day in a date window with its regime classification
(using the live rule set + hysteresis seed chain), attaches forward
returns on UNIV_TOP1000, and prints / writes the per-regime aggregates.

Example:
    python scripts/backtest_regime_report.py \\
        --from 2005-01-01 --to 2025-12-31 \\
        --out reports/regime_validation_20yr

Verdict: the script exits 0 when the four-tier forward-return ordering
(strong_bull >= bull >= cautious_bull >= neutral >= risk_off) holds
monotone-non-decreasing on the 20-day horizon. Otherwise exits 1 with
the offending pair logged — useful as a CI gate before enabling
higher-risk profile defaults.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ai_trading_system.research.backtesting.regime_report import (
    DEFAULT_BENCHMARK,
    DEFAULT_HORIZONS,
    build_regime_forward_return_report,
    write_report,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Regime-stratified forward-return validation report"
    )
    p.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    p.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")
    p.add_argument(
        "--out",
        type=Path,
        default=Path("reports/regime_validation"),
        help="Output directory for JSON + CSV files",
    )
    p.add_argument(
        "--benchmark",
        default=DEFAULT_BENCHMARK,
        help=f"Index code for forward returns (default: {DEFAULT_BENCHMARK})",
    )
    p.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Optional explicit OHLCV DB path. Defaults to research_ohlcv.duckdb.",
    )
    p.add_argument(
        "--horizons",
        default=",".join(str(h) for h in DEFAULT_HORIZONS),
        help="Comma-separated forward horizons in trading days",
    )
    p.add_argument(
        "--gate-horizon",
        type=int,
        default=20,
        help="Horizon (in days) whose ordering verdict drives the exit code",
    )
    p.add_argument(
        "--project-root",
        type=Path,
        default=Path("."),
        help="Project root (used to resolve config/active_regime_rules.yaml)",
    )
    p.add_argument(
        "--quiet", action="store_true", help="Suppress per-regime stdout dump"
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)
    horizons = tuple(int(h) for h in str(args.horizons).split(",") if h.strip())

    report = build_regime_forward_return_report(
        project_root=args.project_root,
        from_date=args.from_date,
        to_date=args.to_date,
        horizons=horizons,
        benchmark=args.benchmark,
        db_path=args.db,
    )

    paths = write_report(report, out_dir=args.out)
    print(f"wrote {paths['summary']}")
    print(f"wrote {paths['csv']}")
    print(f"wrote {paths['daily']}")

    if not args.quiet:
        print("\n=== regime breakdown ===")
        for regime, info in report.summary.get("regime_breakdown", {}).items():
            print(
                f"  {regime:14s} days={info['days_in_regime']:5d} "
                f"({info['pct_of_period']:5.1f}%)"
            )
        print("\n=== forward-return ordering (mean %) ===")
        for horizon, payload in report.summary.get("forward_return_ordering", {}).items():
            verdict = "PASS" if payload["monotone_non_decreasing"] else "FAIL"
            print(f"  {horizon:>4} ({verdict}): " + " | ".join(
                f"{row['regime']}={row['mean_return_pct']:+.2f}%"
                for row in payload["by_rank"]
            ))

    # Exit code gates on the requested horizon's ordering. Phase-7 acceptance
    # is monotone non-decreasing mean returns from risk_off → strong_bull.
    gate_key = f"{args.gate_horizon}d"
    gate = report.summary.get("forward_return_ordering", {}).get(gate_key)
    if gate is None:
        print(f"\nNo ordering verdict available for {gate_key}", file=sys.stderr)
        return 1
    if gate["monotone_non_decreasing"]:
        print(f"\nGO: {gate_key} forward-return ordering holds monotone non-decreasing.")
        return 0
    print(
        f"\nNO-GO: {gate_key} forward-return ordering is NOT monotone non-decreasing.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
