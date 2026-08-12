from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from .models import RunMode, ScreeningParameters
from .service import PersistentScreenerService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the persistent research screener.")
    parser.add_argument("--as-of-date", required=True, type=date.fromisoformat)
    parser.add_argument("--run-mode", required=True, choices=[mode.value for mode in RunMode])
    parser.add_argument("--min-market-cap-cr", type=float, default=1_000.0)
    parser.add_argument("--max-market-cap-cr", type=float, default=100_000.0)
    parser.add_argument("--canary-file", type=Path, help="Override the mode-specific versioned canary fixture.")
    parser.add_argument("--screen-definition")
    parser.add_argument("--screen-version")
    parser.add_argument("--parent-run-id", help="Completed full-universe run to freeze as the filing-discovery cohort.")
    parser.add_argument("--batch-size", type=int, default=25, help="Checkpoint/progress batch size for filing discovery.")
    parser.add_argument("--workers", type=int, default=4, help="Filing-discovery sessions; aggregate pacing is held constant.")
    parser.add_argument("--store-path", type=Path)
    parser.add_argument("--output-root", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    params = ScreeningParameters(
        as_of_date=args.as_of_date,
        run_mode=RunMode(args.run_mode),
        min_market_cap_cr=args.min_market_cap_cr,
        max_market_cap_cr=args.max_market_cap_cr,
        canary_file=args.canary_file,
        screen_definition=args.screen_definition,
        screen_version=args.screen_version,
        parent_run_id=args.parent_run_id,
        batch_size=args.batch_size,
        workers=args.workers,
    )
    result = PersistentScreenerService(store_path=args.store_path, output_root=args.output_root).run(params)
    summary = {key: value for key, value in result.items() if key != "members"}
    if "members" in result:
        summary["member_count"] = len(result["members"])
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
