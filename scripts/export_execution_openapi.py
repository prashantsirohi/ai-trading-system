#!/usr/bin/env python3
"""Export or validate the deterministic execution-API OpenAPI contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.ui.execution_api.app import create_app


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = (
    ROOT / "web" / "execution-console-v2" / "ai-trading-dashboard-starter"
    / "execution-openapi.snapshot.json"
)


def rendered_schema() -> str:
    return json.dumps(create_app().openapi(), indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = rendered_schema()
    if args.check:
        if not args.output.is_file() or args.output.read_text(encoding="utf-8") != expected:
            print(f"Execution API OpenAPI snapshot drift: {args.output}")
            return 1
        print(f"Execution API OpenAPI snapshot is current: {args.output}")
        return 0
    args.output.write_text(expected, encoding="utf-8")
    print(f"Wrote execution API OpenAPI snapshot: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
