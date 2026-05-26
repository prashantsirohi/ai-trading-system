"""Inspect and optionally repair historical control-plane timestamp drift."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from ai_trading_system.platform.db.control_plane_timestamp_repair import (
    apply_control_plane_timestamp_repair,
    dry_run_control_plane_timestamp_repair,
)
from ai_trading_system.platform.db.paths import canonicalize_project_root, get_domain_paths

PROJECT_ROOT = canonicalize_project_root(os.getenv("AI_TRADING_PROJECT_ROOT") or Path.cwd())
DEFAULT_DB_PATH = get_domain_paths(PROJECT_ROOT).root_dir / "control_plane.duckdb"


def run(*, db_path: str | Path, apply: bool) -> int:
    summary = (
        apply_control_plane_timestamp_repair(db_path)
        if apply
        else dry_run_control_plane_timestamp_repair(db_path)
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect and repair UTC/local timestamp drift in control_plane.duckdb.",
    )
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Path to control_plane.duckdb")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the idempotent in-place timestamp repair. Omit for dry-run.",
    )
    args = parser.parse_args()
    return run(db_path=args.db_path, apply=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
