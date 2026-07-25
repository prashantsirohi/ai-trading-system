"""Reconstruct same-run Investigator performance events on a copied store."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ai_trading_system.domains.opportunities.historical_reconstruction import (
    reconstruct_same_run_events,
)
from ai_trading_system.platform.db.paths import (
    canonicalize_project_root,
    get_domain_paths,
)
from ai_trading_system.pipeline.registry import RegistryStore


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconstruct point-in-time Investigator events from same-run artifacts."
    )
    parser.add_argument("--copied-control-plane", required=True, type=Path)
    parser.add_argument("--ohlcv-db", type=Path)
    parser.add_argument("--from-date", required=True, type=date.fromisoformat)
    parser.add_argument("--to-date", required=True, type=date.fromisoformat)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    project_root = canonicalize_project_root()
    paths = get_domain_paths(project_root)
    copied = args.copied_control_plane.expanduser().resolve()
    live = (paths.root_dir / "control_plane.duckdb").resolve()
    if copied == live:
        raise SystemExit(
            "--copied-control-plane must not be the configured operator control plane"
        )
    if not copied.is_file() or copied.is_symlink():
        raise SystemExit(
            "--copied-control-plane must be a regular copied database file"
        )
    if not args.apply:
        raise SystemExit(
            "reconstruction is write-gated; pass --apply for the copied store"
        )
    ohlcv = (args.ohlcv_db or paths.ohlcv_db_path).expanduser().resolve()
    if not ohlcv.is_file():
        raise SystemExit("--ohlcv-db does not exist")
    registry = RegistryStore(
        project_root,
        db_path=copied,
        initialize=True,
        allow_migrations=True,
    )
    result = reconstruct_same_run_events(
        registry,
        ohlcv_db_path=ohlcv,
        from_date=args.from_date,
        to_date=args.to_date,
        apply=bool(args.apply),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
