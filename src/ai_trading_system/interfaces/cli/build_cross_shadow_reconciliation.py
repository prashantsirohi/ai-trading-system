"""CLI: build the offline cross-shadow reconciliation bundle (read-only).

Compares a run's ``pattern_lane_scan.csv`` against the opportunity-registry
shadow (``candidate_episode``) and writes an immutable reconciliation bundle.
Never writes to the registry.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.platform.db.paths import canonicalize_project_root, get_domain_paths
from ai_trading_system.research.pattern_lane_calibration.cross_shadow import write_cross_shadow_bundle


def _resolve_pattern_csv(*, run_id: str, project_root: Path, data_domain: str) -> tuple[Path, Path | None]:
    runs = get_domain_paths(project_root, data_domain).pipeline_runs_dir / run_id / "pattern_lane_scan" / "attempt_1"
    return runs / "pattern_lane_scan.csv", (runs / "pattern_lane_manifest.json")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Offline cross-shadow reconciliation (pattern-lane vs registry).")
    parser.add_argument("--run-id", help="Resolve pattern_lane_scan.csv from this run under DATA_ROOT")
    parser.add_argument("--pattern-lane-csv", type=Path, help="Explicit path (overrides --run-id)")
    parser.add_argument("--pattern-manifest", type=Path)
    parser.add_argument("--control-plane-db", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--through-date", required=True)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--data-domain", default="operational")
    args = parser.parse_args(argv)

    root = canonicalize_project_root(args.project_root)
    pattern_csv, pattern_manifest = args.pattern_lane_csv, args.pattern_manifest
    if pattern_csv is None:
        if not args.run_id:
            parser.error("provide --pattern-lane-csv or --run-id")
        pattern_csv, resolved_manifest = _resolve_pattern_csv(
            run_id=args.run_id, project_root=root, data_domain=args.data_domain)
        pattern_manifest = pattern_manifest or resolved_manifest
    control_plane = args.control_plane_db or (get_domain_paths(root, args.data_domain).root_dir / "control_plane.duckdb")

    result = write_cross_shadow_bundle(
        pattern_lane_csv=pattern_csv, control_plane_db=control_plane,
        output_dir=args.output_dir, through_date=args.through_date,
        project_root=root, pattern_manifest=pattern_manifest,
    )
    print(json.dumps({"status": "completed", **result["summary"], "output_dir": result["output_dir"]},
                     indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
