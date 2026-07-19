"""CLI: does an R1a shadow run count toward the 20-session clock?

Reads a completed run's registered pattern-lane artifacts plus the pipeline
registry (read-only) and prints the eight-point session-gate verdict.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.pipeline.session_gate import (
    evaluate_session_gate,
    write_session_gate_artifacts,
)
from ai_trading_system.platform.db.paths import canonicalize_project_root, get_domain_paths


def run(
    *, run_id: str, project_root: Path, data_domain: str = "operational",
    output_root: Path | None = None, fail_on_not_counted: bool = False,
) -> dict:
    root = canonicalize_project_root(project_root)
    control_plane = get_domain_paths(root, data_domain).root_dir / "control_plane.duckdb"
    registry = RegistryStore(root, db_path=control_plane, initialize=False, allow_migrations=False)
    result = evaluate_session_gate(registry, run_id)
    payload = result.to_dict()
    if output_root is not None:
        payload["artifacts"] = [str(p) for p in write_session_gate_artifacts(result, output_root)]
    payload["exit_code"] = int(fail_on_not_counted and not result.day_counts)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate the R1a shadow session gate for a run.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--data-domain", default="operational")
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--fail-on-not-counted", action="store_true")
    args = parser.parse_args(argv)
    result = run(
        run_id=args.run_id, project_root=args.project_root, data_domain=args.data_domain,
        output_root=args.output_root, fail_on_not_counted=args.fail_on_not_counted,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return int(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
