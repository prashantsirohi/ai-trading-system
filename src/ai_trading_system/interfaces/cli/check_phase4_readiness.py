"""Re-evaluate Phase 4 readiness from immutable Phase 3C-5 evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ai_trading_system.domains.opportunities.calibration import (
    CalibrationConfig,
    evaluate_phase4_readiness,
    write_readiness_artifacts,
)
from ai_trading_system.interfaces.cli.benchmark_phase3c4 import validate_benchmark_paths


def run_readiness(
    *, calibration_manifest: Path, output_root: Path,
    performance_summary: Path | None = None,
    fail_on_not_ready: bool = False,
) -> dict[str, Any]:
    output, _ = validate_benchmark_paths(output_root)
    manifest = json.loads(calibration_manifest.read_text(encoding="utf-8"))
    quality = dict(manifest.get("quality_summary") or {})
    performance = (
        json.loads(performance_summary.read_text(encoding="utf-8"))
        if performance_summary is not None else None
    )
    evidence = dict(manifest.get("readiness_evidence") or {})
    checks, limitations, verdict, development, production = evaluate_phase4_readiness(
        quality=quality, manifest=manifest, config=CalibrationConfig(),
        copied_realistic_performance_summary=performance,
        operator_migrations_applied=bool(evidence.get("operator_migrations_applied", False)),
        real_phase3b_history_present=bool(evidence.get("real_phase3b_history_present", False)),
    )
    paths = write_readiness_artifacts(
        checks=checks, limitations=limitations, verdict=verdict,
        phase4_development_ready=development, phase4_production_ready=production,
        manifest_id=str(manifest["manifest_id"]), output_root=output,
    )
    return {
        "status": "completed", "exit_code": int(fail_on_not_ready and verdict.value == "NOT_READY"),
        "verdict": verdict.value, "phase4_development_ready": development,
        "phase4_production_ready": production,
        "limitations": [item.limitation_id for item in limitations],
        "artifacts": [str(path) for path in paths],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check Phase 4 read-only development readiness.")
    parser.add_argument("--calibration-manifest", type=Path, required=True)
    parser.add_argument("--performance-summary", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--fail-on-not-ready", action="store_true")
    args = parser.parse_args(argv)
    result = run_readiness(
        calibration_manifest=args.calibration_manifest,
        performance_summary=args.performance_summary,
        output_root=args.output_root, fail_on_not_ready=args.fail_on_not_ready,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return int(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
