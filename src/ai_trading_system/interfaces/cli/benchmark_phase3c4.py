"""Reproducible, non-mutating Phase 3C-4 benchmark harness."""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.domains.opportunities.contracts import WeinsteinStage
from ai_trading_system.domains.opportunities.routing import decide_scan_route
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.platform.db.paths import canonicalize_project_root, get_domain_paths
from ai_trading_system.platform.telemetry import CacheMode, PerformanceCollector, PerformanceConfig, ReplayMode, compare_benchmark_summary, compare_semantic_outputs
from ai_trading_system.platform.telemetry.performance import descriptive_statistics


PROJECT_ROOT = canonicalize_project_root(os.getenv("AI_TRADING_PROJECT_ROOT") or Path.cwd())
PROFILE_SIZES = {"small_fixture": 32, "medium_fixture": 512, "copied_realistic": 2048}


def validate_benchmark_paths(
    output_root: Path, *, copied_control_plane: Path | None = None,
) -> tuple[Path, Path | None]:
    """Reject operator stores and symlinked targets before any benchmark writes."""
    live_root = get_domain_paths(PROJECT_ROOT).root_dir.resolve()
    output = output_root.expanduser().resolve()
    if _has_symlink(output_root.expanduser()):
        raise ValueError("refusing symlinked benchmark output path")
    if output == live_root or live_root in output.parents:
        raise ValueError("refusing to write benchmark artifacts inside configured operator DATA_ROOT")
    copied = None
    if copied_control_plane is not None:
        if _has_symlink(copied_control_plane.expanduser()):
            raise ValueError("refusing symlinked copied control-plane path")
        copied = copied_control_plane.expanduser().resolve()
        live_control_plane = (live_root / "control_plane.duckdb").resolve()
        if copied == live_control_plane:
            raise ValueError("refusing configured operator control_plane.duckdb")
        if not copied.is_file():
            raise FileNotFoundError(copied)
    return output, copied


def run_benchmark(
    *, profile: str, cache_mode: str, repetitions: int, output_root: Path,
    as_of: str, copied_control_plane: Path | None = None,
    baseline_summary: Path | None = None, fail_on_threshold: bool = False,
) -> dict[str, Any]:
    if profile not in PROFILE_SIZES:
        raise ValueError(f"unknown fixture profile: {profile}")
    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    output, copied = validate_benchmark_paths(output_root, copied_control_plane=copied_control_plane)
    if profile == "copied_realistic" and copied is None:
        raise ValueError("copied_realistic requires --copied-control-plane")
    output.mkdir(parents=True, exist_ok=True)
    fixed_at = datetime.fromisoformat(f"{as_of}T00:00:00+00:00")
    shared_inputs = _fixture_inputs(PROFILE_SIZES[profile]) if cache_mode == "warm" else None
    first_semantic: dict[str, Any] | None = None
    replay_results: list[dict[str, Any]] = []
    runtimes: list[float] = []
    throughputs: list[float] = []
    final_collector: PerformanceCollector | None = None
    copied_rows = _read_copied_store_counts(copied) if copied is not None else {}

    for repetition in range(1, repetitions + 1):
        inputs = shared_inputs if shared_inputs is not None else _fixture_inputs(PROFILE_SIZES[profile])
        replay_mode = ReplayMode.FIRST_RUN if repetition == 1 else ReplayMode.EXACT_REPLAY
        collector = PerformanceCollector(
            run_id=f"phase3c4-{profile}-{repetition}", as_of=as_of,
            config=PerformanceConfig(), cache_mode=CacheMode(cache_mode.upper()),
            replay_mode=replay_mode,
        )
        with collector.timer(stage_name="scan_router", operation_name="scan_router.total", symbols_in=len(inputs)) as span:
            with collector.timer(stage_name="scan_router", operation_name="scan_router.resolve_decisions", symbols_in=len(inputs)) as decision_span:
                decisions = [decide_scan_route(**item, decided_at=fixed_at) for item in inputs]
                decision_span.counts(rows_out=len(decisions), symbols_out=len(decisions))
            rows = [_decision_row(item) for item in decisions]
            span.counts(rows_out=len(rows), symbols_out=len(rows))
        repetition_dir = output / f"repetition_{repetition}"
        repetition_dir.mkdir(parents=True, exist_ok=True)
        routing_path = repetition_dir / "scan_routing.csv"
        _write_csv(routing_path, rows)
        routing_artifact = StageArtifact.from_file("scan_routing", routing_path, row_count=len(rows), attempt_number=1)
        collector.record_artifact(routing_artifact, column_count=len(rows[0]) if rows else 0)
        semantic = {
            "routing_input_hashes": [row["routing_input_hash"] for row in rows],
            "routing_decision_ids": [row["routing_decision_id"] for row in rows],
            "row_count": len(rows),
            "content_hash": routing_artifact.content_hash,
        }
        replay = compare_semantic_outputs(first_semantic, semantic) if first_semantic is not None else {
            "equivalent": None, "differences": [], "ignored_fields": [],
            "artifact_hash_matches": None, "decision_identity_matches": None,
            "opportunity_identity_matches": None,
        }
        first_semantic = first_semantic or semantic
        replay_results.append(replay)
        collector.write_artifacts(repetition_dir, replay_comparison=replay)
        total = next(metric for metric in collector.metrics if metric.operation_name == "scan_router.total")
        runtimes.append(total.duration_ms)
        throughputs.append(total.symbols_per_second or 0.0)
        final_collector = collector

    assert final_collector is not None
    final_replay = replay_results[-1]
    final_collector.write_artifacts(output, replay_comparison=final_replay)
    summary_path = output / "phase3c4_performance_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update({
        "profile": profile,
        "repetitions": repetitions,
        "runtime_statistics_ms": descriptive_statistics(runtimes),
        "throughput_statistics_symbols_per_second": descriptive_statistics(throughputs),
        "symbols_per_second": throughputs[-1],
        "copied_store_counts": copied_rows,
        "replay_equivalence": final_replay,
        "output_equivalence": final_replay,
    })
    if baseline_summary is not None:
        baseline = json.loads(baseline_summary.read_text(encoding="utf-8"))
        summary["baseline_comparison"] = compare_benchmark_summary(summary, baseline)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    replay_failed = any(item.get("equivalent") is False for item in replay_results)
    threshold_failed = summary.get("performance_status") == "FAIL" or summary.get("baseline_comparison", {}).get("status") == "FAIL"
    return {
        "status": "failed" if replay_failed or (fail_on_threshold and threshold_failed) else "completed",
        "exit_code": 1 if replay_failed or (fail_on_threshold and threshold_failed) else 0,
        "output_root": str(output),
        "summary": summary,
    }


def _fixture_inputs(count: int) -> list[dict[str, Any]]:
    stages = (WeinsteinStage.STAGE_1, WeinsteinStage.TRANSITION_1_TO_2, WeinsteinStage.STAGE_2, WeinsteinStage.STAGE_4)
    return [
        {
            "symbol_id": f"PERF{i:05d}", "exchange": "NSE", "rank_position": i + 1,
            "rank_selected": i < 150, "stage_discovery": i % 4 in {0, 1},
            "stage_promoted": i % 17 == 0, "active_position": i % 101 == 0,
            "recently_exited": i % 97 == 0, "triggered": i % 89 == 0,
            "pending_followthrough": i % 83 == 0, "stock_stage": stages[i % len(stages)],
            "sector_stage": stages[(i // 7) % len(stages)], "market_data_available": True,
        }
        for i in range(count)
    ]


def _decision_row(item: Any) -> dict[str, Any]:
    return {
        "symbol_id": item.symbol_id, "exchange": item.exchange,
        "scan_tier": item.scan_tier.value, "routing_input_hash": item.routing_input_hash,
        "routing_decision_id": item.routing_decision_id,
        "policy_version": item.policy_version,
    }


def _read_copied_store_counts(path: Path | None) -> dict[str, int]:
    if path is None:
        return {}
    result: dict[str, int] = {}
    with duckdb.connect(str(path), read_only=True) as conn:
        names = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        for table in ("pipeline_run", "pipeline_stage_run", "pipeline_artifact"):
            if table in names:
                result[table] = int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
    return result


def _has_symlink(path: Path) -> bool:
    current = path.absolute()
    while True:
        if current in {Path("/tmp"), Path("/private/tmp")}:
            return False
        if current.is_symlink():
            return True
        if current == current.parent:
            return False
        current = current.parent


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) or ["status"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run isolated Phase 3C-4 performance benchmarks.")
    parser.add_argument("--profile", choices=sorted(PROFILE_SIZES), default="small_fixture")
    parser.add_argument("--cache-mode", choices=("cold", "warm"), default="cold")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--copied-control-plane", type=Path)
    parser.add_argument("--baseline-summary", type=Path)
    parser.add_argument("--fail-on-threshold", action="store_true")
    args = parser.parse_args(argv)
    result = run_benchmark(
        profile=args.profile, cache_mode=args.cache_mode, repetitions=args.repetitions,
        output_root=args.output_root, as_of=args.as_of,
        copied_control_plane=args.copied_control_plane,
        baseline_summary=args.baseline_summary, fail_on_threshold=args.fail_on_threshold,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return int(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
