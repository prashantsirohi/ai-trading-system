from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_trading_system.interfaces.cli.benchmark_phase3c4 import main, run_benchmark, validate_benchmark_paths


def test_small_fixture_benchmark_writes_canonical_artifacts_and_exact_replay(tmp_path: Path) -> None:
    output = tmp_path / "benchmark"
    result = run_benchmark(
        profile="small_fixture", cache_mode="warm", repetitions=2,
        output_root=output, as_of="2026-07-15",
    )
    assert result["exit_code"] == 0
    assert result["summary"]["replay_equivalence"]["equivalent"] is True
    assert {path.name for path in output.glob("phase3c4_*")} == {
        "phase3c4_performance_metrics.csv", "phase3c4_performance_summary.json",
        "phase3c4_artifact_metrics.csv", "phase3c4_database_metrics.csv",
        "phase3c4_replay_comparison.json",
    }


def test_cli_returns_zero_for_advisory_thresholds(tmp_path: Path) -> None:
    assert main([
        "--profile", "small_fixture", "--cache-mode", "cold", "--repetitions", "1",
        "--output-root", str(tmp_path / "cli"), "--as-of", "2026-07-15",
    ]) == 0


def test_benchmark_refuses_symlinked_output(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="symlinked"):
        validate_benchmark_paths(link)


def test_baseline_comparison_is_persisted(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps({"total_runtime_ms": 10, "symbols_per_second": 1}), encoding="utf-8")
    result = run_benchmark(
        profile="small_fixture", cache_mode="cold", repetitions=1,
        output_root=tmp_path / "current", as_of="2026-07-15", baseline_summary=baseline,
    )
    assert "baseline_comparison" in result["summary"]
