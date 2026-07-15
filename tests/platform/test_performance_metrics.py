from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.platform.telemetry import CacheMode, PerformanceCollector, PerformanceConfig, PerformanceStatus, ReplayMode, compare_semantic_outputs, process_memory_mb
from ai_trading_system.platform.telemetry.performance import evaluate_lower, evaluate_upper


def test_timer_records_monotonic_duration_counts_and_throughput(tmp_path: Path) -> None:
    collector = PerformanceCollector(run_id="run", as_of="2026-07-15", cache_mode=CacheMode.WARM)
    with collector.timer(stage_name="scan_router", operation_name="scan_router.total", symbols_in=4) as span:
        time.sleep(0.001)
        span.counts(rows_out=4, symbols_out=4)
    metric = collector.metrics[0]
    assert metric.duration_ms > 0
    assert metric.symbols_per_second and metric.symbols_per_second > 0
    assert metric.cache_mode is CacheMode.WARM
    assert metric.status is PerformanceStatus.PASS

    payload = tmp_path / "artifact.json"
    payload.write_text(json.dumps({"ok": True}), encoding="utf-8")
    collector.record_artifact(StageArtifact.from_file("payload", payload, row_count=1), column_count=1)
    artifacts = collector.write_artifacts(tmp_path / "metrics")
    assert {Path(item.uri).name for item in artifacts} == {
        "phase3c4_performance_metrics.csv", "phase3c4_performance_summary.json",
        "phase3c4_artifact_metrics.csv", "phase3c4_database_metrics.csv",
        "phase3c4_replay_comparison.json",
    }


def test_exception_is_recorded_and_reraised() -> None:
    collector = PerformanceCollector(run_id="run", as_of="2026-07-15")
    with pytest.raises(RuntimeError, match="functional"):
        with collector.timer(stage_name="weekly_stage", operation_name="weekly_stage.total"):
            raise RuntimeError("functional")
    assert collector.metrics[0].status is PerformanceStatus.FAIL
    assert collector.metrics[0].metadata["exception_class"] == "RuntimeError"


def test_thresholds_are_typed_and_advisory_by_default() -> None:
    config = PerformanceConfig(weekly_stage_warn_seconds=1, weekly_stage_fail_seconds=2)
    assert config.fail_pipeline is False
    assert evaluate_upper(2, 1, 2) is PerformanceStatus.FAIL
    assert evaluate_lower(0.5, 1, 0.1) is PerformanceStatus.WARN
    with pytest.raises(ValueError):
        PerformanceConfig(weekly_stage_warn_seconds=3, weekly_stage_fail_seconds=2)


def test_memory_units_are_normalized_for_macos_and_linux() -> None:
    _, mac_peak = process_memory_mb(raw_rss=10 * 1024 * 1024, system="Darwin")
    _, linux_peak = process_memory_mb(raw_rss=10 * 1024, system="Linux")
    assert mac_peak == pytest.approx(10)
    assert linux_peak == pytest.approx(10)


def test_semantic_comparison_ignores_only_observational_fields() -> None:
    left = {"routing_decision_ids": ["a"], "content_hash": "x", "duration_ms": 1}
    right = {"routing_decision_ids": ["a"], "content_hash": "x", "duration_ms": 9}
    assert compare_semantic_outputs(left, right)["equivalent"] is True
    right["routing_decision_ids"] = ["b"]
    assert compare_semantic_outputs(left, right)["equivalent"] is False


def test_disabled_threshold_evaluation_is_not_evaluated() -> None:
    collector = PerformanceCollector(
        run_id="run", as_of="2026-07-15",
        config=PerformanceConfig(threshold_evaluation_enabled=False),
        replay_mode=ReplayMode.EXACT_REPLAY,
    )
    with collector.timer(stage_name="scan_router", operation_name="scan_router.total"):
        pass
    assert collector.metrics[0].status is PerformanceStatus.NOT_EVALUATED


def test_memory_and_artifact_thresholds_contribute_to_run_status(tmp_path: Path) -> None:
    collector = PerformanceCollector(
        run_id="run", as_of="2026-07-15",
        config=PerformanceConfig(
            peak_rss_warn_mb=0, peak_rss_fail_mb=0,
            max_artifact_size_warn_mb=0, max_artifact_size_fail_mb=0,
        ),
    )
    with collector.timer(stage_name="scan_router", operation_name="scan_router.total"):
        pass
    artifact_path = tmp_path / "nonempty.csv"
    artifact_path.write_text("value\n1\n", encoding="utf-8")
    collector.record_artifact(StageArtifact.from_file("nonempty", artifact_path, row_count=1))
    summary = collector.summary()
    assert summary["resource_thresholds"] == {"peak_rss": "FAIL", "artifact_size": "FAIL"}
    assert summary["performance_status"] == "FAIL"
