from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.platform.parity.comparison_policy import (
    FieldClass,
    classify_artifact,
    compare_artifact,
    compare_runs,
)


def _write(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def test_classify_artifact_strict_content_telemetry() -> None:
    assert classify_artifact("rank/attempt_1/pattern_scan.csv").artifact_class is FieldClass.STRICT
    assert classify_artifact("scan_router/attempt_1/scan_routing.csv").artifact_class is FieldClass.CONTENT
    assert classify_artifact("performance/attempt_1/phase3c4_performance_metrics.csv").artifact_class is FieldClass.TELEMETRY


def test_byte_identical_is_identical(tmp_path: Path) -> None:
    frame = pd.DataFrame({"symbol_id": ["AAA", "BBB"], "score": [1.0, 2.0]})
    a = _write(tmp_path / "a/final_candidates.csv", frame)
    b = _write(tmp_path / "b/final_candidates.csv", frame)
    cmp = compare_artifact(a, b)
    assert cmp.raw_match and cmp.normalized_match
    assert cmp.verdict == "IDENTICAL"


def test_run_scoped_only_diff_is_content_equivalent(tmp_path: Path) -> None:
    base = {"symbol_id": ["AAA", "BBB"], "scan_tier": ["deep", "stage"]}
    a = _write(tmp_path / "a/scan_routing.csv", pd.DataFrame({**base, "routing_decision_id": ["x1", "x2"]}))
    b = _write(tmp_path / "b/scan_routing.csv", pd.DataFrame({**base, "routing_decision_id": ["y9", "y8"]}))
    cmp = compare_artifact(a, b)
    assert not cmp.raw_match
    assert cmp.normalized_match  # only run-scoped column differs
    assert cmp.verdict == "CONTENT_EQUIVALENT"
    assert cmp.differing_columns == ()


def test_content_column_diff_is_data_diff(tmp_path: Path) -> None:
    a = _write(tmp_path / "a/scan_routing.csv", pd.DataFrame({"symbol_id": ["AAA"], "scan_tier": ["deep"]}))
    b = _write(tmp_path / "b/scan_routing.csv", pd.DataFrame({"symbol_id": ["AAA"], "scan_tier": ["stage"]}))
    cmp = compare_artifact(a, b)
    assert cmp.verdict == "DATA_DIFF"
    assert "scan_tier" in cmp.differing_columns


def test_row_order_only_is_content_equivalent(tmp_path: Path) -> None:
    a = _write(tmp_path / "a/ranked_signals.csv", pd.DataFrame({"symbol_id": ["AAA", "BBB"], "composite": ["9", "3"]}))
    b = _write(tmp_path / "b/ranked_signals.csv", pd.DataFrame({"symbol_id": ["BBB", "AAA"], "composite": ["3", "9"]}))
    cmp = compare_artifact(a, b)
    assert cmp.normalized_match
    assert cmp.verdict in {"IDENTICAL", "CONTENT_EQUIVALENT"}


def test_float_jitter_within_tolerance_is_equivalent(tmp_path: Path) -> None:
    a = _write(tmp_path / "a/universe_valuation_daily.csv", pd.DataFrame({"sector": ["X"], "pe_ttm": ["21.1234561"]}))
    b = _write(tmp_path / "b/universe_valuation_daily.csv", pd.DataFrame({"sector": ["X"], "pe_ttm": ["21.1234559"]}))
    cmp = compare_artifact(a, b)
    assert cmp.normalized_match  # rounds to 6 decimals -> equal


def test_telemetry_additive_rows_allowed(tmp_path: Path) -> None:
    cols = {"stage_name": ["ingest"], "duration_ms": ["10"]}
    a = _write(tmp_path / "a/performance/phase3c4_performance_metrics.csv", pd.DataFrame(cols))
    b = _write(
        tmp_path / "b/performance/phase3c4_performance_metrics.csv",
        pd.DataFrame({"stage_name": ["ingest", "pattern_lane_scan"], "duration_ms": ["10", "400"]}),
    )
    cmp = compare_artifact(a, b)
    assert cmp.artifact_class is FieldClass.TELEMETRY
    assert cmp.verdict == "TELEMETRY"  # extra row allowed, schema matches


def test_compare_runs_control_subtracts_nondeterminism(tmp_path: Path) -> None:
    # A vs B differ on a content column, but A vs C (control) reproduce the same
    # difference -> not flag-caused.
    _write(tmp_path / "A/scan_router/scan_routing.csv", pd.DataFrame({"symbol_id": ["AAA"], "scan_tier": ["deep"]}))
    _write(tmp_path / "B/scan_router/scan_routing.csv", pd.DataFrame({"symbol_id": ["AAA"], "scan_tier": ["stage"]}))
    _write(tmp_path / "C/scan_router/scan_routing.csv", pd.DataFrame({"symbol_id": ["AAA"], "scan_tier": ["stage"]}))
    report = compare_runs(tmp_path / "A", tmp_path / "B", control_c=tmp_path / "C")
    assert report.flag_caused == ()  # reproduced in control

    # Now a diff present in A~B but absent in A~C -> flag-caused.
    _write(tmp_path / "C2/scan_router/scan_routing.csv", pd.DataFrame({"symbol_id": ["AAA"], "scan_tier": ["deep"]}))
    report2 = compare_runs(tmp_path / "A", tmp_path / "B", control_c=tmp_path / "C2")
    assert any(rel.endswith("scan_routing.csv") for rel, _ in report2.flag_caused)
