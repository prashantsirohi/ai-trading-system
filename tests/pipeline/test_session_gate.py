from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.pipeline.session_gate import (
    GateStatus,
    evaluate_session_gate,
    write_session_gate_artifacts,
)
from ai_trading_system.research.pattern_lane_calibration.shadow import validate_signal_rows


# ---- validate_signal_rows -------------------------------------------------

def _good_signals() -> pd.DataFrame:
    return pd.DataFrame([
        {"symbol_id": "AAA", "exchange": "NSE", "as_of_date": "2026-07-17",
         "pattern_family": "flat_base", "scan_lane_as_of": "stage1_base",
         "evidence_origin": "fresh", "r1a_evidence_class": "evidence_supported"},
        {"symbol_id": "BBB", "exchange": "NSE", "as_of_date": "2026-07-17",
         "pattern_family": "head_shoulders", "scan_lane_as_of": "stage1_base",
         "evidence_origin": "carry_forward", "r1a_evidence_class": "suppression_only"},
    ])


def test_validate_signal_rows_accepts_wellformed() -> None:
    ok, issues, count = validate_signal_rows(_good_signals())
    assert ok and count == 0 and issues == []


def test_validate_signal_rows_flags_bad_origin_and_suppression() -> None:
    df = _good_signals()
    df.loc[0, "evidence_origin"] = "maybe"           # invalid origin
    df.loc[1, "r1a_evidence_class"] = "evidence_supported"  # suppression misclassified
    ok, issues, count = validate_signal_rows(df)
    assert not ok and count == 2
    assert any("invalid_evidence_origin" in i for i in issues)
    assert any("suppression_misclassified" in i for i in issues)


def test_validate_signal_rows_flags_blank_key() -> None:
    df = _good_signals()
    df.loc[0, "symbol_id"] = ""
    ok, _issues, count = validate_signal_rows(df)
    assert not ok and count == 1


# ---- session gate ---------------------------------------------------------

class _Artifact:
    def __init__(self, uri: str):
        self.uri = uri


class _FakeRegistry:
    def __init__(self, status_map, artifact_map):
        self._status = status_map
        self._artifacts = artifact_map

    def latest_stage_status_map(self, run_id):
        return self._status

    def get_artifact_map(self, run_id):
        return self._artifacts


def _write_lane_artifacts(tmp_path: Path, *, stale=0, malformed=0, mismatch=0, wall=420.0) -> dict:
    d = tmp_path / "lane"
    d.mkdir()
    summary = {
        "status": "completed", "policy_mismatch_count": mismatch,
        "stale_admitted_as_fresh_count": stale, "malformed_signal_rows": malformed,
        "fallback_rate": 0.22, "operational_side_effects": False,
    }
    (d / "summary.json").write_text(json.dumps(summary))
    (d / "runtime.json").write_text(json.dumps({"total_wall_seconds": wall}))
    (d / "parity.json").write_text(json.dumps({"operational_side_effects": False}))
    (d / "manifest.json").write_text(json.dumps({"operational_side_effects": False}))
    lane = {t: _Artifact(str(d / "x")) for t in (
        "pattern_lane_scan", "pattern_lane_source_diagnostics", "pattern_lane_shadow_report")}
    lane["pattern_lane_summary"] = _Artifact(str(d / "summary.json"))
    lane["pattern_lane_runtime"] = _Artifact(str(d / "runtime.json"))
    lane["pattern_lane_parity_report"] = _Artifact(str(d / "parity.json"))
    lane["pattern_lane_manifest"] = _Artifact(str(d / "manifest.json"))
    return lane


def _status_map(*, lane="completed", weekly="completed", router="completed", opp="completed", opp_ckpt=None):
    return {
        "pattern_lane_scan": {"status": lane},
        "weekly_stage": {"status": weekly},
        "scan_router": {"status": router},
        "opportunities": {"status": opp, "checkpoint": opp_ckpt},
    }


def test_session_gate_passes_clean_run(tmp_path) -> None:
    lane = _write_lane_artifacts(tmp_path)
    reg = _FakeRegistry(_status_map(), {"pattern_lane_scan": lane})
    result = evaluate_session_gate(reg, "RUN")
    assert result.day_counts is True
    assert result.status is GateStatus.PASS


def test_session_gate_tolerates_degraded_opportunities(tmp_path) -> None:
    lane = _write_lane_artifacts(tmp_path)
    reg = _FakeRegistry(_status_map(opp="failed", opp_ckpt={"non_blocking": True}), {"pattern_lane_scan": lane})
    result = evaluate_session_gate(reg, "RUN")
    assert result.day_counts is True  # non-blocking shadow degradation tolerated
    assert result.status is GateStatus.WARN


@pytest.mark.parametrize("kwargs,expect_fail_check", [
    (dict(stale=1), "no_stale_as_fresh"),
    (dict(malformed=2), "no_malformed_signal_rows"),
    (dict(mismatch=1), "policy_diagnostics_pass"),
    (dict(wall=1200.0), "runtime_passes"),
])
def test_session_gate_fails_on_bad_signal(tmp_path, kwargs, expect_fail_check) -> None:
    lane = _write_lane_artifacts(tmp_path, **kwargs)
    reg = _FakeRegistry(_status_map(), {"pattern_lane_scan": lane})
    result = evaluate_session_gate(reg, "RUN")
    assert result.day_counts is False
    failed = {c.check_id for c in result.checks if c.status is GateStatus.FAIL}
    assert expect_fail_check in failed


def test_session_gate_fails_on_missing_artifact(tmp_path) -> None:
    lane = _write_lane_artifacts(tmp_path)
    del lane["pattern_lane_manifest"]
    reg = _FakeRegistry(_status_map(), {"pattern_lane_scan": lane})
    result = evaluate_session_gate(reg, "RUN")
    assert result.day_counts is False


def test_session_gate_fails_on_incomplete_weekly_stage(tmp_path) -> None:
    lane = _write_lane_artifacts(tmp_path)
    reg = _FakeRegistry(_status_map(weekly="failed"), {"pattern_lane_scan": lane})
    result = evaluate_session_gate(reg, "RUN")
    assert result.day_counts is False


def test_write_session_gate_artifacts(tmp_path) -> None:
    lane = _write_lane_artifacts(tmp_path)
    reg = _FakeRegistry(_status_map(), {"pattern_lane_scan": lane})
    result = evaluate_session_gate(reg, "RUN")
    paths = write_session_gate_artifacts(result, tmp_path / "out")
    assert {p.name for p in paths} == {"session_gate_verdict.json", "session_gate_checks.csv", "session_gate.md"}
    verdict = json.loads((tmp_path / "out" / "session_gate_verdict.json").read_text())
    assert verdict["day_counts"] is True
