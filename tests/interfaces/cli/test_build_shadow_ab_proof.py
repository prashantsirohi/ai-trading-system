from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.interfaces.cli.build_shadow_ab_proof import build_proof_bundle


def _write(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _make_run(root: Path, *, mode: str, scan_tier: str = "deep") -> Path:
    run = root / f"run_{mode}"
    # STRICT decision artifact — identical across runs
    _write(run / "rank/attempt_1/pattern_scan.csv", pd.DataFrame({"symbol_id": ["AAA"], "signal": ["x"]}))
    _write(run / "candidates/attempt_1/final_candidates.csv", pd.DataFrame({"symbol_id": ["AAA"], "decision": ["buy"]}))
    # CONTENT artifact differing only by a run-scoped column
    _write(run / "scan_router/attempt_1/scan_routing.csv",
           pd.DataFrame({"symbol_id": ["AAA"], "scan_tier": [scan_tier], "routing_decision_id": [f"id-{mode}"]}))
    # telemetry — B gets an extra row
    perf = pd.DataFrame({"stage_name": ["ingest"], "duration_ms": ["10"]})
    if mode == "shadow":
        perf = pd.DataFrame({"stage_name": ["ingest", "pattern_lane_scan"], "duration_ms": ["10", "400"]})
        _write(run / "pattern_lane_scan/attempt_1/pattern_lane_scan.csv", pd.DataFrame({"symbol_id": ["AAA"]}))
        (run / "pattern_lane_scan/attempt_1/pattern_lane_manifest.json").write_text('{"operational_side_effects": false}')
    _write(run / "performance/attempt_1/phase3c4_performance_metrics.csv", perf)
    return run


def test_build_proof_bundle_pass(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    run_a = _make_run(runs, mode="off")
    run_b = _make_run(runs, mode="shadow")
    run_c = _make_run(runs, mode="control")  # same as A (mode off) content

    result = build_proof_bundle(
        run_a=run_a, run_b=run_b, run_c=run_c,
        staging_root=tmp_path / "staging", git_record_root=tmp_path / "git",
        as_of_date="2026-07-17", run_id="ABRUN", code_commit="7d5f03a67c",
        clone_db_hashes={"ohlcv.duckdb": "abc"}, env_sha256="deadbeef",
        policy_hashes={"r0_policy_hash": "hh"},
    )

    assert result["decision_verdict"] == "PASS"
    assert result["strict_artifacts_byte_identical"] is True
    assert result["flag_caused_legacy_diffs"] == []

    staging = tmp_path / "staging"
    for name in ("experiment_manifest.json", "run_A_inventory.csv", "run_B_inventory.csv",
                 "strict_hash_comparison.csv", "normalized_content_comparison.csv",
                 "decision_dataset_comparison.json", "allowed_nondeterminism.json",
                 "lane_artifact_presence.json", "comparison_summary.md", "bundle.sha256"):
        assert (staging / name).exists(), name

    git = tmp_path / "git"
    for name in ("README.md", "bundle_reference.json", "experiment_manifest.json", "bundle.sha256"):
        assert (git / name).exists(), name

    presence = json.loads((staging / "lane_artifact_presence.json").read_text())
    assert presence["run_b_lane_artifact_count"] >= 1
    assert presence["run_a_has_lane_dir"] is False

    ref = json.loads((git / "bundle_reference.json").read_text())
    assert ref["code_commit"] == "7d5f03a67c"
    assert ref["evidence_tag"].startswith("r1a-safety-proof-2026-07-17-")
    # bundle.sha256 lists files and matches recorded digest
    assert (staging / "bundle.sha256").read_text().strip() != ""


def test_build_proof_bundle_flags_real_data_diff(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    run_a = _make_run(runs, mode="off", scan_tier="deep")
    run_b = _make_run(runs, mode="shadow", scan_tier="stage")   # genuine decision-column change
    run_c = _make_run(runs, mode="control", scan_tier="deep")   # control matches A

    result = build_proof_bundle(
        run_a=run_a, run_b=run_b, run_c=run_c,
        staging_root=tmp_path / "staging", git_record_root=tmp_path / "git",
        as_of_date="2026-07-17", run_id="ABRUN", code_commit="7d5f03a67c",
    )
    assert result["decision_verdict"] == "REVIEW"
    assert any("scan_routing.csv" in d["rel_path"] for d in result["flag_caused_legacy_diffs"])
