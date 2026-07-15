from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from ai_trading_system.interfaces.cli import benchmark_phase3c4
from ai_trading_system.interfaces.cli.build_phase3c5_calibration import (
    load_copied_rows,
    main as build_main,
    run_build,
)
from ai_trading_system.interfaces.cli.check_phase4_readiness import main as readiness_main


def test_small_fixture_cli_writes_machine_readable_artifacts(tmp_path: Path) -> None:
    output = tmp_path / "build"
    result = run_build(profile="small_fixture", output_root=output, as_of="2026-07-15")
    assert result["exit_code"] == 0
    assert result["verdict"] == "READY_WITH_LIMITATIONS"
    assert json.loads((output / "phase3c5_calibration_manifest.json").read_text())["manifest_id"] == result["manifest_id"]
    assert {path.name for path in output.iterdir()} == {
        "phase3c5_calibration_eligible.csv", "phase3c5_calibration_excluded.csv",
        "phase3c5_calibration_quarantined.csv", "phase3c5_calibration_manifest.json",
        "phase3c5_calibration_quality_summary.json", "phase3c5_sample_coverage.csv",
        "phase3c5_exclusion_reasons.csv", "phase3c5_readiness_checks.csv",
        "phase3c5_phase4_readiness.json", "phase3c5_phase4_readiness.md",
    }


def test_exact_fixture_replay_has_same_manifest_and_dataset_hash(tmp_path: Path) -> None:
    first = run_build(profile="small_fixture", output_root=tmp_path / "one", as_of="2026-07-15")
    second = run_build(profile="small_fixture", output_root=tmp_path / "two", as_of="2026-07-15")
    assert first["manifest_id"] == second["manifest_id"]
    assert first["eligible_dataset_hash"] == second["eligible_dataset_hash"]


def test_not_ready_exits_zero_by_default_and_nonzero_when_requested(tmp_path: Path) -> None:
    assert build_main(["--profile", "critical_leakage", "--output-root", str(tmp_path / "default")]) == 0
    assert build_main(["--profile", "critical_leakage", "--output-root", str(tmp_path / "strict"), "--fail-on-not-ready"]) == 1


def test_readiness_cli_accepts_missing_optional_baseline(tmp_path: Path) -> None:
    built = tmp_path / "built"
    run_build(profile="small_fixture", output_root=built, as_of="2026-07-15")
    output = tmp_path / "readiness"
    assert readiness_main([
        "--calibration-manifest", str(built / "phase3c5_calibration_manifest.json"),
        "--output-root", str(output),
    ]) == 0
    payload = json.loads((output / "phase3c5_phase4_readiness.json").read_text())
    assert payload["verdict"] == "READY_WITH_LIMITATIONS"


def test_symlinked_output_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="symlinked"):
        run_build(profile="small_fixture", output_root=link, as_of="2026-07-15")


def test_copied_profile_requires_explicit_copy(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="copied_realistic"):
        run_build(profile="copied_realistic", output_root=tmp_path / "out", as_of="2026-07-15")


def test_copied_profile_accepts_an_explicit_temporary_store(tmp_path: Path) -> None:
    copied = tmp_path / "control_plane_copy.duckdb"
    with duckdb.connect(str(copied)) as conn:
        conn.execute(
            """CREATE TABLE candidate_episode (
                candidate_id VARCHAR, symbol_id VARCHAR, exchange VARCHAR, setup_family VARCHAR
            )"""
        )
        conn.execute(
            """CREATE TABLE candidate_decision_context (
                decision_context_id VARCHAR, candidate_id VARCHAR, decided_at TIMESTAMP,
                decision_stage_status VARCHAR, decision_stage VARCHAR,
                decision_sector_stage VARCHAR, market_regime VARCHAR
            )"""
        )
        conn.execute("INSERT INTO candidate_episode VALUES ('c1', 'ABC', 'NSE', 'breakout')")
        conn.execute(
            """INSERT INTO candidate_decision_context VALUES (
                'd1', 'c1', '2026-01-02 10:00:00', 'LOCKED', 'stage_2', 'stage_2', 'bull'
            )"""
        )
    result = run_build(
        profile="copied_realistic", output_root=tmp_path / "out",
        copied_control_plane=copied, as_of="2026-07-15",
    )
    assert result["exit_code"] == 0
    assert result["verdict"] == "NOT_READY"
    assert result["quality_summary"]["total_rows"] == 1


def test_copied_profile_derives_governance_and_readiness_evidence(tmp_path: Path) -> None:
    copied = tmp_path / "control_plane_copy.duckdb"
    migration_root = Path("src/ai_trading_system/pipeline/migrations")
    with duckdb.connect(str(copied)) as conn:
        conn.execute(
            """CREATE TABLE candidate_episode (
                candidate_id VARCHAR, symbol_id VARCHAR, exchange VARCHAR, setup_family VARCHAR
            )"""
        )
        conn.execute(
            """CREATE TABLE candidate_decision_context (
                decision_context_id VARCHAR, candidate_id VARCHAR, decided_at TIMESTAMP,
                decision_stage_status VARCHAR, decision_stage VARCHAR,
                decision_sector_stage VARCHAR, market_regime VARCHAR
            )"""
        )
        conn.execute(
            """CREATE TABLE pipeline_run (
                run_id VARCHAR PRIMARY KEY, status VARCHAR
            )"""
        )
        conn.execute(
            """CREATE TABLE pipeline_stage_run (
                run_id VARCHAR, stage_name VARCHAR, attempt_number INTEGER, status VARCHAR
            )"""
        )
        for migration in (
            "033_opportunity_phase3b.sql",
            "034_opportunity_phase3c1_governance.sql",
            "035_opportunity_phase3c1a_governance_hardening.sql",
            "036_opportunity_phase3c3_position_monitoring.sql",
        ):
            conn.execute((migration_root / migration).read_text(encoding="utf-8"))
        conn.execute("INSERT INTO candidate_episode VALUES ('c1', 'ABC', 'NSE', 'breakout')")
        conn.execute(
            """INSERT INTO candidate_decision_context VALUES (
                'd1', 'c1', '2026-01-02 10:00:00', 'LOCKED', 'stage_2', 'stage_2', 'bull'
            )"""
        )
        conn.execute("INSERT INTO pipeline_run VALUES ('run-real', 'completed')")
        conn.execute("INSERT INTO pipeline_stage_run VALUES ('run-real', 'weekly_stage', 1, 'completed')")
        conn.execute(
            """INSERT INTO weekly_stock_stage_history (
                observation_id, exchange, symbol_id, sector_id, sector_name, as_of,
                source_week_start, source_week_end, stage_status, effective_stage,
                classifier_version, source_artifact_hash, observation_json, run_id, stage_attempt
            ) VALUES (
                'obs-1', 'NSE', 'ABC', 'TECH', 'Technology', '2026-01-02 09:00:00',
                '2025-12-29', '2026-01-02', 'locked', 'stage_2',
                'classifier-v1', 'source-hash', '{}', 'run-real', 1
            )"""
        )
        conn.execute(
            """INSERT INTO sector_membership_history (
                membership_observation_id, exchange, symbol_id, sector_id, sector_name,
                valid_from, valid_to, membership_trust, point_in_time_valid, source_type,
                source_hash, policy_version, recorded_at, run_id, stage_attempt
            ) VALUES (
                'member-1', 'NSE', 'ABC', 'TECH', 'Technology', '2025-01-01',
                '2026-12-31', 'POINT_IN_TIME_VERIFIED', true, 'historical',
                'membership-hash', 'membership-v1', '2025-12-31 10:00:00', 'run-real', 1
            )"""
        )

    rows = load_copied_rows(copied, as_of="2026-07-15")
    assert rows[0]["membership_trust"] == "POINT_IN_TIME_VERIFIED"
    assert rows[0]["symbol_identity_valid"] is True
    assert rows[0]["was_in_universe_as_of_decision"] is True
    assert rows[0]["authoritative_calibration_eligible"] is True
    assert rows[0]["correction_impact_status"] == "resolved"
    assert rows[0]["lookback_sessions"] == 5

    output = tmp_path / "out"
    result = run_build(
        profile="copied_realistic", output_root=output,
        copied_control_plane=copied, as_of="2026-07-15",
    )
    manifest = json.loads((output / "phase3c5_calibration_manifest.json").read_text())
    assert manifest["readiness_evidence"]["operator_migrations_applied"] is True
    assert manifest["readiness_evidence"]["real_phase3b_history_present"] is True
    assert "OPERATOR_MIGRATIONS_NOT_APPLIED" not in result["limitations"]
    assert "EMPTY_REAL_PHASE3B_HISTORY" not in result["limitations"]

    readiness_output = tmp_path / "readiness"
    assert readiness_main([
        "--calibration-manifest", str(output / "phase3c5_calibration_manifest.json"),
        "--output-root", str(readiness_output),
    ]) == 0
    readiness = json.loads(
        (readiness_output / "phase3c5_phase4_readiness.json").read_text()
    )
    limitation_ids = {item["limitation_id"] for item in readiness["limitations"]}
    assert "OPERATOR_MIGRATIONS_NOT_APPLIED" not in limitation_ids
    assert "EMPTY_REAL_PHASE3B_HISTORY" not in limitation_ids


def test_configured_operator_store_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    live_root = tmp_path / "operator_data"
    live_root.mkdir()
    live_store = live_root / "control_plane.duckdb"
    live_store.touch()
    monkeypatch.setattr(
        benchmark_phase3c4, "get_domain_paths",
        lambda _project_root: SimpleNamespace(root_dir=live_root),
    )
    with pytest.raises(ValueError, match="operator control_plane"):
        run_build(
            profile="copied_realistic", output_root=tmp_path / "out",
            copied_control_plane=live_store, as_of="2026-07-15",
        )
