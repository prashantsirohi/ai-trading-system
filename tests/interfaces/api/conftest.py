from __future__ import annotations

import hashlib
import csv
import json
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.interfaces.api.app import create_app
from ai_trading_system.interfaces.api.config import ApiSettings, SourceProfile


API_KEY = "phase4-test-secret"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def client() -> TestClient:
    settings = ApiSettings(
        source_profile=SourceProfile.SMALL_FIXTURE,
        auth_enabled=True,
        api_key=API_KEY,
    )
    return TestClient(create_app(settings=settings))


@pytest.fixture
def copied_store(tmp_path: Path) -> Path:
    path = tmp_path / "control_plane.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE pipeline_run (run_id VARCHAR PRIMARY KEY)")
    conn.close()
    return path


@pytest.fixture
def copied_client(copied_store: Path) -> TestClient:
    settings = ApiSettings(
        source_profile=SourceProfile.COPIED_STORE,
        copied_control_plane=copied_store,
        auth_enabled=True,
        api_key=API_KEY,
    )
    return TestClient(create_app(settings=settings))


def _csv(path: Path, rows: list[dict[str, object]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def operator_artifact_client(copied_store: Path, tmp_path: Path) -> TestClient:
    root = tmp_path / "evidence" / "pipeline_runs" / "operator-run-1"
    root.mkdir(parents=True)
    common = {"position_cycle_id": "cycle-op", "symbol_id": "ABC", "exchange": "NSE"}
    _csv(root / "active_position_coverage.csv", [{**common, "coverage_status": "FULLY_MONITORED", "effective_scan_tier": "position_monitor", "routing_decision_id": "route-op", "market_data_available": True, "market_data_complete": True, "last_valid_market_timestamp": "2026-07-14T10:00:00Z", "expected_market_session": "2026-07-14", "missing_data_fields": "[]", "investigator_evidence_complete": True, "positive_action_suppressed": False, "suppression_reasons": "[]", "coverage_reasons": '["routed"]', "policy_version": "position-coverage-v1"}])
    _csv(root / "active_position_missing_data.csv", [{**common, "missing_data_fields": '["weekly_close"]', "staleness_sessions": 2, "last_valid_session": "2026-07-10", "expected_session": "2026-07-14", "alert_incident_id": "incident-op", "coverage_status": "ROUTED_WITH_INCOMPLETE_DATA"}])
    _csv(root / "position_monitor_reconciliation.csv", [{**common, "coverage_status": "FULLY_MONITORED", "outcome": "MONITORED"}])
    _csv(root / "position_episode_compatibility.csv", [{**common, "compatibility_status": "ambiguous_multiple_episodes", "candidate_id": "candidate-op", "policy_version": "compat-v1"}])
    _csv(root / "position_recovery_proposals.csv", [{**common, "recovery_proposal_id": "proposal-op", "recovery_mode": "reviewed", "proposal_status": "PROPOSED"}])
    _csv(root / "position_recovery_actions.csv", [{**common, "recovery_proposal_id": "proposal-op", "recovery_action_id": "action-op", "recovery_mode": "reviewed", "reviewed_by": "operator"}])
    manifest = {"run_id": "operator-run-1", "as_of": "2026-07-14T10:00:00Z", "manifest_id": "manifest-op", "policy_version": "phase3c5-calibration-policy-v1", "source_hashes": {"history": "abc"}, "schema_versions": {"samples": "v1"}, "configuration_hash": "cfg", "dataset_hashes": {"eligible": "def"}, "row_counts": {"eligible": 8}, "date_bounds": {"min": "2025-01-01", "max": "2026-07-14"}, "reproducibility_status": "REPRODUCIBLE"}
    (root / "phase3c5_calibration_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    quality = {"run_id": "operator-run-1", "as_of": "2026-07-14T10:00:00Z", "total_rows": 12, "eligible_rows": 8, "excluded_rows": 2, "quarantined_rows": 1, "pending_rows": 1, "class_ratio": {"positive": 4, "negative": 4}, "largest_class_share": 0.5, "date_min": "2025-01-01", "date_max": "2026-07-14", "coverage_counts": {"market_regime": {"bull": 8}, "stock_stage": {"stage_2": 8}, "scan_tier": {"full_investigator": 8}, "setup_family": {"breakout": 8}}, "exclusion_reason_counts": {"MISSING_HISTORY": 2}}
    (root / "phase3c5_calibration_quality_summary.json").write_text(json.dumps(quality), encoding="utf-8")
    readiness = {"run_id": "operator-run-1", "as_of": "2026-07-14T10:00:00Z", "verdict": "READY_WITH_LIMITATIONS", "phase4_development_ready": True, "phase4_production_ready": False, "policy_version": "phase3c5-readiness-policy-v1", "manifest_id": "manifest-op", "limitations": [{"limitation_id": "SINGLE_YEAR_CONCENTRATION", "description": "single year", "production_blocking": True}], "checks": [{"check_id": "HISTORY", "category": "evidence", "severity": "medium", "status": "WARN", "development_blocking": False, "production_blocking": True, "limitation": "SINGLE_YEAR_CONCENTRATION"}]}
    (root / "phase3c5_phase4_readiness.json").write_text(json.dumps(readiness), encoding="utf-8")
    _csv(root / "phase3c5_sample_coverage.csv", [{"dimension": "market_regime", "value": "bull", "count": 8, "status": "PASS"}])
    _csv(root / "phase3c5_exclusion_reasons.csv", [{"exclusion_reason": "MISSING_HISTORY", "count": 2}])
    _csv(root / "phase3c5_calibration_excluded.csv", [{"sample_id": "excluded-op"}])
    _csv(root / "phase3c5_calibration_quarantined.csv", [{"sample_id": "quarantine-op"}])
    _csv(root / "phase3c5_readiness_checks.csv", readiness["checks"])
    performance = {"run_id": "operator-run-1", "as_of": "2026-07-14T10:00:00Z", "policy_version": "phase3c4-performance-v1", "cache_mode": "COLD", "replay_mode": "REPLAY", "functional_status": "UNCHANGED", "performance_status": "PASS", "total_runtime_ms": 125.0, "peak_rss_mb": 64.0, "symbols_processed": 20, "rows_processed": 40, "stage_metrics": {"scan_router": {"duration_ms": 100.0}}, "threshold_evaluations": {"PASS": 2}, "profile": "copied_realistic"}
    (root / "phase3c4_performance_summary.json").write_text(json.dumps(performance), encoding="utf-8")
    _csv(root / "phase3c4_performance_metrics.csv", [{"run_id": "operator-run-1", "stage_name": "scan_router", "operation_name": "scan_router.total", "duration_ms": 100, "symbols_per_second": 20}])
    _csv(root / "phase3c4_artifact_metrics.csv", [{"artifact_name": "scan_routing", "row_count": 20}])
    _csv(root / "phase3c4_database_metrics.csv", [{"stage_name": "scan_router", "operation_name": "persist", "duration_ms": 5}])
    (root / "phase3c4_replay_comparison.json").write_text(json.dumps({"run_id": "operator-run-1", "as_of": "2026-07-14T10:00:00Z", "status": "EXACT_REPLAY"}), encoding="utf-8")
    _csv(root / "routing_conflicts.csv", [{"conflict_code": "REASON_TIER_MISMATCH", "symbol_id": "ABC", "exchange": "NSE", "requested_tier": "full_investigator", "effective_tier": "stage_only", "reason": "rank_selected", "policy_version": "scan-routing-v2", "created_at": "2026-07-14T10:00:00Z", "validation_message": "tier too low"}])
    conn = duckdb.connect(str(copied_store))
    conn.execute("""CREATE TABLE sector_membership_history (
        membership_observation_id VARCHAR, exchange VARCHAR, symbol_id VARCHAR,
        sector_id VARCHAR, membership_trust VARCHAR, valid_from DATE,
        valid_to DATE, recorded_at TIMESTAMP)""")
    conn.executemany("INSERT INTO sector_membership_history VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
        ["member-1", "NSE", "ABC", "TECH", "POINT_IN_TIME_VERIFIED", "2026-01-01", "2026-12-31", "2026-01-01"],
        ["member-2", "NSE", "ABC", "FIN", "POINT_IN_TIME_VERIFIED", "2026-06-01", "2026-12-31", "2026-06-01"],
    ])
    conn.execute("""CREATE TABLE stage_correction_impact (
        impact_id VARCHAR, candidate_id VARCHAR, impact_status VARCHAR,
        impact_link_status VARCHAR, review_required BOOLEAN,
        authoritative_calibration_eligible BOOLEAN)""")
    conn.executemany("INSERT INTO stage_correction_impact VALUES (?, ?, ?, ?, ?, ?)", [
        ["impact-no-match", "candidate-1", "UNRESOLVED_LEGACY", "unresolved_legacy_no_match", True, False],
        ["impact-ambiguous", "candidate-2", "UNRESOLVED_LEGACY", "unresolved_legacy_ambiguous", True, False],
    ])
    conn.close()
    settings = ApiSettings(source_profile=SourceProfile.COPIED_STORE, copied_control_plane=copied_store, artifact_root=tmp_path / "evidence", auth_enabled=True, api_key=API_KEY)
    client = TestClient(create_app(settings=settings))
    client.app.state.test_artifact_root = tmp_path / "evidence"
    return client
