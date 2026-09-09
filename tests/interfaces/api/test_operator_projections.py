import hashlib
import csv

from fastapi.testclient import TestClient

from .conftest import HEADERS


def test_position_route_coverage_does_not_imply_episode_attachment(operator_artifact_client):
    client = operator_artifact_client
    legacy = client.get("/api/v1/positions/coverage", headers=HEADERS).json()["data"][0]
    assert legacy["route_data_covered"] is None
    assert legacy["episode_attached"] is None
    root = client.app.state.test_artifact_root
    path = next(root.rglob("position_monitor_reconciliation.csv"))
    row = {
        "position_cycle_id": "cycle-op", "symbol_id": "ABC", "exchange": "NSE",
        "route_data_covered": True, "episode_attached": False,
        "investigator_evidence_complete": False,
        "outcome": "POSITION_RECOVERY_REQUIRED", "observed_session": "2026-07-14",
        "reconciliation_schema_version": "position-reconciliation-v2",
    }
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    current = client.get("/api/v1/positions/coverage", headers=HEADERS).json()["data"][0]
    assert current["coverage_status"] == "FULLY_MONITORED"  # Router-owned legacy field.
    assert current["route_data_covered"] is True
    assert current["episode_attached"] is False
    assert current["investigator_evidence_complete"] is False
    assert current["reconciliation_observed_session"] == "2026-07-14"


def test_operator_position_calibration_and_performance_projections(operator_artifact_client: TestClient) -> None:
    artifact_root = operator_artifact_client.app.state.test_artifact_root
    source_db = operator_artifact_client.app.state.settings.copied_control_plane
    artifact_before = {path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns) for path in artifact_root.rglob("*") if path.is_file()}
    db_before = hashlib.sha256(source_db.read_bytes()).hexdigest()
    positions = operator_artifact_client.get("/api/v1/positions/coverage", headers=HEADERS).json()
    assert positions["data"][0]["routing_decision_id"] == "route-op"
    assert positions["data"][0]["episode_match_status"] == "ambiguous_multiple_episodes"
    assert positions["meta"]["lineage_meta"]["primary"]["run_id"] == "operator-run-1"
    missing = operator_artifact_client.get("/api/v1/positions/missing-data", headers=HEADERS).json()
    assert missing["data"][0]["alert_incident_id"] == "incident-op"
    proposal = operator_artifact_client.get("/api/v1/positions/recovery-proposals", headers=HEADERS).json()["data"][0]
    assert proposal["action_state"] == "reviewed_action"
    calibration = operator_artifact_client.get("/api/v1/calibration/summary", headers=HEADERS).json()["data"]
    assert calibration["total_samples"] == 12
    assert calibration["formal_verdict"] == "READY_WITH_LIMITATIONS"
    assert calibration["policy_snapshot_coverage"] == {"snapshot-op": 8}
    assert calibration["admission_reason_coverage"] == {"qualified_breakout": 8}
    manifest = operator_artifact_client.get(
        "/api/v1/calibration/manifest", headers=HEADERS,
    ).json()["data"]
    assert manifest["policy_snapshot_ids"] == ["snapshot-op"]
    performance = operator_artifact_client.get("/api/v1/performance/latest", headers=HEADERS).json()["data"]
    assert performance["operation_metrics"][0]["stage_name"] == "scan_router"
    assert operator_artifact_client.get("/api/v1/performance/baselines", headers=HEADERS).json()["data"][0]["run_id"] == "operator-run-1"
    artifact_after = {path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns) for path in artifact_root.rglob("*") if path.is_file()}
    assert artifact_after == artifact_before
    assert hashlib.sha256(source_db.read_bytes()).hexdigest() == db_before
