from ai_trading_system.interfaces.api.services.phase4 import _stage_projection

from .conftest import HEADERS


def test_stock_list_and_detail(client):
    listing = client.get("/api/v1/stocks", headers=HEADERS)
    assert listing.json()["data"][0]["symbol_id"] == "AAA"
    detail = client.get("/api/v1/stocks/AAA", headers=HEADERS)
    assert detail.status_code == 200
    assert detail.json()["data"]["governance_status"] == "AUTHORITATIVE"


def test_canonical_stage_payload_is_projected_to_api_contract():
    projected = _stage_projection("stock", {
        "exchange": "NSE", "symbol_id": "ABC", "as_of": "2026-07-15",
        "source_week_end": "2026-07-14", "source_artifact_hash": "abc123",
        "effective_stage": "stage_2_advancing", "stage_status": "provisional",
        "stage_confidence_score": 80.0, "sector_membership_trust": "OBSERVED_AT_RUN",
    })
    assert projected["observation_id"].startswith("stage-")
    assert projected["stage_confidence"] == 80.0
    assert projected["membership_trust"] == "OBSERVED_AT_RUN"
    assert _stage_projection("stock", projected)["observation_id"] == projected["observation_id"]


def test_missing_detail_is_typed_404(client):
    response = client.get("/api/v1/stocks/MISSING", headers=HEADERS)
    assert response.status_code == 404
    assert response.json()["code"] == "RESOURCE_NOT_FOUND"


def test_routing_preserves_all_reasons_and_structure(client):
    response = client.get("/api/v1/routing/route-aaa", headers=HEADERS)
    data = response.json()["data"]
    assert data["all_reasons"] == ["rank_selected", "stage_promoted"]
    assert data["new_long_structural_block"] is False
    assert data["policy_version"] == "scan-routing-policy-v2"


def test_candidate_and_children(client):
    detail = client.get("/api/v1/candidates/candidate-aaa", headers=HEADERS)
    assert detail.json()["data"]["pre_entry_history_available"] is True
    assert client.get("/api/v1/candidates/candidate-aaa/snapshots", headers=HEADERS).json()["data"]
    assert client.get("/api/v1/candidates/candidate-aaa/decisions", headers=HEADERS).json()["data"]
    assert client.get("/api/v1/candidates/candidate-aaa/outcomes", headers=HEADERS).json()["data"]


def test_position_coverage_exposes_suppression(client):
    data = client.get("/api/v1/positions/coverage/cycle-aaa", headers=HEADERS).json()["data"]
    assert data["position_monitor_present"] is True
    assert data["positive_action_suppressed"] is False


def test_alert_filters(client):
    assert client.get("/api/v1/alerts?severity=critical&status=OPEN", headers=HEADERS).json()["data"]
    assert client.get("/api/v1/alerts?severity=warning", headers=HEADERS).json()["data"] == []


def test_governance_resources(client):
    correction = client.get("/api/v1/governance/stage-corrections", headers=HEADERS).json()["data"][0]
    assert correction["authority"] == "reviewed_operator_correction"
    impact = client.get("/api/v1/governance/correction-impacts", headers=HEADERS).json()["data"][0]
    assert impact["authoritative_calibration_eligible"] is True


def test_calibration_and_performance(client):
    manifest = client.get("/api/v1/calibration/manifest", headers=HEADERS)
    assert manifest.json()["data"]["manifest_id"] == "fixture-manifest"
    performance = client.get("/api/v1/performance/latest", headers=HEADERS)
    assert performance.json()["data"]["functional_status"] == "PASS"
    baseline = client.get("/api/v1/performance/baselines", headers=HEADERS)
    assert baseline.json()["meta"]["partial"] is True


def test_every_required_endpoint_exists(client):
    paths = set(client.app.openapi()["paths"])
    assert len(paths) == 41
    assert "/api/v1/governance/membership-history" in paths
    assert "/api/v1/readiness/checks" in paths
