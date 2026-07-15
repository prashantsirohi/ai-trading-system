from fastapi.testclient import TestClient

from .conftest import HEADERS


def test_routing_conflict_artifact_is_aggregated(operator_artifact_client: TestClient) -> None:
    body = operator_artifact_client.get("/api/v1/governance/conflicts", headers=HEADERS).json()
    assert any(row["conflict_type"] == "REASON_TIER_MISMATCH" for row in body["data"])
    assert any(row["conflict_type"] == "MULTIPLE_AUTHORITATIVE_MEMBERSHIPS" for row in body["data"])
    assert any(row["conflict_type"] == "UNRESOLVED_LEGACY_NO_MATCH" for row in body["data"])
    assert any(row["conflict_type"] == "UNRESOLVED_LEGACY_AMBIGUOUS" for row in body["data"])
    assert "GOVERNANCE_CONFLICT_PRESENT" in body["meta"]["limitations"]
    assert operator_artifact_client.app.state.metrics.governance_conflict_response_count == 1
