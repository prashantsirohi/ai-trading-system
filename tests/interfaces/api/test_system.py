from .conftest import HEADERS


def test_readiness_exposes_exact_limitations(client):
    response = client.get("/api/v1/system/readiness", headers=HEADERS)
    data = response.json()["data"]
    assert data["readiness_status"] == "READY_WITH_LIMITATIONS"
    assert data["phase4_development_ready"] is True
    assert data["phase4_production_ready"] is False
    assert {item["limitation_id"] for item in data["limitations"]} == {
        "SINGLE_YEAR_CONCENTRATION", "COPIED_REALISTIC_BASELINE_MISSING",
        "OPERATOR_MIGRATIONS_NOT_APPLIED", "EMPTY_REAL_PHASE3B_HISTORY",
    }


def test_version_has_policy_versions_and_etag(client):
    response = client.get("/api/v1/system/version", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["data"]["routing_policy_version"] == "scan-routing-policy-v2"
    assert response.headers["ETag"]


def test_etag_supports_not_modified(client):
    first = client.get("/api/v1/system/readiness", headers=HEADERS)
    second = client.get(
        "/api/v1/system/readiness",
        headers={**HEADERS, "If-None-Match": first.headers["ETag"]},
    )
    assert second.status_code == 304
    assert second.content == b""


def test_empty_store_is_partial_not_fabricated(copied_client):
    response = copied_client.get("/api/v1/stocks", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["data"] == []
    assert response.json()["meta"]["partial"] is True
    assert "SOURCE_NOT_MIGRATED" in response.json()["meta"]["limitations"]

