from .conftest import HEADERS


def test_live_is_public(client):
    response = client.get("/api/v1/health/live")
    assert response.status_code == 200
    assert response.json()["data"]["status"] == "live"


def test_ready_is_public_and_not_production_claim(client):
    response = client.get("/api/v1/health/ready")
    assert response.status_code == 200
    assert response.json()["data"]["api_ready"] is True
    assert response.json()["data"]["phase4_production_ready"] is False


def test_request_id_is_validated(client):
    response = client.get("/api/v1/system/readiness", headers={**HEADERS, "X-Request-ID": "trace-123"})
    assert response.headers["X-Request-ID"] == "trace-123"
    rejected = client.get("/api/v1/system/readiness", headers={**HEADERS, "X-Request-ID": "bad id\n"})
    assert rejected.headers["X-Request-ID"] != "bad id"

