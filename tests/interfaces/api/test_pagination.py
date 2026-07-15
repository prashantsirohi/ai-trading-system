from .conftest import HEADERS


def test_default_limit_is_reported(client):
    meta = client.get("/api/v1/routing", headers=HEADERS).json()["meta"]["pagination"]
    assert meta["limit"] == 50


def test_oversized_limit_and_unknown_sort_rejected(client):
    assert client.get("/api/v1/routing?limit=501", headers=HEADERS).status_code == 400
    assert client.get("/api/v1/routing?sort=unknown", headers=HEADERS).status_code == 400


def test_cursor_is_bound_to_filters(client):
    service = client.app.state.service
    second = dict(service.fixture["routing"][0])
    second["decision_id"] = "route-bbb"
    second["symbol_id"] = "BBB"
    service.fixture["routing"].append(second)
    first = client.get("/api/v1/routing?limit=1&sort=decision_id&order=asc", headers=HEADERS).json()
    cursor = first["meta"]["pagination"]["next_cursor"]
    page = client.get(f"/api/v1/routing?limit=1&sort=decision_id&order=asc&cursor={cursor}", headers=HEADERS)
    assert page.status_code == 200
    assert page.json()["data"][0]["decision_id"] == "route-bbb"
    mismatch = client.get(f"/api/v1/routing?limit=1&sort=decision_id&order=asc&symbol=AAA&cursor={cursor}", headers=HEADERS)
    assert mismatch.status_code == 400

