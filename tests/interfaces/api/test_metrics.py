from fastapi.testclient import TestClient

from .conftest import HEADERS


def test_metrics_use_route_templates_and_low_cardinality_labels(client: TestClient) -> None:
    response = client.get("/api/v1/stocks/AAA", headers=HEADERS)
    assert response.status_code == 200
    snapshot = client.app.state.metrics.snapshot()
    labels = [route for route, _method in snapshot["request_count"]]
    assert "/api/v1/stocks/{symbol_id}" in labels
    assert all("AAA" not in label for label in labels)
    assert snapshot["cache_hit_count"] == {}
    assert snapshot["cache_miss_count"] == {}
