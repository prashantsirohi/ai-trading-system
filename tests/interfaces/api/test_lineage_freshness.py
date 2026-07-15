from fastapi.testclient import TestClient

from .conftest import HEADERS


def test_artifact_projection_has_semantic_lineage_and_freshness(operator_artifact_client: TestClient) -> None:
    meta = operator_artifact_client.get("/api/v1/calibration/summary", headers=HEADERS).json()["meta"]
    assert meta["lineage"]
    assert meta["lineage_meta"]["source_consistent"] is True
    assert meta["freshness"]["source_as_of"] == "2026-07-14T10:00:00Z"
    assert meta["freshness"]["freshness_status"] == "FRESH"
