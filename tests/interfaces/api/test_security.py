import logging

from ai_trading_system.interfaces.api.app import create_app
from ai_trading_system.interfaces.api.config import ApiSettings, SourceProfile
from fastapi.testclient import TestClient

from .conftest import API_KEY, HEADERS


def test_unauthenticated_and_invalid_key_rejected(client):
    assert client.get("/api/v1/stocks").status_code == 401
    invalid = client.get("/api/v1/stocks", headers={"Authorization": "Bearer wrong"})
    assert invalid.status_code == 403
    assert invalid.json()["code"] == "AUTHORIZATION_DENIED"


def test_valid_bearer_and_api_key_accepted(client):
    assert client.get("/api/v1/stocks", headers=HEADERS).status_code == 200
    assert client.get("/api/v1/stocks", headers={"X-API-Key": API_KEY}).status_code == 200


def test_mutation_methods_are_rejected(client):
    for method in ("post", "put", "patch", "delete"):
        response = getattr(client, method)("/api/v1/candidates", headers=HEADERS)
        assert response.status_code == 405
        assert response.json()["code"] == "METHOD_NOT_ALLOWED"


def test_unknown_query_and_injection_like_inputs_rejected(client):
    unknown = client.get("/api/v1/stocks?database_path=../../secret", headers=HEADERS)
    assert unknown.status_code == 400
    bad_sort = client.get("/api/v1/routing?sort=decision_id%20DESC%3BDELETE", headers=HEADERS)
    assert bad_sort.status_code == 400


def test_future_and_naive_as_of_rejected(client):
    assert client.get("/api/v1/stocks?as_of=2999-01-01", headers=HEADERS).json()["code"] == "INVALID_AS_OF"
    assert client.get("/api/v1/stocks?as_of=2026-01-01T12:00:00", headers=HEADERS).json()["code"] == "INVALID_AS_OF"


def test_key_is_not_logged(client, caplog):
    caplog.set_level(logging.INFO)
    client.get("/api/v1/stocks", headers=HEADERS)
    assert API_KEY not in caplog.text


def test_rate_limit_is_per_credential():
    settings = ApiSettings(source_profile=SourceProfile.SMALL_FIXTURE, api_key=API_KEY, rate_limit_per_minute=1)
    client = TestClient(create_app(settings=settings))
    assert client.get("/api/v1/stocks", headers=HEADERS).status_code == 200
    assert client.get("/api/v1/stocks", headers=HEADERS).status_code == 429


def test_openapi_has_api_key_scheme_and_no_mutations(client):
    schema = client.app.openapi()
    assert {"BearerAuth", "ApiKeyAuth"} <= set(schema["components"]["securitySchemes"])
    for methods in schema["paths"].values():
        assert not ({"post", "put", "patch", "delete"} & set(methods))
