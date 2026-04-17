from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from tests.smoke.test_execution_api_smoke import _seed_execution_project
from ui.execution_api.app import create_app


SNAPSHOT_DIR = Path(__file__).resolve().parent / "fixtures" / "api_snapshots"
API_HEADERS = {"x-api-key": "local-dev-key"}


def _normalize_value(value, project_root: Path):
    if isinstance(value, dict):
        normalized_dict = {
            key: _normalize_value(item, project_root)
            for key, item in value.items()
            if key != "trust_confidence"
        }
        return normalized_dict
    if isinstance(value, list):
        return [_normalize_value(item, project_root) for item in value]
    if isinstance(value, str):
        normalized = value.replace(str(project_root), "<PROJECT_ROOT>")
        if normalized == value and "pipeline-2026-04-10-smoke" in value and "dashboard_payload.json" in value:
            normalized = value.replace(str(project_root), "<PROJECT_ROOT>")
        return normalized
    return value


def _normalize_snapshot(name: str, payload: dict, project_root: Path) -> dict:
    normalized = _normalize_value(payload, project_root)
    if name == "execution_health":
        normalized["summary"]["payload_age_minutes"] = "<payload_age_minutes>"
    if name == "execution_workspace_pipeline":
        normalized["health"]["summary"]["payload_age_minutes"] = "<payload_age_minutes>"
        normalized["ops_health"]["generated_at"] = "<generated_at>"
    if name == "execution_runs":
        for row in normalized.get("runs", []):
            row["started_at"] = "<started_at>"
    return normalized


def _load_expected(name: str) -> dict:
    return json.loads((SNAPSHOT_DIR / f"{name}.json").read_text(encoding="utf-8"))


def test_execution_api_response_snapshots(monkeypatch, tmp_path: Path) -> None:
    _seed_execution_project(tmp_path)
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    client = TestClient(create_app())

    endpoints = {
        "execution_health": "/api/execution/health",
        "execution_ranking": "/api/execution/ranking?limit=10",
        "execution_workspace_pipeline": "/api/execution/workspace/pipeline?limit=10",
        "execution_runs": "/api/execution/runs",
    }

    for name, endpoint in endpoints.items():
        response = client.get(endpoint, headers=API_HEADERS)
        assert response.status_code == 200
        actual = _normalize_snapshot(name, response.json(), tmp_path)
        expected = _load_expected(name)
        assert actual == expected
