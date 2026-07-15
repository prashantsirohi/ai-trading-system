from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.interfaces.api.app import create_app
from ai_trading_system.interfaces.api.config import ApiSettings, SourceProfile


API_KEY = "phase4-test-secret"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def client() -> TestClient:
    settings = ApiSettings(
        source_profile=SourceProfile.SMALL_FIXTURE,
        auth_enabled=True,
        api_key=API_KEY,
    )
    return TestClient(create_app(settings=settings))


@pytest.fixture
def copied_store(tmp_path: Path) -> Path:
    path = tmp_path / "control_plane.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE pipeline_run (run_id VARCHAR PRIMARY KEY)")
    conn.close()
    return path


@pytest.fixture
def copied_client(copied_store: Path) -> TestClient:
    settings = ApiSettings(
        source_profile=SourceProfile.COPIED_STORE,
        copied_control_plane=copied_store,
        auth_enabled=True,
        api_key=API_KEY,
    )
    return TestClient(create_app(settings=settings))

