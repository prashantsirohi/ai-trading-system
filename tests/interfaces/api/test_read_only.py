import hashlib
import os
import subprocess
import sys
from types import SimpleNamespace

import duckdb
import pytest

from ai_trading_system.interfaces.api.config import ApiSettings, SourceProfile

from .conftest import HEADERS, file_hash


def test_startup_and_get_do_not_change_copied_store(copied_store, copied_client):
    before = file_hash(copied_store)
    response = copied_client.get("/api/v1/system/readiness", headers=HEADERS)
    after = file_hash(copied_store)
    assert response.status_code == 200
    assert before == after


def test_get_does_not_change_artifact(tmp_path, client):
    artifact = tmp_path / "evidence.json"
    artifact.write_text('{"immutable": true}', encoding="utf-8")
    before = hashlib.sha256(artifact.read_bytes()).hexdigest()
    client.get("/api/v1/calibration/manifest", headers=HEADERS)
    assert hashlib.sha256(artifact.read_bytes()).hexdigest() == before


def test_api_package_does_not_import_broker_clients(client):
    code = """
import sys
from ai_trading_system.interfaces.api.app import create_app
create_app(testing=True)
assert 'ai_trading_system.domains.execution.adapters.dhan' not in sys.modules
assert 'dhanhq' not in sys.modules
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_route_audit_has_no_business_mutations(client):
    forbidden = {"POST", "PUT", "PATCH", "DELETE"}
    for route in client.app.routes:
        if getattr(route, "path", "").startswith("/api/v1"):
            assert not (set(getattr(route, "methods", set())) & forbidden)


def test_api_source_has_no_write_sql_or_migration_store():
    source_root = "src/ai_trading_system/interfaces/api"
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in __import__("pathlib").Path(source_root).rglob("*.py")
    ).upper()
    for statement in ("INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE TABLE", "MERGE INTO", "COPY TO"):
        assert statement not in source
    assert "REGISTRYSTORE(" not in source


def test_copied_store_refuses_symlink(tmp_path):
    source = tmp_path / "source.duckdb"
    duckdb.connect(str(source)).close()
    link = tmp_path / "linked.duckdb"
    link.symlink_to(source)
    settings = ApiSettings(source_profile=SourceProfile.COPIED_STORE, copied_control_plane=link)
    with pytest.raises(ValueError, match="symlink"):
        settings.control_plane_path()


def test_copied_store_refuses_operator_path(tmp_path, monkeypatch):
    operator = tmp_path / "control_plane.duckdb"
    duckdb.connect(str(operator)).close()
    monkeypatch.setattr(
        "ai_trading_system.interfaces.api.config.get_domain_paths",
        lambda **_: SimpleNamespace(root_dir=tmp_path),
    )
    settings = ApiSettings(source_profile=SourceProfile.COPIED_STORE, copied_control_plane=operator)
    with pytest.raises(ValueError, match="operator"):
        settings.control_plane_path()
