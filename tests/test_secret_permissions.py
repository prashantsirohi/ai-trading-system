from __future__ import annotations

import logging
import os
import stat
from pathlib import Path

import pytest

from ai_trading_system.domains.publish.channels import oauth_flow
from ai_trading_system.platform.utils import secret_permissions
from ai_trading_system.platform.utils.env import _LOADED_ENV_PATHS, load_project_env
from ai_trading_system.platform.utils.secret_permissions import (
    warn_if_insecure_secret_file,
    write_private_text,
)


@pytest.fixture(autouse=True)
def _clear_permission_warning_cache() -> None:
    secret_permissions._WARNED_SECRET_PATHS.clear()


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics required")
def test_insecure_secret_warning_is_once_and_redacted(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = tmp_path / "token.json"
    secret.write_text("never-log-this-token", encoding="utf-8")
    secret.chmod(0o644)

    with caplog.at_level(logging.WARNING):
        assert warn_if_insecure_secret_file(secret) is True
        assert warn_if_insecure_secret_file(secret) is True

    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 1
    assert str(secret) in messages[0]
    assert "0644" in messages[0]
    assert "never-log-this-token" not in messages[0]


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics required")
def test_project_env_load_warns_for_permissive_file(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("PHASE0_PERMISSION_TEST=loaded\n", encoding="utf-8")
    env_path.chmod(0o644)
    _LOADED_ENV_PATHS.clear()

    with caplog.at_level(logging.WARNING):
        loaded_path = load_project_env(tmp_path)

    assert loaded_path == env_path
    assert any(str(env_path) in record.getMessage() for record in caplog.records)


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics required")
def test_private_write_restricts_new_and_existing_files(tmp_path: Path) -> None:
    secret = tmp_path / "token.json"
    secret.write_text("old", encoding="utf-8")
    secret.chmod(0o644)

    write_private_text(secret, "replacement")

    assert secret.read_text(encoding="utf-8") == "replacement"
    assert stat.S_IMODE(secret.stat().st_mode) == 0o600


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics required")
def test_oauth_flow_writes_private_token_without_printing_refresh_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    credentials_path = tmp_path / "client_secret.json"
    token_path = tmp_path / "token.json"
    credentials_path.write_text("{}", encoding="utf-8")
    credentials_path.chmod(0o600)

    class _Credentials:
        refresh_token = "refresh-token-must-stay-secret"

        @staticmethod
        def to_json() -> str:
            return '{"refresh_token":"refresh-token-must-stay-secret"}'

    class _Flow:
        @staticmethod
        def run_local_server(**_kwargs: object) -> _Credentials:
            return _Credentials()

    monkeypatch.setattr(oauth_flow, "CLIENT_SECRETS_FILE", str(credentials_path))
    monkeypatch.setattr(oauth_flow, "TOKEN_FILE", str(token_path))
    monkeypatch.setattr(
        oauth_flow.InstalledAppFlow,
        "from_client_secrets_file",
        lambda *_args, **_kwargs: _Flow(),
    )

    oauth_flow.run_oauth_flow()

    output = capsys.readouterr().out
    assert "refresh-token-must-stay-secret" not in output
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics required")
def test_oauth_refresh_restricts_existing_token_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_path = tmp_path / "token.json"
    token_path.write_text("{}", encoding="utf-8")
    token_path.chmod(0o644)

    class _Credentials:
        valid = False
        refresh_token = "refresh-secret"

        def refresh(self, _request: object) -> None:
            return None

        @staticmethod
        def to_json() -> str:
            return '{"refresh_token":"refresh-secret"}'

    monkeypatch.setattr(oauth_flow, "TOKEN_FILE", str(token_path))
    monkeypatch.setattr(
        oauth_flow.Credentials,
        "from_authorized_user_file",
        lambda *_args, **_kwargs: _Credentials(),
    )

    oauth_flow.check_token()

    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600
