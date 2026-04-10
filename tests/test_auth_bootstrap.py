from __future__ import annotations

from pathlib import Path

from collectors import daily_update_runner
from collectors.token_manager import DhanTokenManager


def test_token_manager_generates_when_missing(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("DHAN_CLIENT_ID=111\nDHAN_PIN=123456\nDHAN_TOTP=ABCDEFGHIJKLMNOPQRSTUV234567AB\n", encoding="utf-8")
    manager = DhanTokenManager(env_path=str(env_path))
    manager.access_token = ""

    manager.generate_token = lambda: {"status": "success", "access_token": "fresh-token"}

    assert manager.ensure_valid_token() == "fresh-token"
    assert manager.access_token == "fresh-token"


def test_token_manager_refreshes_when_expired(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "DHAN_CLIENT_ID=111\nDHAN_ACCESS_TOKEN=stale\nDHAN_PIN=123456\nDHAN_TOTP=ABCDEFGHIJKLMNOPQRSTUV234567AB\n",
        encoding="utf-8",
    )
    manager = DhanTokenManager(env_path=str(env_path))
    manager.is_token_expired = lambda: True
    manager.generate_token = lambda: {"status": "success", "access_token": "fresh-token"}

    assert manager.ensure_valid_token() == "fresh-token"
    assert manager.access_token == "fresh-token"


def test_token_manager_can_use_renew_when_enabled(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "DHAN_CLIENT_ID=111\nDHAN_ACCESS_TOKEN=stale\nDHAN_PIN=123456\nDHAN_TOTP=ABCDEFGHIJKLMNOPQRSTUV234567AB\nDHAN_ENABLE_RENEW_TOKEN=1\n",
        encoding="utf-8",
    )
    manager = DhanTokenManager(env_path=str(env_path))
    manager.is_token_expired = lambda: False
    manager.is_token_expiring_soon = lambda hours_before_expiry=1: True
    manager.renew_token = lambda: {"status": "success", "access_token": "renewed-token"}

    assert manager.ensure_valid_token() == "renewed-token"
    assert manager.access_token == "renewed-token"


def test_daily_update_runner_bootstraps_missing_token(monkeypatch) -> None:
    class DummyTokenManager:
        client_id = "1110836454"
        api_key = "api-key"

        def ensure_valid_token(self, hours_before_expiry: int = 1):
            return "bootstrapped-token"

    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.use_api = False
            self.dhan = None
            self.client_id = ""
            self.api_key = ""
            self.access_token = ""
            self.token_manager = DummyTokenManager()

        def _init_dhan_client(self) -> None:
            self.dhan = object()

        def _ensure_valid_token(self) -> bool:
            return True

        def run_daily_update_bulk(self, **kwargs):
            return {"symbols_updated": 0, "symbols_errors": 0, "duration_sec": 0.0}

    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)

    result = daily_update_runner.run(
        symbols_only=True,
        features_only=False,
        batch_size=10,
        bulk=True,
        data_domain="operational",
    )

    assert result["symbols_updated"] == 0
