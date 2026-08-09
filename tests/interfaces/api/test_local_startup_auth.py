from __future__ import annotations

import pytest

from ai_trading_system.interfaces.api.config import ApiSettings
from ai_trading_system.interfaces.cli.serve_phase4_api import (
    _configure_startup_auth,
)


def test_loopback_startup_enables_local_development_without_key() -> None:
    settings = _configure_startup_auth(ApiSettings(api_key=None), "127.0.0.1")

    assert settings.local_dev_mode is True
    assert settings.auth_configured() is True


def test_explicit_key_keeps_authentication_enabled() -> None:
    original = ApiSettings(api_key="configured-key")

    assert _configure_startup_auth(original, "127.0.0.1") is original
    assert original.local_dev_mode is False


def test_non_loopback_startup_requires_explicit_key() -> None:
    with pytest.raises(SystemExit, match="PHASE4_API_KEY is required"):
        _configure_startup_auth(ApiSettings(api_key=None), "0.0.0.0")
