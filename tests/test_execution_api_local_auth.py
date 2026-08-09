from __future__ import annotations

import os

import pytest

from ai_trading_system.ui.execution_api.app import (
    LOCAL_DEVELOPMENT_API_KEY,
    _configure_startup_api_key,
    build_parser,
)


def test_execution_api_defaults_to_loopback() -> None:
    assert build_parser().parse_args([]).host == "127.0.0.1"


def test_loopback_startup_supplies_internal_development_handshake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EXECUTION_API_KEY", raising=False)
    _configure_startup_api_key("127.0.0.1")
    assert os.environ["EXECUTION_API_KEY"] == LOCAL_DEVELOPMENT_API_KEY


def test_non_loopback_startup_requires_explicit_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EXECUTION_API_KEY", raising=False)
    with pytest.raises(SystemExit, match="required"):
        _configure_startup_api_key("0.0.0.0")
