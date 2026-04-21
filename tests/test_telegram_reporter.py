from __future__ import annotations

import asyncio
import socket
import ssl
from pathlib import Path

import pytest

from ai_trading_system.domains.publish.channels.telegram import TelegramReporter


class _DummyBot:
    def __init__(self, send_message_impl):
        self._send_message_impl = send_message_impl

    async def send_message(self, **kwargs):
        await self._send_message_impl(**kwargs)


def test_send_message_fails_fast_on_dns_precheck(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("TELEGRAM_DNS_PRECHECK_ENABLED", "1")
    reporter = TelegramReporter(bot_token="token", chat_id="chat", report_dir=tmp_path)
    calls = {"send": 0}

    async def _send_message(**kwargs):
        calls["send"] += 1

    reporter.bot = _DummyBot(_send_message)

    def _dns_fail(_host, _port):
        raise socket.gaierror("nodename nor servname provided")

    monkeypatch.setattr("ai_trading_system.domains.publish.channels.telegram.socket.getaddrinfo", _dns_fail)

    assert reporter.send_message("test") is False
    assert calls["send"] == 0
    assert reporter.last_error_code == "telegram_dns_failure"
    assert "Unable to resolve api.telegram.org" in str(reporter.last_error)
    assert reporter.last_health_check == {
        "status": "failed",
        "kind": "telegram_dns_failure",
        "detail": "Unable to resolve api.telegram.org: nodename nor servname provided",
    }


def test_send_message_classifies_ssl_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    reporter = TelegramReporter(bot_token="token", chat_id="chat", report_dir=tmp_path)

    async def _send_message(**kwargs):
        raise ssl.SSLError("RECORD_LAYER_FAILURE")

    reporter.bot = _DummyBot(_send_message)
    monkeypatch.setattr("ai_trading_system.domains.publish.channels.telegram.socket.getaddrinfo", lambda _host, _port: [object()])

    assert reporter.send_message("test") is False
    assert reporter.last_error_code == "telegram_ssl_failure"
    assert "Telegram SSL failure" in str(reporter.last_error)


def test_send_message_retries_when_configured(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("TELEGRAM_SEND_ATTEMPTS", "2")
    reporter = TelegramReporter(bot_token="token", chat_id="chat", report_dir=tmp_path)
    attempts = {"count": 0}

    async def _send_message(**kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise asyncio.TimeoutError("timed out")

    reporter.bot = _DummyBot(_send_message)
    monkeypatch.setattr("ai_trading_system.domains.publish.channels.telegram.socket.getaddrinfo", lambda _host, _port: [object()])

    assert reporter.send_message("test") is True
    assert attempts["count"] == 2
    assert reporter.last_error is None
    assert reporter.last_error_code is None
