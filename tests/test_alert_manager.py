from __future__ import annotations

from ai_trading_system.pipeline.alerts import AlertManager


class _DummyRegistry:
    def __init__(self) -> None:
        self.calls = []

    def record_alert(
        self,
        *,
        run_id: str,
        alert_type: str,
        severity: str,
        stage_name: str | None,
        message: str,
    ) -> None:
        self.calls.append(
            {
                "run_id": run_id,
                "alert_type": alert_type,
                "severity": severity,
                "stage_name": stage_name,
                "message": message,
            }
        )


def test_alert_manager_records_alert_without_telegram_env(monkeypatch) -> None:
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    registry = _DummyRegistry()
    manager = AlertManager(registry)

    manager.emit(
        run_id="pipeline-2026-04-17",
        alert_type="dq_failure",
        severity="critical",
        message="critical DQ failure",
        stage_name="ingest",
    )

    assert len(registry.calls) == 1
    assert registry.calls[0]["severity"] == "critical"


def test_alert_manager_fans_out_telegram_when_env_present(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    # Opt-in to telegram fan-out at warning level (default is now disabled).
    monkeypatch.setenv("ALERT_TELEGRAM_MIN_SEVERITY", "warning")
    registry = _DummyRegistry()
    manager = AlertManager(registry)
    captured = {}

    def _fake_post(url, json, timeout):  # noqa: ANN001
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return object()

    monkeypatch.setattr("ai_trading_system.pipeline.alerts.requests.post", _fake_post)

    manager.emit(
        run_id="pipeline-2026-04-17",
        alert_type="publish_failure",
        severity="warning",
        message="telegram test",
        stage_name="publish",
    )

    assert len(registry.calls) == 1
    assert captured["url"].startswith("https://api.telegram.org/bottoken/sendMessage")
    assert captured["json"]["chat_id"] == "12345"
    assert "telegram test" in captured["json"]["text"]
    assert captured["timeout"] == 10


def test_alert_manager_skips_telegram_by_default(monkeypatch) -> None:
    """Default behavior: telegram fan-out is disabled even with env vars set.

    The alert is still logged and persisted; only the side-channel notification
    is suppressed. This stops noisy DQ-warning days from spamming the chat.
    """
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.delenv("ALERT_TELEGRAM_MIN_SEVERITY", raising=False)
    registry = _DummyRegistry()
    manager = AlertManager(registry)
    posts: list[dict] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posts.append({"url": url, "json": json, "timeout": timeout})
        return object()

    monkeypatch.setattr("ai_trading_system.pipeline.alerts.requests.post", _fake_post)

    manager.emit(
        run_id="pipeline-2026-04-17",
        alert_type="dq_warning",
        severity="warning",
        message="should not telegram",
        stage_name="ingest",
    )

    # Alert recorded for operator follow-up, but no telegram POST happened.
    assert len(registry.calls) == 1
    assert posts == []


def test_alert_manager_telegram_severity_threshold_drops_warnings(monkeypatch) -> None:
    """When threshold is critical, warnings are dropped but criticals fire."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setenv("ALERT_TELEGRAM_MIN_SEVERITY", "critical")
    registry = _DummyRegistry()
    manager = AlertManager(registry)
    posts: list[dict] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posts.append({"url": url, "json": json, "timeout": timeout})
        return object()

    monkeypatch.setattr("ai_trading_system.pipeline.alerts.requests.post", _fake_post)

    manager.emit(
        run_id="run-1", alert_type="dq_warning", severity="warning",
        message="below threshold", stage_name="ingest",
    )
    manager.emit(
        run_id="run-1", alert_type="stage_failure", severity="critical",
        message="above threshold", stage_name="rank",
    )

    assert len(registry.calls) == 2  # both recorded
    assert len(posts) == 1            # only critical fanned out
    assert "above threshold" in posts[0]["json"]["text"]
