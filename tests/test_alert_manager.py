from __future__ import annotations

from run.alerts import AlertManager


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
    registry = _DummyRegistry()
    manager = AlertManager(registry)
    captured = {}

    def _fake_post(url, json, timeout):  # noqa: ANN001
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return object()

    monkeypatch.setattr("run.alerts.requests.post", _fake_post)

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

