from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.domains.publish.channels import google_sheets_manager
from ai_trading_system.domains.publish.channels.google_sheets_manager import (
    GoogleSheetsManager,
    GoogleSheetsQuotaLimitedError,
)
from ai_trading_system.domains.publish.delivery_manager import PublisherDeliveryManager
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.publish import PublishStage


class _StatusError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.resp = type("Resp", (), {"status": status})()


def _bare_manager() -> GoogleSheetsManager:
    manager = GoogleSheetsManager.__new__(GoogleSheetsManager)
    manager.max_retries = 3
    manager.max_backoff_seconds = 64.0
    manager.write_interval_seconds = 0.0
    manager.requests_attempted = 0
    manager.rows_written = 0
    manager.quota_limited = False
    manager.retry_recommended_after_seconds = None
    manager.last_retryable_error = None
    manager.last_error = None
    manager.spreadsheet = None
    manager.client = None
    return manager


def test_quota_error_triggers_exponential_backoff_and_retry(monkeypatch) -> None:
    manager = _bare_manager()
    sleeps: list[float] = []
    attempts = {"count": 0}

    monkeypatch.setattr(google_sheets_manager.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(google_sheets_manager.random, "uniform", lambda _start, _end: 0.0)

    def _flaky_call():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise _StatusError(429, "RESOURCE_EXHAUSTED quota exceeded")
        return "ok"

    assert manager._execute_with_backoff(_flaky_call, is_write=True) == "ok"
    assert attempts["count"] == 3
    assert sleeps == [1.0, 2.0]
    assert manager.requests_attempted == 3
    assert manager.quota_limited is True


def test_quota_error_after_max_retries_is_non_blocking_retryable_publish_failure(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-10-sheets-quota",
        run_date="2026-04-10",
        stage_name="publish",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational"},
    )
    rank_dir = tmp_path / "data" / "pipeline_runs" / context.run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    ranked_path = rank_dir / "ranked_signals.csv"
    pd.DataFrame([{"symbol_id": "AAA", "composite_score": 91.0, "close": 101.0}]).to_csv(ranked_path, index=False)
    context.artifacts = {
        "rank": {
            "ranked_signals": StageArtifact.from_file("ranked_signals", ranked_path, row_count=1, attempt_number=1),
        }
    }

    def _sheets_quota_fail(_context, _artifact, _datasets):
        raise GoogleSheetsQuotaLimitedError("429 RESOURCE_EXHAUSTED quota exceeded")

    stage = PublishStage(
        channel_handlers={"google_sheets_dashboard": _sheets_quota_fail},
        delivery_manager=PublisherDeliveryManager(max_attempts=1, base_delay_seconds=0, sleep_fn=lambda _seconds: None),
    )

    result = stage.run(context)
    metadata = result.metadata

    assert metadata["google_sheets_quota_limited"] is True
    assert metadata["sheets_requests_attempted"] == 0
    assert metadata["sheets_rows_written"] == 0
    assert "google_sheets_errors" in metadata
    assert "quota exceeded" in metadata["google_sheets_errors"][0]
    target = metadata["targets"][0]
    assert target["channel"] == "google_sheets_dashboard"
    assert target["status"] == "retry_later"
    assert target["retryable"] is True
    assert any("google_sheets_dashboard" in msg for msg in metadata["non_blocking_failures"])
