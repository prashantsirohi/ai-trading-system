from __future__ import annotations

from datetime import datetime

from run import daily_pipeline


class _FakeDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls(2026, 4, 8, 9, 30, 0)


def test_daily_pipeline_defaults_validation_date_to_yesterday(monkeypatch):
    captured: dict = {}

    class FakeOrchestrator:
        def __init__(self, project_root):
            self.project_root = project_root

        def run_pipeline(self, **kwargs):
            captured.update(kwargs)
            return {"run_id": "run-1", "status": "completed", "stages": []}

    monkeypatch.setattr(daily_pipeline, "PipelineOrchestrator", FakeOrchestrator)
    monkeypatch.setattr(daily_pipeline, "datetime", _FakeDateTime)
    monkeypatch.setattr(daily_pipeline, "should_truncate_data", lambda data_domain=None: False)
    monkeypatch.setattr(daily_pipeline, "is_trading_holiday", lambda _now=None: False)
    monkeypatch.setattr(daily_pipeline, "is_weekend", lambda _now=None: False)

    daily_pipeline.main(force=True, stages="ingest")

    assert captured["params"]["bhavcopy_validation_date"] == "2026-04-07"
    assert captured["params"]["bhavcopy_validation_source"] == "bhavcopy"
    assert captured["params"]["nse_primary"] is True
    assert captured["params"]["breakout_engine"] == "v2"
    assert captured["params"]["breakout_symbol_near_high_max_pct"] == 15.0
    assert captured["params"]["breakout_symbol_trend_gate_enabled"] is True
    assert captured["params"]["execution_breakout_linkage"] == "off"


def test_daily_pipeline_keeps_explicit_validation_date(monkeypatch):
    captured: dict = {}

    class FakeOrchestrator:
        def __init__(self, project_root):
            self.project_root = project_root

        def run_pipeline(self, **kwargs):
            captured.update(kwargs)
            return {"run_id": "run-2", "status": "completed", "stages": []}

    monkeypatch.setattr(daily_pipeline, "PipelineOrchestrator", FakeOrchestrator)
    monkeypatch.setattr(daily_pipeline, "datetime", _FakeDateTime)
    monkeypatch.setattr(daily_pipeline, "should_truncate_data", lambda data_domain=None: False)
    monkeypatch.setattr(daily_pipeline, "is_trading_holiday", lambda _now=None: False)
    monkeypatch.setattr(daily_pipeline, "is_weekend", lambda _now=None: False)

    daily_pipeline.main(
        force=True,
        stages="ingest",
        bhavcopy_validation_date="2026-04-05",
        bhavcopy_validation_source="yfinance",
    )

    assert captured["params"]["bhavcopy_validation_date"] == "2026-04-05"
    assert captured["params"]["bhavcopy_validation_source"] == "yfinance"
    assert captured["params"]["nse_primary"] is True
    assert captured["params"]["breakout_engine"] == "v2"
    assert captured["params"]["breakout_symbol_near_high_max_pct"] == 15.0
    assert captured["params"]["breakout_symbol_trend_gate_enabled"] is True
    assert captured["params"]["execution_breakout_linkage"] == "off"
