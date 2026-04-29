from __future__ import annotations

import json
from datetime import datetime

from ai_trading_system.pipeline import daily_pipeline


def test_parse_portfolio_sheet_positions_skips_generated_summary_rows():
    values = [
        ["Symbol", "Qty", "Avg Price"],
        ["RELIANCE", "50", "2500"],
        ["Total Value", "125000", ""],
        ["Cash", "10000", "0"],
        ["Total P&L", "5000", "(4.00%)"],
        ["Positions", "1", ""],
        ["bad symbol", "10", "20"],
        ["TCS", "30", "3,500"],
    ]

    positions = daily_pipeline._parse_portfolio_sheet_positions(values)

    assert positions == [
        {"Symbol": "RELIANCE", "Qty": 50.0, "Avg Price": 2500.0},
        {"Symbol": "TCS", "Qty": 30.0, "Avg Price": 3500.0},
    ]


class _FakeDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls(2026, 4, 8, 9, 30, 0)


class _FakeAfterCutoffDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls(2026, 4, 8, 18, 5, 0)


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
    assert captured["run_date"] == "2026-04-07"
    assert captured["run_id"] == "pipeline-2026-04-07-daily"
    assert captured["params"]["bhavcopy_validation_source"] == "bhavcopy"
    assert captured["params"]["nse_primary"] is True
    assert captured["params"]["breakout_engine"] == "v2"
    assert captured["params"]["breakout_symbol_near_high_max_pct"] == 15.0
    assert captured["params"]["breakout_symbol_trend_gate_enabled"] is True
    assert captured["params"]["execution_breakout_linkage"] == "off"


def test_daily_pipeline_defaults_validation_date_to_today_after_cutoff(monkeypatch):
    captured: dict = {}

    class FakeOrchestrator:
        def __init__(self, project_root):
            self.project_root = project_root

        def run_pipeline(self, **kwargs):
            captured.update(kwargs)
            return {"run_id": "run-1", "status": "completed", "stages": []}

    monkeypatch.setattr(daily_pipeline, "PipelineOrchestrator", FakeOrchestrator)
    monkeypatch.setattr(daily_pipeline, "datetime", _FakeAfterCutoffDateTime)
    monkeypatch.setattr(daily_pipeline, "should_truncate_data", lambda data_domain=None: False)
    monkeypatch.setattr(daily_pipeline, "is_trading_holiday", lambda _now=None: False)
    monkeypatch.setattr(daily_pipeline, "is_weekend", lambda _now=None: False)

    daily_pipeline.main(force=True, stages="ingest")

    assert captured["run_date"] == "2026-04-08"
    assert captured["params"]["bhavcopy_validation_date"] == "2026-04-08"


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
    assert captured["run_date"] == "2026-04-07"
    assert captured["params"]["bhavcopy_validation_source"] == "yfinance"
    assert captured["params"]["nse_primary"] is True
    assert captured["params"]["breakout_engine"] == "v2"
    assert captured["params"]["breakout_symbol_near_high_max_pct"] == 15.0
    assert captured["params"]["breakout_symbol_trend_gate_enabled"] is True
    assert captured["params"]["execution_breakout_linkage"] == "off"


def test_daily_pipeline_reuses_existing_completed_run_and_skips(monkeypatch):
    run_calls: list[dict] = []

    class FakeReader:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, *_args, **_kwargs):
            return self

        def fetchall(self):
            metadata = {"params": {"data_domain": "operational", "canary": False}}
            return [("pipeline-2026-04-07-existing", json.dumps(metadata))]

    class FakeRegistry:
        def _reader(self):
            return FakeReader()

        def _loads(self, payload):
            return json.loads(payload)

        def get_stage_runs(self, run_id):
            return [
                {"stage_name": "ingest", "attempt_number": 1, "status": "completed"},
                {"stage_name": "features", "attempt_number": 1, "status": "completed"},
            ]

        def get_run(self, run_id):
            return {"run_id": run_id, "status": "completed", "metadata": {}}

    class FakeOrchestrator:
        def __init__(self, project_root):
            self.project_root = project_root
            self.registry = FakeRegistry()

        def run_pipeline(self, **kwargs):
            run_calls.append(kwargs)
            return {"run_id": kwargs["run_id"], "status": "completed", "stages": []}

    monkeypatch.setattr(daily_pipeline, "PipelineOrchestrator", FakeOrchestrator)
    monkeypatch.setattr(daily_pipeline, "datetime", _FakeDateTime)
    monkeypatch.setattr(daily_pipeline, "should_truncate_data", lambda data_domain=None: False)
    monkeypatch.setattr(daily_pipeline, "is_trading_holiday", lambda _now=None: False)
    monkeypatch.setattr(daily_pipeline, "is_weekend", lambda _now=None: False)

    result = daily_pipeline.main(force=True, stages="ingest,features")

    assert result["run_id"] == "pipeline-2026-04-07-existing"
    assert run_calls == []
