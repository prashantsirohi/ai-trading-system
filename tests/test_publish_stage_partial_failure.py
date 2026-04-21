from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from analytics.registry import RegistryStore
from core.contracts import StageArtifact, StageContext
from run.publisher import PublisherDeliveryManager
from ai_trading_system.pipeline.contracts import PublishStageError
from run.stages.publish import PublishStage


def test_publish_stage_continues_other_channels_when_telegram_fails(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-10-publish-partial",
        run_date="2026-04-10",
        stage_name="publish",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational"},
    )

    rank_dir = tmp_path / "data" / "pipeline_runs" / context.run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    ranked_path = rank_dir / "ranked_signals.csv"
    breakout_path = rank_dir / "breakout_scan.csv"
    dashboard_path = rank_dir / "dashboard_payload.json"
    sector_path = rank_dir / "sector_dashboard.csv"

    pd.DataFrame([{"symbol_id": "AAA", "composite_score": 91.0, "close": 101.0}]).to_csv(ranked_path, index=False)
    pd.DataFrame([{"symbol_id": "AAA", "breakout_state": "qualified"}]).to_csv(breakout_path, index=False)
    pd.DataFrame([{"Sector": "Finance", "RS_rank": 1}]).to_csv(sector_path, index=False)
    dashboard_path.write_text(json.dumps({"summary": {"run_date": context.run_date}}), encoding="utf-8")

    context.artifacts = {
        "rank": {
            "ranked_signals": StageArtifact.from_file("ranked_signals", ranked_path, row_count=1, attempt_number=1),
            "breakout_scan": StageArtifact.from_file("breakout_scan", breakout_path, row_count=1, attempt_number=1),
            "sector_dashboard": StageArtifact.from_file("sector_dashboard", sector_path, row_count=1, attempt_number=1),
            "dashboard_payload": StageArtifact.from_file("dashboard_payload", dashboard_path, row_count=1, attempt_number=1),
        }
    }

    called_channels: list[str] = []

    def _telegram_fail(_context, _artifact, _datasets):
        called_channels.append("telegram_summary")
        raise RuntimeError("telegram offline")

    def _portfolio_ok(_context, _artifact, _datasets):
        called_channels.append("google_sheets_portfolio")
        return {"report_id": "portfolio-sheet"}

    def _dashboard_ok(_context, _artifact, _datasets):
        called_channels.append("google_sheets_dashboard")
        return {"report_id": "dashboard-sheet"}

    def _quantstats_ok(_context, _artifact, _datasets):
        called_channels.append("quantstats_dashboard_tearsheet")
        return {"report_id": "quantstats-sheet"}

    stage = PublishStage(
        channel_handlers={
            "google_sheets_portfolio": _portfolio_ok,
            "telegram_summary": _telegram_fail,
            "google_sheets_dashboard": _dashboard_ok,
            "quantstats_dashboard_tearsheet": _quantstats_ok,
        },
        delivery_manager=PublisherDeliveryManager(max_attempts=1, base_delay_seconds=0, sleep_fn=lambda _seconds: None),
    )

    with pytest.raises(PublishStageError) as exc_info:
        stage.run(context)

    assert "telegram_summary: telegram offline" in str(exc_info.value)
    assert called_channels == [
        "google_sheets_portfolio",
        "telegram_summary",
        "google_sheets_dashboard",
        "quantstats_dashboard_tearsheet",
    ]

    delivery_logs = registry.get_delivery_logs(context.run_id)
    assert any(log["channel"] == "google_sheets_portfolio" and log["status"] == "delivered" for log in delivery_logs)
    assert any(log["channel"] == "google_sheets_dashboard" and log["status"] == "delivered" for log in delivery_logs)
    assert any(log["channel"] == "quantstats_dashboard_tearsheet" and log["status"] == "delivered" for log in delivery_logs)
    assert any(log["channel"] == "telegram_summary" and log["status"] == "failed" for log in delivery_logs)
