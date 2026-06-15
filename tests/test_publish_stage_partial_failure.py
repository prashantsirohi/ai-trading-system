from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.domains.publish.delivery_manager import PublisherDeliveryManager
from ai_trading_system.pipeline.contracts import PublishStageError
from ai_trading_system.pipeline.stages.publish import PublishStage
from ai_trading_system.pipeline.orchestrator import build_parser


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


def test_publish_stage_does_not_raise_when_optional_channel_fails(tmp_path: Path) -> None:
    """publish_optional channels (currently google_sheets_portfolio) record
    failure in metadata but must NOT raise PublishStageError, so the rest of
    the publish stage (telegram digest) and downstream stages (perf_tracker)
    keep running when the live YF/Sheets call flakes."""
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-10-publish-optional-fail",
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
            "ranked_signals": StageArtifact.from_file(
                "ranked_signals", ranked_path, row_count=1, attempt_number=1
            ),
        }
    }

    called_channels: list[str] = []

    def _portfolio_fail(_context, _artifact, _datasets):
        called_channels.append("google_sheets_portfolio")
        raise RuntimeError("YF rate limit exceeded")

    def _telegram_ok(_context, _artifact, _datasets):
        called_channels.append("telegram_summary")
        return {"report_id": "telegram-digest"}

    stage = PublishStage(
        channel_handlers={
            "google_sheets_portfolio": _portfolio_fail,
            "telegram_summary": _telegram_ok,
        },
        delivery_manager=PublisherDeliveryManager(
            max_attempts=1, base_delay_seconds=0, sleep_fn=lambda _seconds: None
        ),
    )

    # Sanity: the role classification is what makes this test meaningful.
    assert stage.CHANNEL_ROLES["google_sheets_portfolio"] == "publish_optional"

    # The key behavior change: publish stage completes successfully.
    result = stage.run(context)

    # Both channels were called (portfolio failure didn't short-circuit).
    assert "google_sheets_portfolio" in called_channels
    assert "telegram_summary" in called_channels

    # Failure is recorded in metadata for operator visibility but did not raise.
    metadata = result.metadata
    assert "non_blocking_failures" in metadata
    assert any(
        "google_sheets_portfolio" in msg and "YF rate limit" in msg
        for msg in metadata["non_blocking_failures"]
    )
    # And NOT in the blocking failures list.
    assert "failures" not in metadata or not any(
        "google_sheets_portfolio" in msg for msg in metadata.get("failures", [])
    )

    # Delivery log records the failure with the publish_optional role tag.
    delivery_logs = registry.get_delivery_logs(context.run_id)
    portfolio_log = next(
        log for log in delivery_logs if log["channel"] == "google_sheets_portfolio"
    )
    assert portfolio_log["status"] == "failed"
    telegram_log = next(
        log for log in delivery_logs if log["channel"] == "telegram_summary"
    )
    assert telegram_log["status"] == "delivered"


def test_publish_run_log_appends_one_row_per_channel(monkeypatch, tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-10-run-log",
        run_date="2026-04-10",
        stage_name="publish",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational"},
    )
    artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri=str(tmp_path / "ranked_signals.csv"),
        content_hash="rank-hash",
        row_count=1,
    )
    captured_rows: list[dict[str, object]] = []

    def _capture_run_log(rows):
        captured_rows.extend(rows)
        return True

    monkeypatch.setattr(
        "ai_trading_system.domains.publish.channels.google_sheets.publish_run_log_sheet",
        _capture_run_log,
    )

    failure = PublishStage()._publish_run_log(
        context,
        artifact,
        [
            {
                "channel": "google_sheets_dashboard",
                "delivery_role": "publish_optional",
                "status": "delivered",
                "dedupe_key": "dash-key",
                "attempt_number": 1,
            },
            {
                "channel": "telegram_summary",
                "delivery_role": "informational",
                "status": "failed",
                "dedupe_key": "telegram-key",
                "attempt_number": 1,
                "error_message": "telegram offline",
            },
        ],
    )

    assert failure is None
    assert [row["channel"] for row in captured_rows] == ["google_sheets_dashboard", "telegram_summary"]
    assert captured_rows[0]["run_id"] == context.run_id
    assert captured_rows[0]["run_date"] == context.run_date
    assert captured_rows[0]["status"] == "delivered"
    assert captured_rows[0]["delivery_role"] == "publish_optional"
    assert captured_rows[0]["artifact_hash"] == "rank-hash"
    assert captured_rows[0]["dedupe_key"] == "dash-key"
    assert captured_rows[1]["status"] == "failed"
    assert captured_rows[1]["error_message"] == "telegram offline"


def test_publish_stage_does_not_register_legacy_investigator_tabs(tmp_path: Path) -> None:
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-10-handlers",
        run_date="2026-04-10",
        stage_name="publish",
        attempt_number=1,
        registry=RegistryStore(tmp_path),
        params={"data_domain": "operational"},
    )
    handlers = PublishStage()._build_handlers(
        context,
        {
            "ranked_signals": pd.DataFrame([{"symbol_id": "AAA", "composite_score": 90.0}]),
            "dashboard_payload": {"summary": {"run_date": "2026-04-10"}},
            "investigator_scores": pd.DataFrame([{"symbol_id": "AAA", "final_score": 90.0}]),
        },
    )

    assert "google_sheets_dashboard" in handlers
    assert "google_sheets_investigator" not in handlers


def test_orchestrator_accepts_bypass_dedupe_channels_flag() -> None:
    args = build_parser().parse_args(
        [
            "--stages",
            "publish",
            "--bypass-dedupe-channels",
            "google_sheets_dashboard,google_sheets_watchlist",
        ]
    )

    assert args.bypass_dedupe_channels == "google_sheets_dashboard,google_sheets_watchlist"
