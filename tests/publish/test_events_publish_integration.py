from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.domains.publish.delivery_manager import PublisherDeliveryManager
from ai_trading_system.domains.publish.telegram_summary_builder import build_telegram_summary
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.publish import PublishStage


def _context(tmp_path: Path) -> StageContext:
    events_dir = tmp_path / "data" / "operational" / "pipeline_runs" / "r1" / "events" / "attempt_1"
    events_dir.mkdir(parents=True)
    snapshot = events_dir / "market_events_snapshot.json"
    snapshot.write_text(
        json.dumps(
            {
                "market_intel_status": "ok",
                "events": [
                    {
                        "symbol": "RELIANCE",
                        "category": "capex_expansion",
                        "tier": "A",
                        "importance_score": 8.5,
                        "materiality_label": "high",
                        "event_hash": "h-snap",
                        "title": "Capex announced",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    enrichment = events_dir / "events_enrichment.json"
    enrichment.write_text(
        json.dumps(
            {
                "signals": [
                    {
                        "trigger": {"symbol": "RELIANCE", "trigger_type": "breakout"},
                        "event_hashes": ["h-enrich"],
                        "event_count": 1,
                        "severity": "high",
                        "top_category": "capex_expansion",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    summary = events_dir / "events_summary.json"
    summary.write_text(json.dumps({"market_intel_status": "ok"}), encoding="utf-8")
    return StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "control.duckdb",
        run_id="r1",
        run_date="2026-05-01",
        stage_name="publish",
        attempt_number=1,
        artifacts={
            "events": {
                "market_events_snapshot": StageArtifact.from_file("market_events_snapshot", snapshot),
                "events_enrichment": StageArtifact.from_file("events_enrichment", enrichment),
                "events_summary": StageArtifact.from_file("events_summary", summary),
            }
        },
    )


def test_publish_stage_loads_events_and_event_hashes(tmp_path):
    stage = PublishStage(operation=lambda ctx: {})
    datasets = {"dashboard_payload": {"ranked_leaders": [{"symbol_id": "RELIANCE"}]}}
    stage._attach_event_datasets(_context(tmp_path), datasets)
    assert datasets["event_hashes"] == ["h-enrich", "h-snap"]
    assert datasets["dashboard_payload"]["events_index"]
    assert datasets["dashboard_payload"]["ranked_leaders"][0]["events"]


def test_telegram_summary_includes_important_events():
    text = build_telegram_summary(
        run_date="2026-05-01",
        datasets={
            "dashboard_payload": {"summary": {"run_date": "2026-05-01"}},
            "market_events_snapshot": {
                "events": [
                    {
                        "symbol": "RELIANCE",
                        "category": "capex_expansion",
                        "tier": "A",
                        "importance_score": 8.5,
                        "materiality_label": "high",
                        "title": "Capex announced",
                    }
                ]
            },
        },
    )
    assert "Important Events" in text
    assert "RELIANCE" in text


def test_dedupe_key_includes_event_hashes(tmp_path):
    path = tmp_path / "ranked.csv"
    path.write_text("symbol_id\nRELIANCE\n", encoding="utf-8")
    manager = PublisherDeliveryManager()
    base = StageArtifact.from_file("ranked_signals", path)
    with_events = StageArtifact.from_file(
        "ranked_signals",
        path,
        metadata={"event_hashes": ["h1"]},
    )
    with_other_events = StageArtifact.from_file(
        "ranked_signals",
        path,
        metadata={"event_hashes": ["h2"]},
    )
    assert manager.build_dedupe_key("telegram", base) != manager.build_dedupe_key("telegram", with_events)
    assert manager.build_dedupe_key("telegram", with_events) != manager.build_dedupe_key("telegram", with_other_events)
