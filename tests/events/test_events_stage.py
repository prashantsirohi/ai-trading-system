"""Tests for the EventsStage pipeline wrapper."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.pipeline.contracts import StageContext
from ai_trading_system.pipeline.stages.events import (
    EventsStage,
    EventsStageConfig,
)


# ---------------------------------------------------------------- fixtures


@dataclass
class _FakeEvent:
    symbol: str
    primary_category: str
    event_tier: str
    importance_score: float
    trust_score: float
    event_hash: str
    title: str = "test"


@dataclass
class _FakeBulkDeal:
    symbol: str
    trade_date: date
    exchange: str = "NSE"
    side: str = "BUY"
    quantity: int = 100_000
    avg_price: float = 1000.0
    deal_value_cr: float | None = 50.0
    is_block: bool = False
    client_name: str | None = "ICICI"
    deal_hash: str = "h1"
    bulk_deal_id: int = 1


class _FakeQuerier:
    def __init__(
        self,
        events: dict[str, list[_FakeEvent]] | None = None,
        bulk_deals: list[_FakeBulkDeal] | None = None,
    ):
        self._events = events or {}
        self._deals = bulk_deals or []

    def get_events_for_symbol(self, symbol, **kwargs):
        return list(self._events.get(symbol, []))

    def get_bulk_deals(self, **kwargs):
        return list(self._deals)


def _write_breakout_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["symbol", "tier", "score"])
        writer.writeheader()
        writer.writerows(rows)


def _make_context(
    *,
    project_root: Path,
    run_id: str = "r-test-1",
    run_date: str = "2026-05-01",
    breakout_path: Path | None = None,
) -> StageContext:
    from ai_trading_system.pipeline.contracts import StageArtifact

    artifacts: dict[str, dict[str, StageArtifact]] = {}
    if breakout_path is not None:
        artifacts["rank"] = {
            "breakout_scan": StageArtifact.from_file(
                "breakout_scan", breakout_path,
            ),
        }
    return StageContext(
        project_root=project_root,
        db_path=project_root / "data" / "operational" / "control_plane.duckdb",
        run_id=run_id,
        run_date=run_date,
        stage_name="events",
        attempt_number=1,
        params={},
        artifacts=artifacts,
    )


# ---------------------------------------------------------------- tests


def test_stage_disabled_short_circuits(tmp_path):
    stage = EventsStage(query_service_factory=lambda: _FakeQuerier())
    ctx = _make_context(project_root=tmp_path)
    ctx.params["events_enabled"] = False
    result = stage.run(ctx)
    assert result.metadata["events_enabled"] is False
    assert result.metadata["skipped"] is True
    assert result.artifacts == []


def test_stage_emits_empty_artifacts_when_no_triggers(tmp_path):
    stage = EventsStage(query_service_factory=lambda: _FakeQuerier())
    ctx = _make_context(project_root=tmp_path)
    result = stage.run(ctx)
    assert result.metadata["trigger_count"] == 0
    artifact_types = {a.artifact_type for a in result.artifacts}
    assert artifact_types == {
        "market_events_snapshot", "events_triggers", "events_enrichment", "events_summary",
    }
    # Empty enrichment payload should still be valid JSON
    enrich = next(a for a in result.artifacts if a.artifact_type == "events_enrichment")
    payload = json.loads(Path(enrich.uri).read_text())
    assert payload == {"signals": []}


def test_stage_enriches_breakout_triggers(tmp_path):
    breakout = tmp_path / "rank" / "breakout_scan.csv"
    _write_breakout_csv(breakout, [
        {"symbol": "RELIANCE", "tier": "A", "score": "85"},
        {"symbol": "TCS",      "tier": "B", "score": "70"},
        {"symbol": "INFY",     "tier": "C", "score": "50"},  # filtered out
    ])
    querier = _FakeQuerier(
        events={
            "RELIANCE": [
                _FakeEvent("RELIANCE", "capex_expansion", "A", 8.5, 95.0, "h-rel"),
            ],
        },
    )
    stage = EventsStage(query_service_factory=lambda: querier)
    ctx = _make_context(project_root=tmp_path, breakout_path=breakout)
    result = stage.run(ctx)

    assert result.metadata["trigger_count"] == 2  # only Tier A + B
    assert result.metadata["event_count"] == 1
    assert "snapshot_event_count" in result.metadata

    enrich = next(a for a in result.artifacts if a.artifact_type == "events_enrichment")
    payload = json.loads(Path(enrich.uri).read_text())
    rel = next(
        s for s in payload["signals"] if s["trigger"]["symbol"] == "RELIANCE"
    )
    assert rel["top_category"] == "capex_expansion"
    assert rel["event_count"] == 1
    assert rel["event_hashes"] == ["h-rel"]


def test_stage_consumes_bulk_deals_from_market_intel(tmp_path):
    querier = _FakeQuerier(
        events={
            "RELIANCE": [
                _FakeEvent("RELIANCE", "capex_expansion", "A", 8.5, 95.0, "h"),
            ],
        },
        bulk_deals=[
            _FakeBulkDeal(
                symbol="RELIANCE", trade_date=date(2026, 5, 1),
                deal_value_cr=120.0, client_name="ICICI Pru MF",
            ),
        ],
    )
    stage = EventsStage(query_service_factory=lambda: querier)
    ctx = _make_context(project_root=tmp_path)
    result = stage.run(ctx)
    triggers_csv = next(
        a for a in result.artifacts if a.artifact_type == "events_triggers"
    )
    rows = list(csv.DictReader(Path(triggers_csv.uri).open()))
    assert any(
        r["symbol"] == "RELIANCE" and r["trigger_type"] == "bulk_deal"
        for r in rows
    )


def test_stage_summary_counts_by_severity(tmp_path):
    breakout = tmp_path / "rank" / "breakout_scan.csv"
    _write_breakout_csv(breakout, [
        {"symbol": "REL", "tier": "A", "score": "85"},
    ])
    # No events for REL → breakout-with-no-events → low-info
    querier = _FakeQuerier()
    stage = EventsStage(query_service_factory=lambda: querier)
    ctx = _make_context(project_root=tmp_path, breakout_path=breakout)
    result = stage.run(ctx)

    summary_path = next(
        a for a in result.artifacts if a.artifact_type == "events_summary"
    )
    summary = json.loads(Path(summary_path.uri).read_text())
    assert summary["by_severity"].get("low-info", 0) >= 1


def test_stage_tolerates_market_intel_outage(tmp_path):
    breakout = tmp_path / "rank" / "breakout_scan.csv"
    _write_breakout_csv(breakout, [
        {"symbol": "REL", "tier": "A", "score": "85"},
    ])

    def _broken_factory():
        raise RuntimeError("collector down")

    stage = EventsStage(query_service_factory=_broken_factory)
    ctx = _make_context(project_root=tmp_path, breakout_path=breakout)
    # Should not raise; emits artifacts with empty event lists
    result = stage.run(ctx)
    enrich = next(a for a in result.artifacts if a.artifact_type == "events_enrichment")
    payload = json.loads(Path(enrich.uri).read_text())
    # The breakout trigger still flows; its enrichment is empty
    assert any(s["trigger"]["symbol"] == "REL" for s in payload["signals"])
    rel = next(s for s in payload["signals"] if s["trigger"]["symbol"] == "REL")
    assert rel["events"] == []


def test_stage_in_orchestrator_pipeline_order():
    """Regression: EventsStage must be registered between rank and execute."""
    from ai_trading_system.pipeline import orchestrator

    assert "events" in orchestrator.PIPELINE_ORDER
    rank_idx = orchestrator.PIPELINE_ORDER.index("rank")
    events_idx = orchestrator.PIPELINE_ORDER.index("events")
    execute_idx = orchestrator.PIPELINE_ORDER.index("execute")
    assert rank_idx < events_idx < execute_idx
    assert "events" in orchestrator.SUPPORTED_STAGES


def test_orchestrator_cli_default_includes_events():
    from ai_trading_system.pipeline.orchestrator import build_parser

    args = build_parser().parse_args([])
    assert "events" in args.stages.split(",")
