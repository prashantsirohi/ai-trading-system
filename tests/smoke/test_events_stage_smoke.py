"""End-to-end smoke test for the events pipeline stage.

Stands up a real ``market_intel`` DuckDB, seeds it with a tracked entity,
a resolved corporate-action event, and a bulk deal, then runs the full
EventsStage pipeline (trigger collection + enrichment + noise filter +
artifact writing) and asserts the output artifacts contain the expected
enriched-signal payload.

This is the closest the host repo gets to integration testing without
spinning up the live collector process.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from ai_trading_system.integrations import market_intel_client
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.events import EventsStage


@pytest.fixture(autouse=True)
def _reset_client_cache():
    market_intel_client.reset_cache()
    yield
    market_intel_client.reset_cache()


def _seed_market_intel_db(db_path: str) -> None:
    """Initialize schema and write one capex event + one bulk deal for RELIANCE."""
    from market_intel.storage.db import Database

    Database(db_path=db_path)  # creates schema

    conn = duckdb.connect(db_path)
    try:
        # tracked_entity
        conn.execute(
            """
            INSERT INTO tracked_entity (symbol, company_name, sector, market_cap_cr)
            VALUES (?, ?, ?, ?)
            """,
            ["RELIANCE", "Reliance Industries Limited", "Energy", 1_500_000.0],
        )
        # raw_event for the capex announcement
        conn.execute(
            """
            INSERT INTO raw_event (
                source, source_type, symbol, company_name, title, description,
                event_date, published_at, link, raw_payload_json, event_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "nse_rss", "rss", "RELIANCE", "Reliance Industries Limited",
                "Capex announcement: Rs. 15,000 crore Jamnagar PCG expansion",
                "Reliance announces Rs. 15,000 crore capex for Jamnagar.",
                datetime(2026, 4, 28, 10, 0),
                datetime(2026, 4, 28, 10, 0),
                "https://example.com/r1",
                "{}", "hash-rel-capex-1",
            ],
        )
        raw_id = conn.execute(
            "SELECT raw_event_id FROM raw_event WHERE event_hash = ?",
            ["hash-rel-capex-1"],
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO resolved_event (
                raw_event_id, primary_category, importance_score, trust_score,
                alert_level, is_official, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [raw_id, "capex_expansion", 8.5, 95.0, "critical", True, "resolved"],
        )
        # Bulk deal on RELIANCE: ICICI Pru MF buying ₹120Cr
        conn.execute(
            """
            INSERT INTO bulk_deal (
                trade_date, symbol, exchange, client_name, side,
                quantity, avg_price, deal_value_cr, is_block, deal_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "2026-04-30", "RELIANCE", "NSE", "ICICI Pru MF", "BUY",
                500_000, 2400.0, 120.0, False, "bulk-rel-001",
            ],
        )
    finally:
        conn.close()


def _write_breakout_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["symbol", "tier", "score"],
        )
        writer.writeheader()
        writer.writerow({"symbol": "RELIANCE", "tier": "A", "score": "85"})


def test_events_stage_end_to_end(tmp_path, monkeypatch):
    # 1. Seed a real market_intel DuckDB
    mi_db = tmp_path / "market_intel.duckdb"
    _seed_market_intel_db(str(mi_db))

    # Point the client at this DB
    monkeypatch.setenv("AI_TRADING_MARKET_INTEL_DB", str(mi_db))

    # 2. Set up a breakout artifact for the rank stage
    project_root = tmp_path / "project"
    project_root.mkdir()
    breakout_csv = project_root / "data" / "operational" / "pipeline_runs" \
        / "r-test-1" / "rank" / "attempt_1" / "breakout_scan.csv"
    _write_breakout_csv(breakout_csv)

    # 3. Build a StageContext that points at the rank artifact
    ctx = StageContext(
        project_root=project_root,
        db_path=project_root / "data" / "operational" / "control_plane.duckdb",
        run_id="r-test-1",
        run_date="2026-05-01",
        stage_name="events",
        attempt_number=1,
        params={},
        artifacts={
            "rank": {
                "breakout_scan": StageArtifact.from_file(
                    "breakout_scan", breakout_csv,
                ),
            },
        },
    )

    # 4. Run the events stage with the default chain (no overrides)
    stage = EventsStage()
    result = stage.run(ctx)

    # 5. Verify artifacts
    assert result.metadata["trigger_count"] >= 2  # bulk_deal + breakout
    assert result.metadata["event_count"] >= 1
    artifact_types = {a.artifact_type for a in result.artifacts}
    assert artifact_types == {
        "events_triggers", "events_enrichment", "events_summary",
    }

    # Inspect the enrichment payload
    enrich_path = next(
        a.uri for a in result.artifacts if a.artifact_type == "events_enrichment"
    )
    payload = json.loads(Path(enrich_path).read_text())

    # The breakout trigger for RELIANCE should have at least the capex event
    rel_signals = [
        s for s in payload["signals"]
        if s["trigger"]["symbol"] == "RELIANCE"
    ]
    assert rel_signals, "Expected at least one signal for RELIANCE"

    # At least one signal should have the capex event attached
    sig_with_event = next(
        (s for s in rel_signals if s["event_count"] >= 1),
        None,
    )
    assert sig_with_event is not None, (
        "Expected at least one RELIANCE signal to have the capex event "
        f"attached; got: {rel_signals}"
    )
    assert sig_with_event["top_category"] == "capex_expansion"
    # Severity is high (volume_shock+TierA OR breakout+TierA event)
    assert sig_with_event["severity"] in ("high", "medium"), \
        f"Unexpected severity: {sig_with_event['severity']}"
    # event_hashes referencing market_intel
    assert "hash-rel-capex-1" in sig_with_event["event_hashes"]

    # 6. Verify the bulk-deal trigger is also present
    trigger_types = {s["trigger"]["trigger_type"] for s in payload["signals"]}
    assert "bulk_deal" in trigger_types
    assert "breakout" in trigger_types

    # 7. Confirm summary counts make sense
    summary_path = next(
        a.uri for a in result.artifacts if a.artifact_type == "events_summary"
    )
    summary = json.loads(Path(summary_path).read_text())
    assert summary["trigger_count"] == result.metadata["trigger_count"]
    assert summary["event_count"] >= 1
    assert summary["by_trigger_type"].get("breakout", 0) >= 1
    assert summary["by_trigger_type"].get("bulk_deal", 0) >= 1


def test_events_stage_smoke_handles_missing_market_intel_db(tmp_path):
    """If market_intel.duckdb doesn't exist yet (collector hasn't run), the
    stage should still complete with empty artifacts rather than crash."""

    # Point at a non-existent path
    project_root = tmp_path / "project"
    project_root.mkdir()
    breakout_csv = project_root / "data" / "operational" / "pipeline_runs" \
        / "r-test-2" / "rank" / "attempt_1" / "breakout_scan.csv"
    _write_breakout_csv(breakout_csv)

    ctx = StageContext(
        project_root=project_root,
        db_path=project_root / "data" / "operational" / "control_plane.duckdb",
        run_id="r-test-2",
        run_date="2026-05-01",
        stage_name="events",
        attempt_number=1,
        params={"events_enabled": True},
        artifacts={
            "rank": {
                "breakout_scan": StageArtifact.from_file(
                    "breakout_scan", breakout_csv,
                ),
            },
        },
    )

    # No env var; default path data/market_intel.duckdb does not exist
    stage = EventsStage(
        query_service_factory=lambda: _raise(RuntimeError("not configured")),
    )
    result = stage.run(ctx)

    # Stage produced artifacts (breakout trigger flowed through; events empty)
    artifact_types = {a.artifact_type for a in result.artifacts}
    assert artifact_types == {
        "events_triggers", "events_enrichment", "events_summary",
    }
    enrich_path = next(
        a.uri for a in result.artifacts if a.artifact_type == "events_enrichment"
    )
    payload = json.loads(Path(enrich_path).read_text())
    # At least the breakout trigger should be present with empty events
    rel = next(
        (s for s in payload["signals"] if s["trigger"]["symbol"] == "RELIANCE"),
        None,
    )
    assert rel is not None
    assert rel["event_count"] == 0


def _raise(exc):
    raise exc
