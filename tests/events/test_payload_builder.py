"""Tests for the events publish-payload builder."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

import pytest

from ai_trading_system.domains.events.enrichment_service import EnrichedSignal
from ai_trading_system.domains.events.payload_builder import (
    apply_events_overlay,
    build_events_of_the_week_section,
    format_telegram_block,
    format_telegram_digest,
)
from ai_trading_system.domains.events.triggers import Trigger


# --------------------------------------------------------------------------- fakes


@dataclass
class _Event:
    title: str
    primary_category: str
    event_date: datetime | None = None
    sector: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def _now() -> datetime:
    return datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _trig(symbol: str = "RELIANCE", trigger_type: str = "volume_shock", **meta) -> Trigger:
    return Trigger(
        symbol=symbol,
        trigger_type=trigger_type,
        as_of_date=date(2026, 5, 1),
        trigger_strength=meta.pop("strength", 1.5),
        trigger_metadata=meta,
    )


# --------------------------------------------------------------------------- telegram


def test_format_telegram_block_volume_shock_with_events():
    sig = EnrichedSignal(
        trigger=_trig(z_score=4.2, turnover_cr=1820),
        events=[
            _Event(
                title="₹15,000Cr Jamnagar expansion",
                primary_category="capex_expansion",
                event_date=datetime(2026, 4, 28, tzinfo=timezone.utc),
            ),
        ],
        materiality_label="high",
        top_category="capex_expansion",
        severity="high",
    )
    text = format_telegram_block(sig, now=_now())
    assert "RELIANCE" in text
    assert "Volume z=4.2" in text
    assert "Jamnagar" in text
    assert "capex_expansion" in text
    # 3 days ago
    assert "3d ago" in text


def test_format_telegram_block_bulk_deal_with_client_name():
    sig = EnrichedSignal(
        trigger=_trig(
            symbol="INFY", trigger_type="bulk_deal",
            client_name="ICICI Pru MF", side="BUY", deal_value_cr=120.0,
        ),
        events=[],
        severity="medium",
    )
    text = format_telegram_block(sig, now=_now())
    assert "INFY" in text
    assert "ICICI Pru MF" in text
    assert "₹120Cr" in text


def test_format_telegram_block_breakout_with_no_events():
    sig = EnrichedSignal(
        trigger=_trig(symbol="WIPRO", trigger_type="breakout", tier="A", score=82.0),
        events=[],
        severity="low-info",
    )
    text = format_telegram_block(sig, now=_now())
    assert "WIPRO" in text
    assert "Tier-A" in text


def test_format_telegram_block_shows_corroboration_when_marked():
    ev = _Event(
        title="Buyback approval",
        primary_category="buyback",
        event_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    ev._corroborated = True  # type: ignore[attr-defined]
    sig = EnrichedSignal(
        trigger=_trig(symbol="TCS"),
        events=[ev],
        top_category="buyback",
        severity="high",
    )
    text = format_telegram_block(sig, now=_now())
    assert "NSE+BSE corroborated" in text


def test_format_telegram_block_truncates_overflow():
    events = [
        _Event(
            title=f"Event {i}",
            primary_category="results",
            event_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
        )
        for i in range(10)
    ]
    sig = EnrichedSignal(
        trigger=_trig(),
        events=events,
        top_category="results",
        severity="medium",
    )
    text = format_telegram_block(sig, max_events=3, now=_now())
    assert "+7 more" in text
    # Don't include 10 individual lines
    assert text.count("• results") == 3


def test_format_telegram_block_shows_suppress_reason():
    sig = EnrichedSignal(
        trigger=_trig(),
        events=[],
        suppressed=True,
        suppress_reason="all_categories_off_whitelist",
    )
    text = format_telegram_block(sig, now=_now())
    assert "suppressed" in text
    assert "off_whitelist" in text


def test_format_telegram_digest_skips_suppressed_and_concatenates():
    s1 = EnrichedSignal(trigger=_trig(symbol="A"), events=[], severity="high")
    s2 = EnrichedSignal(
        trigger=_trig(symbol="B"), events=[],
        suppressed=True, suppress_reason="dedup",
    )
    s3 = EnrichedSignal(trigger=_trig(symbol="C"), events=[], severity="medium")
    digest = format_telegram_digest([s1, s2, s3], now=_now())
    assert "A " in digest and "C " in digest
    # Suppressed signals are not in the digest
    assert "B  Volume" not in digest


def test_format_telegram_digest_empty_input():
    assert format_telegram_digest([], now=_now()) == "No actionable events today."


# --------------------------------------------------------------------------- dashboard


def test_apply_events_overlay_attaches_events_to_matching_rows():
    payload = {
        "ranked_signals": [
            {"symbol": "RELIANCE", "score": 85},
            {"symbol": "TCS",      "score": 72},
            {"symbol": "INFY",     "score": 70},
        ],
    }
    s1 = EnrichedSignal(
        trigger=_trig(symbol="RELIANCE"),
        events=[_Event("e1", "capex_expansion")],
        top_category="capex_expansion",
        severity="high",
    )
    s2 = EnrichedSignal(
        trigger=_trig(symbol="TCS", trigger_type="bulk_deal", deal_value_cr=50),
        events=[],
        severity="medium",
    )
    out = apply_events_overlay(payload, [s1, s2])
    rel_row = next(r for r in out["ranked_signals"] if r["symbol"] == "RELIANCE")
    assert "events" in rel_row
    assert len(rel_row["events"]) == 1
    tcs_row = next(r for r in out["ranked_signals"] if r["symbol"] == "TCS")
    assert "events" in tcs_row
    # INFY had no signal — no events key attached
    infy_row = next(r for r in out["ranked_signals"] if r["symbol"] == "INFY")
    assert "events" not in infy_row
    # Top-level summary is always present
    assert "events_index" in out
    assert {row["symbol"] for row in out["events_index"]} == {"RELIANCE", "TCS"}


def test_apply_events_overlay_handles_none_payload():
    assert apply_events_overlay(None, []) is None


def test_apply_events_overlay_skips_lists_of_non_dicts():
    payload = {"some_strings": ["a", "b"], "ranked_signals": []}
    out = apply_events_overlay(payload, [])
    assert out["events_index"] == []
    assert out["some_strings"] == ["a", "b"]


# --------------------------------------------------------------------------- weekly PDF


def test_build_events_of_the_week_section_basic():
    s1 = EnrichedSignal(
        trigger=_trig(symbol="REL"),
        events=[_Event("Capex expansion announcement", "capex_expansion")],
        top_category="capex_expansion",
        severity="high",
    )
    s2 = EnrichedSignal(
        trigger=_trig(symbol="TCS"),
        events=[_Event("Buyback", "buyback")],
        top_category="buyback",
        severity="medium",
    )
    s3 = EnrichedSignal(
        trigger=_trig(symbol="WIPRO"),
        events=[],
        suppressed=True,
        suppress_reason="dedup",
    )
    section = build_events_of_the_week_section([s1, s2, s3], top_n=10)
    # Suppressed s3 doesn't count
    assert section["headline_count"] == 2
    assert section["by_severity"]["high"] == 1
    assert section["by_severity"]["medium"] == 1
    # Sorted by severity desc — high first
    assert section["top_signals"][0]["symbol"] == "REL"
    assert section["top_signals"][0]["headline"] == "Capex expansion announcement"


def test_build_events_of_the_week_caps_top_n():
    sigs = [
        EnrichedSignal(
            trigger=_trig(symbol=f"S{i}"),
            events=[_Event(f"E{i}", "results")],
            top_category="results",
            severity="medium",
        )
        for i in range(15)
    ]
    section = build_events_of_the_week_section(sigs, top_n=5)
    assert len(section["top_signals"]) == 5
    assert section["headline_count"] == 15  # raw count, not capped


# --------------------------------------------------------------------------- delivery dedup


def test_delivery_manager_dedup_key_changes_with_event_hashes():
    """Phase 6.4 — build_dedupe_key incorporates event_hashes from
    artifact metadata so the same content with new events triggers a
    re-send, and identical content+events is deduped."""
    from ai_trading_system.domains.publish.delivery_manager import (
        PublisherDeliveryManager,
    )
    from ai_trading_system.pipeline.contracts import StageArtifact

    mgr = PublisherDeliveryManager()

    artifact_no_events = StageArtifact(
        artifact_type="x", uri="path/a", content_hash="abc",
    )
    artifact_with_events = StageArtifact(
        artifact_type="x", uri="path/a", content_hash="abc",
        metadata={"event_hashes": ["h1", "h2"]},
    )
    artifact_with_diff_events = StageArtifact(
        artifact_type="x", uri="path/a", content_hash="abc",
        metadata={"event_hashes": ["h1", "h3"]},
    )
    # Reordered hashes for the same set should NOT change the key
    artifact_with_reordered_events = StageArtifact(
        artifact_type="x", uri="path/a", content_hash="abc",
        metadata={"event_hashes": ["h2", "h1"]},
    )

    base = mgr.build_dedupe_key("telegram", artifact_no_events)
    with_events = mgr.build_dedupe_key("telegram", artifact_with_events)
    diff_events = mgr.build_dedupe_key("telegram", artifact_with_diff_events)
    reordered = mgr.build_dedupe_key("telegram", artifact_with_reordered_events)

    # Adding events changes the key (re-send instead of dedup-collapse)
    assert base != with_events
    # Different event-hash set => different key
    assert with_events != diff_events
    # Same set in different order => same key (order-insensitive sorting)
    assert with_events == reordered


def test_delivery_manager_dedup_key_backward_compatible():
    """Artifacts without events_metadata produce the same key they did before."""
    from ai_trading_system.domains.publish.delivery_manager import (
        PublisherDeliveryManager,
    )
    from ai_trading_system.pipeline.contracts import StageArtifact
    import hashlib

    mgr = PublisherDeliveryManager()
    artifact = StageArtifact(
        artifact_type="x", uri="path/b", content_hash="zzz",
    )
    expected = hashlib.sha256("telegram:zzz".encode("utf-8")).hexdigest()
    assert mgr.build_dedupe_key("telegram", artifact) == expected
