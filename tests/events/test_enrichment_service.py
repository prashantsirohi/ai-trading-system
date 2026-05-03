"""Tests for the events EnrichmentService."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

import pytest

from ai_trading_system.domains.events.enrichment_service import (
    EnrichedSignal,
    EnrichmentService,
    summarize,
)
from ai_trading_system.domains.events.triggers import Trigger


# ---------------------------------------------------------------- fakes


@dataclass
class _FakeEvent:
    symbol: str
    primary_category: str
    event_tier: str
    importance_score: float
    trust_score: float
    event_hash: str
    title: str = "test"


class _FakeQuerier:
    def __init__(self, events_by_symbol: dict[str, list[_FakeEvent]]):
        self._events = events_by_symbol
        self.calls: list[dict] = []

    def get_events_for_symbol(self, symbol, **kwargs):
        self.calls.append({"symbol": symbol, **kwargs})
        return list(self._events.get(symbol, []))


class _RaisingQuerier:
    def get_events_for_symbol(self, symbol, **kwargs):
        raise RuntimeError("DB outage")


# ---------------------------------------------------------------- service


def test_enrich_attaches_events_to_trigger():
    events = {
        "RELIANCE": [
            _FakeEvent("RELIANCE", "capex_expansion", "A", 8.5, 95.0, "h-rel-1"),
        ],
    }
    svc = EnrichmentService(query_service=_FakeQuerier(events))
    trig = Trigger(
        symbol="RELIANCE", trigger_type="volume_shock",
        as_of_date=date(2026, 5, 1), trigger_strength=1.4,
    )
    out = svc.enrich([trig])
    assert len(out) == 1
    sig = out[0]
    assert len(sig.events) == 1
    assert sig.event_hashes == ["h-rel-1"]
    assert sig.top_category == "capex_expansion"
    assert sig.severity == "high"  # volume_shock + Tier A


def test_enrich_picks_highest_importance_as_top_category():
    events = {
        "REL": [
            _FakeEvent("REL", "results", "B", 7.5, 95.0, "h1"),
            _FakeEvent("REL", "demerger", "A", 9.3, 95.0, "h2"),
            _FakeEvent("REL", "dividend", "B", 7.0, 95.0, "h3"),
        ],
    }
    svc = EnrichmentService(query_service=_FakeQuerier(events))
    trig = Trigger("REL", "breakout", date(2026, 5, 1), 1.0, {})
    out = svc.enrich([trig])
    assert out[0].top_category == "demerger"


def test_enrich_handles_no_events():
    svc = EnrichmentService(query_service=_FakeQuerier({}))
    trig = Trigger("UNKNOWN", "bulk_deal", date(2026, 5, 1), 1.0, {})
    out = svc.enrich([trig])
    assert out[0].events == []
    assert out[0].event_hashes == []
    # bulk_deal alone with no event => severity medium
    assert out[0].severity == "medium"


def test_enrich_handles_query_errors_gracefully():
    svc = EnrichmentService(query_service=_RaisingQuerier())
    trig = Trigger("REL", "volume_shock", date(2026, 5, 1), 1.5, {})
    out = svc.enrich([trig])
    assert out[0].events == []
    assert out[0].suppressed is False  # not suppressed; just empty


def test_severity_low_info_for_breakout_with_no_events():
    svc = EnrichmentService(query_service=_FakeQuerier({}))
    trig = Trigger("REL", "breakout", date(2026, 5, 1), 0.7, {})
    out = svc.enrich([trig])
    assert out[0].severity == "low-info"


def test_severity_high_for_critical_materiality():
    """If a noise filter tags an event with _materiality_label='critical', the
    enrichment service should bump severity to high regardless of trigger type."""

    class _MarkingFilter:
        def apply(self, *, trigger, events):
            for ev in events:
                ev._materiality_label = "critical"
            return events, None

    events = {
        "REL": [_FakeEvent("REL", "buyback", "A", 9.0, 95.0, "hb")],
    }
    svc = EnrichmentService(
        query_service=_FakeQuerier(events),
        noise_filter=_MarkingFilter(),
    )
    trig = Trigger("REL", "breakout", date(2026, 5, 1), 0.5, {})
    out = svc.enrich([trig])
    assert out[0].materiality_label == "critical"
    assert out[0].severity == "high"


def test_noise_filter_suppress_reason_propagates():
    class _SuppressAll:
        def apply(self, *, trigger, events):
            return [], "all_filtered_by_test"

    events = {
        "REL": [_FakeEvent("REL", "agm_notice", "IGNORE", 2.0, 95.0, "hi")],
    }
    svc = EnrichmentService(
        query_service=_FakeQuerier(events),
        noise_filter=_SuppressAll(),
    )
    trig = Trigger("REL", "breakout", date(2026, 5, 1), 0.5, {})
    out = svc.enrich([trig])
    assert out[0].suppressed is True
    assert out[0].suppress_reason == "all_filtered_by_test"
    assert out[0].events == []


def test_query_kwargs_use_lookback_and_min_trust():
    querier = _FakeQuerier({})
    svc = EnrichmentService(
        query_service=querier,
        lookback_days=14,
        min_trust=85.0,
        per_trigger_event_limit=5,
        tiers=("A",),
    )
    trig = Trigger("X", "breakout", date(2026, 5, 1), 1.0, {})
    svc.enrich([trig])
    assert len(querier.calls) == 1
    call = querier.calls[0]
    assert call["min_trust"] == 85.0
    assert call["limit"] == 5
    assert tuple(call["tiers"]) == ("A",)
    # since = as_of - 14 days
    expected_since = datetime(2026, 4, 17, tzinfo=timezone.utc)
    assert call["since"] == expected_since


# ---------------------------------------------------------------- summarize


def test_summarize_counts_by_dimensions():
    events = {
        "REL": [_FakeEvent("REL", "capex_expansion", "A", 8.5, 95.0, "h1")],
        "TCS": [_FakeEvent("TCS", "buyback", "A", 9.2, 95.0, "h2")],
    }
    svc = EnrichmentService(query_service=_FakeQuerier(events))
    triggers = [
        Trigger("REL", "volume_shock", date(2026, 5, 1), 2.0, {}),
        Trigger("TCS", "bulk_deal",    date(2026, 5, 1), 1.2, {}),
        Trigger("INFY", "breakout",    date(2026, 5, 1), 0.7, {}),
    ]
    signals = svc.enrich(triggers)
    summary = summarize(signals)
    assert summary["trigger_count"] == 3
    assert summary["event_count"] == 2
    assert summary["by_trigger_type"]["volume_shock"] == 1
    assert summary["by_trigger_type"]["bulk_deal"] == 1
    assert summary["by_trigger_type"]["breakout"] == 1
    assert summary["by_top_category"]["capex_expansion"] == 1
    assert summary["by_severity"]["high"] >= 1


def test_enriched_signal_to_dict_is_json_safe():
    sig = EnrichedSignal(
        trigger=Trigger("REL", "breakout", date(2026, 5, 1), 0.7, {"tier": "A"}),
        events=[],
        materiality_label="medium",
        top_category=None,
        severity="low-info",
    )
    payload = sig.to_dict()
    import json
    json.dumps(payload)  # must not raise
    assert payload["trigger"]["symbol"] == "REL"
    assert payload["severity"] == "low-info"
