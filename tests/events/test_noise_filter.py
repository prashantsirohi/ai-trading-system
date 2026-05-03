"""Unit tests for the events noise-reduction filter chain."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from ai_trading_system.domains.events.noise_filter import (
    CategoryWhitelistFilter,
    CorroborationFilter,
    FilterChain,
    MaterialityFilter,
    PerSymbolDedupFilter,
    TimeDecayFilter,
    TrustGateFilter,
    UniverseFilter,
    build_default_filter_chain,
    load_default_config,
)
from ai_trading_system.domains.events.triggers import Trigger


# --------------------------------------------------------------------------- fakes


@dataclass
class _Event:
    symbol: str
    primary_category: str
    title: str = ""
    description: str = ""
    trust_score: float = 95.0
    importance_score: float = 8.0
    event_date: datetime | None = None
    published_at: datetime | None = None
    source: str = "nse_rss"
    event_hash: str = "h"
    extra: dict[str, Any] = field(default_factory=dict)


def _trig(symbol: str = "REL") -> Trigger:
    from datetime import date
    return Trigger(symbol=symbol, trigger_type="volume_shock",
                   as_of_date=date(2026, 5, 1), trigger_strength=1.5,
                   trigger_metadata={})


# --------------------------------------------------------------------------- whitelist


def test_whitelist_drops_off_list_and_reports_reason():
    f = CategoryWhitelistFilter(whitelist=frozenset({"capex_expansion"}))
    events = [
        _Event("REL", "capex_expansion"),
        _Event("REL", "agm_notice"),
        _Event("REL", "newspaper_publication"),
    ]
    kept, reason = f.apply(trigger=_trig(), events=events)
    assert [e.primary_category for e in kept] == ["capex_expansion"]
    assert reason is None  # something survived


def test_whitelist_emits_reason_when_all_dropped():
    f = CategoryWhitelistFilter(whitelist=frozenset({"capex_expansion"}))
    events = [_Event("REL", "agm_notice"), _Event("REL", "nav_update")]
    kept, reason = f.apply(trigger=_trig(), events=events)
    assert kept == []
    assert reason == "all_categories_off_whitelist"


def test_whitelist_handles_empty_input():
    f = CategoryWhitelistFilter(whitelist=frozenset({"capex_expansion"}))
    kept, reason = f.apply(trigger=_trig(), events=[])
    assert kept == [] and reason is None


# --------------------------------------------------------------------------- trust


def test_trust_gate_drops_low_trust():
    f = TrustGateFilter(min_trust=80.0)
    events = [
        _Event("REL", "capex_expansion", trust_score=95.0),
        _Event("REL", "capex_expansion", trust_score=50.0),
        _Event("REL", "capex_expansion", trust_score=80.0),  # boundary
    ]
    kept, reason = f.apply(trigger=_trig(), events=events)
    assert len(kept) == 2
    assert reason is None


def test_trust_gate_reports_when_all_below():
    f = TrustGateFilter(min_trust=90.0)
    events = [_Event("REL", "capex_expansion", trust_score=80.0)]
    kept, reason = f.apply(trigger=_trig(), events=events)
    assert kept == []
    assert "min_trust" in reason


# --------------------------------------------------------------------------- materiality


def test_materiality_drops_low_for_known_values():
    """A ₹500Cr capex on a ₹50,000Cr company is below the medium threshold."""
    f = MaterialityFilter(
        market_cap_provider=lambda sym: 50_000e7,  # ₹50,000Cr in INR
        drop_below="medium",
        thresholds={
            "capex_expansion": {"medium": 0.02, "high": 0.05, "critical": 0.10},
        },
    )
    ev = _Event(
        "REL", "capex_expansion",
        title="Capex announcement: Rs. 500 crore expansion",
    )
    kept, _ = f.apply(trigger=_trig(), events=[ev])
    assert kept == []  # 500/50000 = 1% < 2% medium threshold


def test_materiality_keeps_high_for_known_values():
    """₹500Cr capex on a ₹1,000Cr company is critical."""
    f = MaterialityFilter(
        market_cap_provider=lambda sym: 1_000e7,
        drop_below="medium",
        thresholds={
            "capex_expansion": {"medium": 0.02, "high": 0.05, "critical": 0.10},
        },
    )
    ev = _Event(
        "REL", "capex_expansion",
        title="Capex announcement: Rs. 500 crore expansion",
    )
    kept, _ = f.apply(trigger=_trig(), events=[ev])
    assert len(kept) == 1
    assert kept[0]._materiality_label == "critical"
    assert kept[0]._material_pct == pytest.approx(0.5)


def test_materiality_keeps_when_market_cap_unknown():
    """Missing data shouldn't suppress — the noise filter is conservative."""
    f = MaterialityFilter(
        market_cap_provider=lambda sym: None,
        drop_below="medium",
    )
    ev = _Event(
        "REL", "capex_expansion",
        title="Capex announcement: Rs. 500 crore expansion",
    )
    kept, _ = f.apply(trigger=_trig(), events=[ev])
    assert len(kept) == 1
    # Marker present but neutral
    assert kept[0]._materiality_label == "medium"


def test_materiality_keeps_when_no_amount_in_text():
    """Categories without a deal value (like board meeting) should pass."""
    f = MaterialityFilter(
        market_cap_provider=lambda sym: 50_000e7,
        drop_below="medium",
    )
    ev = _Event("REL", "buyback", title="Notice of buyback approval")
    kept, _ = f.apply(trigger=_trig(), events=[ev])
    assert len(kept) == 1


# --------------------------------------------------------------------------- time decay


def _at(days_ago: int) -> datetime:
    return datetime(2026, 5, 1, tzinfo=timezone.utc) - timedelta(days=days_ago)


def test_time_decay_drops_old_routine():
    f = TimeDecayFilter(
        routine_lookback_days=30, extended_lookback_days=90,
        extended_categories=frozenset({"mna_partnership"}),
        as_of=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    fresh = _Event("REL", "results", event_date=_at(5))
    old = _Event("REL", "results", event_date=_at(60))
    kept, reason = f.apply(trigger=_trig(), events=[fresh, old])
    assert kept == [fresh]
    assert reason is None


def test_time_decay_extended_window_for_mna():
    f = TimeDecayFilter(
        routine_lookback_days=30, extended_lookback_days=90,
        extended_categories=frozenset({"mna_partnership"}),
        as_of=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    old_mna = _Event("REL", "mna_partnership", event_date=_at(60))
    old_results = _Event("REL", "results", event_date=_at(60))
    kept, _ = f.apply(trigger=_trig(), events=[old_mna, old_results])
    assert kept == [old_mna]


def test_time_decay_keeps_event_with_no_date():
    f = TimeDecayFilter(
        as_of=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    ev = _Event("REL", "results", event_date=None, published_at=None)
    kept, _ = f.apply(trigger=_trig(), events=[ev])
    assert kept == [ev]


def test_time_decay_reports_when_all_outside():
    f = TimeDecayFilter(
        routine_lookback_days=10, extended_lookback_days=20,
        as_of=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    old = _Event("REL", "results", event_date=_at(60))
    kept, reason = f.apply(trigger=_trig(), events=[old])
    assert kept == []
    assert reason == "all_outside_lookback"


# --------------------------------------------------------------------------- per-symbol dedup


class _MockConn:
    """Minimal connection stub: returns rows for queries we care about."""

    def __init__(self, rows: list[tuple] | None = None):
        self.rows = rows or []
        self.queries: list[tuple[str, list]] = []

    def execute(self, sql: str, params=None):
        self.queries.append((sql, list(params or [])))
        return _MockCursor(self.rows[:1] if self.rows else [])

    def close(self):
        pass


class _MockCursor:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


def test_per_symbol_dedup_suppresses_when_match_exists():
    conn = _MockConn(rows=[('["h"]',)])
    f = PerSymbolDedupFilter(conn_provider=lambda: conn, lookback_days=7)
    ev = _Event("REL", "capex_expansion")
    kept, reason = f.apply(trigger=_trig("REL"), events=[ev])
    assert kept == []
    assert "per_symbol_dedup" in reason
    # Confirm the SQL params were what we expected
    assert conn.queries
    _, params = conn.queries[0]
    assert "REL" in params and "capex_expansion" in params and 7 in params


def test_per_symbol_dedup_passes_when_no_match():
    conn = _MockConn(rows=[])
    f = PerSymbolDedupFilter(conn_provider=lambda: conn)
    ev = _Event("REL", "capex_expansion")
    kept, reason = f.apply(trigger=_trig("REL"), events=[ev])
    assert kept == [ev]
    assert reason is None


def test_per_symbol_dedup_no_provider_passes_through():
    f = PerSymbolDedupFilter(conn_provider=None)
    ev = _Event("REL", "capex_expansion")
    kept, _ = f.apply(trigger=_trig(), events=[ev])
    assert kept == [ev]


# --------------------------------------------------------------------------- corroboration


def test_corroboration_marks_dual_source_within_window():
    f = CorroborationFilter(window_hours=24)
    nse = _Event(
        "REL", "buyback",
        event_date=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
        source="nse_rss",
    )
    bse = _Event(
        "REL", "buyback",
        event_date=datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc),
        source="bse_corp",
    )
    kept, _ = f.apply(trigger=_trig(), events=[nse, bse])
    assert kept == [nse, bse]
    assert getattr(nse, "_corroborated", False) is True
    assert getattr(bse, "_corroborated", False) is True


def test_corroboration_skips_outside_window():
    f = CorroborationFilter(window_hours=24)
    nse = _Event(
        "REL", "buyback",
        event_date=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
        source="nse_rss",
    )
    bse = _Event(
        "REL", "buyback",
        event_date=datetime(2026, 5, 3, 10, 0, tzinfo=timezone.utc),  # 48h later
        source="bse_corp",
    )
    f.apply(trigger=_trig(), events=[nse, bse])
    assert getattr(nse, "_corroborated", False) is False
    assert getattr(bse, "_corroborated", False) is False


def test_corroboration_skips_same_source():
    f = CorroborationFilter(window_hours=24)
    a = _Event(
        "REL", "buyback",
        event_date=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
        source="nse_rss",
    )
    b = _Event(
        "REL", "buyback",
        event_date=datetime(2026, 5, 1, 11, 0, tzinfo=timezone.utc),
        source="nse_rss",
    )
    f.apply(trigger=_trig(), events=[a, b])
    assert getattr(a, "_corroborated", False) is False


# --------------------------------------------------------------------------- universe


def test_universe_filter_drops_off_universe():
    f = UniverseFilter(universe=frozenset({"REL", "TCS"}))
    events = [
        _Event("REL", "buyback"),
        _Event("UNKNOWN", "buyback"),
    ]
    kept, _ = f.apply(trigger=_trig(), events=events)
    assert [e.symbol for e in kept] == ["REL"]


def test_universe_filter_no_universe_is_passthrough():
    f = UniverseFilter(universe=None)
    events = [_Event("ANYTHING", "buyback")]
    kept, _ = f.apply(trigger=_trig(), events=events)
    assert kept == events


# --------------------------------------------------------------------------- chain


def test_filter_chain_runs_filters_in_order_and_first_reason_wins():
    chain = FilterChain(filters=[
        CategoryWhitelistFilter(whitelist=frozenset({"capex_expansion"})),
        TrustGateFilter(min_trust=80.0),
    ])
    events = [
        _Event("REL", "agm_notice", trust_score=95.0),
        _Event("REL", "newspaper_publication", trust_score=95.0),
    ]
    kept, reason = chain.apply(trigger=_trig(), events=events)
    assert kept == []
    # Whitelist runs first, so its reason wins
    assert reason == "all_categories_off_whitelist"


def test_filter_chain_lets_passing_events_through_full_chain():
    chain = FilterChain(filters=[
        CategoryWhitelistFilter(whitelist=frozenset({"capex_expansion"})),
        TrustGateFilter(min_trust=80.0),
    ])
    ev = _Event("REL", "capex_expansion", trust_score=95.0)
    kept, reason = chain.apply(trigger=_trig(), events=[ev])
    assert kept == [ev] and reason is None


def test_default_chain_loads_config_and_works():
    cfg = load_default_config()
    assert cfg, "events_filters.json should ship in platform/config/"
    chain = build_default_filter_chain(config=cfg)
    # Without market_cap_provider/conn_provider, the chain still has whitelist
    # + trust + time-decay + corroboration as a minimum.
    assert len(chain.filters) >= 3

    ev = _Event("REL", "capex_expansion", trust_score=95.0,
                event_date=datetime(2026, 5, 1, tzinfo=timezone.utc))
    kept, reason = chain.apply(trigger=_trig(), events=[ev])
    assert kept == [ev]
    assert reason is None


def test_default_chain_drops_off_whitelist_categories():
    cfg = load_default_config()
    chain = build_default_filter_chain(config=cfg)
    ev = _Event("REL", "agm_notice", trust_score=95.0,
                event_date=datetime(2026, 5, 1, tzinfo=timezone.utc))
    kept, reason = chain.apply(trigger=_trig(), events=[ev])
    assert kept == []
    assert reason == "all_categories_off_whitelist"
