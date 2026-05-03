"""Tests for the bulk-deal + breakout trigger collectors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pytest

from ai_trading_system.domains.events.trigger_collector import (
    collect_breakout_triggers,
    collect_bulk_deal_triggers,
    merge_triggers,
)
from ai_trading_system.domains.events.triggers import Trigger


# --------------------------------------------------------------------------- bulk deals

@dataclass
class _FakeBulkDeal:
    symbol: str
    trade_date: date
    exchange: str = "NSE"
    side: str = "BUY"
    quantity: int | None = 100_000
    avg_price: float | None = 1000.0
    deal_value_cr: float | None = 10.0
    is_block: bool = False
    client_name: str | None = "ICICI Pru MF"
    deal_hash: str = "h1"
    bulk_deal_id: int = 1


class _FakeQuerySvc:
    def __init__(self, deals: list[_FakeBulkDeal]):
        self.deals = deals
        self.calls: list[dict] = []

    def get_bulk_deals(self, **kwargs):
        self.calls.append(kwargs)
        return list(self.deals)


def test_collect_bulk_deal_triggers_basic():
    svc = _FakeQuerySvc([
        _FakeBulkDeal("RELIANCE", date(2026, 5, 1), deal_value_cr=120.0),
        _FakeBulkDeal("TCS", date(2026, 5, 2), deal_value_cr=50.0,
                      client_name="LIC", side="SELL"),
    ])
    triggers = collect_bulk_deal_triggers(
        as_of_date=date(2026, 5, 2),
        query_service=svc,
    )
    assert len(triggers) == 2
    assert {t.symbol for t in triggers} == {"RELIANCE", "TCS"}
    rel = next(t for t in triggers if t.symbol == "RELIANCE")
    assert rel.trigger_type == "bulk_deal"
    # ₹120Cr → strength 1.2
    assert rel.trigger_strength == pytest.approx(1.2)
    assert rel.trigger_metadata["client_name"] == "ICICI Pru MF"


def test_collect_bulk_deal_triggers_universe_filter():
    svc = _FakeQuerySvc([
        _FakeBulkDeal("RELIANCE", date(2026, 5, 1)),
        _FakeBulkDeal("UNTRACKED", date(2026, 5, 1)),
    ])
    triggers = collect_bulk_deal_triggers(
        as_of_date=date(2026, 5, 2),
        query_service=svc,
        universe_symbols={"RELIANCE", "TCS"},
    )
    assert {t.symbol for t in triggers} == {"RELIANCE"}


def test_collect_bulk_deal_triggers_dedupes_same_day_same_symbol():
    svc = _FakeQuerySvc([
        _FakeBulkDeal("RELIANCE", date(2026, 5, 1), deal_value_cr=120.0),
        _FakeBulkDeal("RELIANCE", date(2026, 5, 1), side="SELL",
                      client_name="Goldman", deal_value_cr=80.0),
    ])
    triggers = collect_bulk_deal_triggers(
        as_of_date=date(2026, 5, 1), query_service=svc,
    )
    assert len(triggers) == 1


def test_collect_bulk_deal_triggers_strength_clamped():
    svc = _FakeQuerySvc([
        _FakeBulkDeal("BIG", date(2026, 5, 1), deal_value_cr=5000.0),
    ])
    triggers = collect_bulk_deal_triggers(
        as_of_date=date(2026, 5, 1), query_service=svc,
    )
    assert triggers[0].trigger_strength == pytest.approx(5.0)


def test_collect_bulk_deal_triggers_handles_empty():
    svc = _FakeQuerySvc([])
    triggers = collect_bulk_deal_triggers(
        as_of_date=date(2026, 5, 1), query_service=svc,
    )
    assert triggers == []


# --------------------------------------------------------------------------- breakout


def _write_breakout_csv(path: Path, rows: list[dict]) -> None:
    import csv
    if not rows:
        path.write_text("symbol,tier,score\n", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_collect_breakout_triggers_filters_to_tier_a_b(tmp_path):
    csv_path = tmp_path / "breakout_scan.csv"
    _write_breakout_csv(csv_path, [
        {"symbol": "RELIANCE", "tier": "A", "score": "85"},
        {"symbol": "TCS",      "tier": "B", "score": "70"},
        {"symbol": "INFY",     "tier": "C", "score": "55"},
        {"symbol": "WIPRO",    "tier": "D", "score": "30"},
    ])
    triggers = collect_breakout_triggers(
        csv_path, as_of_date=date(2026, 5, 1),
    )
    syms = {t.symbol for t in triggers}
    assert syms == {"RELIANCE", "TCS"}
    rel = next(t for t in triggers if t.symbol == "RELIANCE")
    assert rel.trigger_type == "breakout"
    assert rel.trigger_strength == pytest.approx(0.85)
    assert rel.trigger_metadata["tier"] == "A"


def test_collect_breakout_triggers_missing_file_returns_empty(tmp_path):
    triggers = collect_breakout_triggers(
        tmp_path / "nope.csv", as_of_date=date(2026, 5, 1),
    )
    assert triggers == []


def test_collect_breakout_triggers_alt_column_names(tmp_path):
    csv_path = tmp_path / "breakout_scan.csv"
    _write_breakout_csv(csv_path, [
        {"symbol_id": "ITC", "breakout_tier": "A", "stage2_score": "92"},
    ])
    triggers = collect_breakout_triggers(
        csv_path, as_of_date=date(2026, 5, 1),
    )
    assert len(triggers) == 1
    assert triggers[0].symbol == "ITC"
    assert triggers[0].trigger_strength == pytest.approx(0.92)


def test_collect_breakout_triggers_universe_filter(tmp_path):
    csv_path = tmp_path / "breakout_scan.csv"
    _write_breakout_csv(csv_path, [
        {"symbol": "A", "tier": "A", "score": "80"},
        {"symbol": "B", "tier": "A", "score": "80"},
    ])
    triggers = collect_breakout_triggers(
        csv_path, as_of_date=date(2026, 5, 1),
        universe_symbols={"A"},
    )
    assert {t.symbol for t in triggers} == {"A"}


# --------------------------------------------------------------------------- merge


def test_merge_triggers_dedupes_by_key():
    a = Trigger(symbol="REL", trigger_type="volume_shock",
                as_of_date=date(2026, 5, 1), trigger_strength=2.0)
    b = Trigger(symbol="REL", trigger_type="volume_shock",
                as_of_date=date(2026, 5, 1), trigger_strength=1.5)
    c = Trigger(symbol="REL", trigger_type="bulk_deal",
                as_of_date=date(2026, 5, 1), trigger_strength=1.2)
    out = merge_triggers([a, b], [c])
    keys = {t.dedupe_key() for t in out}
    assert len(out) == 2
    assert keys == {a.dedupe_key(), c.dedupe_key()}


def test_merge_triggers_preserves_order():
    a = Trigger("REL", "volume_shock", date(2026, 5, 1), 2.0, {})
    b = Trigger("TCS", "bulk_deal",    date(2026, 5, 1), 1.0, {})
    c = Trigger("INFY", "breakout",    date(2026, 5, 1), 0.8, {})
    out = merge_triggers([a], [b], [c])
    assert [t.symbol for t in out] == ["REL", "TCS", "INFY"]
