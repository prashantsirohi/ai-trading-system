"""Smoke tests for the market_intel client wrapper.

Verifies caching, env-var resolution, and that the lazy import surface works
when market_intel is installed.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ai_trading_system.integrations import market_intel_client


@pytest.fixture(autouse=True)
def _reset_client_cache():
    market_intel_client.reset_cache()
    yield
    market_intel_client.reset_cache()


def test_resolve_db_path_explicit():
    assert market_intel_client.resolve_db_path("foo.db") == "foo.db"


def test_resolve_db_path_env(monkeypatch):
    monkeypatch.setenv("AI_TRADING_MARKET_INTEL_DB", "/tmp/mi.db")
    assert market_intel_client.resolve_db_path() == "/tmp/mi.db"


def test_resolve_db_path_default(monkeypatch):
    monkeypatch.delenv("AI_TRADING_MARKET_INTEL_DB", raising=False)
    assert market_intel_client.resolve_db_path() == "data/market_intel.duckdb"


def test_get_event_query_service_caches(monkeypatch, tmp_path):
    from market_intel.storage.db import Database

    db_path = str(tmp_path / "mi.duckdb")
    Database(db_path=db_path).close()
    svc1 = market_intel_client.get_event_query_service(db_path=db_path)
    svc2 = market_intel_client.get_event_query_service(db_path=db_path)
    assert svc1 is svc2


def test_service_returns_empty_for_unseeded_db(tmp_path):
    from market_intel.storage.db import Database

    db_path = str(tmp_path / "mi.duckdb")
    Database(db_path=db_path).close()
    svc = market_intel_client.get_event_query_service(db_path=db_path)
    out = svc.get_events_for_symbol(
        "RELIANCE",
        since=datetime(2026, 1, 1, tzinfo=timezone.utc),
        min_trust=80.0,
    )
    assert out == []


def test_get_event_query_service_does_not_create_missing_db(tmp_path):
    db_path = tmp_path / "missing.duckdb"
    with pytest.raises(FileNotFoundError):
        market_intel_client.get_event_query_service(db_path=str(db_path))
    assert not db_path.exists()
