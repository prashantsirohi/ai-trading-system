from __future__ import annotations

from dataclasses import replace

from ai_trading_system.domains.opportunities.registry.models import REGISTRY_SCHEMA_VERSION
from ai_trading_system.domains.opportunities.registry.schema import TABLES, VIEWS


def test_schema_is_complete_and_reinitializes(opportunity_store) -> None:
    opportunity_store.initialize_schema()
    opportunity_store.initialize_schema()
    with opportunity_store.registry._reader() as conn:  # noqa: SLF001
        relations = {row[0] for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()}
        assert set(TABLES) | set(VIEWS) <= relations
        assert conn.execute(
            "SELECT schema_version FROM opportunity_registry_schema WHERE schema_name = ?",
            ["opportunity_registry"],
        ).fetchone()[0] == REGISTRY_SCHEMA_VERSION


def test_episode_identity_replay_family_exchange_and_reentry(opportunity_store, episode_request) -> None:
    first = opportunity_store.open_episode(episode_request)
    replay = opportunity_store.open_episode(episode_request)
    other_family = opportunity_store.open_episode(
        replace(episode_request, setup_family="post breakout reentry", admission_identity="run-1:ABC:reentry")
    )
    other_exchange = opportunity_store.open_episode(
        replace(episode_request, exchange="BSE", admission_identity="run-1:ABC:BSE")
    )

    assert replay == first
    assert first.candidate_id != first.symbol_id and first.setup_id != first.symbol_id
    assert other_family.episode_number == 2
    assert other_family.candidate_id != first.candidate_id
    assert other_exchange.episode_number == 1
    assert opportunity_store.find_open_episode(exchange="NSE", symbol_id="ABC") is not None
