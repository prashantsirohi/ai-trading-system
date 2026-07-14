from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from ai_trading_system.domains.opportunities.contracts import CandidateState
from ai_trading_system.domains.opportunities.registry.models import (
    AppendStatus,
    EpisodeStatus,
    OpportunityRegistryConflictError,
    SnapshotObservation,
)

NOW = datetime(2026, 7, 14, 10, tzinfo=timezone.utc)


def test_snapshot_append_replay_conflict_and_as_of(opportunity_store, episode_request, snapshot_builder) -> None:
    episode = opportunity_store.open_episode(episode_request)
    monday = snapshot_builder(episode)
    created = opportunity_store.append_snapshot(monday)
    duplicate = opportunity_store.append_snapshot(monday)
    assert created.status is AppendStatus.CREATED
    assert duplicate.status is AppendStatus.DUPLICATE

    changed = SnapshotObservation(replace(monday.snapshot, days_in_state=2), monday.observed_at, monday.lineage)
    with pytest.raises(OpportunityRegistryConflictError):
        opportunity_store.append_snapshot(changed)

    friday_at = NOW + timedelta(days=3)
    friday = snapshot_builder(episode, at=friday_at, lifecycle=CandidateState.READY, opportunity_score=91)
    opportunity_store.append_snapshot(friday)
    assert opportunity_store.current_state(episode.candidate_id).current_lifecycle_state == "ready"
    historical = opportunity_store.state_as_of(episode.candidate_id, NOW + timedelta(hours=1))
    assert historical.current_lifecycle_state == "discovered"
    assert historical.latest_opportunity_score == 85
    assert len([e for e in opportunity_store.timeline(episode.candidate_id).entries if e.record_type == "snapshot"]) == 2


def test_close_is_idempotent_and_blocks_history(opportunity_store, episode_request, snapshot_builder, lineage) -> None:
    episode = opportunity_store.open_episode(episode_request)
    opportunity_store.append_snapshot(snapshot_builder(episode))
    close_at = NOW + timedelta(days=4)
    closed = opportunity_store.close_episode(
        episode.candidate_id, status=EpisodeStatus.FAILED, closed_at=close_at,
        closing_reason="setup invalidated", lineage=lineage,
    )
    replay = opportunity_store.close_episode(
        episode.candidate_id, status=EpisodeStatus.FAILED, closed_at=close_at,
        closing_reason="setup invalidated", lineage=lineage,
    )
    assert replay == closed
    with pytest.raises(ValueError, match="closed"):
        opportunity_store.append_snapshot(snapshot_builder(episode, at=close_at + timedelta(days=1)))
    as_of_before_close = opportunity_store.state_as_of(episode.candidate_id, close_at - timedelta(hours=1))
    assert as_of_before_close.episode_status is EpisodeStatus.OPEN
