from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from ai_trading_system.domains.opportunities.position_monitoring import (
    PositionEpisodeCompatibility,
    evaluate_position_episode_compatibility,
)


OPENED = datetime(2026, 7, 10, tzinfo=timezone.utc)


def _episode(candidate_id: str, *, family: str = "breakout", status: str = "OPEN", started=None):
    return SimpleNamespace(
        candidate_id=candidate_id,
        setup_family=family,
        admission_identity="ordinary-admission",
        episode_status=SimpleNamespace(value=status),
        episode_started_at=started or OPENED - timedelta(days=10),
    )


def _state(candidate_id: str, *, transition=None, lifecycle: str = "triggered"):
    return SimpleNamespace(
        candidate_id=candidate_id,
        last_transition_at=transition,
        current_lifecycle_state=lifecycle,
    )


def test_compatible_episode_requires_trigger_timing_evidence() -> None:
    result = evaluate_position_episode_compatibility(
        position_cycle_id="cycle-1", position_opened_at=OPENED,
        episodes=[_episode("candidate-1")],
        current_states=[_state("candidate-1", transition=OPENED - timedelta(days=1))],
    )
    assert result.status is PositionEpisodeCompatibility.COMPATIBLE
    assert result.candidate_id == "candidate-1"


def test_same_symbol_single_incompatible_episode_does_not_attach() -> None:
    result = evaluate_position_episode_compatibility(
        position_cycle_id="cycle-1", position_opened_at=OPENED,
        episodes=[_episode("candidate-1")], current_states=[_state("candidate-1")],
    )
    assert result.status is PositionEpisodeCompatibility.INSUFFICIENT_EVIDENCE
    assert result.candidate_id is None


def test_multiple_open_episodes_are_ambiguous_and_closed_episode_stays_closed() -> None:
    ambiguous = evaluate_position_episode_compatibility(
        position_cycle_id="cycle-1", position_opened_at=OPENED,
        episodes=[_episode("one"), _episode("two")],
        current_states=[
            _state("one", transition=OPENED), _state("two", transition=OPENED)
        ],
    )
    closed = evaluate_position_episode_compatibility(
        position_cycle_id="cycle-1", position_opened_at=OPENED,
        episodes=[_episode("closed", status="CLOSED")], current_states=[],
    )
    assert ambiguous.status is PositionEpisodeCompatibility.AMBIGUOUS_MULTIPLE_EPISODES
    assert ambiguous.candidate_id is None
    assert closed.status is PositionEpisodeCompatibility.CLOSED_EPISODE


def test_temporal_mismatch_is_rejected() -> None:
    result = evaluate_position_episode_compatibility(
        position_cycle_id="cycle-1", position_opened_at=OPENED,
        episodes=[_episode("candidate-1")],
        current_states=[_state("candidate-1", transition=OPENED - timedelta(days=20))],
    )
    assert result.status is PositionEpisodeCompatibility.TEMPORAL_MISMATCH
