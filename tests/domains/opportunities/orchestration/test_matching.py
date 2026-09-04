from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from ai_trading_system.domains.opportunities.orchestration.contracts import (
    SetupFamily,
    SetupMatchOutcome,
)
from ai_trading_system.domains.opportunities.orchestration.matching import (
    INCOMPATIBLE_TECHNICAL_FAMILY,
    MULTIPLE_COMPATIBLE_TECHNICAL,
    match_open_episode,
)


NOW = datetime(2026, 7, 16, tzinfo=timezone.utc)


def _episode(candidate_id: str, family: SetupFamily):
    return SimpleNamespace(
        candidate_id=candidate_id,
        setup_id=f"setup-{candidate_id}",
        exchange="NSE",
        symbol_id="ABC",
        episode_status=SimpleNamespace(value="OPEN"),
        setup_family=family.value,
        episode_started_at=NOW,
    )


def _match(family: SetupFamily, episodes):
    return match_open_episode(
        exchange="NSE",
        symbol_id="ABC",
        setup_family=family,
        as_of=NOW,
        episodes=episodes,
        current_states=(),
    )


def test_breakout_supersedes_exactly_one_open_momentum_episode():
    momentum = _episode("momentum", SetupFamily.MOMENTUM_LEADER)
    result = _match(SetupFamily.BREAKOUT, (momentum,))
    assert result.outcome is SetupMatchOutcome.SUPERSEDES
    assert result.candidate_id == momentum.candidate_id


def test_two_momentum_episodes_remain_conflict():
    result = _match(
        SetupFamily.BREAKOUT,
        (
            _episode("momentum-1", SetupFamily.MOMENTUM_LEADER),
            _episode("momentum-2", SetupFamily.MOMENTUM_LEADER),
        ),
    )
    assert result.outcome is SetupMatchOutcome.CONFLICT
    assert result.reason_code == INCOMPATIBLE_TECHNICAL_FAMILY


def test_multiple_progression_compatible_episodes_remain_conflict():
    result = _match(
        SetupFamily.BREAKOUT,
        (
            _episode("early", SetupFamily.EARLY_ACCUMULATION),
            _episode("base", SetupFamily.BASE_BUILDING),
        ),
    )
    assert result.outcome is SetupMatchOutcome.CONFLICT
    assert result.reason_code == MULTIPLE_COMPATIBLE_TECHNICAL


def test_progression_precedes_momentum_supersession():
    result = _match(
        SetupFamily.BREAKOUT,
        (
            _episode("momentum", SetupFamily.MOMENTUM_LEADER),
            _episode("base", SetupFamily.BASE_BUILDING),
        ),
    )
    assert result.outcome is SetupMatchOutcome.PROGRESSION
    assert result.candidate_id == "base"


def test_non_breakout_cannot_supersede_momentum():
    result = _match(
        SetupFamily.BASE_BUILDING,
        (_episode("momentum", SetupFamily.MOMENTUM_LEADER),),
    )
    assert result.outcome is SetupMatchOutcome.CONFLICT
    assert result.reason_code == INCOMPATIBLE_TECHNICAL_FAMILY


def test_parallel_fundamental_and_investigator_lanes_do_not_block_technical_lane():
    parallel = (
        _episode("fundamental", SetupFamily.FUNDAMENTAL_THESIS),
        _episode("investigator", SetupFamily.INVESTIGATOR_PRIMARY),
    )
    assert (
        _match(SetupFamily.BREAKOUT, parallel).outcome is SetupMatchOutcome.NEW_EPISODE
    )


def test_parallel_lane_is_ignored_while_technical_progression_is_matched():
    result = _match(
        SetupFamily.BREAKOUT,
        (
            _episode("fundamental", SetupFamily.FUNDAMENTAL_THESIS),
            _episode("base", SetupFamily.BASE_BUILDING),
        ),
    )
    assert result.outcome is SetupMatchOutcome.PROGRESSION
    assert result.candidate_id == "base"


def test_exact_multi_exact_and_new_episode_regressions():
    exact = _episode("exact", SetupFamily.BREAKOUT)
    assert _match(SetupFamily.BREAKOUT, (exact,)).outcome is SetupMatchOutcome.EXACT
    assert (
        _match(
            SetupFamily.BREAKOUT,
            (exact, _episode("exact-2", SetupFamily.BREAKOUT)),
        ).outcome
        is SetupMatchOutcome.CONFLICT
    )
    assert _match(SetupFamily.BREAKOUT, ()).outcome is SetupMatchOutcome.NEW_EPISODE
