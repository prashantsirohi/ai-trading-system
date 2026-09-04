"""Deterministic matching of source bundles to open episodes."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

from ai_trading_system.domains.opportunities.registry.models import (
    CandidateCurrentState,
    CandidateEpisodeRecord,
)

from .contracts import EpisodeMatch, SetupFamily, SetupMatchOutcome


_PROGRESSION = (
    SetupFamily.EARLY_ACCUMULATION.value,
    SetupFamily.BASE_BUILDING.value,
    SetupFamily.STAGE_1_TO_2_TRANSITION.value,
    SetupFamily.BREAKOUT.value,
    SetupFamily.POST_BREAKOUT_FOLLOWTHROUGH.value,
)

# Public alias for the setup-family-v1 policy fingerprint (ADR-0006 A3).
SETUP_FAMILY_PROGRESSION = _PROGRESSION

# Public constant for the setup-family-v1.1 policy fingerprint (ADR-0006 A1).
SETUP_FAMILY_SUPERSESSION = {
    SetupFamily.MOMENTUM_LEADER.value: SetupFamily.BREAKOUT.value
}

# Parallel discovery lanes may coexist for one symbol. Matching and ambiguity
# checks are scoped to the incoming setup's lane so a fundamental thesis cannot
# block an independent technical progression, and vice versa.
SETUP_FAMILY_LANES = {
    SetupFamily.INVESTIGATOR_PRIMARY.value: "investigator_primary",
    SetupFamily.FUNDAMENTAL_THESIS.value: "fundamental_thesis",
    SetupFamily.EARLY_ACCUMULATION.value: "technical_progression",
    SetupFamily.BASE_BUILDING.value: "technical_progression",
    SetupFamily.STAGE_1_TO_2_TRANSITION.value: "technical_progression",
    SetupFamily.BREAKOUT.value: "technical_progression",
    SetupFamily.POST_BREAKOUT_FOLLOWTHROUGH.value: "technical_progression",
    SetupFamily.PULLBACK_REENTRY.value: "technical_progression",
    SetupFamily.MOMENTUM_LEADER.value: "technical_progression",
    SetupFamily.MANUAL.value: "manual",
    SetupFamily.POSITION_STATE_RECOVERY.value: "position_state_recovery",
}

MULTIPLE_EXACT_FAMILY = "MULTIPLE_EXACT_FAMILY_EPISODES"
MULTIPLE_COMPATIBLE_TECHNICAL = "MULTIPLE_COMPATIBLE_TECHNICAL_EPISODES"
INCOMPATIBLE_TECHNICAL_FAMILY = "INCOMPATIBLE_TECHNICAL_FAMILY"


def match_open_episode(
    *,
    exchange: str,
    symbol_id: str,
    setup_family: SetupFamily,
    as_of: datetime,
    episodes: Iterable[CandidateEpisodeRecord],
    current_states: Iterable[CandidateCurrentState],
    progression_max_days: int = 30,
) -> EpisodeMatch:
    incoming_lane = SETUP_FAMILY_LANES[setup_family.value]
    matching = [
        episode
        for episode in episodes
        if episode.exchange == exchange
        and episode.symbol_id == symbol_id
        and episode.episode_status.value == "OPEN"
        and SETUP_FAMILY_LANES.get(episode.setup_family) == incoming_lane
    ]
    exact = [
        episode for episode in matching if episode.setup_family == setup_family.value
    ]
    if len(exact) == 1:
        return EpisodeMatch(
            SetupMatchOutcome.EXACT, exact[0].candidate_id, exact[0].setup_id
        )
    if len(exact) > 1:
        return EpisodeMatch(
            SetupMatchOutcome.CONFLICT,
            None,
            None,
            ("multiple exact-family open episodes",),
            MULTIPLE_EXACT_FAMILY,
        )
    target_index = (
        _PROGRESSION.index(setup_family.value)
        if setup_family.value in _PROGRESSION
        else -1
    )
    state_by_id = {state.candidate_id: state for state in current_states}
    compatible: list[CandidateEpisodeRecord] = []
    if target_index >= 0:
        for episode in matching:
            if (
                episode.setup_family not in _PROGRESSION
                or _PROGRESSION.index(episode.setup_family) > target_index
            ):
                continue
            state = state_by_id.get(episode.candidate_id)
            last_at = (
                state.last_snapshot_at
                if state and state.last_snapshot_at
                else episode.episode_started_at
            )
            if 0 <= (as_of - last_at).days <= progression_max_days:
                compatible.append(episode)
    if len(compatible) == 1:
        return EpisodeMatch(
            SetupMatchOutcome.PROGRESSION,
            compatible[0].candidate_id,
            compatible[0].setup_id,
            ("setup family progressed; immutable episode family retained",),
        )
    if (
        setup_family is SetupFamily.BREAKOUT
        and len(matching) == 1
        and SETUP_FAMILY_SUPERSESSION.get(matching[0].setup_family)
        == setup_family.value
    ):
        return EpisodeMatch(
            SetupMatchOutcome.SUPERSEDES,
            matching[0].candidate_id,
            matching[0].setup_id,
            ("open momentum_leader episode superseded by qualified breakout",),
        )
    if len(compatible) > 1:
        return EpisodeMatch(
            SetupMatchOutcome.CONFLICT,
            None,
            None,
            ("multiple compatible technical episodes",),
            MULTIPLE_COMPATIBLE_TECHNICAL,
        )
    if matching:
        return EpisodeMatch(
            SetupMatchOutcome.CONFLICT,
            None,
            None,
            ("open technical episode family is incompatible",),
            INCOMPATIBLE_TECHNICAL_FAMILY,
        )
    return EpisodeMatch(SetupMatchOutcome.NEW_EPISODE, None, None)
