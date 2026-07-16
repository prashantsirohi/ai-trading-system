"""Deterministic matching of source bundles to open episodes."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

from ai_trading_system.domains.opportunities.registry.models import CandidateCurrentState, CandidateEpisodeRecord

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


def match_open_episode(
    *, exchange: str, symbol_id: str, setup_family: SetupFamily, as_of: datetime,
    episodes: Iterable[CandidateEpisodeRecord], current_states: Iterable[CandidateCurrentState],
    progression_max_days: int = 30,
) -> EpisodeMatch:
    matching = [episode for episode in episodes if episode.exchange == exchange and episode.symbol_id == symbol_id and episode.episode_status.value == "OPEN"]
    exact = [episode for episode in matching if episode.setup_family == setup_family.value]
    if len(exact) == 1:
        return EpisodeMatch(SetupMatchOutcome.EXACT, exact[0].candidate_id, exact[0].setup_id)
    if len(exact) > 1:
        return EpisodeMatch(SetupMatchOutcome.CONFLICT, None, None, ("multiple exact-family open episodes",))
    target_index = _PROGRESSION.index(setup_family.value) if setup_family.value in _PROGRESSION else -1
    state_by_id = {state.candidate_id: state for state in current_states}
    compatible: list[CandidateEpisodeRecord] = []
    if target_index >= 0:
        for episode in matching:
            if episode.setup_family not in _PROGRESSION or _PROGRESSION.index(episode.setup_family) > target_index:
                continue
            state = state_by_id.get(episode.candidate_id)
            last_at = state.last_snapshot_at if state and state.last_snapshot_at else episode.episode_started_at
            if 0 <= (as_of - last_at).days <= progression_max_days:
                compatible.append(episode)
    if len(compatible) == 1:
        return EpisodeMatch(SetupMatchOutcome.PROGRESSION, compatible[0].candidate_id, compatible[0].setup_id, ("setup family progressed; immutable episode family retained",))
    if len(compatible) > 1 or matching:
        return EpisodeMatch(SetupMatchOutcome.CONFLICT, None, None, ("open episode family is incompatible or ambiguous",))
    return EpisodeMatch(SetupMatchOutcome.NEW_EPISODE, None, None)
