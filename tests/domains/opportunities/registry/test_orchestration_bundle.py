from __future__ import annotations

from types import SimpleNamespace

import pytest

from ai_trading_system.domains.opportunities.contracts import CandidateState
from ai_trading_system.domains.opportunities.registry.identity import make_candidate_id, make_setup_id
from ai_trading_system.domains.opportunities.registry.models import OrchestrationBundle, TransitionObservation


def test_atomic_orchestration_bundle_rolls_back_episode_and_snapshot_on_invalid_transition(
    opportunity_store, episode_request, snapshot_builder, lineage,
):
    setup_id = make_setup_id(
        exchange=episode_request.exchange,
        symbol_id=episode_request.symbol_id,
        setup_family=episode_request.setup_family,
        admission_identity=episode_request.admission_identity,
        episode_started_at=episode_request.episode_started_at,
    )
    candidate_id = make_candidate_id(setup_id)
    episode = SimpleNamespace(
        candidate_id=candidate_id,
        setup_id=setup_id,
        symbol_id="ABC",
        exchange="NSE",
    )
    snapshot = snapshot_builder(episode)
    transition = TransitionObservation(
        candidate_id, setup_id, CandidateState.DISCOVERED, CandidateState.DISCOVERED,
        "rank_admission", episode_request.episode_started_at, "pending", "lifecycle-policy-v1", {}, lineage,
    )
    bundle = OrchestrationBundle(
        candidate_id=candidate_id,
        episode_request=episode_request,
        snapshot=snapshot,
        transition=transition,
    )
    with pytest.raises(ValueError, match="from_state and to_state"):
        opportunity_store.append_orchestration_bundle(bundle)
    assert opportunity_store.get_episode(candidate_id) is None
