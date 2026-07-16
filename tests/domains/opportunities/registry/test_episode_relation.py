from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    OpportunitySnapshot,
    ProgressStatus,
)
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    ClosureReason,
    EpisodeRelationType,
    SETUP_FAMILY_RULE_VERSION,
)
from ai_trading_system.domains.opportunities.registry.identity import (
    make_candidate_id,
    make_setup_id,
)
from ai_trading_system.domains.opportunities.registry.models import (
    EpisodeStatus,
    EpisodeSupersession,
    OpportunityObservation,
    OpportunityRegistryConflictError,
    OrchestrationBundle,
)


NOW = datetime(2026, 7, 16, 10, tzinfo=timezone.utc)


def _successor_request(episode_request, lineage):
    return replace(
        episode_request,
        setup_family="breakout",
        admission_identity="qualified-breakout",
        episode_started_at=NOW + timedelta(days=1),
        opening_reason="qualified_breakout",
        lineage=lineage,
    )


def _bundle(predecessor, successor_request, lineage, **changes):
    setup_id = make_setup_id(
        exchange=successor_request.exchange,
        symbol_id=successor_request.symbol_id,
        setup_family=successor_request.setup_family,
        admission_identity=successor_request.admission_identity,
        episode_started_at=successor_request.episode_started_at,
    )
    candidate_id = make_candidate_id(setup_id)
    supersession = EpisodeSupersession(
        predecessor_candidate_id=predecessor.candidate_id,
        relation_type=EpisodeRelationType.MOMENTUM_SUPERSEDED_BY_BREAKOUT.value,
        related_at=successor_request.episode_started_at,
        closing_reason=ClosureReason.SUPERSEDED_BY_NEW_EPISODE.value,
        rule_version=SETUP_FAMILY_RULE_VERSION,
        lineage=lineage,
        contract_version=successor_request.contract_version,
    )
    return replace(
        OrchestrationBundle(
            candidate_id=candidate_id,
            episode_request=successor_request,
            supersession=supersession,
        ),
        **changes,
    )


def _open_momentum(opportunity_store, episode_request, **changes):
    values = {
        "setup_family": "momentum_leader",
        "admission_identity": "momentum-leader",
        "episode_started_at": NOW,
        **changes,
    }
    return opportunity_store.open_episode(
        replace(episode_request, **values)
    )


def test_supersession_closes_predecessor_opens_successor_and_reads_both_directions(
    opportunity_store, episode_request, lineage
):
    predecessor = _open_momentum(opportunity_store, episode_request)
    bundle = _bundle(predecessor, _successor_request(episode_request, lineage), lineage)

    result = opportunity_store.append_orchestration_bundle(bundle)

    assert result.episode.episode_status is EpisodeStatus.OPEN
    assert result.superseded_candidate_id == predecessor.candidate_id
    closed = opportunity_store.get_episode(predecessor.candidate_id)
    assert closed.episode_status is EpisodeStatus.CLOSED
    assert closed.closing_reason == ClosureReason.SUPERSEDED_BY_NEW_EPISODE.value
    predecessor_side = opportunity_store.list_episode_relations(predecessor.candidate_id)
    successor_side = opportunity_store.list_episode_relations(result.episode.candidate_id)
    assert predecessor_side == successor_side
    assert len(predecessor_side) == 1
    assert predecessor_side[0].successor_candidate_id == result.episode.candidate_id
    assert opportunity_store.find_episode_relation(
        predecessor_candidate_id=predecessor.candidate_id,
        relation_type=EpisodeRelationType.MOMENTUM_SUPERSEDED_BY_BREAKOUT.value,
    ) == predecessor_side[0]


def test_supersession_bundle_replay_is_idempotent(
    opportunity_store, episode_request, lineage
):
    predecessor = _open_momentum(opportunity_store, episode_request)
    bundle = _bundle(predecessor, _successor_request(episode_request, lineage), lineage)
    opportunity_store.append_orchestration_bundle(bundle)
    replay = opportunity_store.append_orchestration_bundle(bundle)
    episodes = opportunity_store.list_episodes(exchange="NSE", symbol_id="ABC")
    assert len(episodes) == 2
    assert len(opportunity_store.list_episode_relations(predecessor.candidate_id)) == 1
    assert replay.append_results[0].duplicate


def test_concurrent_supersession_replay_creates_one_relation(
    opportunity_store, episode_request, lineage
):
    predecessor = _open_momentum(opportunity_store, episode_request)
    bundle = _bundle(predecessor, _successor_request(episode_request, lineage), lineage)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _: opportunity_store.append_orchestration_bundle(bundle),
                range(2),
            )
        )
    assert sum(result.append_results[0].created for result in results) == 1
    assert len(opportunity_store.list_episode_relations(predecessor.candidate_id)) == 1


def test_failure_after_relation_rolls_back_open_close_and_relation(
    opportunity_store, episode_request, lineage
):
    predecessor = _open_momentum(opportunity_store, episode_request)
    successor = _successor_request(episode_request, lineage)
    bundle = _bundle(predecessor, successor, lineage)
    invalid_observation = OpportunityObservation(
        bundle.candidate_id,
        "wrong-setup-id",
        successor.episode_started_at,
        successor.episode_started_at,
        OpportunitySnapshot(
            95,
            1,
            99,
            -5,
            ProgressStatus.IMPROVING,
            {},
            "rank-v1",
            successor.episode_started_at,
        ),
        lineage,
    )

    with pytest.raises(ValueError, match="record identity"):
        opportunity_store.append_orchestration_bundle(
            replace(bundle, opportunity=invalid_observation)
        )

    assert opportunity_store.get_episode(predecessor.candidate_id).episode_status is EpisodeStatus.OPEN
    assert opportunity_store.get_episode(bundle.candidate_id) is None
    assert opportunity_store.list_episode_relations(predecessor.candidate_id) == ()


def test_store_rechecks_exactly_one_open_momentum_episode(
    opportunity_store, episode_request, lineage
):
    predecessor = _open_momentum(opportunity_store, episode_request)
    _open_momentum(
        opportunity_store,
        episode_request,
        admission_identity="second-momentum",
        episode_started_at=NOW + timedelta(hours=1),
    )
    bundle = _bundle(predecessor, _successor_request(episode_request, lineage), lineage)
    with pytest.raises(ValueError, match="exactly one"):
        opportunity_store.append_orchestration_bundle(bundle)
    assert opportunity_store.get_episode(bundle.candidate_id) is None


def test_wrong_predecessor_family_fails_closed(
    opportunity_store, episode_request, lineage
):
    predecessor = opportunity_store.open_episode(
        replace(
            episode_request,
            setup_family="base_building",
            admission_identity="base",
            episode_started_at=NOW,
        )
    )
    bundle = _bundle(predecessor, _successor_request(episode_request, lineage), lineage)
    with pytest.raises(ValueError, match="momentum_leader to breakout"):
        opportunity_store.append_orchestration_bundle(bundle)


def test_terminal_predecessor_with_different_close_reason_conflicts(
    opportunity_store, episode_request, lineage
):
    predecessor = _open_momentum(opportunity_store, episode_request)
    opportunity_store.close_episode(
        predecessor.candidate_id,
        status=EpisodeStatus.CLOSED,
        closed_at=NOW + timedelta(hours=1),
        closing_reason="manual_close",
        lineage=lineage,
    )
    bundle = _bundle(predecessor, _successor_request(episode_request, lineage), lineage)
    with pytest.raises(
        OpportunityRegistryConflictError, match="candidate_episode_close"
    ):
        opportunity_store.append_orchestration_bundle(bundle)
    assert opportunity_store.get_episode(bundle.candidate_id) is None
