"""ADR-0006 A3: column-only policy stamping and replay-hash compatibility."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateDecision,
    CandidateState,
    DecisionContextSnapshot,
    FollowthroughStatus,
    StageStatus,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.registry.models import (
    AppendStatus,
    DecisionContextObservation,
    EpisodeStatus,
    TransitionObservation,
)
from ai_trading_system.domains.opportunities.contracts import TransitionReason


NOW = datetime(2026, 7, 14, 10, tzinfo=timezone.utc)


def _rows(store, sql, params):
    with store.registry._reader() as conn:  # noqa: SLF001
        return conn.execute(sql, params).fetchall()


def test_episode_open_and_close_stamp_policy_snapshot(opportunity_store, episode_request, lineage) -> None:
    stamped_open = replace(lineage, policy_snapshot_id="snap-open")
    episode = opportunity_store.open_episode(replace(episode_request, lineage=stamped_open))
    assert episode.policy_snapshot_id == "snap-open"
    assert episode.closed_policy_snapshot_id is None

    closed = opportunity_store.close_episode(
        episode.candidate_id, status=EpisodeStatus.FAILED, closed_at=NOW + timedelta(days=1),
        closing_reason="failed setup", lineage=replace(lineage, policy_snapshot_id="snap-close"),
    )
    assert closed.policy_snapshot_id == "snap-open"
    assert closed.closed_policy_snapshot_id == "snap-close"


def test_transition_and_decision_rows_carry_policy_snapshot(
    opportunity_store, episode_request, snapshot_builder, lineage
) -> None:
    stamped = replace(lineage, policy_snapshot_id="snap-1")
    episode = opportunity_store.open_episode(replace(episode_request, lineage=stamped))
    snapshot_result = opportunity_store.append_snapshot(snapshot_builder(episode))
    opportunity_store.append_transition(TransitionObservation(
        episode.candidate_id, episode.setup_id, CandidateState.UNSEEN, CandidateState.DISCOVERED,
        TransitionReason.RANK_ADMISSION.value, NOW, snapshot_result.record_id, "lifecycle-v1", {}, stamped,
    ))
    decision = CandidateDecision(
        episode.candidate_id, episode.setup_id, CandidateAction.WATCH, ActionEligibility.NOT_APPLICABLE,
        80, 0, ("monitor",), (), (), "wait for setup", "action-v1", NOW,
    )
    context = DecisionContextSnapshot(
        decision_stage=WeinsteinStage.STAGE_2, decision_stage_status=StageStatus.LOCKED,
        decision_stage_as_of=NOW, decision_locked_stage=WeinsteinStage.STAGE_2,
        decision_provisional_stage=WeinsteinStage.UNKNOWN, decision_stage_confidence=80,
        decision_sector_stage=WeinsteinStage.STAGE_2, decision_sector_stage_status=StageStatus.LOCKED,
        decision_sector_stage_confidence=80, opportunity_score=93, evidence_score=94,
        lifecycle_state=CandidateState.DISCOVERED, followthrough_status=FollowthroughStatus.NOT_APPLICABLE,
        market_regime="bull", sector_regime="leading", rank_model_version="rank-v1",
        evidence_model_version="investigator-v1", stage_classifier_version="weekly-stage-v1",
        action_policy_version="action-v1", execution_policy_version="execution-v1",
        portfolio_context_summary={"blocked": False},
    )
    opportunity_store.append_decision_context(DecisionContextObservation(decision, context, stamped))

    transition_rows = _rows(
        opportunity_store, "SELECT policy_snapshot_id FROM candidate_transition WHERE candidate_id = ?",
        [episode.candidate_id],
    )
    decision_rows = _rows(
        opportunity_store, "SELECT policy_snapshot_id FROM candidate_decision_context WHERE candidate_id = ?",
        [episode.candidate_id],
    )
    assert [row[0] for row in transition_rows] == ["snap-1"]
    assert [row[0] for row in decision_rows] == ["snap-1"]


def test_replay_hash_ignores_policy_snapshot_id(opportunity_store, episode_request, snapshot_builder) -> None:
    episode = opportunity_store.open_episode(episode_request)
    observation = snapshot_builder(episode)
    first = opportunity_store.append_snapshot(observation)
    assert first.status is AppendStatus.CREATED

    restamped = replace(
        observation, lineage=replace(observation.lineage, policy_snapshot_id="post-a3-snapshot"),
    )
    second = opportunity_store.append_snapshot(restamped)
    assert second.status is AppendStatus.DUPLICATE
    assert second.record_id == first.record_id
