from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateDecision,
    CandidateState,
    DecisionContextSnapshot,
    FollowthroughStatus,
    OutcomeAttribution,
    OutcomeAttributionRecord,
    EvidenceSnapshot,
    EvidenceVerdict,
    OpportunitySnapshot,
    ProgressSnapshot,
    ProgressStatus,
    RiskLevel,
    StageStatus,
    TransitionReason,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.registry.models import (
    AttributionObservation,
    DecisionContextObservation,
    EpisodeStatus,
    EvidenceObservation,
    OpportunityObservation,
    OpportunityRegistryConflictError,
    ProgressObservation,
    SnapshotObservation,
    StageObservation,
    StageScope,
    TransitionObservation,
)
from ai_trading_system.domains.opportunities.registry.identity import make_candidate_id, make_setup_id

NOW = datetime(2026, 7, 14, 10, tzinfo=timezone.utc)


def opportunity(at: datetime, score: float) -> OpportunitySnapshot:
    return OpportunitySnapshot(score, 2, 99, -3, ProgressStatus.IMPROVING,
                               {"relative_strength": 90}, "rank-v1", at)


def evidence(at: datetime, score: float) -> EvidenceSnapshot:
    return EvidenceSnapshot(score, EvidenceVerdict.HIGH_CONVICTION, 80, 85, 90, 82, 76, 85, 80,
                            RiskLevel.LOW, RiskLevel.LOW, ("volume expansion",), (), (), "investigator-v1", at)


def test_all_observation_families_and_current_state(
    opportunity_store, episode_request, snapshot_builder, stage_factory, sector_factory, lineage
) -> None:
    episode = opportunity_store.open_episode(episode_request)
    snapshot = replace(
        snapshot_builder(episode),
        last_progress_at=NOW,
        last_retention_counted_session=NOW.date(),
    )
    stock = stage_factory()
    sector = sector_factory(stage=stock)
    stock_obs = StageObservation(episode.candidate_id, episode.setup_id, StageScope.STOCK,
                                 episode.symbol_id, episode.symbol_id, stock, NOW, lineage)
    sector_obs = StageObservation(episode.candidate_id, episode.setup_id, StageScope.SECTOR,
                                  sector.sector_id, sector.sector_name, sector, NOW, lineage)
    snapshot_result, stock_result, sector_result = opportunity_store.append_snapshot_bundle(
        snapshot=snapshot, stock_stage=stock_obs, sector_stage=sector_obs
    )
    assert snapshot_result.created and stock_result.created and sector_result.created
    replay = opportunity_store.append_snapshot(
        replace(
            snapshot,
            last_progress_at=NOW + timedelta(hours=1),
            last_retention_counted_session=NOW.date() + timedelta(days=1),
        )
    )
    assert replay.duplicate

    opportunity_store.append_opportunity_observation(
        OpportunityObservation(episode.candidate_id, episode.setup_id, NOW, NOW, opportunity(NOW, 93), lineage)
    )
    opportunity_store.append_evidence_observation(
        EvidenceObservation(episode.candidate_id, episode.setup_id, NOW, NOW, "investigator",
                            "domains.investigator", "verdict", evidence(NOW, 94), {"complete": True}, lineage)
    )
    progress = ProgressSnapshot(ProgressStatus.IMPROVING, NOW, rank_velocity_improved=True)
    opportunity_store.append_progress(
        ProgressObservation(episode.candidate_id, episode.setup_id, NOW, progress, 0,
                            "progress-v1", {"note": "rank improved"}, lineage)
    )
    transition = TransitionObservation(
        episode.candidate_id, episode.setup_id, CandidateState.UNSEEN, CandidateState.DISCOVERED,
        TransitionReason.RANK_ADMISSION.value, NOW, snapshot_result.record_id, "lifecycle-v1", {}, lineage,
    )
    opportunity_store.append_transition(transition)

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
    opportunity_store.append_decision_context(DecisionContextObservation(decision, context, lineage))
    attribution = OutcomeAttributionRecord(
        episode.candidate_id, episode.setup_id, OutcomeAttribution.VALID_SIGNAL_NORMAL_FAILURE,
        None, 70, "attribution-v1", ("structure valid",), None, NOW + timedelta(hours=1),
    )
    opportunity_store.append_attribution(AttributionObservation(attribution, lineage))

    current = opportunity_store.current_state(episode.candidate_id)
    assert current.latest_opportunity_score == 93
    assert current.latest_evidence_score == 94
    assert current.last_progress_at == NOW
    assert current.last_retention_counted_session == NOW.date()
    assert current.current_stock_stage == WeinsteinStage.STAGE_2.value
    assert current.current_sector_stage == WeinsteinStage.STAGE_2.value
    assert current.current_progress_status == ProgressStatus.IMPROVING.value
    assert current.last_transition_at == NOW
    assert {entry.record_type for entry in opportunity_store.timeline(episode.candidate_id).entries} >= {
        "episode_open", "snapshot", "stock" if False else "stage", "opportunity", "evidence",
        "progress", "transition", "decision", "attribution",
    }


def test_transition_chronology_normalizes_aware_timestamp_to_utc(
    opportunity_store, episode_request, snapshot_builder, lineage
) -> None:
    episode = opportunity_store.open_episode(episode_request)
    snapshot = opportunity_store.append_snapshot(snapshot_builder(episode))
    first_at = NOW + timedelta(hours=2)
    opportunity_store.append_transition(TransitionObservation(
        episode.candidate_id, episode.setup_id, CandidateState.UNSEEN, CandidateState.DISCOVERED,
        TransitionReason.RANK_ADMISSION.value, first_at, snapshot.record_id,
        "lifecycle-v1", {}, lineage,
    ))

    ist = timezone(timedelta(hours=5, minutes=30))
    out_of_order_ist = datetime(2026, 7, 14, 17, 0, tzinfo=ist)  # 11:30 UTC
    with pytest.raises(ValueError, match="chronology must be non-decreasing"):
        opportunity_store.append_transition(TransitionObservation(
            episode.candidate_id, episode.setup_id,
            CandidateState.DISCOVERED, CandidateState.INVESTIGATING,
            TransitionReason.EVIDENCE_IMPROVED.value, out_of_order_ist,
            snapshot.record_id, "lifecycle-v1", {}, lineage,
        ))


def test_batch_conflict_rolls_back_new_rows(opportunity_store, episode_request, snapshot_builder) -> None:
    episode = opportunity_store.open_episode(episode_request)
    first = snapshot_builder(episode)
    opportunity_store.append_snapshot(first)
    later = snapshot_builder(episode, at=NOW + timedelta(days=1), opportunity_score=90)
    conflict = SnapshotObservation(replace(first.snapshot, days_in_state=9), first.observed_at, first.lineage)
    with pytest.raises(OpportunityRegistryConflictError):
        opportunity_store.append_snapshots_batch([later, conflict])
    assert opportunity_store.state_as_of(episode.candidate_id, NOW + timedelta(days=2)).latest_opportunity_score == 85


def test_stage_repainting_and_new_episode_after_close(
    opportunity_store, episode_request, snapshot_builder, stage_factory, sector_factory, lineage
) -> None:
    episode = opportunity_store.open_episode(episode_request)
    tuesday = stage_factory(
        status=StageStatus.PROVISIONAL, provisional=WeinsteinStage.TRANSITION_1_TO_2,
        locked=WeinsteinStage.STAGE_1, confidence=76,
    )
    opportunity_store.append_stage_observation(
        StageObservation(episode.candidate_id, episode.setup_id, StageScope.STOCK, episode.symbol_id,
                         episode.symbol_id, tuesday, NOW, lineage)
    )
    friday_at = NOW + timedelta(days=3)
    friday = replace(
        stage_factory(status=StageStatus.LOCKED, provisional=WeinsteinStage.UNKNOWN,
                      locked=WeinsteinStage.STAGE_1, confidence=70),
        stage_as_of=friday_at, stage_locked_at=friday_at, source_week_start=date(2026, 7, 13),
        source_week_end=date(2026, 7, 17),
    )
    opportunity_store.append_stage_observation(
        StageObservation(episode.candidate_id, episode.setup_id, StageScope.STOCK, episode.symbol_id,
                         episode.symbol_id, friday, friday_at, replace(lineage, run_id="run-2", source_artifact_hash="hash-2"))
    )
    assert opportunity_store.state_as_of(episode.candidate_id, NOW + timedelta(hours=1)).current_stock_stage == WeinsteinStage.TRANSITION_1_TO_2.value
    assert opportunity_store.current_state(episode.candidate_id).current_stock_stage == WeinsteinStage.STAGE_1.value

    opportunity_store.close_episode(episode.candidate_id, status=EpisodeStatus.FAILED,
                                    closed_at=friday_at + timedelta(days=1), closing_reason="failed", lineage=lineage)
    new_request = replace(
        episode_request, admission_identity="run-april:ABC", episode_started_at=NOW + timedelta(days=90),
        lineage=replace(lineage, run_id="run-april", source_artifact_hash="hash-april"),
    )
    second = opportunity_store.open_episode(new_request)
    assert second.episode_number == 2
    assert second.candidate_id != episode.candidate_id
    assert len(opportunity_store.list_episodes(exchange="NSE", symbol_id="ABC")) == 2


def test_episode_snapshot_rollback_and_concurrent_replay(
    opportunity_store, episode_request, snapshot_builder
) -> None:
    existing = opportunity_store.open_episode(episode_request)
    replay_snapshot = snapshot_builder(existing)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: opportunity_store.append_snapshot(replay_snapshot), range(4)))
    assert sum(result.created for result in results) == 1
    assert sum(result.duplicate for result in results) == 3

    later_start = NOW + timedelta(days=10)
    request = replace(
        episode_request, symbol_id="XYZ", admission_identity="run-2:XYZ", episode_started_at=later_start
    )
    setup_id = make_setup_id(
        exchange=request.exchange, symbol_id=request.symbol_id, setup_family=request.setup_family,
        admission_identity=request.admission_identity, episode_started_at=request.episode_started_at,
    )
    candidate_id = make_candidate_id(setup_id)
    invalid_snapshot = replace(
        replay_snapshot.snapshot, candidate_id=candidate_id, setup_id=setup_id, symbol_id="XYZ",
        as_of=later_start - timedelta(days=1),
    )
    with pytest.raises(ValueError, match="precede episode start"):
        opportunity_store.open_episode_with_initial_snapshot(
            request, SnapshotObservation(invalid_snapshot, replay_snapshot.observed_at, replay_snapshot.lineage)
        )
    assert opportunity_store.get_episode(candidate_id) is None
