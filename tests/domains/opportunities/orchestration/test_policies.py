from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    EvidenceSnapshot,
    EvidenceVerdict,
    FollowthroughStatus,
    OpportunitySnapshot,
    ProgressStatus,
    RiskLevel,
    StageStatus,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.orchestration.admission import evaluate_admission
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    OpportunityShadowConfig,
    OpportunitySourceBundle,
    SectorGateEvidence,
)
from ai_trading_system.domains.opportunities.orchestration.contracts import BreakoutEvidence
from ai_trading_system.domains.opportunities.orchestration.retention import evaluate_retention
from ai_trading_system.domains.opportunities.orchestration.transitions import evaluate_transition


NOW = datetime(2026, 7, 14, tzinfo=timezone.utc)


def _bundle(stage_factory, sector_factory, *, stock=None, evidence=90, lifecycle=None, followthrough=FollowthroughStatus.UNKNOWN):
    stock = stock or stage_factory(status=StageStatus.LOCKED, locked=WeinsteinStage.STAGE_2)
    return OpportunitySourceBundle(
        "ABC", "NSE", NOW,
        OpportunitySnapshot(90, 1, 99, None, ProgressStatus.UNKNOWN, {}, "rank-v1", NOW),
        EvidenceSnapshot(evidence, EvidenceVerdict.HIGH_CONVICTION, 80, 90, 90, None, None, 90, None, RiskLevel.LOW, RiskLevel.LOW, (), (), (), "inv-v1", NOW),
        stock,
        sector_factory(),
        lifecycle,
        followthrough,
        sector_name="Capital Goods",
        market_regime="bull",
        sector_regime="leading",
    )


def test_rank_admission_and_stage3_block(stage_factory, sector_factory):
    bundle = _bundle(stage_factory, sector_factory)
    result = evaluate_admission(bundle, OpportunityShadowConfig())
    assert result.admitted
    stage3 = replace(bundle, stock_stage=stage_factory(locked=WeinsteinStage.STAGE_3))
    assert not evaluate_admission(stage3, OpportunityShadowConfig()).admitted


def test_ready_trigger_requires_real_event(stage_factory, sector_factory):
    bundle = _bundle(stage_factory, sector_factory)
    result = evaluate_transition(CandidateState.READY, bundle)
    assert not result.allowed
    assert "missing legitimate breakout trigger" in result.blockers


def test_confirmed_followthrough_collapses_missing_pending_observation(stage_factory, sector_factory):
    bundle = _bundle(stage_factory, sector_factory, followthrough=FollowthroughStatus.CONFIRMED)
    result = evaluate_transition(CandidateState.TRIGGERED, bundle)
    assert result.proposed_state is CandidateState.CONFIRMED
    assert result.metadata["collapsed_pending_followthrough"] is True


@pytest.mark.parametrize(
    "cause",
    [
        "missing_sector_mapping",
        "latest_only_untrusted_membership",
        "insufficient_constituent_coverage",
        "sector_not_stage_2",
        "sector_snapshot_not_locked",
        "sector_locked_snapshot_missing",
    ],
)
def test_provisional_trigger_emits_exact_sector_gate_taxonomy(
    stage_factory, sector_factory, cause
):
    stock = stage_factory(
        status=StageStatus.PROVISIONAL,
        provisional=WeinsteinStage.TRANSITION_1_TO_2,
        confidence=90,
    )
    bundle = replace(
        _bundle(stage_factory, sector_factory, stock=stock),
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
        sector_gate=SectorGateEvidence(taxonomy_cause=cause),
    )
    result = evaluate_transition(CandidateState.READY, bundle)
    assert not result.allowed
    assert cause in result.blockers


def test_provisional_trigger_uses_prior_locked_stage2_not_current_provisional(
    stage_factory, sector_factory
):
    stock = stage_factory(
        status=StageStatus.PROVISIONAL,
        provisional=WeinsteinStage.TRANSITION_1_TO_2,
        confidence=90,
    )
    current_sector = sector_factory(
        stage=stage_factory(
            status=StageStatus.PROVISIONAL,
            provisional=WeinsteinStage.TRANSITION_1_TO_2,
        )
    )
    bundle = replace(
        _bundle(stage_factory, sector_factory, stock=stock),
        sector_stage=current_sector,
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
        sector_gate=SectorGateEvidence(
            prior_locked_stage=WeinsteinStage.STAGE_2,
            current_provisional_stage=WeinsteinStage.TRANSITION_1_TO_2,
        ),
    )
    result = evaluate_transition(CandidateState.READY, bundle)
    assert result.allowed
    assert result.proposed_state is CandidateState.TRIGGERED


def test_normal_locked_stage2_does_not_consult_sector_gate(stage_factory, sector_factory):
    bundle = replace(
        _bundle(stage_factory, sector_factory),
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
        sector_gate=SectorGateEvidence(taxonomy_cause="missing_sector_mapping"),
    )
    assert evaluate_transition(CandidateState.READY, bundle).allowed


def test_retention_uses_age_and_stagnation_independently():
    retained = evaluate_retention(
        state=CandidateState.SETUP_FORMING, days_in_state=21, days_without_progress=5,
        progress_status=ProgressStatus.STABLE, stock_stage=WeinsteinStage.STAGE_1,
    )
    closed = evaluate_retention(
        state=CandidateState.SETUP_FORMING, days_in_state=21, days_without_progress=11,
        progress_status=ProgressStatus.STALLED, stock_stage=WeinsteinStage.STAGE_1,
    )
    assert retained.retain
    assert closed.close_episode
