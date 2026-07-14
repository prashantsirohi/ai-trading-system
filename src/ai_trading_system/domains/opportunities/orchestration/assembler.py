"""Candidate snapshot assembly without persistence side effects."""

from __future__ import annotations

from datetime import timedelta

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateSnapshot,
    CandidateState,
    FollowthroughStatus,
    SectorStageSnapshot,
    StageConfidenceBand,
    StageSnapshot,
    StageStatus,
    StageTransitionReason,
    WeinsteinStage,
)
from .contracts import LEGACY_STAGE_CONFIDENCE_VERSION, OpportunitySourceBundle


def unknown_stage(bundle: OpportunitySourceBundle, *, classifier_version: str) -> StageSnapshot:
    week_end = bundle.as_of.date()
    week_start = week_end - timedelta(days=week_end.weekday())
    return StageSnapshot(
        provisional_stage=WeinsteinStage.UNKNOWN,
        locked_stage=WeinsteinStage.UNKNOWN,
        effective_stage=WeinsteinStage.UNKNOWN,
        stage_status=StageStatus.UNKNOWN,
        confidence_score=0.0,
        confidence_band=StageConfidenceBand.UNKNOWN,
        confidence_components=None,
        stage_as_of=bundle.as_of,
        stage_locked_at=None,
        source_week_start=week_start,
        source_week_end=week_end,
        previous_locked_stage=None,
        weeks_in_locked_stage=0,
        provisional_persistence_days=0,
        transition_reason=StageTransitionReason.UNKNOWN,
        classifier_version=classifier_version,
        confidence_formula_version=LEGACY_STAGE_CONFIDENCE_VERSION,
    )


def assemble_candidate_snapshot(
    *, candidate_id: str, setup_id: str, bundle: OpportunitySourceBundle,
    lifecycle_state: CandidateState, days_in_state: int, days_without_progress: int,
    active_position: bool,
) -> CandidateSnapshot | None:
    if bundle.opportunity is None or bundle.evidence is None:
        return None
    stock = bundle.stock_stage or unknown_stage(bundle, classifier_version="stock-stage-unavailable-v1")
    sector = bundle.sector_stage or SectorStageSnapshot(
        sector_id=bundle.sector_name.upper().replace(" ", "_") or "UNKNOWN",
        sector_name=bundle.sector_name or "unknown",
        stage_snapshot=unknown_stage(bundle, classifier_version="sector-stage-unavailable-v1"),
        sector_relative_strength_state="unknown",
        sector_rotation_state="unknown",
    )
    followthrough = _compatible_followthrough(lifecycle_state, bundle.followthrough_status)
    return CandidateSnapshot(
        candidate_id=candidate_id,
        setup_id=setup_id,
        symbol_id=bundle.symbol_id,
        exchange=bundle.exchange,
        as_of=bundle.as_of,
        opportunity=bundle.opportunity,
        evidence=bundle.evidence,
        lifecycle_state=lifecycle_state,
        followthrough_status=followthrough,
        stock_stage=stock,
        sector_stage=sector,
        market_regime=bundle.market_regime,
        sector_regime=bundle.sector_regime,
        days_in_state=max(days_in_state, 0),
        days_without_progress=max(days_without_progress, 0),
        active_position=active_position,
        latest_action=CandidateAction.WATCH,
        eligibility=ActionEligibility.UNKNOWN,
    )


def _compatible_followthrough(state: CandidateState, supplied: FollowthroughStatus) -> FollowthroughStatus:
    pending = {FollowthroughStatus.PENDING_1D, FollowthroughStatus.PENDING_3D, FollowthroughStatus.PENDING_5D}
    if state is CandidateState.PENDING_FOLLOWTHROUGH:
        return supplied if supplied in pending else FollowthroughStatus.PENDING_3D
    if state is CandidateState.CONFIRMED:
        return FollowthroughStatus.CONFIRMED
    return FollowthroughStatus.UNKNOWN if supplied in pending else supplied
