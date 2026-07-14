from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateSnapshot,
    CandidateState,
    EvidenceSnapshot,
    EvidenceVerdict,
    FollowthroughStatus,
    OpportunitySnapshot,
    ProgressStatus,
    RiskLevel,
    StageConfidenceBand,
    StageConfidenceComponents,
    StageSnapshot,
    StageStatus,
    StageTransitionReason,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.validation import select_stage_for_use


NOW = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)


def _opportunity() -> OpportunitySnapshot:
    return OpportunitySnapshot(85, 2, 99, -3, ProgressStatus.IMPROVING, {"relative_strength": 90}, "rank-v1", NOW)


def _evidence() -> EvidenceSnapshot:
    return EvidenceSnapshot(
        88,
        EvidenceVerdict.HIGH_CONVICTION,
        80,
        85,
        90,
        82,
        76,
        85,
        80,
        RiskLevel.LOW,
        RiskLevel.LOW,
        ("volume expansion",),
        (),
        (),
        "investigator-v1",
        NOW,
    )


def test_provisional_and_locked_stage_selection(stage_factory) -> None:
    provisional = stage_factory(
        status=StageStatus.PROVISIONAL,
        provisional=WeinsteinStage.TRANSITION_1_TO_2,
        locked=WeinsteinStage.STAGE_1,
        confidence=76,
    )
    assert provisional.stage_locked_at is None
    assert provisional.effective_stage is WeinsteinStage.TRANSITION_1_TO_2
    assert select_stage_for_use(provisional, "monitoring") is WeinsteinStage.TRANSITION_1_TO_2
    assert select_stage_for_use(provisional, "normal_entry") is WeinsteinStage.STAGE_1
    assert select_stage_for_use(provisional, "early_entry") is WeinsteinStage.TRANSITION_1_TO_2


def test_unknown_stage_accepts_unknown_confidence_band() -> None:
    snapshot = StageSnapshot(
        provisional_stage=WeinsteinStage.UNKNOWN,
        locked_stage=WeinsteinStage.UNKNOWN,
        effective_stage=WeinsteinStage.UNKNOWN,
        stage_status=StageStatus.UNKNOWN,
        confidence_score=0,
        confidence_band=StageConfidenceBand.UNKNOWN,
        confidence_components=StageConfidenceComponents(0, 0, 0, 0, 0, 0),
        stage_as_of=NOW,
        stage_locked_at=None,
        source_week_start=date(2026, 7, 6),
        source_week_end=date(2026, 7, 10),
        previous_locked_stage=None,
        weeks_in_locked_stage=0,
        provisional_persistence_days=0,
        transition_reason=StageTransitionReason.UNKNOWN,
        classifier_version="weekly-stage-v1",
    )
    assert snapshot.confidence_band is StageConfidenceBand.UNKNOWN


def test_stage_validation_rejects_invalid_lock_week_and_time(stage_factory) -> None:
    values = stage_factory().__dict__ if hasattr(stage_factory(), "__dict__") else None
    assert values is None  # slots keep contracts lightweight
    with pytest.raises(ValueError, match="stage_locked_at"):
        stage_factory(status=StageStatus.LOCKED).__class__(
            **{
                field: getattr(stage_factory(status=StageStatus.LOCKED), field)
                for field in stage_factory(status=StageStatus.LOCKED).__dataclass_fields__
                if field != "stage_locked_at"
            },
            stage_locked_at=None,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        original = stage_factory()
        original.__class__(
            **{
                field: (datetime(2026, 7, 14, 10, 0) if field == "stage_as_of" else getattr(original, field))
                for field in original.__dataclass_fields__
            }
        )
    with pytest.raises(ValueError, match="source_week_start"):
        original = stage_factory()
        original.__class__(
            **{
                field: (date(2026, 7, 12) if field == "source_week_start" else getattr(original, field))
                for field in original.__dataclass_fields__
            }
        )


def test_candidate_followthrough_validation(stage_factory, sector_factory) -> None:
    base = dict(
        candidate_id="candidate-1",
        setup_id="setup-1",
        symbol_id="AAA",
        exchange="NSE",
        as_of=NOW,
        opportunity=_opportunity(),
        evidence=_evidence(),
        stock_stage=stage_factory(),
        sector_stage=sector_factory(),
        market_regime="bull",
        sector_regime="leading",
        days_in_state=2,
        days_without_progress=1,
        active_position=False,
        latest_action=CandidateAction.WATCH,
        eligibility=ActionEligibility.NOT_APPLICABLE,
    )
    pending = CandidateSnapshot(
        lifecycle_state=CandidateState.PENDING_FOLLOWTHROUGH,
        followthrough_status=FollowthroughStatus.PENDING_3D,
        **base,
    )
    assert pending.followthrough_status is FollowthroughStatus.PENDING_3D
    confirmed = CandidateSnapshot(
        lifecycle_state=CandidateState.CONFIRMED,
        followthrough_status=FollowthroughStatus.CONFIRMED,
        **base,
    )
    assert confirmed.lifecycle_state is CandidateState.CONFIRMED
    with pytest.raises(ValueError, match="pending follow-through status requires"):
        CandidateSnapshot(
            lifecycle_state=CandidateState.DISCOVERED,
            followthrough_status=FollowthroughStatus.PENDING_3D,
            **base,
        )
    with pytest.raises(ValueError, match="days_in_state"):
        CandidateSnapshot(
            lifecycle_state=CandidateState.DISCOVERED,
            followthrough_status=FollowthroughStatus.NOT_APPLICABLE,
            **{**base, "days_in_state": -1},
        )
