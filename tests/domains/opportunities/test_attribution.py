from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from ai_trading_system.domains.opportunities.attribution import evaluate_stage_attribution
from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    DecisionContextSnapshot,
    FollowthroughStatus,
    OutcomeAttribution,
    OutcomeAttributionRecord,
    RegimeShockEvidence,
    StageForwardObservation,
    StageStatus,
    WeinsteinStage,
)


NOW = datetime(2026, 7, 14, tzinfo=timezone.utc)


def _context(*, status: StageStatus = StageStatus.LOCKED, stage: WeinsteinStage = WeinsteinStage.STAGE_2):
    return DecisionContextSnapshot(
        decision_stage=stage,
        decision_stage_status=status,
        decision_stage_as_of=NOW,
        decision_locked_stage=WeinsteinStage.STAGE_2 if status is StageStatus.LOCKED else WeinsteinStage.STAGE_1,
        decision_provisional_stage=stage if status is StageStatus.PROVISIONAL else WeinsteinStage.UNKNOWN,
        decision_stage_confidence=82,
        decision_sector_stage=WeinsteinStage.STAGE_2,
        decision_sector_stage_status=StageStatus.LOCKED,
        decision_sector_stage_confidence=80,
        opportunity_score=85,
        evidence_score=88,
        lifecycle_state=CandidateState.READY,
        followthrough_status=FollowthroughStatus.NOT_APPLICABLE,
        market_regime="bull",
        sector_regime="leading",
        rank_model_version="rank-v1",
        evidence_model_version="investigator-v1",
        stage_classifier_version="weekly-v1",
        action_policy_version="action-v1",
        execution_policy_version="execution-v1",
        portfolio_context_summary={"heat": 0.2},
    )


def _opposite(week: int) -> StageForwardObservation:
    return StageForwardObservation(week, date(2026, 7, 10), True, True, True, True)


def test_same_week_provisional_reversal_is_not_classification_error() -> None:
    result = evaluate_stage_attribution(
        decision_context=_context(status=StageStatus.PROVISIONAL, stage=WeinsteinStage.TRANSITION_1_TO_2),
        same_week_locked_stage=WeinsteinStage.STAGE_1,
    )
    assert result.category is OutcomeAttribution.PROVISIONAL_STAGE_NONCONFIRMATION


def test_locked_stage2_invalidating_in_window_is_classification_error() -> None:
    result = evaluate_stage_attribution(decision_context=_context(), observations=(_opposite(2), _opposite(3)))
    assert result.category is OutcomeAttribution.STAGE_CLASSIFICATION_ERROR
    assert len(result.supporting_evidence) == 2


def test_later_deterioration_is_valid_transition() -> None:
    observations = (
        StageForwardObservation(2, date(2026, 7, 10), False, False, False, False),
        StageForwardObservation(3, date(2026, 7, 17), False, False, False, False),
        StageForwardObservation(4, date(2026, 7, 24), False, False, False, False),
        _opposite(5),
        _opposite(6),
    )
    result = evaluate_stage_attribution(decision_context=_context(), observations=observations)
    assert result.category is OutcomeAttribution.STAGE_TRANSITION_AFTER_VALID_ENTRY


def test_shock_and_neutral_defaults() -> None:
    shock = evaluate_stage_attribution(
        decision_context=_context(),
        shock_evidence=RegimeShockEvidence(True, ("broad-market circuit event",)),
    )
    assert shock.category is OutcomeAttribution.EXOGENOUS_REGIME_SHOCK
    undetermined = evaluate_stage_attribution(decision_context=_context(), evidence_complete=False)
    assert undetermined.category is OutcomeAttribution.UNDETERMINED
    normal = evaluate_stage_attribution(
        decision_context=_context(),
        observations=(StageForwardObservation(2, date(2026, 7, 10), False, False, False, False),),
    )
    assert normal.category is OutcomeAttribution.VALID_SIGNAL_NORMAL_FAILURE


def test_classification_error_record_requires_supporting_evidence() -> None:
    with pytest.raises(ValueError, match="supporting evidence"):
        OutcomeAttributionRecord(
            "candidate-1",
            "setup-1",
            OutcomeAttribution.STAGE_CLASSIFICATION_ERROR,
            None,
            90,
            "stage-attribution-v1",
            (),
            None,
            NOW,
        )
