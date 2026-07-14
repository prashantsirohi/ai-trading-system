from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateDecision,
    CandidateState,
    RiskLevel,
    StageStatus,
    StructuralGuardConfig,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.policy import (
    evaluate_early_entry_stage_guard,
    evaluate_normal_entry_stage_guard,
)
from ai_trading_system.domains.opportunities.validation import validate_candidate_decision


def _early(stage_factory, sector_factory, **overrides):
    stock = overrides.pop(
        "stock_stage",
        stage_factory(
            status=StageStatus.PROVISIONAL,
            provisional=WeinsteinStage.TRANSITION_1_TO_2,
            locked=WeinsteinStage.STAGE_1,
            confidence=78,
        ),
    )
    sector = overrides.pop("sector_stage", sector_factory())
    return evaluate_early_entry_stage_guard(
        stock_stage=stock,
        sector_stage=sector,
        lifecycle_state=overrides.pop("lifecycle_state", CandidateState.READY),
        evidence_score=overrides.pop("evidence_score", 85),
        extension_risk=overrides.pop("extension_risk", RiskLevel.LOW),
        market_regime=overrides.pop("market_regime", "bull"),
        **overrides,
    )


def test_early_entry_happy_path_is_conditionally_eligible(stage_factory, sector_factory) -> None:
    result = _early(stage_factory, sector_factory)
    assert result.passed
    assert result.eligibility is ActionEligibility.CONDITIONALLY_ELIGIBLE
    assert result.recommended_max_size_multiplier == 0.35


@pytest.mark.parametrize(
    "change",
    [
        {"evidence_score": 79},
        {"lifecycle_state": CandidateState.SETUP_FORMING},
        {"extension_risk": RiskLevel.HIGH},
        {"market_regime": "risk_off"},
        {"portfolio_blockers": ("portfolio heat exceeded",)},
    ],
)
def test_early_entry_blocks_non_structural_requirements(stage_factory, sector_factory, change) -> None:
    result = _early(stage_factory, sector_factory, **change)
    assert not result.passed
    assert result.eligibility is ActionEligibility.BLOCKED
    assert result.recommended_max_size_multiplier == 0


def test_early_entry_blocks_low_confidence_both_provisional_and_stage4_sector(stage_factory, sector_factory) -> None:
    low = stage_factory(
        status=StageStatus.PROVISIONAL,
        provisional=WeinsteinStage.TRANSITION_1_TO_2,
        locked=WeinsteinStage.STAGE_1,
        confidence=74,
    )
    assert not _early(stage_factory, sector_factory, stock_stage=low).passed

    provisional_sector = sector_factory(
        stage=stage_factory(
            status=StageStatus.PROVISIONAL,
            provisional=WeinsteinStage.TRANSITION_1_TO_2,
            locked=WeinsteinStage.STAGE_1,
        )
    )
    both = _early(stage_factory, sector_factory, sector_stage=provisional_sector)
    assert not both.passed
    assert any("both stock and sector provisional" in blocker for blocker in both.blockers)

    stage4_sector = sector_factory(stage=stage_factory(locked=WeinsteinStage.STAGE_4))
    assert not _early(stage_factory, sector_factory, sector_stage=stage4_sector).passed


def test_pilot_multiplier_cannot_exceed_cap() -> None:
    with pytest.raises(ValueError, match="0.40"):
        StructuralGuardConfig(pilot_size_multiplier=0.41)


def test_normal_entry_requires_locked_stage2(stage_factory, sector_factory) -> None:
    eligible = evaluate_normal_entry_stage_guard(
        stock_stage=stage_factory(),
        sector_stage=sector_factory(),
        sector_regime="leading",
    )
    assert eligible.passed
    assert eligible.eligibility is ActionEligibility.ELIGIBLE

    provisional = stage_factory(
        status=StageStatus.PROVISIONAL,
        provisional=WeinsteinStage.STAGE_2,
        locked=WeinsteinStage.STAGE_1,
    )
    assert not evaluate_normal_entry_stage_guard(
        stock_stage=provisional,
        sector_stage=sector_factory(),
        sector_regime="leading",
    ).passed
    assert not evaluate_normal_entry_stage_guard(
        stock_stage=stage_factory(locked=WeinsteinStage.STAGE_3),
        sector_stage=sector_factory(),
        sector_regime="leading",
    ).passed
    assert not evaluate_normal_entry_stage_guard(
        stock_stage=stage_factory(),
        sector_stage=sector_factory(stage=stage_factory(locked=WeinsteinStage.STAGE_3)),
        sector_regime="leading",
    ).passed


def test_enter_decision_cannot_ignore_failed_guard(stage_factory, sector_factory) -> None:
    guard = _early(stage_factory, sector_factory, evidence_score=40)
    decision = CandidateDecision(
        candidate_id="candidate-1",
        setup_id="setup-1",
        action=CandidateAction.ENTER,
        eligibility=ActionEligibility.ELIGIBLE,
        confidence=80,
        size_multiplier=0.35,
        reasons=("pilot",),
        blockers=(),
        warnings=(),
        next_required_event=None,
        policy_version="future-policy-v1",
        decided_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
    )
    with pytest.raises(ValueError, match="passing structural guard"):
        validate_candidate_decision(decision, structural_guard=guard)
