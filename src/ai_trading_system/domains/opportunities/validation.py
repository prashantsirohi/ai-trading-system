"""Pure validation and derivation helpers for opportunity contracts."""

from __future__ import annotations

from typing import Literal

from .contracts import (
    CandidateAction,
    CandidateDecision,
    CandidateRetentionPolicy,
    CandidateRetentionRule,
    CandidateState,
    StageConfidenceBand,
    StageConfidenceComponents,
    StageConfidenceResult,
    StageSnapshot,
    StructuralGuardResult,
    WeinsteinStage,
)


StageUseCase = Literal["monitoring", "normal_entry", "early_entry"]


def confidence_band_for_score(score: float) -> StageConfidenceBand:
    """Return the canonical confidence band for a 0-100 score."""
    value = float(score)
    if not 0.0 <= value <= 100.0:
        raise ValueError("confidence score must be between 0 and 100")
    if value < 50.0:
        return StageConfidenceBand.LOW
    if value < 65.0:
        return StageConfidenceBand.MEDIUM
    if value < 80.0:
        return StageConfidenceBand.HIGH
    return StageConfidenceBand.VERY_HIGH


def calculate_stage_confidence(components: StageConfidenceComponents) -> StageConfidenceResult:
    """Calculate the versioned stage confidence score without loading data."""
    score = (
        0.25 * components.ma_slope_quality
        + 0.20 * components.price_position_quality
        + 0.20 * components.relative_strength_quality
        + 0.15 * components.base_breakout_quality
        + 0.10 * components.volume_confirmation
        + 0.10 * components.transition_persistence
        - components.failed_breakout_penalty
    )
    clamped = min(100.0, max(0.0, score))
    return StageConfidenceResult(score=clamped, band=confidence_band_for_score(clamped))


def derive_monitoring_stage(
    provisional_stage: WeinsteinStage,
    locked_stage: WeinsteinStage,
) -> WeinsteinStage:
    """Prefer the informational provisional stage, falling back to locked."""
    return provisional_stage if provisional_stage is not WeinsteinStage.UNKNOWN else locked_stage


def select_stage_for_use(snapshot: StageSnapshot, use_case: StageUseCase) -> WeinsteinStage:
    """Select stage explicitly for monitoring, normal entry, or early entry."""
    if use_case == "monitoring":
        return derive_monitoring_stage(snapshot.provisional_stage, snapshot.locked_stage)
    if use_case == "normal_entry":
        return snapshot.locked_stage
    if use_case == "early_entry":
        return (
            snapshot.provisional_stage
            if snapshot.provisional_stage is WeinsteinStage.TRANSITION_1_TO_2
            else snapshot.locked_stage
        )
    raise ValueError(f"unsupported stage use case: {use_case!r}")


def validate_candidate_decision(
    decision: CandidateDecision,
    *,
    structural_guard: StructuralGuardResult | None = None,
) -> None:
    """Validate decision invariants that depend on an optional guard result."""
    if (
        structural_guard is not None
        and decision.action in {CandidateAction.ENTER, CandidateAction.ADD}
        and decision.eligibility.value in {"eligible", "conditionally_eligible"}
        and not structural_guard.passed
    ):
        raise ValueError("eligible enter/add decision requires a passing structural guard")


def default_candidate_retention_policy() -> CandidateRetentionPolicy:
    """Return the complete Phase-1 retention policy without activating it."""
    rules = (
        CandidateRetentionRule(CandidateState.UNSEEN, 0, 0, actively_retained=False),
        CandidateRetentionRule(CandidateState.DISCOVERED, 5, 3),
        CandidateRetentionRule(CandidateState.INVESTIGATING, 10, 5),
        CandidateRetentionRule(CandidateState.EARLY_ACCUMULATION, 40, 15),
        CandidateRetentionRule(CandidateState.SETUP_FORMING, 20, 10),
        CandidateRetentionRule(CandidateState.READY, 10, 5),
        CandidateRetentionRule(CandidateState.TRIGGERED, 3, 3),
        CandidateRetentionRule(
            CandidateState.PENDING_FOLLOWTHROUGH,
            None,
            None,
            controlled_by_followthrough_window=True,
        ),
        CandidateRetentionRule(CandidateState.CONFIRMED, None, 10),
        CandidateRetentionRule(CandidateState.ADVANCING, None, 10),
        CandidateRetentionRule(CandidateState.EXTENDED, None, 1, review_daily=True),
        CandidateRetentionRule(CandidateState.WEAKENING, 5, 3),
        CandidateRetentionRule(CandidateState.FAILED, 0, 0),
        CandidateRetentionRule(CandidateState.EXITED, 0, 0),
        CandidateRetentionRule(CandidateState.ARCHIVED, 0, 0, actively_retained=False),
    )
    return CandidateRetentionPolicy(rules=rules)
