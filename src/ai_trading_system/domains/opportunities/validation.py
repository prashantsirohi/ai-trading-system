"""Pure validation and derivation helpers for opportunity contracts."""

from __future__ import annotations

from types import MappingProxyType
from typing import Literal, Mapping

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

# Single source of truth for stage-confidence-v1 semantics. The policy
# fingerprint (ADR-0006 A3) hashes these exact objects at runtime, so editing
# a value without bumping the version label raises
# POLICY_VERSION_CONTENT_MISMATCH instead of silently drifting.
STAGE_CONFIDENCE_WEIGHTS: Mapping[str, float] = MappingProxyType({
    "ma_slope_quality": 0.25,
    "price_position_quality": 0.20,
    "relative_strength_quality": 0.20,
    "base_breakout_quality": 0.15,
    "volume_confirmation": 0.10,
    "transition_persistence": 0.10,
})
STAGE_CONFIDENCE_BAND_BOUNDS: Mapping[str, float] = MappingProxyType({
    "low_below": 50.0,
    "medium_below": 65.0,
    "high_below": 80.0,
})


def confidence_band_for_score(score: float) -> StageConfidenceBand:
    """Return the canonical confidence band for a 0-100 score."""
    value = float(score)
    if not 0.0 <= value <= 100.0:
        raise ValueError("confidence score must be between 0 and 100")
    if value < STAGE_CONFIDENCE_BAND_BOUNDS["low_below"]:
        return StageConfidenceBand.LOW
    if value < STAGE_CONFIDENCE_BAND_BOUNDS["medium_below"]:
        return StageConfidenceBand.MEDIUM
    if value < STAGE_CONFIDENCE_BAND_BOUNDS["high_below"]:
        return StageConfidenceBand.HIGH
    return StageConfidenceBand.VERY_HIGH


def calculate_stage_confidence(components: StageConfidenceComponents) -> StageConfidenceResult:
    """Calculate the versioned stage confidence score without loading data."""
    weights = STAGE_CONFIDENCE_WEIGHTS
    score = (
        weights["ma_slope_quality"] * components.ma_slope_quality
        + weights["price_position_quality"] * components.price_position_quality
        + weights["relative_strength_quality"] * components.relative_strength_quality
        + weights["base_breakout_quality"] * components.base_breakout_quality
        + weights["volume_confirmation"] * components.volume_confirmation
        + weights["transition_persistence"] * components.transition_persistence
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
