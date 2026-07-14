"""Explicit, warning-bearing adapters for currently emitted legacy values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from .contracts import (
    CandidateState,
    EvidenceVerdict,
    FollowthroughStatus,
    ProgressStatus,
    WeinsteinStage,
)


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class CompatibilityResult(Generic[T]):
    value: T
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class LegacyWeeklyStageResult:
    stage: WeinsteinStage
    confidence_score: float
    warnings: tuple[str, ...] = ()


_STAGE_VALUES = {
    "S1": WeinsteinStage.STAGE_1,
    "STAGE_1": WeinsteinStage.STAGE_1,
    "STAGE_1_BASE": WeinsteinStage.STAGE_1,
    "STAGE_1_ACCUMULATION": WeinsteinStage.STAGE_1,
    "STAGE_1_LATE": WeinsteinStage.STAGE_1,
    "STAGE_1_BREAKOUT_READY": WeinsteinStage.STAGE_1,
    "STAGE_1_REPAIR": WeinsteinStage.STAGE_1,
    "S1_TO_S2": WeinsteinStage.TRANSITION_1_TO_2,
    "S1_TO_S2_TRANSITION": WeinsteinStage.TRANSITION_1_TO_2,
    "STAGE_1_TO_2": WeinsteinStage.TRANSITION_1_TO_2,
    "STAGE_1_TO_2_ACCUMULATION": WeinsteinStage.TRANSITION_1_TO_2,
    "STAGE1_TO_STAGE2": WeinsteinStage.TRANSITION_1_TO_2,
    "S2": WeinsteinStage.STAGE_2,
    "STAGE2": WeinsteinStage.STAGE_2,
    "STRONG_STAGE2": WeinsteinStage.STAGE_2,
    "STAGE2_UPTREND": WeinsteinStage.STAGE_2,
    "STAGE_2": WeinsteinStage.STAGE_2,
    "STAGE_2_CONFIRMED": WeinsteinStage.STAGE_2,
    "STAGE_2_EARLY": WeinsteinStage.STAGE_2,
    "S2_CONFIRMED": WeinsteinStage.STAGE_2,
    "S2_TO_S3": WeinsteinStage.TRANSITION_2_TO_3,
    "S3": WeinsteinStage.STAGE_3,
    "STAGE_3": WeinsteinStage.STAGE_3,
    "STAGE_3_DISTRIBUTION": WeinsteinStage.STAGE_3,
    "S3_TO_S4": WeinsteinStage.TRANSITION_3_TO_4,
    "S4": WeinsteinStage.STAGE_4,
    "STAGE_4": WeinsteinStage.STAGE_4,
    "STAGE_4_DECLINE": WeinsteinStage.STAGE_4,
    "S4_TO_S1": WeinsteinStage.TRANSITION_4_TO_1,
    "UNDEFINED": WeinsteinStage.UNKNOWN,
    "UNKNOWN": WeinsteinStage.UNKNOWN,
    "NON_STAGE2": WeinsteinStage.UNKNOWN,
}


def map_legacy_stage(value: object) -> CompatibilityResult[WeinsteinStage]:
    """Map a known legacy stage label; unknown values remain explicit."""
    normalized = str(value or "").strip().upper().replace("-", "_")
    stage = _STAGE_VALUES.get(normalized)
    if stage is None:
        return CompatibilityResult(
            WeinsteinStage.UNKNOWN,
            (f"unrecognized legacy stage value {value!r}; mapped to unknown",),
        )
    return CompatibilityResult(stage)


def adapt_legacy_weekly_stage(stage_label: object, confidence: object) -> LegacyWeeklyStageResult:
    """Adapt the weekly engine's label and fractional confidence explicitly."""
    mapped = map_legacy_stage(stage_label)
    warnings = list(mapped.warnings)
    try:
        raw = float(confidence)
    except (TypeError, ValueError):
        raw = 0.0
        warnings.append(f"invalid legacy stage confidence {confidence!r}; used 0")
    if not 0.0 <= raw <= 1.0:
        raise ValueError("legacy weekly stage confidence must be between 0 and 1")
    warnings.append("converted legacy weekly confidence from 0-1 to canonical 0-100")
    return LegacyWeeklyStageResult(mapped.value, raw * 100.0, tuple(warnings))


def map_legacy_followthrough(value: object) -> CompatibilityResult[FollowthroughStatus]:
    normalized = str(value or "").strip().upper()
    mapping = {
        "NOT_APPLICABLE": FollowthroughStatus.NOT_APPLICABLE,
        "PENDING_1D": FollowthroughStatus.PENDING_1D,
        "PENDING_3D": FollowthroughStatus.PENDING_3D,
        "PENDING_5D": FollowthroughStatus.PENDING_5D,
        "CONFIRMED": FollowthroughStatus.CONFIRMED,
        "FAILED": FollowthroughStatus.FAILED,
        "FAILED_3D": FollowthroughStatus.FAILED,
        "EXPIRED": FollowthroughStatus.EXPIRED,
        "UNKNOWN": FollowthroughStatus.UNKNOWN,
    }
    if normalized in mapping:
        return CompatibilityResult(mapping[normalized])
    return CompatibilityResult(
        FollowthroughStatus.UNKNOWN,
        (f"unrecognized legacy follow-through value {value!r}; mapped to unknown",),
    )


def map_legacy_evidence_verdict(value: object) -> CompatibilityResult[EvidenceVerdict]:
    normalized = str(value or "").strip().upper()
    mapping = {
        "HIGH_CONVICTION": EvidenceVerdict.HIGH_CONVICTION,
        "MEDIUM_CONVICTION": EvidenceVerdict.MEDIUM_CONVICTION,
        "WATCH_ONLY": EvidenceVerdict.WATCH_ONLY,
        "NOISE_TRAP": EvidenceVerdict.NOISE_TRAP,
    }
    if normalized in mapping:
        return CompatibilityResult(mapping[normalized])
    return CompatibilityResult(
        EvidenceVerdict.UNKNOWN,
        (f"unrecognized Investigator verdict {value!r}; mapped to unknown",),
    )


def map_stage1_lifecycle(
    value: object,
    *,
    pattern_promotion_state: object | None = None,
) -> CompatibilityResult[CandidateState | None]:
    """Map Stage-1 lifecycle values, requiring context for promotion pending."""
    normalized = str(value or "").strip().upper()
    mapping = {
        "DISCOVERED": CandidateState.DISCOVERED,
        "BASE_BUILDING": CandidateState.SETUP_FORMING,
        "ACCUMULATING": CandidateState.EARLY_ACCUMULATION,
        "LATE_STAGE1": CandidateState.SETUP_FORMING,
        "BREAKOUT_READY": CandidateState.READY,
        "REGRESSED": CandidateState.WEAKENING,
        "STALE_BASE": CandidateState.WEAKENING,
        "INVALIDATED": CandidateState.FAILED,
        "ARCHIVED": CandidateState.ARCHIVED,
    }
    if normalized in mapping:
        return CompatibilityResult(mapping[normalized])
    if normalized == "PROMOTION_PENDING":
        promotion = str(pattern_promotion_state or "").strip().upper()
        if promotion == "PENDING_3D":
            return CompatibilityResult(CandidateState.PENDING_FOLLOWTHROUGH)
        if promotion == "BREAKOUT_ATTEMPT":
            return CompatibilityResult(CandidateState.TRIGGERED)
        return CompatibilityResult(
            None,
            ("PROMOTION_PENDING requires PENDING_3D or BREAKOUT_ATTEMPT context; no lifecycle value returned",),
        )
    return CompatibilityResult(
        None,
        (f"unrecognized Stage-1 lifecycle value {value!r}; no lifecycle value returned",),
    )


def map_investigator_status(value: object) -> CompatibilityResult[CandidateState | None]:
    """Map only unambiguous Investigator lifecycle statuses."""
    normalized = str(value or "").strip().upper()
    if normalized == "NEW_TRIGGER":
        return CompatibilityResult(CandidateState.DISCOVERED)
    if normalized in {"DROPPED", "ARCHIVED"}:
        return CompatibilityResult(CandidateState.ARCHIVED)
    if normalized in {"TRACKING", "ACTIVE_RESEARCH", "HIGH_CONVICTION", "WATCHLIST"}:
        return CompatibilityResult(
            None,
            (f"Investigator status {normalized} encodes monitoring/conviction, not canonical lifecycle; no lifecycle value returned",),
        )
    return CompatibilityResult(
        None,
        (f"unrecognized Investigator status {value!r}; no lifecycle value returned",),
    )


def map_candidate_tracker_progress(value: object) -> CompatibilityResult[ProgressStatus]:
    """Map tracker health to progress, never to lifecycle."""
    normalized = str(value or "").strip().upper()
    mapping = {
        "STRONG_IMPROVING": ProgressStatus.IMPROVING,
        "IMPROVING": ProgressStatus.IMPROVING,
        "STABLE": ProgressStatus.STABLE,
        "WATCH_CAREFULLY": ProgressStatus.STALLED,
        "DETERIORATING": ProgressStatus.DETERIORATING,
        "RESULT_FAILURE": ProgressStatus.DETERIORATING,
        "TECHNICAL_FAILURE": ProgressStatus.DETERIORATING,
        "REMOVE_FROM_TRACKING": ProgressStatus.DETERIORATING,
    }
    if normalized in mapping:
        return CompatibilityResult(mapping[normalized])
    return CompatibilityResult(
        ProgressStatus.UNKNOWN,
        (f"unrecognized candidate-tracker status {value!r}; mapped to unknown progress",),
    )
