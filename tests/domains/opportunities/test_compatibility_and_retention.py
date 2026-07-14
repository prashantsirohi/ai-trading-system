from __future__ import annotations

import pytest

from ai_trading_system.domains.opportunities.compatibility import (
    adapt_legacy_weekly_stage,
    map_candidate_tracker_progress,
    map_investigator_status,
    map_legacy_evidence_verdict,
    map_legacy_followthrough,
    map_legacy_stage,
    map_stage1_lifecycle,
)
from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    EvidenceVerdict,
    FollowthroughStatus,
    ProgressStatus,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.validation import default_candidate_retention_policy


@pytest.mark.parametrize(
    ("legacy", "expected"),
    [
        ("S1", WeinsteinStage.STAGE_1),
        ("STAGE_1_ACCUMULATION", WeinsteinStage.STAGE_1),
        ("S1_TO_S2", WeinsteinStage.TRANSITION_1_TO_2),
        ("stage1_to_stage2", WeinsteinStage.TRANSITION_1_TO_2),
        ("S2", WeinsteinStage.STAGE_2),
        ("stage2", WeinsteinStage.STAGE_2),
        ("strong_stage2", WeinsteinStage.STAGE_2),
        ("S3", WeinsteinStage.STAGE_3),
        ("S4", WeinsteinStage.STAGE_4),
        ("UNDEFINED", WeinsteinStage.UNKNOWN),
    ],
)
def test_legacy_stage_values(legacy: str, expected: WeinsteinStage) -> None:
    assert map_legacy_stage(legacy).value is expected


def test_unknown_stage_warns_and_fractional_confidence_is_explicit() -> None:
    unknown = map_legacy_stage("mystery")
    assert unknown.value is WeinsteinStage.UNKNOWN
    assert unknown.warnings
    adapted = adapt_legacy_weekly_stage("S2", 0.83)
    assert adapted.confidence_score == pytest.approx(83)
    assert any("0-1" in warning for warning in adapted.warnings)
    with pytest.raises(ValueError, match="between 0 and 1"):
        adapt_legacy_weekly_stage("S2", 83)


@pytest.mark.parametrize(
    ("legacy", "expected"),
    [
        ("PENDING_3D", FollowthroughStatus.PENDING_3D),
        ("CONFIRMED", FollowthroughStatus.CONFIRMED),
        ("FAILED_3D", FollowthroughStatus.FAILED),
        ("UNKNOWN", FollowthroughStatus.UNKNOWN),
    ],
)
def test_followthrough_compatibility(legacy: str, expected: FollowthroughStatus) -> None:
    assert map_legacy_followthrough(legacy).value is expected


def test_verdict_lifecycle_and_tracker_mappings_are_source_specific() -> None:
    assert map_legacy_evidence_verdict("HIGH_CONVICTION").value is EvidenceVerdict.HIGH_CONVICTION
    assert map_stage1_lifecycle("ACCUMULATING").value is CandidateState.EARLY_ACCUMULATION
    assert map_stage1_lifecycle("BREAKOUT_READY").value is CandidateState.READY
    assert map_stage1_lifecycle("PROMOTION_PENDING", pattern_promotion_state="PENDING_3D").value is CandidateState.PENDING_FOLLOWTHROUGH
    ambiguous = map_investigator_status("HIGH_CONVICTION")
    assert ambiguous.value is None
    assert ambiguous.warnings
    assert map_candidate_tracker_progress("STRONG_IMPROVING").value is ProgressStatus.IMPROVING
    assert map_candidate_tracker_progress("TECHNICAL_FAILURE").value is ProgressStatus.DETERIORATING


def test_retention_policy_covers_every_state_with_separate_limits() -> None:
    policy = default_candidate_retention_policy()
    by_state = {rule.state: rule for rule in policy.rules}
    assert set(by_state) == set(CandidateState)
    assert by_state[CandidateState.DISCOVERED].max_days_in_state == 5
    assert by_state[CandidateState.DISCOVERED].max_days_without_progress == 3
    assert by_state[CandidateState.PENDING_FOLLOWTHROUGH].controlled_by_followthrough_window
    assert by_state[CandidateState.CONFIRMED].max_days_in_state is None
    assert by_state[CandidateState.EXTENDED].review_daily


@pytest.mark.parametrize(
    "legacy",
    ["BASE_BUILDING", "ACCUMULATING", "LATE_STAGE1", "BREAKOUT_READY", "REGRESSED", "STALE_BASE", "INVALIDATED", "ARCHIVED"],
)
def test_every_current_stage1_lifecycle_value_has_an_explicit_mapping(legacy: str) -> None:
    assert map_stage1_lifecycle(legacy).value is not None


@pytest.mark.parametrize(
    "legacy",
    ["NEW_TRIGGER", "TRACKING", "ACTIVE_RESEARCH", "HIGH_CONVICTION", "WATCHLIST", "DROPPED", "ARCHIVED"],
)
def test_every_current_investigator_status_is_explicitly_handled(legacy: str) -> None:
    result = map_investigator_status(legacy)
    assert result.value is not None or result.warnings


@pytest.mark.parametrize(
    "legacy",
    ["STRONG_IMPROVING", "IMPROVING", "STABLE", "WATCH_CAREFULLY", "DETERIORATING", "RESULT_FAILURE", "TECHNICAL_FAILURE", "REMOVE_FROM_TRACKING"],
)
def test_every_candidate_tracker_status_maps_to_progress(legacy: str) -> None:
    assert map_candidate_tracker_progress(legacy).value is not ProgressStatus.UNKNOWN
