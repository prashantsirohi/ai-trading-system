from __future__ import annotations

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    FollowthroughStatus,
    STAGE_CONFIDENCE_FORMULA_VERSION,
    StageConfidenceBand,
    StageConfidenceComponents,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.validation import calculate_stage_confidence, confidence_band_for_score


def test_canonical_enum_values_are_stable() -> None:
    assert WeinsteinStage.STAGE_1.value == "stage_1_basing"
    assert WeinsteinStage.TRANSITION_1_TO_2.value == "transition_1_to_2"
    assert WeinsteinStage.STAGE_4.value == "stage_4_declining"
    assert CandidateState.PENDING_FOLLOWTHROUGH.value == "pending_followthrough"
    assert "pending_3d" not in {state.value for state in CandidateState}
    assert FollowthroughStatus.PENDING_3D.value == "pending_3d"


def test_stage_confidence_weighting_penalty_and_version() -> None:
    components = StageConfidenceComponents(100, 80, 60, 40, 20, 0, failed_breakout_penalty=5)
    result = calculate_stage_confidence(components)
    assert result.score == pytest.approx(56.0)
    assert result.band is StageConfidenceBand.MEDIUM
    assert result.formula_version == STAGE_CONFIDENCE_FORMULA_VERSION


def test_stage_confidence_clamps_and_validates_components() -> None:
    low = calculate_stage_confidence(StageConfidenceComponents(0, 0, 0, 0, 0, 0, 25))
    assert low.score == 0
    with pytest.raises(ValueError, match="between 0 and 100"):
        StageConfidenceComponents(101, 0, 0, 0, 0, 0)
    with pytest.raises(ValueError, match="non-negative"):
        StageConfidenceComponents(0, 0, 0, 0, 0, 0, -1)


@pytest.mark.parametrize(
    ("score", "band"),
    [
        (0, StageConfidenceBand.LOW),
        (49, StageConfidenceBand.LOW),
        (50, StageConfidenceBand.MEDIUM),
        (64, StageConfidenceBand.MEDIUM),
        (65, StageConfidenceBand.HIGH),
        (79, StageConfidenceBand.HIGH),
        (80, StageConfidenceBand.VERY_HIGH),
        (100, StageConfidenceBand.VERY_HIGH),
    ],
)
def test_confidence_band_boundaries(score: float, band: StageConfidenceBand) -> None:
    assert confidence_band_for_score(score) is band
