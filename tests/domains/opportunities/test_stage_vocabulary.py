"""Contract tests for the legacy/canonical stage vocabulary bridge.

Two spellings coexist in the stores: the legacy `S1..S4/UNDEFINED` codes and
the canonical `WeinsteinStage` values. The mapping between them is
deliberately asymmetric, so these tests assert totality in one direction and
partiality in the other rather than a round trip that cannot exist.
"""

from __future__ import annotations

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    LEGACY_STAGE_MAP,
    WeinsteinStage,
    is_transition,
    legacy_code_for,
    normalize_stage,
    stage_family,
)

TRANSITIONS = {
    WeinsteinStage.TRANSITION_1_TO_2,
    WeinsteinStage.TRANSITION_2_TO_3,
    WeinsteinStage.TRANSITION_3_TO_4,
    WeinsteinStage.TRANSITION_4_TO_1,
}


def test_legacy_to_canonical_is_total() -> None:
    """Every legacy code resolves to a distinct canonical stage."""

    resolved = {code: normalize_stage(code) for code in LEGACY_STAGE_MAP}
    assert set(resolved) == {"S1", "S2", "S3", "S4", "UNDEFINED"}
    assert all(isinstance(stage, WeinsteinStage) for stage in resolved.values())
    assert len(set(resolved.values())) == len(resolved)


def test_canonical_to_legacy_is_partial_exactly_at_the_transitions() -> None:
    """The four transition members are precisely the ones with no legacy code."""

    without_code = {
        stage for stage in WeinsteinStage if legacy_code_for(stage) is None
    }
    assert without_code == TRANSITIONS

    with_code = {stage for stage in WeinsteinStage if legacy_code_for(stage)}
    assert with_code == set(WeinsteinStage) - TRANSITIONS
    assert len(with_code) == 5


def test_settled_stages_round_trip() -> None:
    """Round tripping is lossless for the five stages that have a legacy code."""

    for stage in set(WeinsteinStage) - TRANSITIONS:
        code = legacy_code_for(stage)
        assert code is not None
        assert normalize_stage(code) is stage


def test_is_transition_agrees_with_the_mapping() -> None:
    for stage in WeinsteinStage:
        assert is_transition(stage) is (stage in TRANSITIONS)


def test_every_stage_has_a_family() -> None:
    families = {stage: stage_family(stage) for stage in WeinsteinStage}
    assert set(families.values()) <= {
        "stage_1",
        "stage_2",
        "stage_3",
        "stage_4",
        "unknown",
    }
    assert len(families) == len(WeinsteinStage)


@pytest.mark.parametrize(
    ("stage", "expected"),
    [
        (WeinsteinStage.TRANSITION_1_TO_2, "stage_1"),
        (WeinsteinStage.TRANSITION_2_TO_3, "stage_2"),
        (WeinsteinStage.TRANSITION_3_TO_4, "stage_3"),
        (WeinsteinStage.TRANSITION_4_TO_1, "stage_4"),
    ],
)
def test_transition_family_is_the_stage_being_left(
    stage: WeinsteinStage, expected: str
) -> None:
    """A transition has not confirmed its destination, so it reports its origin."""

    assert stage_family(stage) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("S2", WeinsteinStage.STAGE_2),
        ("s2", WeinsteinStage.STAGE_2),
        (" S2 ", WeinsteinStage.STAGE_2),
        ("stage_2_advancing", WeinsteinStage.STAGE_2),
        ("STAGE_2_ADVANCING", WeinsteinStage.STAGE_2),
        (WeinsteinStage.STAGE_2, WeinsteinStage.STAGE_2),
        ("transition_1_to_2", WeinsteinStage.TRANSITION_1_TO_2),
    ],
)
def test_normalize_accepts_either_spelling(
    value: object, expected: WeinsteinStage
) -> None:
    assert normalize_stage(value) is expected


@pytest.mark.parametrize("value", [None, "", "   ", "nonsense", 7])
def test_normalize_degrades_to_unknown(value: object) -> None:
    """One odd row must not fail an entire read."""

    assert normalize_stage(value) is WeinsteinStage.UNKNOWN


def test_mapping_is_immutable() -> None:
    with pytest.raises(TypeError):
        LEGACY_STAGE_MAP["S5"] = WeinsteinStage.UNKNOWN  # type: ignore[index]


def test_coverage_module_reuses_the_same_mapping() -> None:
    """coverage.py must not keep a divergent private copy."""

    from ai_trading_system.domains.opportunities import coverage

    assert coverage.LEGACY_STAGE_MAP is LEGACY_STAGE_MAP
