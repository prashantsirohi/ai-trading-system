from __future__ import annotations

from copy import deepcopy

import pytest

from ai_trading_system.domains.opportunities.calibration import (
    CalibrationConfig,
    CalibrationIntegrityError,
    EligibilityStatus,
    ExclusionReason,
    OutcomeStatus,
    build_calibration_dataset,
    compare_manifests,
    deterministic_sample_id,
    evaluate_calibration_eligibility,
)
from ai_trading_system.interfaces.cli.build_phase3c5_calibration import fixture_rows


def _row() -> dict:
    return fixture_rows("small_fixture", count=1)[0]


def test_valid_point_in_time_sample_is_eligible() -> None:
    result = evaluate_calibration_eligibility(_row())
    assert result.eligibility_status is EligibilityStatus.ELIGIBLE
    assert result.eligible is True


def test_input_available_after_decision_is_excluded() -> None:
    row = _row()
    row["input_available_at"] = "2026-01-01T00:00:00+00:00"
    result = evaluate_calibration_eligibility(row)
    assert ExclusionReason.INPUT_AVAILABLE_AFTER_DECISION.value in result.exclusion_reasons


def test_late_correction_does_not_leak_backward() -> None:
    row = _row()
    row.update({"correction_used": True, "correction_recorded_at": "2026-01-01T00:00:00+00:00"})
    result = evaluate_calibration_eligibility(row)
    assert ExclusionReason.LATE_CORRECTION_NOT_AVAILABLE_AS_OF_DECISION.value in result.exclusion_reasons


@pytest.mark.parametrize("field,reason", [
    ("stage_governance_conflict", ExclusionReason.STAGE_GOVERNANCE_CONFLICT),
    ("stage_governance_cycle", ExclusionReason.STAGE_SUPERSESSION_CYCLE),
])
def test_stage_governance_defects_are_quarantined(field: str, reason: ExclusionReason) -> None:
    row = _row()
    row[field] = True
    result = evaluate_calibration_eligibility(row)
    assert result.eligibility_status is EligibilityStatus.QUARANTINED
    assert reason.value in result.exclusion_reasons


def test_membership_trust_guards() -> None:
    latest = _row()
    latest["membership_trust"] = "LATEST_ONLY_BACKFILL"
    assert ExclusionReason.SECTOR_MEMBERSHIP_LATEST_ONLY.value in evaluate_calibration_eligibility(latest).exclusion_reasons
    observed = _row()
    observed["membership_trust"] = "OBSERVED_AT_RUN"
    observed["membership_recorded_at"] = "2026-01-01T00:00:00+00:00"
    assert ExclusionReason.INPUT_AVAILABLE_AFTER_DECISION.value in evaluate_calibration_eligibility(observed).exclusion_reasons


@pytest.mark.parametrize("status,reason", [
    ("unresolved_legacy_no_match", ExclusionReason.UNRESOLVED_LEGACY_NO_MATCH),
    ("unresolved_legacy_ambiguous", ExclusionReason.UNRESOLVED_LEGACY_AMBIGUOUS),
])
def test_unresolved_correction_impacts_are_quarantined(status: str, reason: ExclusionReason) -> None:
    row = _row()
    row["correction_impact_status"] = status
    result = evaluate_calibration_eligibility(row)
    assert result.eligibility_status is EligibilityStatus.QUARANTINED
    assert reason.value in result.exclusion_reasons


def test_resolved_correction_impact_may_be_eligible() -> None:
    assert evaluate_calibration_eligibility(_row()).correction_impact_resolved is True


def test_review_required_correction_is_excluded_not_quarantined() -> None:
    row = _row()
    row["correction_review_required"] = True
    result = evaluate_calibration_eligibility(row)
    assert result.eligibility_status is EligibilityStatus.EXCLUDED
    assert ExclusionReason.CORRECTION_IMPACT_REVIEW_REQUIRED.value in result.exclusion_reasons


def test_omitted_verification_fields_fail_closed() -> None:
    row = _row()
    for field in (
        "stage_observation_verified", "correction_impact_status",
        "authoritative_calibration_eligible", "market_data_complete",
    ):
        row.pop(field)
    result = evaluate_calibration_eligibility(row)
    assert result.eligible is False
    assert ExclusionReason.STAGE_OBSERVATION_UNVERIFIED.value in result.exclusion_reasons
    assert ExclusionReason.CORRECTION_IMPACT_REVIEW_REQUIRED.value in result.exclusion_reasons
    assert ExclusionReason.MARKET_DATA_INCOMPLETE.value in result.exclusion_reasons


def test_recovered_position_only_is_excluded_from_entry_calibration() -> None:
    row = _row()
    row.update({"recovered_from_position_state": True, "pre_entry_history_available": False})
    result = evaluate_calibration_eligibility(row)
    assert ExclusionReason.RECOVERED_POSITION_ONLY_HISTORY.value in result.exclusion_reasons
    assert ExclusionReason.PRE_ENTRY_HISTORY_UNAVAILABLE.value in result.exclusion_reasons


@pytest.mark.parametrize("status,expected", [
    (OutcomeStatus.PENDING, EligibilityStatus.PENDING_OUTCOME),
    (OutcomeStatus.RIGHT_CENSORED, EligibilityStatus.EXCLUDED),
    (OutcomeStatus.MISSING_PRICE_PATH, EligibilityStatus.EXCLUDED),
    (OutcomeStatus.CORPORATE_ACTION_UNRESOLVED, EligibilityStatus.QUARANTINED),
])
def test_outcome_censoring_statuses_are_not_valid_zeroes(status: OutcomeStatus, expected: EligibilityStatus) -> None:
    row = _row()
    row["outcome_status"] = status.value
    row["outcome_label"] = None
    if status is OutcomeStatus.PENDING:
        row["outcome_available_at"] = None
        row["outcome_window_end"] = None
    result = evaluate_calibration_eligibility(row)
    assert result.eligibility_status is expected


def test_sample_identity_is_deterministic_and_episode_specific() -> None:
    row = _row()
    first = deterministic_sample_id(row, policy_version="v1")
    assert first == deterministic_sample_id(deepcopy(row), policy_version="v1")
    row["entity_id"] = "different-episode"
    assert first != deterministic_sample_id(row, policy_version="v1")


def test_duplicate_sample_is_excluded() -> None:
    row = _row()
    result = build_calibration_dataset([row, deepcopy(row)], dataset_name="dup", dataset_purpose="entry", as_of="2026-07-15")
    assert result.quality_summary["duplicate_sample_count"] == 1
    assert result.verdict.value == "NOT_READY"


def test_manifest_replay_is_deterministic_and_timestamp_independent() -> None:
    rows = fixture_rows("small_fixture")
    first = build_calibration_dataset(rows, dataset_name="fixture", dataset_purpose="entry", as_of="2026-07-15", source_database_hashes={"fixture": "same"})
    second = build_calibration_dataset(rows, dataset_name="fixture", dataset_purpose="entry", as_of="2026-07-15", source_database_hashes={"fixture": "same"})
    replay = deepcopy(second.manifest)
    replay["created_at"] = "2099-01-01T00:00:00+00:00"
    assert first.manifest["created_at"] != replay["created_at"]
    assert compare_manifests(first.manifest, replay)["equivalent"] is True


def test_source_hash_or_policy_change_creates_new_manifest() -> None:
    rows = fixture_rows("small_fixture")
    first = build_calibration_dataset(rows, dataset_name="fixture", dataset_purpose="entry", as_of="2026-07-15", source_database_hashes={"fixture": "a"})
    changed_source = build_calibration_dataset(rows, dataset_name="fixture", dataset_purpose="entry", as_of="2026-07-15", source_database_hashes={"fixture": "b"})
    changed_policy = build_calibration_dataset(rows, dataset_name="fixture", dataset_purpose="entry", as_of="2026-07-15", config=CalibrationConfig(policy_version="phase3c5-calibration-policy-v2"), source_database_hashes={"fixture": "a"})
    assert len({first.manifest["manifest_id"], changed_source.manifest["manifest_id"], changed_policy.manifest["manifest_id"]}) == 3


def test_same_manifest_with_different_dataset_hash_is_integrity_failure() -> None:
    rows = fixture_rows("small_fixture")
    first = build_calibration_dataset(rows, dataset_name="fixture", dataset_purpose="entry", as_of="2026-07-15", source_database_hashes={"fixture": "same"})
    expected = dict(first.manifest)
    expected["eligible_dataset_hash"] = "tampered"
    with pytest.raises(CalibrationIntegrityError):
        build_calibration_dataset(rows, dataset_name="fixture", dataset_purpose="entry", as_of="2026-07-15", source_database_hashes={"fixture": "same"}, expected_manifest=expected)
