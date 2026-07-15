from __future__ import annotations

from ai_trading_system.domains.opportunities.calibration import (
    EligibilityStatus,
    ExclusionReason,
    build_calibration_dataset,
    evaluate_calibration_eligibility,
)
from ai_trading_system.interfaces.cli.build_phase3c5_calibration import fixture_rows


def _row() -> dict:
    return fixture_rows("small_fixture", count=1)[0]


def test_historical_delisted_loser_is_retained_when_identity_is_valid() -> None:
    row = _row()
    row.update({"delisting_status": "delisted_later", "outcome_label": "negative"})
    assert evaluate_calibration_eligibility(row).eligibility_status is EligibilityStatus.ELIGIBLE


def test_current_only_symbol_join_is_detected() -> None:
    row = _row()
    row["universe_source"] = "current_only"
    assert ExclusionReason.CURRENT_ONLY_UNIVERSE.value in evaluate_calibration_eligibility(row).exclusion_reasons


def test_resolved_symbol_rename_is_allowed() -> None:
    row = _row()
    row.update({"listing_status_as_of_decision": "renamed", "symbol_identity_valid": True})
    assert evaluate_calibration_eligibility(row).eligible is True


def test_unknown_delisting_status_is_flagged() -> None:
    row = _row()
    row["delisting_status"] = "unknown"
    assert ExclusionReason.DELISTING_STATUS_UNRESOLVED.value in evaluate_calibration_eligibility(row).exclusion_reasons


def test_failed_candidates_are_retained() -> None:
    row = _row()
    row.update({"candidate_state": "failed", "outcome_label": "negative"})
    assert evaluate_calibration_eligibility(row).eligible is True


def test_winner_only_dataset_is_rejected() -> None:
    result = build_calibration_dataset(
        fixture_rows("winner_only"), dataset_name="winner_only",
        dataset_purpose="entry", as_of="2026-07-15",
    )
    assert result.quality_summary["winner_only_dataset"] is True
    assert result.verdict.value == "NOT_READY"
