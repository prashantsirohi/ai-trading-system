from __future__ import annotations

from datetime import date

import pytest

from ai_trading_system.domains.opportunities.orchestration.convergence import (
    LaneAssessment,
    LaneEvaluationState,
    LaneFreshness,
    build_convergence_view,
)


SESSION = date(2026, 9, 4)


def test_lane_assessment_details_are_immutable() -> None:
    assessment = LaneAssessment(
        LaneEvaluationState.KNOWN,
        "TRACKED",
        True,
        LaneFreshness.FRESH,
        "hash",
        details={"score": 70},
    )

    with pytest.raises(TypeError):
        assessment.details["score"] = 71  # type: ignore[index]


def _build(
    *,
    investigator_rows=None,
    receipts=None,
    fundamental_rows=None,
    pattern_rows=None,
    universe=(("NSE", "ABC"),),
    investigator_hash="investigator-hash",
    receipt_hash="receipt-hash",
    fundamental_hash="fundamental-hash",
    pattern_hash="pattern-hash",
):
    return build_convergence_view(
        session_date=SESSION,
        policy_snapshot_id="policy-hash",
        universe_keys=universe,
        investigator_rows=investigator_rows or [],
        investigator_receipts=receipts or [],
        fundamental_rows=fundamental_rows or [],
        pattern_rows=pattern_rows or [],
        investigator_artifact_hash=investigator_hash,
        investigator_receipt_artifact_hash=receipt_hash,
        fundamental_artifact_hash=fundamental_hash,
        pattern_artifact_hash=pattern_hash,
    )


def test_builds_one_fresh_all_lane_row_with_exclusive_cohort() -> None:
    rows, readiness = _build(
        investigator_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "trade_date": "2026-09-04",
                "trigger_reason": "WEEKLY_GAINER",
                "move_tag": "WEEKLY_MOMENTUM",
                "final_score": 72,
            }
        ],
        receipts=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "trade_date": "2026-09-04",
                "tracked": True,
                "decision_state": "TRACKED",
            }
        ],
        fundamental_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "as_of": "2026-09-04",
                "primary_thesis": "QUALITY_COMPOUNDER",
                "classification_status": "QUALIFIED",
                "admission_eligible": True,
                "source_data_hash": "fundamental-row-hash",
            }
        ],
        pattern_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "session_date": "2026-09-04",
                "pattern_evaluation_state": "KNOWN",
                "pattern_member": True,
                "primary_pattern_family": "vcp",
                "primary_pattern_state": "confirmed",
                "lane_freshness": "FRESH",
                "evidence_hash": "pattern-row-hash",
                "signal_count": 1,
            }
        ],
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["convergence_cohort"] == "I_F_P"
    assert row["investigator_member"] is True
    assert row["fundamental_member"] is True
    assert row["pattern_member"] is True
    assert row["policy_snapshot_id"] == "policy-hash"
    assert all(item["status"] == "PASS" for item in readiness)


def test_subthreshold_weekly_gainer_is_known_but_not_primary_member() -> None:
    rows, _ = _build(
        investigator_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "trade_date": "2026-09-04",
                "trigger_reason": "WEEKLY_GAINER",
                "move_tag": "WEEKLY_MOMENTUM",
                "final_score": 64.99,
            }
        ],
        receipts=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "trade_date": "2026-09-04",
                "tracked": True,
                "decision_state": "TRACKED",
            }
        ],
        pattern_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "session_date": "2026-09-04",
                "pattern_evaluation_state": "NONE",
                "pattern_member": False,
                "lane_freshness": "FRESH",
                "evidence_hash": "pattern-row-hash",
            }
        ],
    )

    row = rows[0]
    assert row["investigator_evaluation_state"] == "KNOWN"
    assert row["investigator_member"] is False
    assert row["investigator_primary_review_eligible"] is False
    assert row["convergence_cohort"] == "NONE"


def test_explicit_missing_none_and_not_eligible_states() -> None:
    rows, readiness = _build(
        receipts=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "trade_date": "2026-09-04",
                "tracked": False,
                "decision_state": "EXCLUDED",
                "reason_codes": "WEEKLY_RETURN_NOT_ABOVE_THRESHOLD",
            }
        ],
        fundamental_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "as_of": "2026-09-04",
                "classification_status": "UNCLASSIFIED",
                "admission_eligible": False,
                "admission_blockers": '["NO_QUALIFYING_THESIS"]',
            }
        ],
        pattern_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "session_date": "2026-09-04",
                "pattern_evaluation_state": "NONE",
                "pattern_member": False,
                "lane_freshness": "FRESH",
                "evidence_hash": "pattern-row-hash",
            }
        ],
    )

    row = rows[0]
    assert row["investigator_evaluation_state"] == "NOT_ELIGIBLE"
    assert row["fundamental_evaluation_state"] == "NOT_ELIGIBLE"
    assert row["pattern_evaluation_state"] == "NONE"
    assert row["convergence_cohort"] == "NONE"
    assert (
        next(
            item
            for item in readiness
            if item["check_id"] == "OPPORTUNITY_CONVERGENCE_UNEXPLAINED_UNKNOWN"
        )["status"]
        == "PASS"
    )


def test_future_evidence_is_error_and_never_enters_cohort() -> None:
    rows, readiness = _build(
        investigator_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "trade_date": "2026-09-05",
                "trigger_reason": "WEEKLY_GAINER",
                "final_score": 90,
            }
        ],
        receipts=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "trade_date": "2026-09-05",
                "tracked": True,
            }
        ],
        pattern_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "session_date": "2026-09-05",
                "pattern_evaluation_state": "KNOWN",
                "pattern_member": True,
                "lane_freshness": "FRESH",
                "evidence_hash": "pattern-row-hash",
            }
        ],
    )

    assert rows[0]["investigator_evaluation_state"] == "ERROR"
    assert rows[0]["investigator_member"] is False
    assert rows[0]["pattern_evaluation_state"] == "ERROR"
    assert rows[0]["pattern_member"] is False
    errors = next(
        item
        for item in readiness
        if item["check_id"] == "OPPORTUNITY_CONVERGENCE_SOURCE_ERRORS"
    )
    assert errors["status"] == "FAIL"


def test_missing_pattern_assessment_is_unexplained_unknown() -> None:
    rows, readiness = _build(
        investigator_hash=None,
        receipt_hash=None,
        fundamental_hash=None,
    )

    assert rows[0]["investigator_evaluation_state"] == "NOT_EVALUATED"
    assert rows[0]["fundamental_evaluation_state"] == "NOT_EVALUATED"
    assert rows[0]["pattern_evaluation_state"] == "UNKNOWN"
    unknown = next(
        item
        for item in readiness
        if item["check_id"] == "OPPORTUNITY_CONVERGENCE_UNEXPLAINED_UNKNOWN"
    )
    assert unknown["status"] == "FAIL"


def test_stale_eligible_lane_is_excluded_and_fails_active_freshness() -> None:
    rows, readiness = _build(
        fundamental_rows=[
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "as_of": "2026-09-03",
                "primary_thesis": "QUALITY_COMPOUNDER",
                "classification_status": "QUALIFIED",
                "admission_eligible": True,
            }
        ],
        investigator_hash=None,
        receipt_hash=None,
        pattern_hash=None,
    )

    assert rows[0]["fundamental_admission_eligible"] is True
    assert rows[0]["fundamental_freshness"] == "STALE"
    assert rows[0]["fundamental_member"] is False
    assert rows[0]["convergence_cohort"] == "NONE"
    freshness = next(
        item
        for item in readiness
        if item["check_id"] == "OPPORTUNITY_CONVERGENCE_ACTIVE_FRESHNESS"
    )
    assert freshness["observed"] == 0
    assert freshness["expected"] == 1
    assert freshness["status"] == "FAIL"


def test_output_and_hashes_are_deterministic() -> None:
    kwargs = {
        "receipts": [
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "trade_date": "2026-09-04",
                "tracked": False,
                "decision_state": "EXCLUDED",
            }
        ],
        "pattern_rows": [
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "session_date": "2026-09-04",
                "pattern_evaluation_state": "NONE",
                "pattern_member": False,
                "lane_freshness": "FRESH",
                "evidence_hash": "pattern-row-hash",
            }
        ],
    }

    first, _ = _build(**kwargs)
    second, _ = _build(**kwargs)

    assert first == second
