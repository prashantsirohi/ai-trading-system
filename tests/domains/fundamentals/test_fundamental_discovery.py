from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from ai_trading_system.domains.fundamentals.contracts import FundamentalThesisFamily
from ai_trading_system.domains.fundamentals.discovery import (
    classify_fundamental_universe,
    evaluate_theses,
    fundamental_source_hash,
    persist_discovery,
    project_cached_classification,
)


def _row(**changes) -> pd.Series:
    values = {
        "symbol_id": "TEST",
        "symbol": "TEST",
        "exchange": "NSE",
        "statement_basis": "consolidated",
        "source_report_date": "2026-03-31",
        "source_available_at": "2026-05-15",
        "structural_stage": "stage_1_basing",
        "daily_context_complete": True,
        "valuation_history_bucket": "BELOW_OWN_MEDIAN",
        "roce": 25.0,
        "debt_to_equity": 0.2,
        "debt_to_equity_prev": 0.5,
        "opm": 22.0,
        "sales_growth_3y": 35.0,
        "profit_growth_3y": 30.0,
        "sales_yoy_pct": 35.0,
        "profit_yoy_pct": 60.0,
        "profit_qoq_pct": 20.0,
        "operating_profit_yoy_pct": 50.0,
        "opm_yoy_change_bps": 200.0,
        "cash_from_operations_last_year": 120.0,
        "cash_from_operations_previous_year": -10.0,
        "free_cash_flow_last_year": 100.0,
        "free_cash_flow_previous_year": -20.0,
        "net_profit_cr": 100.0,
        "net_profit_previous_year": 50.0,
        "profit_same_q_ly": -5.0,
        "interest_coverage": 8.0,
        "dividend_yield": 2.0,
        "dividend_payout_pct": 30.0,
        "quarterly_result_bucket": "ACCELERATING",
        "industry": "Industrials",
    }
    values.update(changes)
    return pd.Series(values)


def test_all_seven_families_are_evaluated_and_can_overlap() -> None:
    evaluations = evaluate_theses(_row())
    assert tuple(item.family for item in evaluations) == tuple(FundamentalThesisFamily)
    assert all(item.passed for item in evaluations)


@pytest.mark.parametrize("free_cash_flow", [-25.0, 25.0])
def test_manorama_high_growth_cashflow_regression(free_cash_flow: float) -> None:
    row = _row(
        symbol_id="MANORAMA",
        symbol="MANORAMA",
        free_cash_flow_last_year=free_cash_flow,
        hard_red_flag=True,
        sales_growth_3y=40.0,
        cash_from_operations_last_year=30.0,
        debt_to_equity=0.4,
    )
    evaluation = next(
        item for item in evaluate_theses(row)
        if item.family is FundamentalThesisFamily.HIGH_GROWTH_EMERGING
    )
    assert evaluation.passed is True


def test_manorama_float_main_board_flag_remains_eligible() -> None:
    snapshots, _ = classify_fundamental_universe(
        pd.DataFrame([
            _row(
                symbol_id="MANORAMA",
                symbol="MANORAMA",
                is_not_sme=1.0,
                structural_stage="stage_2_advancing",
            )
        ]),
        as_of="2026-08-17",
    )

    assert snapshots[0].admission_eligible is True
    assert "SME_INELIGIBLE" not in snapshots[0].admission_blockers


@pytest.mark.parametrize("is_not_sme", [0, 0.0, "0", "false", False])
def test_confirmed_sme_values_are_ineligible(is_not_sme: object) -> None:
    snapshots, _ = classify_fundamental_universe(
        pd.DataFrame([_row(is_not_sme=is_not_sme)]),
        as_of="2026-08-17",
    )

    assert snapshots[0].classification_status == "EXCLUDED"
    assert "SME_INELIGIBLE" in snapshots[0].admission_blockers


@pytest.mark.parametrize("is_not_sme", [None, float("nan"), "unknown"])
def test_missing_sme_evidence_is_not_misclassified_as_confirmed_sme(
    is_not_sme: object,
) -> None:
    snapshots, _ = classify_fundamental_universe(
        pd.DataFrame([_row(is_not_sme=is_not_sme)]),
        as_of="2026-08-17",
    )

    assert "SME_INELIGIBLE" not in snapshots[0].admission_blockers


def test_negative_fcf_high_growth_fails_when_cash_and_leverage_do_not_support_it() -> None:
    row = _row(
        free_cash_flow_last_year=-25.0,
        cash_from_operations_last_year=-5.0,
        debt_to_equity=1.5,
    )
    evaluation = next(
        item for item in evaluate_theses(row)
        if item.family is FundamentalThesisFamily.HIGH_GROWTH_EMERGING
    )
    assert evaluation.passed is False


def test_daily_projection_fields_do_not_change_fundamental_source_hash() -> None:
    first = _row(structural_stage="stage_1_basing", valuation_history_bucket="BELOW_OWN_MEDIAN")
    second = _row(structural_stage="stage_2_advancing", valuation_history_bucket="EXPENSIVE_VS_HISTORY")
    assert fundamental_source_hash(first) == fundamental_source_hash(second)


def test_stage_blocking_and_point_in_time_exclusion() -> None:
    snapshots, _ = classify_fundamental_universe(
        pd.DataFrame([
            _row(structural_stage="stage_4_declining"),
            _row(symbol_id="FUTURE", symbol="FUTURE", source_available_at="2026-09-01"),
        ]),
        as_of="2026-08-15",
    )
    assert "STAGE_BLOCKED" in snapshots[0].admission_blockers
    assert "FUTURE_DATED_INPUT" in snapshots[1].admission_blockers


def test_identical_source_and_policy_is_idempotently_persisted() -> None:
    snapshots, _ = classify_fundamental_universe(pd.DataFrame([_row()]), as_of="2026-08-15")
    conn = duckdb.connect(":memory:")
    assert persist_discovery(conn, snapshots) == (1, 1)
    assert persist_discovery(conn, snapshots) == (0, 0)


def test_cached_classification_reuses_accounts_but_reprojects_daily_context() -> None:
    snapshots, _ = classify_fundamental_universe(pd.DataFrame([_row()]), as_of="2026-08-15")
    conn = duckdb.connect(":memory:")
    persist_discovery(conn, snapshots)
    cached = conn.execute("SELECT * FROM fundamental_thesis_classification").df().iloc[0].to_dict()
    changed_daily = _row(
        structural_stage="stage_4_declining",
        valuation_history_bucket="EXPENSIVE_VS_HISTORY",
    )
    projected = project_cached_classification(changed_daily, cached, as_of="2026-08-16")
    assert projected.source_data_hash == snapshots[0].source_data_hash
    assert projected.primary_thesis == snapshots[0].primary_thesis
    assert projected.admission_eligible is False
    assert "STAGE_BLOCKED" in projected.admission_blockers


@pytest.mark.parametrize("cached_date,projection_date,expected_status,blocker", [
    ("2026-08-15", "2026-05-14", "EXCLUDED", "FUTURE_DATED_INPUT"),
    ("2026-05-14", "2026-05-15", "QUALIFIED", None),
    ("2028-08-15", "2026-08-15", "QUALIFIED", None),
    ("2026-08-15", "2028-08-15", "STALE", "STALE_FUNDAMENTAL_SOURCE"),
])
def test_cached_admission_matches_fresh_classification_at_requested_date(cached_date, projection_date, expected_status, blocker):
    row = _row()
    snapshots, _ = classify_fundamental_universe(pd.DataFrame([row]), as_of=cached_date)
    with duckdb.connect(":memory:") as conn:
        persist_discovery(conn, snapshots)
        cached = conn.execute("SELECT * FROM fundamental_thesis_classification").df().iloc[0].to_dict()
        projected = project_cached_classification(row, cached, as_of=projection_date)
        fresh, _ = classify_fundamental_universe(pd.DataFrame([row]), as_of=projection_date)
        assert projected.classification_status == expected_status == fresh[0].classification_status
        assert projected.admission_blockers == fresh[0].admission_blockers
        assert projected.admission_eligible == fresh[0].admission_eligible
        if blocker:
            assert blocker in projected.admission_blockers
        persist_discovery(conn, [projected])
        assert conn.execute("SELECT COUNT(*) FROM fundamental_thesis_classification").fetchone()[0] == 1


def test_corrected_projection_version_coexists_with_old_evidence():
    from dataclasses import replace
    snapshot = classify_fundamental_universe(pd.DataFrame([_row()]), as_of="2026-08-15")[0][0]
    with duckdb.connect(":memory:") as conn:
        assert persist_discovery(conn, [replace(snapshot, admission_version="fundamental-thesis-admission-v1")]) == (1, 1)
        assert persist_discovery(conn, [snapshot]) == (0, 1)
        assert persist_discovery(conn, [snapshot]) == (0, 0)
        assert conn.execute("SELECT COUNT(*) FROM fundamental_thesis_projection").fetchone()[0] == 2


def test_conflicting_same_policy_daily_projection_fails_instead_of_silently_drifting():
    snapshot = classify_fundamental_universe(
        pd.DataFrame([_row(structural_stage="unknown", daily_context_complete=False)]),
        as_of="2026-08-15",
    )[0][0]
    corrected = classify_fundamental_universe(
        pd.DataFrame([_row(structural_stage="stage_2_advancing", daily_context_complete=True)]),
        as_of="2026-08-15",
    )[0][0]

    with duckdb.connect(":memory:") as conn:
        assert persist_discovery(conn, [snapshot]) == (1, 1)
        with pytest.raises(ValueError, match="Conflicting immutable fundamental projection"):
            persist_discovery(conn, [corrected])
        stored = conn.execute(
            "SELECT admission_eligible, admission_blockers_json FROM fundamental_thesis_projection"
        ).fetchone()

    assert stored[0] is False
    assert "DAILY_CONTEXT_INCOMPLETE" in stored[1]
