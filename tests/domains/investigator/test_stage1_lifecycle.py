import pandas as pd

from ai_trading_system.domains.investigator.stage1_lifecycle import build_stage1_lifecycle


def _row(**values):
    base = {
        "symbol_id": "AAA", "stage1_eligible": True, "stage1_substate": "STAGE_1_BASE",
        "stage1_maturity_score": 55.0, "stage1_emerging_rank": 42,
        "stage1_block_reasons": "[]", "pattern_promotion_state": "DEVELOPING",
        "golden_cross_status": "APPROACHING", "execution_eligible": False,
    }
    base.update(values)
    return pd.DataFrame([base])


def test_eligible_entry_and_transition_are_auditable():
    state, transitions, summary = build_stage1_lifecycle(_row(), pd.DataFrame(), run_date="2026-07-10")
    assert state.iloc[0]["stage1_lifecycle_state"] == "BASE_BUILDING"
    assert transitions.iloc[0]["to_lifecycle_state"] == "BASE_BUILDING"
    assert summary["stage1_execution_eligible_rows"] == 0


def test_missing_eligibility_does_not_create_a_discovery():
    state, transitions, _ = build_stage1_lifecycle(pd.DataFrame([{"symbol_id": "NON_STAGE1", "stage1_eligible": float("nan")}]), pd.DataFrame(), run_date="2026-07-10")
    assert state.empty
    assert transitions.empty


def test_quiet_previous_candidate_is_retained_as_data_pending():
    previous, _, _ = build_stage1_lifecycle(_row(), pd.DataFrame(), run_date="2026-07-10")
    state, transitions, _ = build_stage1_lifecycle(pd.DataFrame(), previous, run_date="2026-07-11")
    assert state.iloc[0]["stage1_lifecycle_state"] == "BASE_BUILDING"
    assert state.iloc[0]["stage1_evaluation_status"] == "DATA_PENDING"
    assert transitions.empty


def test_stage4_invalidates_and_pending_pattern_handoff_is_explicit():
    previous, _, _ = build_stage1_lifecycle(_row(stage1_substate="STAGE_1_LATE"), pd.DataFrame(), run_date="2026-07-10")
    pending, _, _ = build_stage1_lifecycle(
        _row(stage1_substate="STAGE_1_BREAKOUT_READY", promotion_eligibility=True, pattern_promotion_state="PENDING_3D"), previous, run_date="2026-07-11"
    )
    assert pending.iloc[0]["stage1_lifecycle_state"] == "PROMOTION_PENDING"
    invalid, transitions, _ = build_stage1_lifecycle(
        _row(stage1_eligible=False, stage1_block_reasons='["STAGE4_HARD_GUARD"]'), pending, run_date="2026-07-12"
    )
    assert invalid.iloc[0]["stage1_lifecycle_state"] == "INVALIDATED"
    assert "STAGE4_HARD_GUARD" in transitions.iloc[0]["transition_reason_codes"]
