from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.publish.dashboard import (
    _stage1_action_queue_frame,
    _stage1_changes_frame,
    _stage1_current_frame,
    _stage1_exits_frame,
    _stage1_summary_frames,
)


def _bundle() -> dict:
    critical = {
        "run_id": "run-1", "symbol_id": "AAA", "trade_date": "2026-07-11",
        "stage1_lifecycle_state": "PROMOTION_PENDING", "stage1_substate": "STAGE_1_BREAKOUT_READY",
        "stage1_maturity_score": 84, "stage1_emerging_score": 87, "stage1_emerging_rank": 1,
        "operator_priority": "CRITICAL", "operator_status": "ACT_NOW", "operator_action": "CHECK_BREAKOUT",
        "operator_reason": "Promotion pending", "operator_queue_eligible": True, "execution_eligible": False,
    }
    monitor = {
        "run_id": "run-1", "symbol_id": "BBB", "trade_date": "2026-07-11",
        "stage1_lifecycle_state": "BASE_BUILDING", "stage1_maturity_score": 45,
        "stage1_emerging_rank": 20, "operator_priority": "LOW", "operator_status": "MONITOR",
        "operator_action": "NO_ACTION", "operator_reason": "Base Building", "operator_queue_eligible": False,
        "execution_eligible": False,
    }
    exit_row = {
        "run_id": "run-1", "symbol_id": "EXIT", "trade_date": "2026-07-11",
        "stage1_lifecycle_state": "REGRESSED", "stage1_previous_lifecycle_state": "BREAKOUT_READY",
        "stage1_maturity_score": 60, "stage1_score_peak": 80, "regression_reason": "WEAKER_RS",
        "operator_reason": "Regressed after weaker RS", "execution_eligible": False,
    }
    return {
        "summary": {"as_of": "2026-07-11", "active_count": 2, "base_building_count": 1,
                    "accumulating_count": 0, "late_stage1_count": 0, "breakout_ready_count": 0,
                    "promotion_pending_count": 1, "progressions_today": 1, "regressions_today": 1,
                    "invalidated_today": 0, "top_emerging_candidates": [critical, monitor]},
        "current": {"rows": [critical, monitor]},
        "transitions": {"rows": [{"symbol_id": "AAA", "trade_date": "2026-07-11",
            "from_lifecycle_state": "BREAKOUT_READY", "to_lifecycle_state": "PROMOTION_PENDING",
            "stage1_score_before": 80, "stage1_score_after": 84, "emerging_rank_before": 3,
            "emerging_rank_after": 1, "transition_summary": "BREAKOUT_READY → PROMOTION_PENDING"}]},
        "exits": {"rows": [exit_row]},
        "context_by_symbol": {"AAA": critical, "BBB": monitor, "EXIT": exit_row},
    }


def test_stage1_sheet_frames_use_canonical_operator_fields() -> None:
    bundle = _bundle()
    current = _stage1_current_frame(bundle, "run-1")
    queue = _stage1_action_queue_frame(bundle, "run-1")
    changes = _stage1_changes_frame(bundle, "run-1")
    exits = _stage1_exits_frame(bundle, "run-1")
    assert current.columns[:6].tolist() == ["Priority", "Symbol", "Lifecycle", "Substate", "Operator Status", "Operator Action"]
    assert current["Symbol"].tolist() == ["AAA", "BBB"]
    assert queue["Symbol"].tolist() == ["AAA"]
    assert queue.iloc[0]["Operator Action"] == "CHECK_BREAKOUT"
    assert changes.iloc[0]["Score Change"] == 4
    assert changes.iloc[0]["Rank Improvement"] == 2
    assert changes.iloc[0]["Operator Interpretation"] == "Promotion pending"
    assert exits.iloc[0]["Exit Type"] == "REGRESSED"
    assert exits.iloc[0]["Reason"] == "WEAKER_RS"
    assert set(current["Pipeline Run ID"]) == {"run-1"}


def test_stage1_summary_and_empty_frames_have_stable_schema() -> None:
    counts, leaders = _stage1_summary_frames(_bundle())
    assert counts.set_index("Metric").loc["Active Stage-1", "Value"] == 2
    assert leaders["Symbol"].tolist() == ["AAA", "BBB"]
    empty = {"summary": {"as_of": "2026-07-11", "top_emerging_candidates": []}, "current": {"rows": []}, "transitions": {"rows": []}, "exits": {"rows": []}}
    assert "Operator Reason" in _stage1_current_frame(empty, "run-empty").columns
    assert "Operator Interpretation" in _stage1_changes_frame(empty, "run-empty").columns
    assert "Operator Note" in _stage1_exits_frame(empty, "run-empty").columns


def test_same_run_stage1_frames_are_idempotent() -> None:
    bundle = _bundle()
    pd.testing.assert_frame_equal(_stage1_current_frame(bundle, "run-1"), _stage1_current_frame(bundle, "run-1"))
    pd.testing.assert_frame_equal(_stage1_changes_frame(bundle, "run-1"), _stage1_changes_frame(bundle, "run-1"))
    pd.testing.assert_frame_equal(_stage1_action_queue_frame(bundle, "run-1"), _stage1_action_queue_frame(bundle, "run-1"))
