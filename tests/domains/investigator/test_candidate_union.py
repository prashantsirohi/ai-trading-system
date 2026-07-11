from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.candidate_union import (
    build_candidate_union,
    eligible_previous_watchlist,
)
from ai_trading_system.domains.investigator.repeat_tracker import build_repeat_tracker


def test_early_accumulation_only_enters_union() -> None:
    candidates, diagnostics = build_candidate_union(
        event_intake=pd.DataFrame(),
        early_accumulation=pd.DataFrame([{"symbol_id": " early ", "close": 120.0}]),
    )

    assert candidates.iloc[0]["symbol_id"] == "EARLY"
    assert candidates.iloc[0]["candidate_sources"] == "EARLY_ACCUMULATION"
    assert candidates.iloc[0]["primary_candidate_source"] == "EARLY_ACCUMULATION"
    assert bool(candidates.iloc[0]["new_candidate_today"]) is True
    assert diagnostics["early_accumulation_only_rows"] == 1


def test_only_eligible_canonical_stage1_rows_enter_union() -> None:
    candidates, diagnostics = build_candidate_union(
        event_intake=pd.DataFrame(),
        early_accumulation=pd.DataFrame([
            {"symbol_id": "KEEP", "stage1_maturity_score": 72, "stage1_eligible": True},
            {"symbol_id": "BLOCK", "stage1_maturity_score": 85, "stage1_eligible": False},
        ]),
    )
    assert candidates["symbol_id"].tolist() == ["KEEP"]
    assert candidates.iloc[0]["primary_candidate_source"] == "STAGE1_SCAN"
    assert candidates.iloc[0]["trigger_reason"] == "STAGE1_SCAN"
    assert diagnostics["candidate_source_counts"]["STAGE1_SCAN"] == 1


def test_blocked_stage1_context_reaches_independently_admitted_event() -> None:
    candidates, _ = build_candidate_union(
        event_intake=pd.DataFrame([{"symbol_id": "BLOCK", "trigger_reason": "DAILY_GAINER"}]),
        early_accumulation=pd.DataFrame([{
            "symbol_id": "BLOCK", "stage1_maturity_score": 84,
            "stage1_eligible": False, "stage1_substate": "NOT_STAGE1",
            "stage1_block_reasons": '["STAGE4_HARD_GUARD"]',
        }]),
    )
    row = candidates.iloc[0]
    assert row["primary_candidate_source"] == "DAILY_GAINER"
    assert "STAGE1_SCAN" not in row["candidate_sources"]
    assert row["stage1_substate"] == "NOT_STAGE1"
    assert row["stage1_block_reasons"] == '["STAGE4_HARD_GUARD"]'


def test_duplicate_multi_source_candidate_is_deterministic_and_complete() -> None:
    event = pd.DataFrame(
        [
            {"symbol_id": "AAA", "trigger_reason": "WEEKLY_GAINER", "close": None},
            {"symbol_id": "aaa", "trigger_reason": "DAILY_GAINER", "close": 110.0, "volume": 3000},
        ]
    )
    candidates, diagnostics = build_candidate_union(
        event_intake=event,
        early_accumulation=pd.DataFrame([{"symbol_id": "AAA", "early_accumulation_score": 82}]),
        previous_watchlist=pd.DataFrame(
            [{"symbol_id": "AAA", "status": "WATCHLIST", "stage_label": "STAGE_1_BASE", "close": 100.0}]
        ),
        ranked=pd.DataFrame([{"symbol_id": "AAA", "composite_score": 70}]),
        stock_scan=pd.DataFrame([{"symbol_id": "AAA", "rank_position": 12}]),
        breakout_scan=pd.DataFrame([{"symbol_id": "AAA", "qualified": True}]),
    )

    assert len(candidates) == 1
    row = candidates.iloc[0]
    assert row["candidate_sources"] == (
        "DAILY_GAINER|WEEKLY_GAINER|EARLY_ACCUMULATION|PREVIOUS_WATCHLIST|"
        "BREAKOUT_CONTEXT|RANK_CONTEXT|STOCK_SCAN_CONTEXT"
    )
    assert row["primary_candidate_source"] == "DAILY_GAINER"
    assert row["close"] == 110.0
    assert row["early_accumulation_score"] == 82
    assert row["rank_position"] == 12
    assert bool(row["new_candidate_today"]) is False
    assert diagnostics["multi_source_candidate_rows"] == 1


def test_context_only_rows_do_not_create_candidates() -> None:
    candidates, diagnostics = build_candidate_union(
        event_intake=pd.DataFrame(),
        early_accumulation=pd.DataFrame(),
        ranked=pd.DataFrame([{"symbol_id": "RANK_ONLY"}]),
        stock_scan=pd.DataFrame([{"symbol_id": "SCAN_ONLY"}]),
        breakout_scan=pd.DataFrame([{"symbol_id": "BREAKOUT_ONLY"}]),
    )

    assert candidates.empty
    assert diagnostics["candidate_union_rows"] == 0


def test_previous_watchlist_filters_closed_failed_and_hard_trap_rows() -> None:
    previous = pd.DataFrame(
        [
            {"symbol_id": "KEEP", "status": "WATCHLIST", "s1_promotion_state": "S1_ACCUMULATION"},
            {"symbol_id": "DROP", "status": "DROPPED", "s1_promotion_state": "S1_ACCUMULATION"},
            {"symbol_id": "ARCH", "status": "ARCHIVED", "s1_promotion_state": "S1_ACCUMULATION"},
            {"symbol_id": "FAIL", "status": "WATCHLIST", "s1_promotion_state": "FAILED_S1"},
            {"symbol_id": "GRAD", "status": "WATCHLIST", "s1_promotion_state": "S2_CONFIRMED"},
            {"symbol_id": "INVALID", "status": "WATCHLIST", "pattern_lifecycle_state": "invalidated"},
            {"symbol_id": "TRAP", "status": "WATCHLIST", "hard_trap_flag": True},
        ]
    )

    eligible = eligible_previous_watchlist(previous)

    assert eligible["symbol_id"].tolist() == ["KEEP"]


def test_previous_watchlist_only_refresh_does_not_increment_repeat_appearance() -> None:
    history = pd.DataFrame(
        [
            {
                "symbol_id": "KEEP",
                "trade_date": "2026-05-01",
                "close": 100,
                "final_score": 50,
                "candidate_sources": "EARLY_ACCUMULATION",
            }
        ]
    )
    current = pd.DataFrame(
        [
            {
                "symbol_id": "KEEP",
                "trade_date": "2026-05-07",
                "close": 105,
                "final_score": 52,
                "candidate_sources": "PREVIOUS_WATCHLIST",
            }
        ]
    )

    repeat = build_repeat_tracker(current_scores=current, historical_daily_log=history)

    row = repeat.iloc[0]
    assert row["appearance_count_20d"] == 1
    assert row["last_seen_date"] == "2026-05-01"
    assert row["days_since_last_seen"] == 6
    assert row["score_current"] == 52
