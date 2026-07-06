from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.payload import _investigator_score, _pattern_confirmation, build_investigator_payload


def test_payload_derives_decision_scores_and_trap_categories() -> None:
    active = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-07",
                "final_score": 70,
                "volume_delivery_score": 20,
                "sector_support_score": 10,
                "trigger_quality_score": 20,
            },
            {
                "symbol_id": "TRAP",
                "trade_date": "2026-05-07",
                "verdict": "NOISE_TRAP",
                "price_progression_pct": -4,
            },
        ]
    )
    repeat = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "repeat_score": 95,
                "appearance_count_20d": 4,
                "price_progression_pct": 12,
                "rank_change_20d": -20,
                "volume_escalation": True,
                "high_priority_repeat": True,
            }
        ]
    )
    traps = pd.DataFrame(
        [
            {
                "symbol_id": "TRAP",
                "trade_date": "2026-05-07",
                "drop_reason": "ONE_CANDLE_DRAMA",
                "verdict": "NOISE_TRAP",
                "appearance_count_20d": 2,
            }
        ]
    )

    payload = build_investigator_payload(
        run_id="run-1",
        run_date="2026-05-07",
        summary={"daily_gainer_count": 2, "active_count": 2, "trap_count": 1, "archived_count": 0},
        today_gainers=pd.DataFrame([{"symbol_id": "AAA"}, {"symbol_id": "TRAP"}]),
        scores=active,
        repeat_tracker=repeat,
        active_watchlist=active,
        trap_log=traps,
        archive=pd.DataFrame(),
        previous_summary={"daily_gainers": 1, "active_queue": 1, "traps": 0},
    )

    assert payload["decision_queue"][0]["symbol_id"] == "AAA"
    assert payload["decision_queue"][0]["decision_verdict"] in {"Investigate", "High Conviction"}
    assert payload["trap_radar"][0]["trap_category"] == "One-day spike"
    assert payload["summary_deltas"]["daily_gainers"] == 1
    assert payload["summary"]["repeat_ge3"] == 1
    assert payload["summary"]["new_in_window"] == payload["summary"]["new_candidates"]
    assert payload["summary"]["trap_count"] == 1
    assert payload["summary"]["fresh_trap_today"] == 1
    assert payload["summary"]["repeat_trap"] == 1
    assert payload["charts"]["funnel_today"][0]["label"] == "Investigator Intake (today)"
    assert payload["charts"]["funnel_window"][0]["key"] == "new_window"
    assert payload["charts"]["trend"][0]["date"] == "2026-05-07"
    assert payload["charts"]["trend"][0]["traps"] == 1


def test_payload_uses_total_intake_for_summary_and_funnel() -> None:
    payload = build_investigator_payload(
        run_id="run-1",
        run_date="2026-05-07",
        summary={
            "total_intake_count": 6,
            "daily_gainer_count": 2,
            "weekly_gainer_count": 3,
            "stealth_accumulation_count": 1,
            "active_count": 2,
            "trap_count": 3,
            "archived_count": 0,
        },
        today_gainers=pd.DataFrame([{"symbol_id": "AAA"}]),
        scores=pd.DataFrame([{"symbol_id": "AAA"}]),
        repeat_tracker=pd.DataFrame(),
        active_watchlist=pd.DataFrame([{"symbol_id": "AAA"}]),
        trap_log=pd.DataFrame([{"symbol_id": "TRAP1"}, {"symbol_id": "TRAP2"}, {"symbol_id": "TRAP3"}]),
        archive=pd.DataFrame(),
    )

    assert payload["summary"]["total_intake"] == 6
    assert payload["summary"]["total_intake_count"] == 6
    assert payload["summary"]["daily_gainers"] == 6
    assert payload["summary"]["daily_gainer_count"] == 2
    assert payload["summary"]["weekly_gainer_count"] == 3
    assert payload["summary"]["stealth_accumulation_count"] == 1
    assert payload["summary"]["trap_rate"] == 0.5
    assert payload["charts"]["funnel"][0] == {"key": "intake", "label": "Investigator Intake", "count": 6}
    assert payload["charts"]["funnel_today"][0] == {"key": "intake", "label": "Investigator Intake (today)", "count": 6}


def test_payload_exposes_investigator_early_accumulation_table_and_empty_state() -> None:
    payload = build_investigator_payload(
        run_id="run-1",
        run_date="2026-05-07",
        summary={"investigator_early_accumulation_count": 1},
        today_gainers=pd.DataFrame(),
        scores=pd.DataFrame(),
        repeat_tracker=pd.DataFrame(),
        active_watchlist=pd.DataFrame(),
        trap_log=pd.DataFrame(),
        archive=pd.DataFrame(),
        investigator_early_accumulation=pd.DataFrame(
            [
                {
                    "symbol": "AAA",
                    "symbol_id": "AAA",
                    "sector": "Industrials",
                    "close": 100,
                    "early_accumulation_score": 82,
                    "early_accumulation_rank": 1,
                    "early_purity_bucket": "true_early",
                    "pattern_family": "cup_handle",
                    "pattern_age_days": 5,
                    "base_pattern_freshness_score": 90,
                    "above_200dma_reclaim_score": 75,
                    "delivery_accumulation_score": 60,
                    "momentum_recovery_score": 70,
                    "volume_confirmation_score": 65,
                    "active_rank_pctile": 55,
                    "breakout_qualified": False,
                    "graduation_status": "pattern_confirmed",
                    "watchlist_reason": "Fresh base",
                }
            ]
        ),
    )

    assert payload["summary"]["investigator_early_accumulation_count"] == 1
    assert payload["investigator_early_accumulation"][0]["symbol"] == "AAA"
    assert {
        "symbol",
        "sector",
        "early_accumulation_score",
        "early_purity_bucket",
        "base_pattern_freshness_score",
        "above_200dma_reclaim_score",
        "delivery_accumulation_score",
        "momentum_recovery_score",
        "volume_confirmation_score",
        "active_rank_pctile",
        "breakout_qualified",
        "graduation_status",
        "watchlist_reason",
    }.issubset(payload["investigator_early_accumulation"][0])

    empty_payload = build_investigator_payload(
        run_id="run-1",
        run_date="2026-05-07",
        summary={},
        today_gainers=pd.DataFrame(),
        scores=pd.DataFrame(),
        repeat_tracker=pd.DataFrame(),
        active_watchlist=pd.DataFrame(),
        trap_log=pd.DataFrame(),
        archive=pd.DataFrame(),
    )
    assert empty_payload["investigator_early_accumulation"] == []
    assert empty_payload["summary"]["investigator_early_accumulation_count"] == 0


def test_pattern_confirmation_counts_all_s1_states_and_sorts_top_setups() -> None:
    confirmation = _pattern_confirmation(
        pd.DataFrame(
            [
                {"symbol_id": "FAIL", "s1_promotion_state": "FAILED_S1", "pattern_score": 95, "setup_quality": 95},
                {"symbol_id": "ACC", "s1_promotion_state": "S1_ACCUMULATION", "pattern_score": 50, "setup_quality": 40},
                {"symbol_id": "BASE", "s1_promotion_state": "S1_BASE_FORMING", "pattern_score": 60, "setup_quality": 40},
                {"symbol_id": "NEAR", "s1_promotion_state": "S1_NEAR_BREAKOUT", "pattern_score": 66, "setup_quality": 61},
                {"symbol_id": "TRANS", "s1_promotion_state": "S1_TO_S2_TRANSITION", "pattern_score": 72, "setup_quality": 63},
                {"symbol_id": "CONF", "s1_promotion_state": "S2_CONFIRMED", "pattern_score": 70, "setup_quality": 62},
            ]
        )
    )

    assert confirmation["failed_s1"] == 1
    assert confirmation["s1_base_forming"] == 1
    assert confirmation["s1_accumulation"] == 1
    assert confirmation["s1_near_breakout"] == 1
    assert confirmation["s1_to_s2_transition"] == 1
    assert confirmation["s2_confirmed"] == 1
    assert [row["symbol_id"] for row in confirmation["top_setups"][:3]] == ["CONF", "TRANS", "NEAR"]


def test_payload_pattern_state_lifts_investigator_score_without_changing_final_score() -> None:
    active = pd.DataFrame(
        [
            {
                "symbol_id": "FAIL",
                "trade_date": "2026-05-07",
                "final_score": 60,
                "trigger_quality_score": 1,
                "s1_promotion_state": "FAILED_S1",
            },
            {
                "symbol_id": "CONF",
                "trade_date": "2026-05-07",
                "final_score": 60,
                "trigger_quality_score": 1,
                "s1_promotion_state": "S2_CONFIRMED",
            },
        ]
    )

    payload = build_investigator_payload(
        run_id="run-1",
        run_date="2026-05-07",
        summary={"daily_gainer_count": 2, "active_count": 2, "trap_count": 0, "archived_count": 0},
        today_gainers=pd.DataFrame([{"symbol_id": "FAIL"}, {"symbol_id": "CONF"}]),
        scores=active,
        repeat_tracker=pd.DataFrame(),
        active_watchlist=active,
        trap_log=pd.DataFrame(),
        archive=pd.DataFrame(),
    )

    records = {row["symbol_id"]: row for row in payload["decision_queue"]}
    assert records["CONF"]["investigator_score"] > records["FAIL"]["investigator_score"]
    assert records["CONF"]["final_score"] == records["FAIL"]["final_score"] == 60


def test_investigator_score_uses_higher_of_move_setup_and_pattern_setup() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "MOVE",
                "trigger_quality_score": 20,
                "s1_promotion_state": "FAILED_S1",
                "price_progression_pct": 0,
                "rank_change_20d": 0,
            },
            {
                "symbol_id": "PATTERN",
                "trigger_quality_score": 1,
                "s1_promotion_state": "S2_CONFIRMED",
                "price_progression_pct": 0,
                "rank_change_20d": 0,
            },
        ]
    )

    scores = _investigator_score(frame)

    assert scores.iloc[0] == scores.iloc[1] == 30.0
