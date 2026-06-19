from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.payload import _investigator_score, build_investigator_payload


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
    assert payload["charts"]["funnel_today"][0]["label"] == "Daily Gainers (today)"
    assert payload["charts"]["funnel_window"][0]["key"] == "new_window"
    assert payload["charts"]["trend"][0]["date"] == "2026-05-07"
    assert payload["charts"]["trend"][0]["traps"] == 1


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
