from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.payload import build_investigator_payload


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
    traps = pd.DataFrame([{"symbol_id": "TRAP", "drop_reason": "ONE_CANDLE_DRAMA", "verdict": "NOISE_TRAP"}])

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
