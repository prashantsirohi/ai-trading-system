from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.lifecycle import apply_lifecycle
from ai_trading_system.domains.investigator.scoring import finalize_scores


def test_missing_fundamentals_caps_high_conviction_at_medium() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "price_structure_score": 15,
                "volume_delivery_score": 20,
                "fundamental_score": 10,
                "trigger_quality_score": 20,
                "sector_support_score": 10,
                "buyer_fingerprint_score": 15,
                "composite_score": 90,
                "credible_trigger": True,
                "hard_trap_flag": False,
                "fa_missing": True,
            }
        ]
    )

    scored = finalize_scores(frame)

    assert scored.iloc[0]["final_score"] == 100
    assert scored.iloc[0]["verdict"] == "MEDIUM_CONVICTION"


def test_hard_trap_forces_noise_trap() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "TRAP",
                "price_structure_score": 15,
                "volume_delivery_score": 20,
                "fundamental_score": 20,
                "trigger_quality_score": 20,
                "sector_support_score": 10,
                "buyer_fingerprint_score": 15,
                "composite_score": 90,
                "credible_trigger": True,
                "hard_trap_flag": True,
                "fa_missing": False,
            }
        ]
    )

    scored = finalize_scores(frame)

    assert scored.iloc[0]["verdict"] == "NOISE_TRAP"


def test_lifecycle_one_candle_drama_archive_reason() -> None:
    scores = pd.DataFrame(
        [
            {
                "symbol_id": "XYZ",
                "trade_date": "2026-05-01",
                "verdict": "WATCH_ONLY",
                "final_score": 42,
                "composite_score": 50,
                "credible_trigger": False,
                "sector_support_score": 0,
                "sector_rotation_active": False,
                "long_upper_wick_trap": False,
                "low_delivery_flag": False,
                "fa_improvement": False,
                "sector_clustering": False,
            }
        ]
    )
    repeat = pd.DataFrame(
        [
            {
                "symbol_id": "XYZ",
                "first_seen_date": "2026-05-01",
                "last_seen_date": "2026-05-01",
                "days_since_last_seen": 5,
                "appearance_count_20d": 1,
                "score_current": 42,
                "score_peak": 42,
                "rank_current": 80,
                "rank_change_20d": 0,
                "price_progression_pct": -4.5,
                "volume_escalation": False,
                "sector_cluster_count": 0,
            }
        ]
    )

    active, archived = apply_lifecycle(scores, repeat)

    assert active.empty
    assert archived.iloc[0]["status"] == "DROPPED"
    assert archived.iloc[0]["drop_reason"] == "ONE_CANDLE_DRAMA"


def test_lifecycle_keeps_repeat_accumulation_beyond_window() -> None:
    scores = pd.DataFrame(
        [
            {
                "symbol_id": "ABC",
                "trade_date": "2026-05-01",
                "verdict": "MEDIUM_CONVICTION",
                "final_score": 62,
                "composite_score": 70,
                "credible_trigger": True,
                "sector_support_score": 5,
                "sector_rotation_active": False,
                "long_upper_wick_trap": False,
                "low_delivery_flag": False,
                "fa_improvement": False,
                "sector_clustering": False,
            }
        ]
    )
    repeat = pd.DataFrame(
        [
            {
                "symbol_id": "ABC",
                "first_seen_date": "2026-05-01",
                "last_seen_date": "2026-05-31",
                "days_since_last_seen": 25,
                "appearance_count_20d": 4,
                "score_current": 62,
                "score_peak": 72,
                "rank_current": 20,
                "rank_change_20d": -10,
                "price_progression_pct": 12.0,
                "volume_escalation": True,
                "sector_cluster_count": 1,
                "high_priority_repeat": True,
            }
        ]
    )

    active, archived = apply_lifecycle(scores, repeat)

    assert archived.empty
    assert active.iloc[0]["status"] == "ACTIVE_RESEARCH"
