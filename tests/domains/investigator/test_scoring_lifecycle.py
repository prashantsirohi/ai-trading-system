from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.lifecycle import apply_lifecycle
from ai_trading_system.domains.investigator.move_classifier import classify_move
from ai_trading_system.domains.investigator.repeat_tracker import build_repeat_tracker
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


def test_missing_rank_is_neutral_not_low_rank_trap() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "NORANK",
                "price_structure_score": 8,
                "volume_delivery_score": 12,
                "fundamental_score": 12,
                "trigger_quality_score": 5,
                "sector_support_score": 0,
                "buyer_fingerprint_score": 7,
                "composite_score": pd.NA,
                "credible_trigger": False,
                "hard_trap_flag": False,
                "fa_missing": False,
            }
        ]
    )

    scored = finalize_scores(frame)

    assert scored.iloc[0]["ranking_overlay_score"] == 0
    assert scored.iloc[0]["final_score"] == 44
    assert scored.iloc[0]["verdict"] == "WATCH_ONLY"


def test_repeat_tracker_counts_current_trade_date_once_when_history_excludes_today() -> None:
    current = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-07",
                "close": 110,
                "volume_ratio_20": 2.5,
                "composite_score": 70,
                "rank_position": 42,
                "final_score": 48,
                "sector": "Finance",
            }
        ]
    )
    prior_history = pd.DataFrame(
        columns=["symbol_id", "trade_date", "close", "volume_ratio_20", "composite_score", "rank_position", "final_score", "sector"]
    )

    repeat = build_repeat_tracker(current_scores=current, historical_daily_log=prior_history)

    assert repeat.iloc[0]["appearance_count_20d"] == 1
    assert repeat.iloc[0]["repeat_score"] == 8


def test_repeat_tracker_counts_trigger_types_over_20d() -> None:
    current = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-21",
                "close": 112,
                "volume_ratio_20": 2.5,
                "trigger_reason": "STEALTH_ACCUMULATION",
                "final_score": 48,
            }
        ]
    )
    history = pd.DataFrame(
        [
            {"symbol_id": "AAA", "trade_date": "2026-05-05", "close": 100, "volume_ratio_20": 2.0, "trigger_reason": "DAILY_GAINER"},
            {"symbol_id": "AAA", "trade_date": "2026-05-10", "close": 105, "volume_ratio_20": 2.1, "trigger_reason": "WEEKLY_GAINER"},
        ]
    )

    repeat = build_repeat_tracker(current_scores=current, historical_daily_log=history)

    row = repeat.iloc[0]
    assert row["daily_gainer_count_20d"] == 1
    assert row["weekly_gainer_count_20d"] == 1
    assert row["stealth_count_20d"] == 1


def test_weekly_and_stealth_survive_move_classifier_as_credible_triggers() -> None:
    frame = pd.DataFrame(
        [
            {"symbol_id": "WWW", "trigger_reason": "WEEKLY_GAINER"},
            {"symbol_id": "STEALTH", "trigger_reason": "STEALTH_ACCUMULATION"},
        ]
    )

    classified = classify_move(frame)

    assert classified.loc[classified["symbol_id"].eq("WWW"), "move_tag"].iloc[0] == "WEEKLY_MOMENTUM"
    assert classified.loc[classified["symbol_id"].eq("WWW"), "trigger_quality_score"].iloc[0] == 14
    assert classified.loc[classified["symbol_id"].eq("WWW"), "credible_trigger"].iloc[0]
    assert classified.loc[classified["symbol_id"].eq("STEALTH"), "move_tag"].iloc[0] == "STEALTH_ACCUMULATION"
    assert classified.loc[classified["symbol_id"].eq("STEALTH"), "trigger_quality_score"].iloc[0] == 13
    assert classified.loc[classified["symbol_id"].eq("STEALTH"), "credible_trigger"].iloc[0]


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


def test_lifecycle_does_not_drop_weekly_or_stealth_as_one_candle_drama() -> None:
    scores = pd.DataFrame(
        [
            {
                "symbol_id": "WWW",
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
                "trigger_reason": "WEEKLY_GAINER",
            },
            {
                "symbol_id": "STEALTH",
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
                "trigger_reason": "STEALTH_ACCUMULATION",
            },
        ]
    )
    repeat = pd.DataFrame(
        [
            {
                "symbol_id": "WWW",
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
            },
            {
                "symbol_id": "STEALTH",
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
            },
        ]
    )

    active, archived = apply_lifecycle(scores, repeat)

    assert set(active["symbol_id"]) == {"WWW", "STEALTH"}
    assert archived.empty


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
