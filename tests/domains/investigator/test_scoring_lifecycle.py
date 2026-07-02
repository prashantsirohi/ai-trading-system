from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.lifecycle import apply_lifecycle
from ai_trading_system.domains.investigator.move_classifier import classify_move
from ai_trading_system.domains.investigator.repeat_tracker import build_repeat_tracker
from ai_trading_system.domains.investigator.scoring import finalize_scores, final_gate


FINAL_GATE_EXPECTED_COLUMNS = [
    "symbol_id",
    "trade_date",
    "verdict",
    "final_score",
    "thesis",
    "invalidation_level",
    "exit_plan",
    "gate_status",
    "hard_trap_flag",
    "credible_trigger",
]


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


def test_high_conviction_is_assigned_by_final_score_threshold() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "HIGH",
                "price_structure_score": 15,
                "volume_delivery_score": 20,
                "fundamental_score": 20,
                "trigger_quality_score": 9,
                "sector_support_score": 8,
                "buyer_fingerprint_score": 0,
                "composite_score": 70,
                "credible_trigger": True,
                "hard_trap_flag": False,
                "fa_missing": False,
            },
            {
                "symbol_id": "MED",
                "price_structure_score": 15,
                "volume_delivery_score": 20,
                "fundamental_score": 20,
                "trigger_quality_score": 9,
                "sector_support_score": 7,
                "buyer_fingerprint_score": 0,
                "composite_score": 70,
                "credible_trigger": True,
                "hard_trap_flag": False,
                "fa_missing": False,
            },
        ]
    )

    scored = finalize_scores(frame)

    # Current intended behavior: HIGH_CONVICTION is assigned by final_score >= 80.
    assert scored.loc[scored["symbol_id"].eq("HIGH"), "final_score"].iloc[0] == 80
    assert scored.loc[scored["symbol_id"].eq("HIGH"), "verdict"].iloc[0] == "HIGH_CONVICTION"
    assert scored.loc[scored["symbol_id"].eq("MED"), "final_score"].iloc[0] == 79
    assert scored.loc[scored["symbol_id"].eq("MED"), "verdict"].iloc[0] == "MEDIUM_CONVICTION"


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


def test_final_gate_excludes_high_score_hard_trap_noise_trap() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "TRAP",
                "trade_date": "2026-05-07",
                "verdict": "NOISE_TRAP",
                "final_score": 72,
                "hard_trap_flag": True,
                "credible_trigger": True,
                "low": 95,
                "close": 100,
            }
        ]
    )

    gate = final_gate(frame)

    assert gate.empty
    assert list(gate.columns) == FINAL_GATE_EXPECTED_COLUMNS


def test_final_gate_includes_medium_conviction_with_review_defaults() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "GOOD",
                "trade_date": "2026-05-07",
                "verdict": "MEDIUM_CONVICTION",
                "final_score": 61,
                "hard_trap_flag": False,
                "credible_trigger": True,
                "trigger_reason": "STEALTH_ACCUMULATION",
                "move_tag": "SECTOR_ROTATION",
                "sector": "Capital Goods",
                "low": 94.25,
                "close": 101.0,
            }
        ]
    )

    gate = final_gate(frame)

    assert len(gate) == 1
    row = gate.iloc[0]
    assert row["symbol_id"] == "GOOD"
    assert row["gate_status"] == "PENDING"
    assert str(row["thesis"]).strip()
    assert "Stealth Accumulation" in row["thesis"]
    assert row["invalidation_level"] == "94.25"
    assert str(row["exit_plan"]).strip()
    assert not bool(row["hard_trap_flag"])
    assert bool(row["credible_trigger"])


def test_final_gate_uses_close_discount_when_no_invalidation_or_low() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "FALLBACK",
                "trade_date": "2026-05-07",
                "verdict": "HIGH_CONVICTION",
                "final_score": 84,
                "hard_trap_flag": False,
                "credible_trigger": False,
                "trigger_reason": "DAILY_GAINER",
                "close": 100.0,
            }
        ]
    )

    gate = final_gate(frame)

    assert len(gate) == 1
    assert gate.iloc[0]["invalidation_level"] == "93"


def test_final_gate_empty_input_returns_compatible_columns() -> None:
    gate = final_gate(pd.DataFrame())

    assert gate.empty
    assert list(gate.columns) == FINAL_GATE_EXPECTED_COLUMNS


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


def test_repeat_tracker_counts_same_date_sector_peers() -> None:
    current = pd.DataFrame(
        [
            {"symbol_id": "AAA", "trade_date": "2026-05-21", "close": 112, "volume_ratio_20": 2.5, "final_score": 58, "sector": "Capital Goods"},
            {"symbol_id": "BBB", "trade_date": "2026-05-21", "close": 98, "volume_ratio_20": 2.1, "final_score": 56, "sector": "Capital Goods"},
            {"symbol_id": "CCC", "trade_date": "2026-05-21", "close": 76, "volume_ratio_20": 1.9, "final_score": 55, "sector": "Capital Goods"},
        ]
    )

    repeat = build_repeat_tracker(current_scores=current, historical_daily_log=pd.DataFrame())

    clusters = repeat.set_index("symbol_id")["sector_cluster_count"].to_dict()
    assert clusters == {"AAA": 3, "BBB": 3, "CCC": 3}


def test_repeat_tracker_sector_cluster_count_isolated_by_sector() -> None:
    current = pd.DataFrame(
        [
            {"symbol_id": "AAA", "trade_date": "2026-05-21", "close": 112, "volume_ratio_20": 2.5, "final_score": 58, "sector": "Capital Goods"},
            {"symbol_id": "BBB", "trade_date": "2026-05-21", "close": 98, "volume_ratio_20": 2.1, "final_score": 56, "sector": "Capital Goods"},
            {"symbol_id": "CCC", "trade_date": "2026-05-21", "close": 76, "volume_ratio_20": 1.9, "final_score": 55, "sector": "FMCG"},
        ]
    )

    repeat = build_repeat_tracker(current_scores=current, historical_daily_log=pd.DataFrame())

    clusters = repeat.set_index("symbol_id")["sector_cluster_count"].to_dict()
    assert clusters == {"AAA": 2, "BBB": 2, "CCC": 1}


def test_repeat_tracker_missing_sector_does_not_crash() -> None:
    current = pd.DataFrame(
        [
            {"symbol_id": "AAA", "trade_date": "2026-05-21", "close": 112, "volume_ratio_20": 2.5, "final_score": 58, "sector": ""},
            {"symbol_id": "BBB", "trade_date": "2026-05-21", "close": 98, "volume_ratio_20": 2.1, "final_score": 56, "sector": pd.NA},
        ]
    )

    repeat = build_repeat_tracker(current_scores=current, historical_daily_log=pd.DataFrame())

    clusters = repeat.set_index("symbol_id")["sector_cluster_count"].to_dict()
    assert clusters == {"AAA": 0, "BBB": 0}


def test_repeat_tracker_counts_history_even_if_prior_row_was_archived() -> None:
    current = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-07",
                "close": 110,
                "volume_ratio_20": 2.5,
                "rank_position": 35,
                "final_score": 58,
            }
        ]
    )
    history = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-01",
                "close": 100,
                "volume_ratio_20": 1.0,
                "rank_position": 80,
                "final_score": 30,
                "drop_reason": "LOW_DELIVERY_NO_REPEAT",
            }
        ]
    )

    repeat = build_repeat_tracker(current_scores=current, historical_daily_log=history)

    row = repeat.iloc[0]
    assert row["appearance_count_20d"] == 2
    assert round(row["price_progression_pct"], 1) == 10
    assert row["rank_change_20d"] == -45


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


def test_sector_rotation_currently_overrides_move_tag_priority() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "WWW",
                "trigger_reason": "WEEKLY_GAINER",
                "sector_rotation_active": True,
            }
        ]
    )

    classified = classify_move(frame)

    # Guardrail for current behavior: sector context can override the trigger move tag.
    assert classified.iloc[0]["move_tag"] == "SECTOR_ROTATION"


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


def test_lifecycle_noise_trap_reason_precedes_low_delivery_reason() -> None:
    scores = pd.DataFrame(
        [
            {
                "symbol_id": "TRAP",
                "trade_date": "2026-05-01",
                "verdict": "NOISE_TRAP",
                "final_score": 30,
                "composite_score": 30,
                "credible_trigger": False,
                "sector_support_score": 0,
                "sector_rotation_active": False,
                "long_upper_wick_trap": False,
                "low_delivery_flag": True,
                "fa_improvement": False,
                "sector_clustering": False,
            }
        ]
    )
    repeat = pd.DataFrame(
        [
            {
                "symbol_id": "TRAP",
                "first_seen_date": "2026-05-01",
                "last_seen_date": "2026-05-01",
                "days_since_last_seen": 5,
                "appearance_count_20d": 1,
                "price_progression_pct": -2,
            }
        ]
    )

    active, archived = apply_lifecycle(scores, repeat)

    assert active.empty
    assert archived.iloc[0]["drop_reason"] == "NOISE_TRAP"


def test_lifecycle_same_day_fresh_trigger_is_not_archived_by_aging_rules() -> None:
    scores = pd.DataFrame(
        [
            {
                "symbol_id": "FRESH",
                "trade_date": "2026-05-07",
                "verdict": "WATCH_ONLY",
                "final_score": 42,
                "composite_score": 50,
                "credible_trigger": False,
                "sector_support_score": 0,
                "sector_rotation_active": False,
                "long_upper_wick_trap": False,
                "low_delivery_flag": True,
                "fa_improvement": False,
                "sector_clustering": False,
            }
        ]
    )
    repeat = pd.DataFrame(
        [
            {
                "symbol_id": "FRESH",
                "first_seen_date": "2026-05-07",
                "last_seen_date": "2026-05-07",
                "days_since_last_seen": 0,
                "appearance_count_20d": 1,
                "score_current": 42,
                "score_peak": 42,
                "rank_current": 80,
                "rank_change_20d": 0,
                "price_progression_pct": -2,
                "volume_escalation": False,
                "sector_cluster_count": 0,
            }
        ]
    )

    active, archived = apply_lifecycle(scores, repeat)

    assert archived.empty
    assert active.iloc[0]["status"] == "WATCHLIST"


def test_lifecycle_allows_archived_symbol_back_when_current_evidence_improves() -> None:
    current = pd.DataFrame(
        [
            {
                "symbol_id": "RETURN",
                "trade_date": "2026-05-07",
                "close": 112,
                "volume_ratio_20": 2.5,
                "rank_position": 35,
                "final_score": 58,
            }
        ]
    )
    archived_history = pd.DataFrame(
        [
            {
                "symbol_id": "RETURN",
                "trade_date": "2026-05-01",
                "close": 100,
                "volume_ratio_20": 1.0,
                "rank_position": 90,
                "final_score": 30,
                "drop_reason": "LOW_DELIVERY_NO_REPEAT",
            }
        ]
    )
    repeat = build_repeat_tracker(current_scores=current, historical_daily_log=archived_history)
    scores = current.assign(
        verdict="MEDIUM_CONVICTION",
        composite_score=70,
        credible_trigger=True,
        sector_support_score=0,
        sector_rotation_active=False,
        long_upper_wick_trap=False,
        low_delivery_flag=False,
        fa_improvement=False,
        sector_clustering=False,
    )

    active, archived = apply_lifecycle(scores, repeat)

    assert archived.empty
    assert active.iloc[0]["symbol_id"] == "RETURN"
    assert active.iloc[0]["status"] == "ACTIVE_RESEARCH"


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
