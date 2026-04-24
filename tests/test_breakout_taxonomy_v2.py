from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.ranking.breakout import (
    _prepare_rank_context,
    compute_breakout_v2_scores,
)


def _volume_confirmation_candidate(
    symbol_id: str,
    *,
    ratio_confirmed: bool = False,
    z20: float | None = None,
    z50: float | None = None,
    price_breakout: bool = True,
) -> dict[str, object]:
    return {
        "symbol_id": symbol_id,
        "setup_quality": 80.0,
        "breakout_pct": 1.2,
        "is_resistance_breakout_50d": price_breakout,
        "is_high_52w_breakout": price_breakout,
        "is_consolidation_breakout": price_breakout,
        "is_volume_confirmed_breakout": ratio_confirmed,
        "rel_strength_score": 85.0,
        "sector_rs_value": 0.72,
        "sector_rs_percentile": 88.0,
        "volume_zscore_20": z20,
        "volume_zscore_50": z50,
    }


def test_breakout_score_contract_matches_v2_spec() -> None:
    candidates = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "setup_quality": 75.0,
                "breakout_pct": 1.2,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": True,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 85.0,
                "sector_rs_value": 0.72,
                "sector_rs_percentile": 88.0,
            }
        ]
    )

    out = compute_breakout_v2_scores(
        candidates,
        market_bias="BULLISH",
        breadth_score=68.0,
        market_bias_allowlist=["BULLISH", "NEUTRAL"],
        min_breadth_score=45.0,
        sector_rs_percentile_min=60.0,
        breakout_qualified_min_score=3,
    )

    assert int(out.iloc[0]["breakout_score"]) == 8
    assert int(out.iloc[0]["breakout_rank"]) == 1
    assert out.iloc[0]["breakout_state"] == "qualified"


def test_breakout_rank_tie_breaks_by_setup_quality() -> None:
    candidates = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "setup_quality": 92.0,
                "breakout_pct": 1.0,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": False,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": False,
                "rel_strength_score": 81.0,
                "sector_rs_value": 0.61,
                "sector_rs_percentile": 70.0,
            },
            {
                "symbol_id": "BBB",
                "setup_quality": 80.0,
                "breakout_pct": 1.0,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": False,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": False,
                "rel_strength_score": 81.0,
                "sector_rs_value": 0.62,
                "sector_rs_percentile": 71.0,
            },
        ]
    )

    out = compute_breakout_v2_scores(
        candidates,
        market_bias="BULLISH",
        breadth_score=60.0,
        sector_rs_percentile_min=60.0,
        breakout_qualified_min_score=3,
    )

    assert out.iloc[0]["symbol_id"] == "AAA"
    assert int(out.iloc[0]["breakout_rank"]) == 1
    assert out.iloc[1]["symbol_id"] == "BBB"
    assert int(out.iloc[1]["breakout_rank"]) == 2


def test_breakout_state_filtered_by_regime_and_sector_gates() -> None:
    candidates = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "setup_quality": 90.0,
                "breakout_pct": 1.3,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": True,
                "is_consolidation_breakout": False,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 90.0,
                "sector_rs_value": 0.40,
                "sector_rs_percentile": 25.0,
            }
        ]
    )

    out = compute_breakout_v2_scores(
        candidates,
        market_bias="BEARISH",
        breadth_score=30.0,
        market_bias_allowlist=["BULLISH", "NEUTRAL"],
        min_breadth_score=45.0,
        sector_rs_percentile_min=60.0,
        breakout_qualified_min_score=3,
    )

    assert out.iloc[0]["breakout_state"] == "filtered_by_regime"
    assert "market_bias_not_allowed" in out.iloc[0]["filter_reason"]
    assert "breadth_below_threshold" in out.iloc[0]["filter_reason"]
    assert "sector_rs_below_percentile" in out.iloc[0]["filter_reason"]


def test_symbol_trend_tiers_and_reason_codes_are_deterministic() -> None:
    candidates = pd.DataFrame(
        [
            {
                "symbol_id": "A",
                "setup_quality": 90.0,
                "breakout_pct": 1.0,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": False,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 85.0,
                "sector_rs_value": 0.80,
                "sector_rs_percentile": 90.0,
                "above_sma200": True,
                "sma50_slope_20d_pct": 1.2,
                "near_52w_high_pct": 4.0,
            },
            {
                "symbol_id": "B",
                "setup_quality": 80.0,
                "breakout_pct": 1.1,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": False,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 85.0,
                "sector_rs_value": 0.80,
                "sector_rs_percentile": 90.0,
                "above_sma200": True,
                "sma50_slope_20d_pct": -0.1,
                "near_52w_high_pct": 5.0,
            },
            {
                "symbol_id": "C",
                "setup_quality": 70.0,
                "breakout_pct": 1.2,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": False,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 85.0,
                "sector_rs_value": 0.80,
                "sector_rs_percentile": 90.0,
                "above_sma200": False,
                "sma50_slope_20d_pct": -0.5,
                "near_52w_high_pct": 35.0,
            },
        ]
    )

    out = compute_breakout_v2_scores(
        candidates,
        market_bias="BULLISH",
        breadth_score=60.0,
        market_bias_allowlist=["BULLISH", "NEUTRAL"],
        min_breadth_score=45.0,
        sector_rs_percentile_min=60.0,
        breakout_symbol_trend_gate_enabled=True,
        breakout_symbol_near_high_max_pct=15.0,
    )
    lookup = {row["symbol_id"]: row for _, row in out.iterrows()}

    assert lookup["A"]["candidate_tier"] == "A"
    assert lookup["A"]["breakout_state"] == "qualified"
    assert lookup["A"]["symbol_trend_reasons"] == "ABOVE_SMA200,SMA50_SLOPE_POSITIVE,NEAR_52W_HIGH"

    assert lookup["B"]["candidate_tier"] == "B"
    assert lookup["B"]["breakout_state"] == "watchlist"
    assert "SMA50_SLOPE_NEGATIVE" in lookup["B"]["symbol_trend_reasons"]

    assert lookup["C"]["candidate_tier"] == "C"
    assert lookup["C"]["filtered_by_symbol_trend"] is True
    assert lookup["C"]["breakout_state"] == "filtered_by_symbol_trend"
    assert "BELOW_SMA200" in lookup["C"]["symbol_trend_reasons"]
    assert "FAR_FROM_52W_HIGH" in lookup["C"]["symbol_trend_reasons"]


def test_breakout_z20_can_confirm_when_ratio_is_weak() -> None:
    out = compute_breakout_v2_scores(
        pd.DataFrame(
            [
                _volume_confirmation_candidate(
                    "AAA",
                    ratio_confirmed=False,
                    z20=2.5,
                )
            ]
        ),
        market_bias="BULLISH",
        breadth_score=68.0,
        market_bias_allowlist=["BULLISH", "NEUTRAL"],
        min_breadth_score=45.0,
        sector_rs_percentile_min=60.0,
        breakout_qualified_min_score=3,
    )

    row = out.iloc[0]
    assert bool(row["is_volume_ratio_confirmed"]) is False
    assert bool(row["is_z20_confirmed"]) is True
    assert bool(row["is_any_volume_confirmed"]) is True
    assert bool(row["is_any_volume_confirmed_breakout"]) is True
    assert int(row["breakout_score"]) == 8
    assert row["breakout_state"] == "qualified"


def test_breakout_z50_fallback_applies_when_z20_is_nan() -> None:
    out = compute_breakout_v2_scores(
        pd.DataFrame(
            [
                _volume_confirmation_candidate(
                    "AAA",
                    ratio_confirmed=False,
                    z20=float("nan"),
                    z50=2.4,
                )
            ]
        ),
        market_bias="BULLISH",
        breadth_score=68.0,
        market_bias_allowlist=["BULLISH", "NEUTRAL"],
        min_breadth_score=45.0,
        sector_rs_percentile_min=60.0,
        breakout_qualified_min_score=3,
    )

    row = out.iloc[0]
    assert bool(row["is_z20_confirmed"]) is False
    assert bool(row["is_z50_confirmed"]) is True
    assert bool(row["is_any_volume_confirmed"]) is True
    assert bool(row["is_any_volume_confirmed_breakout"]) is True
    assert int(row["breakout_score"]) == 8


def test_breakout_combined_volume_confirmation_gets_single_bounded_boost() -> None:
    out = compute_breakout_v2_scores(
        pd.DataFrame(
            [
                _volume_confirmation_candidate("RATIO", ratio_confirmed=True),
                _volume_confirmation_candidate("Z20", ratio_confirmed=False, z20=2.6),
                _volume_confirmation_candidate("COMBINED", ratio_confirmed=True, z20=2.6),
            ]
        ),
        market_bias="BULLISH",
        breadth_score=68.0,
        market_bias_allowlist=["BULLISH", "NEUTRAL"],
        min_breadth_score=45.0,
        sector_rs_percentile_min=60.0,
        breakout_qualified_min_score=3,
    )
    scores = out.set_index("symbol_id")["breakout_score"].astype(int).to_dict()

    assert scores["RATIO"] == 8
    assert scores["Z20"] == 8
    assert scores["COMBINED"] == 10
    assert scores["COMBINED"] > scores["RATIO"]
    assert scores["COMBINED"] > scores["Z20"]


def test_breakout_zscore_does_not_help_without_price_breakout() -> None:
    out = compute_breakout_v2_scores(
        pd.DataFrame(
            [
                _volume_confirmation_candidate(
                    "AAA",
                    ratio_confirmed=False,
                    z20=3.1,
                    price_breakout=False,
                )
            ]
        ),
        market_bias="BULLISH",
        breadth_score=68.0,
        market_bias_allowlist=["BULLISH", "NEUTRAL"],
        min_breadth_score=45.0,
        sector_rs_percentile_min=60.0,
        breakout_qualified_min_score=3,
    )

    row = out.iloc[0]
    assert bool(row["is_any_volume_confirmed"]) is True
    assert bool(row["is_any_volume_confirmed_breakout"]) is False
    assert int(row["breakout_score"]) == 2


def test_breakout_missing_zscore_columns_degrade_to_ratio_logic() -> None:
    out = compute_breakout_v2_scores(
        pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "setup_quality": 75.0,
                    "breakout_pct": 1.2,
                    "is_resistance_breakout_50d": True,
                    "is_high_52w_breakout": True,
                    "is_consolidation_breakout": True,
                    "is_volume_confirmed_breakout": True,
                    "rel_strength_score": 85.0,
                    "sector_rs_value": 0.72,
                    "sector_rs_percentile": 88.0,
                }
            ]
        ),
        market_bias="BULLISH",
        breadth_score=68.0,
        market_bias_allowlist=["BULLISH", "NEUTRAL"],
        min_breadth_score=45.0,
        sector_rs_percentile_min=60.0,
        breakout_qualified_min_score=3,
    )

    row = out.iloc[0]
    assert bool(row["is_volume_ratio_confirmed"]) is True
    assert bool(row["is_z20_confirmed"]) is False
    assert bool(row["is_z50_confirmed"]) is False
    assert bool(row["is_any_volume_confirmed"]) is True
    assert int(row["breakout_score"]) == 8


def test_breakout_uses_structural_stage2_as_authoritative_gate() -> None:
    candidates = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "setup_quality": 95.0,
                "breakout_pct": 1.0,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": True,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 88.0,
                "sector_rs_value": 0.8,
                "sector_rs_percentile": 90.0,
                "stage2_score": 92.0,
                "is_stage2_structural": True,
                "is_stage2_candidate": True,
                "stage2_hard_fail_reason": "",
                "stage2_fail_reason": "",
            },
            {
                "symbol_id": "BBB",
                "setup_quality": 92.0,
                "breakout_pct": 1.1,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": True,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 90.0,
                "sector_rs_value": 0.8,
                "sector_rs_percentile": 90.0,
                "stage2_score": 95.0,
                "is_stage2_structural": False,
                "is_stage2_candidate": True,
                "stage2_hard_fail_reason": "sma200_slope_negative",
                "stage2_fail_reason": "sma200_slope_negative,rs_below_85th_pctile",
            },
        ]
    )

    out = compute_breakout_v2_scores(
        candidates,
        market_bias="BULLISH",
        breadth_score=60.0,
        sector_rs_percentile_min=60.0,
    )
    lookup = {row["symbol_id"]: row for _, row in out.iterrows()}

    assert lookup["AAA"]["breakout_state"] == "qualified"
    assert lookup["AAA"]["candidate_tier"] == "A"
    assert bool(lookup["AAA"]["stage2_gate_passed"]) is True

    assert lookup["BBB"]["breakout_state"] == "watchlist"
    assert lookup["BBB"]["candidate_tier"] == "C"
    assert bool(lookup["BBB"]["stage2_gate_passed"]) is False
    assert lookup["BBB"]["filter_reason"] == "sma200_slope_negative,rs_below_85th_pctile"


def test_breakout_structural_rejection_uses_hard_fail_reason() -> None:
    candidates = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "setup_quality": 90.0,
                "breakout_pct": 1.0,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": False,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 86.0,
                "sector_rs_value": 0.8,
                "sector_rs_percentile": 90.0,
                "stage2_score": 42.0,
                "is_stage2_structural": False,
                "is_stage2_candidate": False,
                "stage2_hard_fail_reason": "below_sma200,far_from_52w_high",
                "stage2_fail_reason": "below_sma200,far_from_52w_high,rs_below_70th_pctile",
            }
        ]
    )

    out = compute_breakout_v2_scores(
        candidates,
        market_bias="BULLISH",
        breadth_score=60.0,
        sector_rs_percentile_min=60.0,
    )

    assert out.iloc[0]["breakout_state"] == "filtered_by_symbol_trend"
    assert out.iloc[0]["filter_reason"] == "below_sma200,far_from_52w_high"


def test_breakout_falls_back_when_structural_columns_are_absent() -> None:
    candidates = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "setup_quality": 90.0,
                "breakout_pct": 1.0,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": False,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 86.0,
                "sector_rs_value": 0.8,
                "sector_rs_percentile": 90.0,
                "stage2_score": 90.0,
                "stage2_fail_reason": "",
            },
            {
                "symbol_id": "BBB",
                "setup_quality": 88.0,
                "breakout_pct": 1.1,
                "is_resistance_breakout_50d": True,
                "is_high_52w_breakout": False,
                "is_consolidation_breakout": True,
                "is_volume_confirmed_breakout": True,
                "rel_strength_score": 84.0,
                "sector_rs_value": 0.8,
                "sector_rs_percentile": 90.0,
                "stage2_score": 45.0,
                "stage2_fail_reason": "non_stage2",
            },
        ]
    )

    out = compute_breakout_v2_scores(
        candidates,
        market_bias="BULLISH",
        breadth_score=60.0,
        sector_rs_percentile_min=60.0,
    )
    lookup = {row["symbol_id"]: row for _, row in out.iterrows()}

    assert lookup["AAA"]["breakout_state"] == "qualified"
    assert lookup["AAA"]["candidate_tier"] == "A"
    assert lookup["BBB"]["breakout_state"] == "filtered_by_symbol_trend"
    assert lookup["BBB"]["filter_reason"] == "non_stage2"


def test_prepare_rank_context_preserves_stage2_columns_for_breakout_scan() -> None:
    ranked = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "rel_strength_score": 88.0,
                "sector_rs_value": 0.81,
                "stage2_score": 92.0,
                "is_stage2_structural": True,
                "is_stage2_candidate": True,
                "is_stage2_uptrend": True,
                "stage2_label": "strong_stage2",
                "stage2_hard_fail_reason": "",
                "stage2_fail_reason": "",
            }
        ]
    )

    prepared = _prepare_rank_context(ranked)

    row = prepared.iloc[0]
    assert row["stage2_score"] == 92.0
    assert bool(row["is_stage2_structural"]) is True
    assert bool(row["is_stage2_candidate"]) is True
    assert row["stage2_label"] == "strong_stage2"
