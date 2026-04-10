from __future__ import annotations

import pandas as pd

from channel.breakout_scan import compute_breakout_v2_scores


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
