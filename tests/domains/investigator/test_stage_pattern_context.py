from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.stage_pattern_context import (
    classify_stage_row,
    enrich_investigator_context,
    rank_pattern_symbols,
)


def test_stage_classifier_labels_confirmed_early_decline_and_unknown() -> None:
    confirmed = classify_stage_row(
        pd.Series(
            {
                "close": 120,
                "sma_50": 110,
                "sma_200": 90,
                "sma50_slope_20d_pct": 2.0,
                "sma200_slope_20d_pct": 0.5,
                "near_52w_high_pct": 8.0,
            }
        )
    )
    early = classify_stage_row(
        pd.Series(
            {
                "close": 105,
                "sma_50": 108,
                "sma_200": 95,
                "sma50_slope_20d_pct": 1.0,
                "sma200_slope_20d_pct": -0.2,
                "near_52w_high_pct": 20.0,
            }
        )
    )
    decline = classify_stage_row(
        pd.Series(
            {
                "close": 80,
                "sma_50": 90,
                "sma_200": 100,
                "sma50_slope_20d_pct": -1.0,
                "near_52w_high_pct": 40.0,
                "relative_strength": 25.0,
            }
        )
    )
    unknown = classify_stage_row(pd.Series({"close": 100}))

    assert confirmed["stage_label"] == "STAGE_2_CONFIRMED"
    assert early["stage_label"] == "STAGE_2_EARLY"
    assert decline["stage_label"] == "STAGE_4_DECLINE"
    assert unknown["stage_label"] == "UNKNOWN"


def test_enrich_context_joins_pattern_and_breakout_with_defaults() -> None:
    candidates = pd.DataFrame([{"symbol_id": "AAA", "close": 100}, {"symbol_id": "BBB", "close": 80}])
    ranked = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "close": 120,
                "sma_50": 110,
                "sma_200": 90,
                "sma50_slope_20d_pct": 2.0,
                "sma200_slope_20d_pct": 0.2,
                "near_52w_high_pct": 7.0,
                "composite_score": 88,
                "relative_strength": 75,
            },
            {
                "symbol_id": "BBB",
                "close": 80,
                "sma_200": 100,
                "sma50_slope_20d_pct": -1.0,
                "near_52w_high_pct": 35.0,
                "relative_strength": 20,
            },
        ]
    )
    pattern = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "pattern_family": "cup handle",
                "pattern_lifecycle_state": "confirmed",
                "pattern_score": 82,
                "setup_quality": 76,
            }
        ]
    )
    breakout = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "setup_family": "range_breakout",
                "breakout_score": 90,
                "candidate_tier": "A",
                "breakout_state": "qualified",
            }
        ]
    )

    enriched, diagnostics = enrich_investigator_context(
        candidates,
        ranked=ranked,
        breakout_scan=breakout,
        pattern_scan=pattern,
    )
    rows = enriched.set_index("symbol_id")

    assert rows.loc["AAA", "stage_label"] == "STAGE_2_CONFIRMED"
    assert rows.loc["AAA", "pattern_family"] == "CUP_HANDLE"
    assert rows.loc["AAA", "pattern_state"] == "CONFIRMED"
    assert rows.loc["AAA", "setup_quality_bucket"] == "HIGH"
    assert rows.loc["AAA", "candidate_tier"] == "A"
    assert bool(rows.loc["AAA", "qualified_breakout"])
    assert rows.loc["BBB", "pattern_family"] == "NONE"
    assert rows.loc["BBB", "candidate_tier"] == "NONE"
    assert diagnostics["pattern_matched_rows"] == 1
    assert diagnostics["breakout_matched_rows"] == 1


def test_missing_optional_artifacts_do_not_crash_and_rank_pattern_symbols() -> None:
    enriched, diagnostics = enrich_investigator_context(
        pd.DataFrame([{"symbol_id": "aaa", "close": 100}]),
        ranked=pd.DataFrame([{"symbol_id": "AAA"}]),
        breakout_scan=pd.DataFrame(),
        pattern_scan=pd.DataFrame(),
    )

    assert enriched.iloc[0]["symbol_id"] == "AAA"
    assert enriched.iloc[0]["pattern_family"] == "NONE"
    assert enriched.iloc[0]["candidate_tier"] == "NONE"
    assert "pattern_scan missing" in diagnostics["warnings"]
    assert rank_pattern_symbols(pd.DataFrame([{"symbol_id": "aaa"}, {"symbol_id": "BBB"}])) == {"AAA", "BBB"}
