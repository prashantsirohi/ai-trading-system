from __future__ import annotations

import pandas as pd
import pytest

from ai_trading_system.domains.investigator.stage_pattern_context import (
    classify_stage_row,
    enrich_investigator_context,
    normalise_stage_inputs,
    rank_pattern_symbols,
)


def _stage_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol_id": "AAA",
        "close": 110.0,
        "sma_50": 105.0,
        "sma_200": 100.0,
        "sma50_slope_20d_pct": 1.0,
        "sma200_slope_20d_pct": 0.5,
        "near_52w_high_pct": 10.0,
        "relative_strength": 70.0,
    }
    row.update(overrides)
    return row


def test_stage_input_normalisation_direct_alias_and_derived_sources() -> None:
    frame = pd.DataFrame(
        [
            _stage_row(symbol_id="DIRECT"),
            _stage_row(
                symbol_id="ALIAS",
                sma_200=pd.NA,
                sma200=100.0,
                sma50_slope_20d_pct=pd.NA,
                sma_50_slope_20d_pct=1.2,
                sma200_slope_20d_pct=pd.NA,
                sma_200_slope_20d_pct=0.4,
                near_52w_high_pct=pd.NA,
                distance_from_52w_high_pct=12.0,
            ),
            _stage_row(
                symbol_id="DERIVED",
                sma_200=pd.NA,
                above_200dma_pct=10.0,
                near_52w_high_pct=pd.NA,
                high_52w=125.0,
            ),
            _stage_row(
                symbol_id="DERIVED_ALIAS",
                sma_200=pd.NA,
                close_vs_sma200_pct=10.0,
            ),
        ]
    )

    rows = normalise_stage_inputs(frame).set_index("symbol_id")

    assert rows.loc["DIRECT", "stage_sma200_source"] == "DIRECT"
    assert rows.loc["DIRECT", "stage_sma50_slope_source"] == "DIRECT"
    assert rows.loc["DIRECT", "stage_near_high_source"] == "DIRECT"
    assert rows.loc["DIRECT", "stage_input_confidence"] == "HIGH"
    assert rows.loc["ALIAS", "stage_sma200_source"] == "ALIAS"
    assert rows.loc["ALIAS", "stage_sma50_slope_source"] == "ALIAS"
    assert rows.loc["ALIAS", "stage_sma200_slope_source"] == "ALIAS"
    assert rows.loc["ALIAS", "stage_near_high_source"] == "ALIAS"
    assert rows.loc["DERIVED", "sma_200"] == pytest.approx(100.0)
    assert rows.loc["DERIVED", "near_52w_high_pct"] == 12.0
    assert rows.loc["DERIVED", "stage_sma200_source"] == "DERIVED"
    assert rows.loc["DERIVED", "stage_near_high_source"] == "DERIVED"
    assert rows.loc["DERIVED", "stage_input_confidence"] == "MEDIUM"
    assert rows.loc["DERIVED_ALIAS", "sma_200"] == pytest.approx(100.0)
    assert rows.loc["DERIVED_ALIAS", "stage_sma200_source"] == "DERIVED"


def test_stage_sma200_derivation_rejects_invalid_denominator() -> None:
    normalized = normalise_stage_inputs(
        pd.DataFrame([_stage_row(sma_200=pd.NA, above_200dma_pct=float("inf"))])
    ).iloc[0]

    assert pd.isna(normalized["sma_200"])
    assert normalized["stage_sma200_source"] == "MISSING"
    assert normalized["stage_input_complete"] is False or not bool(normalized["stage_input_complete"])


def test_stage_input_boolean_slope_fallback_does_not_fabricate_numeric_value() -> None:
    row = _stage_row(
        sma50_slope_20d_pct=pd.NA,
        sma50_slope_positive=True,
        sma200_slope_20d_pct=pd.NA,
        sma200_slope_positive=False,
    )

    normalized = normalise_stage_inputs(pd.DataFrame([row])).iloc[0]

    assert pd.isna(normalized["sma50_slope_20d_pct"])
    assert pd.isna(normalized["sma200_slope_20d_pct"])
    assert bool(normalized["sma50_slope_positive"]) is True
    assert bool(normalized["sma200_slope_positive"]) is False
    assert normalized["stage_sma50_slope_source"] == "BOOLEAN_FALLBACK"
    assert normalized["stage_sma200_slope_source"] == "BOOLEAN_FALLBACK"
    assert normalized["stage_input_confidence"] == "LOW"


def test_stage_near_high_prox_conversion_and_invalid_value() -> None:
    valid = _stage_row(near_52w_high_pct=pd.NA, high_52w=pd.NA, prox_high=82.0)
    invalid = _stage_row(near_52w_high_pct=pd.NA, high_52w=pd.NA, prox_high=120.0)

    rows = normalise_stage_inputs(pd.DataFrame([valid, invalid]))

    assert rows.iloc[0]["near_52w_high_pct"] == 18.0
    assert rows.iloc[0]["stage_near_high_source"] == "DERIVED"
    assert rows.iloc[1]["stage_near_high_source"] == "MISSING"
    assert "near_52w_high_pct" in rows.iloc[1]["stage_input_missing_fields"]


def test_stage_classifier_covers_base_distribution_and_hard_override() -> None:
    base = classify_stage_row(pd.Series(_stage_row(close=102, sma_50=103, near_52w_high_pct=30)))
    distribution = classify_stage_row(
        pd.Series(_stage_row(distribution_flag=True, close=110, sma_50=105, near_52w_high_pct=8))
    )

    assert base["stage_label"] == "STAGE_1_BASE"
    assert distribution["stage_label"] == "STAGE_3_DISTRIBUTION"


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
                "relative_strength": 75.0,
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
                "relative_strength": 65.0,
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
                "sma200_slope_20d_pct": -1.0,
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


def test_rank_and_stock_context_coalesce_fields_instead_of_dropping_rank_values() -> None:
    ranked = pd.DataFrame([_stage_row(symbol_id="AAA", composite_score=80)])
    stock = pd.DataFrame([{"symbol_id": "AAA", "close": 111, "rank_position": 1}])

    enriched, diagnostics = enrich_investigator_context(
        pd.DataFrame([{"symbol_id": "AAA"}]),
        ranked=ranked,
        stock_scan=stock,
    )

    row = enriched.iloc[0]
    assert row["close"] == 111
    assert row["sma_200"] == 100
    assert row["sma50_slope_20d_pct"] == 1.0
    assert row["stage_label"] == "STAGE_2_CONFIRMED"
    assert diagnostics["stage_input_complete_rows"] == 1
