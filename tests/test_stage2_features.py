from __future__ import annotations

import numpy as np
import pandas as pd

from ai_trading_system.domains.features.feature_store import STAGE2_FEATURE_COLUMNS
from ai_trading_system.domains.features.indicators import (
    _STAGE2_OUTPUT_COLS,
    add_stage2_features,
    add_volume_zscore_features,
)


def _make_frame(
    *,
    n: int = 260,
    close_start: float = 100.0,
    close_end: float = 200.0,
    sma_200_start: float = 90.0,
    sma_200_end: float = 110.0,
    near_52w_high_pct: float = 5.0,
    rel_strength_score: float = 90.0,
    volume_ratio_20: float = 2.0,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "close": np.linspace(close_start, close_end, n),
            "sma_200": np.linspace(sma_200_start, sma_200_end, n),
            "near_52w_high_pct": np.full(n, near_52w_high_pct, dtype=float),
            "rel_strength_score": np.full(n, rel_strength_score, dtype=float),
            "volume_ratio_20": np.full(n, volume_ratio_20, dtype=float),
        }
    )


def test_stage2_output_columns_and_feature_store_contract_include_structural_fields() -> None:
    expected = {
        "is_stage2_structural",
        "is_stage2_candidate",
        "is_stage2_uptrend",
        "stage2_hard_fail_reason",
        "stage2_fail_reason",
        "volume_zscore_20",
        "volume_zscore_50",
    }

    assert expected.issubset(set(_STAGE2_OUTPUT_COLS))
    assert expected.issubset(set(STAGE2_FEATURE_COLUMNS))


def test_structural_stage2_truth_table_and_alias_hold() -> None:
    out = add_stage2_features(_make_frame())
    last = out.iloc[-1]

    assert bool(last["is_stage2_structural"]) is True
    assert bool(last["is_stage2_candidate"]) is True
    assert bool(last["is_stage2_uptrend"]) is True
    assert bool(last["is_stage2_uptrend"]) == bool(last["is_stage2_structural"])
    assert last["stage2_label"] == "strong_stage2"
    assert float(last["stage2_score"]) == 100.0
    assert last["stage2_hard_fail_reason"] == ""
    assert last["stage2_fail_reason"] == ""


def test_non_structural_high_score_becomes_candidate_not_stage2() -> None:
    out = add_stage2_features(
        _make_frame(
            sma_200_start=100.0,
            sma_200_end=100.0,
            near_52w_high_pct=20.0,
            rel_strength_score=72.0,
            volume_ratio_20=1.0,
        )
    )
    last = out.iloc[-1]

    assert bool(last["is_stage2_structural"]) is False
    assert bool(last["is_stage2_candidate"]) is True
    assert bool(last["is_stage2_uptrend"]) is False
    assert last["stage2_label"] == "stage1_to_stage2"
    assert 50.0 <= float(last["stage2_score"]) < 70.0
    assert "sma200_slope_negative" in str(last["stage2_hard_fail_reason"])
    assert str(last["stage2_fail_reason"]).startswith("sma200_slope_negative")


def test_structural_pass_with_mid_score_maps_to_stage2_label() -> None:
    out = add_stage2_features(
        _make_frame(
            near_52w_high_pct=20.0,
            rel_strength_score=72.0,
            volume_ratio_20=1.0,
        )
    )
    last = out.iloc[-1]

    assert bool(last["is_stage2_structural"]) is True
    assert bool(last["is_stage2_candidate"]) is True
    assert last["stage2_label"] == "stage2"
    assert 70.0 <= float(last["stage2_score"]) < 85.0
    assert last["stage2_hard_fail_reason"] == ""
    assert "rs_below_85th_pctile" in str(last["stage2_fail_reason"])


def test_stage2_hard_fail_reason_captures_each_structural_predicate() -> None:
    base = _make_frame()

    below_sma150 = base.copy()
    below_sma150.loc[below_sma150.index[-1], "close"] = 80.0

    below_sma200 = _make_frame(sma_200_start=210.0, sma_200_end=230.0)
    far_from_high = _make_frame(near_52w_high_pct=30.0)
    negative_slope = _make_frame(sma_200_start=120.0, sma_200_end=110.0)

    results = {
        "below_sma150": add_stage2_features(below_sma150).iloc[-1]["stage2_hard_fail_reason"],
        "below_sma200": add_stage2_features(below_sma200).iloc[-1]["stage2_hard_fail_reason"],
        "far_from_52w_high": add_stage2_features(far_from_high).iloc[-1]["stage2_hard_fail_reason"],
        "sma200_slope_negative": add_stage2_features(negative_slope).iloc[-1]["stage2_hard_fail_reason"],
    }

    assert "below_sma150" in str(results["below_sma150"])
    assert "below_sma200" in str(results["below_sma200"])
    assert "sma150_below_sma200" in str(results["below_sma200"])
    assert "far_from_52w_high" in str(results["far_from_52w_high"])
    assert "sma200_slope_negative" in str(results["sma200_slope_negative"])


def test_missing_or_immature_structural_inputs_fail_conservatively() -> None:
    out = add_stage2_features(
        pd.DataFrame(
            {
                "close": np.linspace(100.0, 140.0, 120),
                "near_52w_high_pct": np.nan,
                "rel_strength_score": 80.0,
                "volume_ratio_20": 1.5,
            }
        )
    )
    last = out.iloc[-1]

    assert bool(last["is_stage2_structural"]) is False
    assert bool(last["is_stage2_uptrend"]) is False
    assert "below_sma200" in str(last["stage2_hard_fail_reason"])
    assert "sma200_slope_negative" in str(last["stage2_hard_fail_reason"])
    assert "far_from_52w_high" in str(last["stage2_hard_fail_reason"])


def test_stage2_fail_reason_orders_hard_reasons_before_soft_reasons() -> None:
    out = add_stage2_features(
        _make_frame(
            sma_200_start=100.0,
            sma_200_end=100.0,
            near_52w_high_pct=30.0,
            rel_strength_score=40.0,
            volume_ratio_20=1.0,
        )
    )
    fail_reason = str(out.iloc[-1]["stage2_fail_reason"]).split(",")

    assert fail_reason[:2] == ["sma200_slope_negative", "far_from_52w_high"]
    assert "rs_below_70th_pctile" in fail_reason
    assert "weak_volume" in fail_reason


def test_add_volume_zscore_features_uses_shifted_prior_windows_and_safe_nan() -> None:
    frame = pd.DataFrame(
        {
            "symbol_id": (["AAA"] * 22) + (["BBB"] * 52),
            "volume": ([100.0] * 20) + [300.0, 100.0] + ([100.0] * 50) + [400.0, 100.0],
        }
    )

    out = add_volume_zscore_features(frame)
    aaa = out[out["symbol_id"] == "AAA"].reset_index(drop=True)
    bbb = out[out["symbol_id"] == "BBB"].reset_index(drop=True)

    assert {"volume_zscore_20", "volume_zscore_50"}.issubset(out.columns)
    assert np.isnan(aaa.iloc[19]["volume_zscore_20"])
    assert np.isnan(aaa.iloc[20]["volume_zscore_20"])
    assert float(aaa.iloc[21]["volume_zscore_20"]) < 0.0
    assert np.isnan(bbb.iloc[49]["volume_zscore_50"])
    assert np.isnan(bbb.iloc[50]["volume_zscore_50"])
    assert float(bbb.iloc[51]["volume_zscore_50"]) < 0.0
