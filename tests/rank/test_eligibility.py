from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.ranking.eligibility import apply_rank_eligibility


def test_apply_rank_eligibility_marks_rejections() -> None:
    frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "close": 100.0, "feature_ready": True, "liquidity_score": 0.8},
            {"symbol_id": "BBB", "close": 10.0, "feature_ready": True, "liquidity_score": 0.8},
            {"symbol_id": "CCC", "close": 100.0, "feature_ready": False, "liquidity_score": 0.8},
            {"symbol_id": "DDD", "close": 100.0, "feature_ready": True, "liquidity_score": 0.1},
        ]
    )

    out = apply_rank_eligibility(frame, min_price=20.0, min_liquidity_score=0.2)

    assert out["eligible_rank"].tolist() == [True, False, False, False]
    assert out.loc[out["symbol_id"] == "BBB", "rejection_reasons"].iloc[0] == ["min_price"]
    assert out.loc[out["symbol_id"] == "CCC", "rejection_reasons"].iloc[0] == ["feature_not_ready"]
    assert out.loc[out["symbol_id"] == "DDD", "rejection_reasons"].iloc[0] == ["insufficient_liquidity"]


def test_apply_rank_eligibility_prefers_structural_stage2_gate_when_available() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "close": 100.0,
                "feature_ready": True,
                "liquidity_score": 0.8,
                "stage2_score": 92.0,
                "is_stage2_structural": True,
                "stage2_hard_fail_reason": "",
            },
            {
                "symbol_id": "BBB",
                "close": 100.0,
                "feature_ready": True,
                "liquidity_score": 0.8,
                "stage2_score": 95.0,
                "is_stage2_structural": False,
                "stage2_hard_fail_reason": "sma200_slope_negative",
            },
        ]
    )

    out = apply_rank_eligibility(
        frame,
        min_price=20.0,
        min_liquidity_score=0.2,
        stage2_gate_enabled=True,
        stage2_min_score=70.0,
    )

    assert out["eligible_rank"].tolist() == [True, False]
    assert out.loc[out["symbol_id"] == "BBB", "rejection_reasons"].iloc[0] == [
        "stage2:sma200_slope_negative"
    ]
