from __future__ import annotations

import pandas as pd

from services.rank.eligibility import apply_rank_eligibility


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

