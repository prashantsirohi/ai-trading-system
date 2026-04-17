from __future__ import annotations

import pandas as pd

from services.rank.factors import compute_penalty_score


def test_compute_penalty_score_is_additive() -> None:
    frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "close": 100.0, "sma_200": 110.0, "liquidity_score": 0.1, "atr_14": 10.0},
            {"symbol_id": "BBB", "close": 100.0, "sma_200": 90.0, "liquidity_score": 0.9, "atr_14": 2.0},
        ]
    )

    out = compute_penalty_score(frame)

    assert out.loc[out["symbol_id"] == "AAA", "penalty_score"].iloc[0] == 25.0
    assert out.loc[out["symbol_id"] == "BBB", "penalty_score"].iloc[0] == 0.0

