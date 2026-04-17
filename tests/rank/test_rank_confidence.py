from __future__ import annotations

import pandas as pd
import pytest

from services.rank.composite import compute_rank_confidence


def test_compute_rank_confidence_uses_feature_eligibility_and_penalty() -> None:
    frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "feature_confidence": 0.9, "eligible_rank": True, "penalty_score": 10.0},
            {"symbol_id": "BBB", "feature_confidence": 0.8, "eligible_rank": False, "penalty_score": 0.0},
        ]
    )

    out = compute_rank_confidence(frame)

    assert out.loc[out["symbol_id"] == "AAA", "rank_confidence"].iloc[0] == pytest.approx(0.81)
    assert out.loc[out["symbol_id"] == "BBB", "rank_confidence"].iloc[0] == 0.0

