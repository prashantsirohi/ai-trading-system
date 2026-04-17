from __future__ import annotations

import pandas as pd

from features.feature_store import add_feature_confidence


def test_add_feature_confidence_respects_readiness_and_provider_confidence() -> None:
    frame = pd.DataFrame(
        {
            "feature_ready": [True, False, True, True],
            "provider_confidence": [0.9, 0.8, 1.5, -0.2],
        }
    )

    out = add_feature_confidence(frame)

    assert out["feature_confidence"].tolist() == [0.9, 0.0, 1.0, 0.0]

