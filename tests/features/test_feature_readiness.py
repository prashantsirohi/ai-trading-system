from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.features.feature_store import add_feature_readiness


def test_add_feature_readiness_flags_insufficient_lookback() -> None:
    frame = pd.DataFrame(
        {
            "symbol_id": ["AAA", "AAA", "AAA", "BBB", "BBB"],
            "close": [100, 101, 102, 50, 51],
        }
    )

    out = add_feature_readiness(frame, min_lookback=3)

    assert out["feature_ready"].tolist() == [False, False, True, False, False]
