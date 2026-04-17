from __future__ import annotations

import pandas as pd

from features.pattern_features import compute_pattern_preconditions


def test_compute_pattern_preconditions_adds_expected_columns() -> None:
    rows = 40
    frame = pd.DataFrame(
        {
            "high": [100 + (i * 0.5) for i in range(rows)],
            "low": [98 + (i * 0.5) for i in range(rows)],
            "close": [99 + (i * 0.5) for i in range(rows)],
            "atr_14": [1.2 + (i * 0.01) for i in range(rows)],
        }
    )

    out = compute_pattern_preconditions(frame)

    for col in [
        "base_tightness",
        "consolidation_range_pct",
        "volatility_contraction",
        "pullback_depth_pct",
        "resistance_slope",
    ]:
        assert col in out.columns
        assert out[col].notna().any()

