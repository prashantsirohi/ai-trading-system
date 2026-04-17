from __future__ import annotations

import pandas as pd
import pytest

from features.indicators import add_multi_timeframe_returns


def test_add_multi_timeframe_returns_computes_per_symbol() -> None:
    frame = pd.DataFrame(
        {
            "symbol_id": ["AAA"] * 6 + ["BBB"] * 6,
            "close": [100, 101, 102, 103, 104, 105, 50, 49, 48, 47, 46, 45],
        }
    )

    out = add_multi_timeframe_returns(frame)

    aaa_last = out[out["symbol_id"] == "AAA"].iloc[-1]
    bbb_last = out[out["symbol_id"] == "BBB"].iloc[-1]

    assert aaa_last["return_5d"] == pytest.approx(0.05)
    assert bbb_last["return_5d"] == pytest.approx(-0.1)
    assert out["return_20d"].isna().all()
    assert out["return_252d"].isna().all()
