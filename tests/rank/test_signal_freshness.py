from __future__ import annotations

import pandas as pd

from services.rank.factors import add_signal_freshness


def test_add_signal_freshness_derives_age_from_timestamp() -> None:
    frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "timestamp": "2026-04-15"},
            {"symbol_id": "BBB", "timestamp": "2026-04-13"},
        ]
    )

    out = add_signal_freshness(frame)

    aaa = out.loc[out["symbol_id"] == "AAA"].iloc[0]
    bbb = out.loc[out["symbol_id"] == "BBB"].iloc[0]

    assert aaa["signal_age"] == 0
    assert bbb["signal_age"] == 2
    assert aaa["signal_decay_score"] == 1.0
    assert 0.0 <= bbb["signal_decay_score"] < 1.0

