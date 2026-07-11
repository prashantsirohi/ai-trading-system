from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.investigator.fundamentals import score_fundamentals


def test_fundamental_snapshot_replaces_stale_context_columns_without_suffixes() -> None:
    frame = pd.DataFrame([{
        "symbol_id": "AAA", "fundamental_status": "STALE",
        "fundamental_status_x": "STALE", "fundamental_status_y": "STALE",
        "revenue_yoy": -10.0,
    }])
    snapshot = pd.DataFrame([{"symbol_id": "AAA", "fundamental_status": "AVAILABLE", "revenue_yoy": 12.0}])
    out = score_fundamentals(frame, snapshot)
    assert out.loc[0, "fundamental_status"] == "AVAILABLE"
    assert out.loc[0, "revenue_yoy"] == 12.0
    assert not any(column.endswith("_x") or column.endswith("_y") for column in out.columns)
