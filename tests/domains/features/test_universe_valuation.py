from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.features.universe_valuation import compute_universe_valuation_daily, valuation_zone
from ai_trading_system.domains.features.valuation_cycle_features import compute_valuation_cycle_features


def test_universe_valuation_uses_profitable_pe_and_loss_mcap_pct() -> None:
    rows = []
    dates = pd.date_range("2025-01-01", periods=65, freq="D")
    for idx, day in enumerate(dates):
        rows.extend(
            [
                {
                    "universe_id": "UNIV_TOP2_MCAP",
                    "date": day.date(),
                    "index_level_equal_weight": 1000 + idx,
                    "index_level_mcap_weight": 1000 + idx * 2,
                    "symbol": "PROFIT",
                    "market_cap_cr": 1000.0 + idx,
                    "ttm_net_profit_cr": 100.0,
                },
                {
                    "universe_id": "UNIV_TOP2_MCAP",
                    "date": day.date(),
                    "index_level_equal_weight": 1000 + idx,
                    "index_level_mcap_weight": 1000 + idx * 2,
                    "symbol": "LOSS",
                    "market_cap_cr": 500.0,
                    "ttm_net_profit_cr": -20.0,
                },
            ]
        )

    result = compute_universe_valuation_daily(pd.DataFrame(rows))
    latest = result.sort_values("date").iloc[-1]
    cycle = compute_valuation_cycle_features(result)

    assert round(float(latest["pe_ttm"]), 4) == round((1000 + 64) / 100, 4)
    assert round(float(latest["loss_mcap_pct"]), 4) == round(500 / (1000 + 64 + 500), 4)
    assert latest["pe_3y_median"] is not None
    assert latest["pe_percentile_3y"] is not None
    assert not cycle.empty
    assert "pe_distance_from_200dma" in cycle.columns


def test_valuation_zone_boundaries() -> None:
    assert valuation_zone(5) == "deep_value_panic"
    assert valuation_zone(20) == "cheap"
    assert valuation_zone(50) == "fair"
    assert valuation_zone(70) == "expensive"
    assert valuation_zone(85) == "late_bull"
    assert valuation_zone(95) == "bubble_top_risk"
