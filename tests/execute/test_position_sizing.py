from __future__ import annotations

from analytics.risk_manager import compute_atr_position_size as risk_compute_atr_position_size
from execution.policies import compute_atr_position_size as policy_compute_atr_position_size


def test_compute_atr_position_size_available_and_consistent() -> None:
    qty_policy = policy_compute_atr_position_size(
        capital=100_000.0,
        risk_per_trade=0.01,
        entry_price=100.0,
        atr=2.0,
        atr_multiple=2.0,
    )
    qty_risk = risk_compute_atr_position_size(
        capital=100_000.0,
        risk_per_trade=0.01,
        entry_price=100.0,
        atr=2.0,
        atr_multiple=2.0,
    )

    assert qty_policy == 250
    assert qty_risk == 250

