from __future__ import annotations

from ai_trading_system.domains.execution.portfolio import POSITION_STATES, check_portfolio_constraints


def test_position_states_scaffold_exists() -> None:
    assert POSITION_STATES == ["candidate", "active", "partial", "exit"]


def test_check_portfolio_constraints_enforces_limits() -> None:
    candidate = {"symbol_id": "AAA", "sector_name": "IT"}
    state = {
        "open_positions_count": 10,
        "sector_exposure": {"IT": 0.35},
        "symbol_weights": {"AAA": 0.15},
    }

    result = check_portfolio_constraints(
        candidate,
        state,
        max_positions=10,
        max_sector_exposure=0.30,
        max_single_stock_weight=0.10,
    )

    assert result["allowed"] is False
    assert "max_positions_reached" in result["reasons"]
    assert "max_sector_exposure_exceeded" in result["reasons"]
    assert "max_single_stock_weight_exceeded" in result["reasons"]
