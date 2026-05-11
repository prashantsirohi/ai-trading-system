"""Constraint tests."""

from dataclasses import replace

from ai_trading_system.domains.risk import PortfolioSnapshot
from ai_trading_system.domains.risk.portfolio_constraints import check_constraints


def test_allows_within_limits(candidate, market, empty_portfolio, base_config):
    # 100 shares * 100 close = 10_000 → 1% of 1M, well under 12% cap
    allowed, reasons = check_constraints(candidate, market, 100, empty_portfolio, base_config)
    assert allowed is True
    assert reasons == []


def test_blocks_when_stock_weight_exceeded(candidate, market, empty_portfolio, base_config):
    # 2000 shares * 100 = 200_000 → 20% of 1M, breaks 12% cap
    allowed, reasons = check_constraints(candidate, market, 2000, empty_portfolio, base_config)
    assert allowed is False
    assert "max_stock_weight_exceeded" in reasons


def test_blocks_when_sector_cap_exceeded(candidate, market, base_config):
    portfolio = PortfolioSnapshot(
        cash=1_000_000.0,
        equity=1_000_000.0,
        positions=(),
        sector_exposure={"TECH": 0.28},  # already 28%, adding 5% → 33% > 30%
    )
    allowed, reasons = check_constraints(candidate, market, 500, portfolio, base_config)
    assert allowed is False
    assert "max_sector_exposure_exceeded" in reasons


def test_blocks_when_max_positions_reached(candidate, market, base_config, make_position):
    portfolio = PortfolioSnapshot(
        cash=1_000_000.0,
        equity=1_000_000.0,
        positions=tuple(make_position(symbol_id=f"S{i}") for i in range(8)),
        sector_exposure={},
    )
    allowed, reasons = check_constraints(candidate, market, 50, portfolio, base_config)
    assert allowed is False
    assert "max_positions_reached" in reasons


def test_zero_equity_blocked(candidate, market, base_config):
    portfolio = PortfolioSnapshot(cash=0.0, equity=0.0)
    allowed, reasons = check_constraints(candidate, market, 100, portfolio, base_config)
    assert allowed is False
    assert "zero_equity" in reasons
