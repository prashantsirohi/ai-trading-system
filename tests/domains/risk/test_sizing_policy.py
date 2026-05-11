"""Sizing tests."""

from dataclasses import replace

from ai_trading_system.domains.risk import EntryDecision
from ai_trading_system.domains.risk.sizing_policy import size_position


def test_equal_weight_sizing(market, empty_portfolio, base_config):
    entry = EntryDecision(should_enter=True, reasons=("entry_confirmed",), initial_stop=95.0)
    shares = size_position(market, empty_portfolio, entry, base_config)
    # equity=1_000_000, slots=8 → slot=125_000; capped at max_stock_weight=12% = 120_000.
    # shares = 120_000 // 100 = 1200.
    assert shares == 1200


def test_atr_risk_sizing(market, empty_portfolio, base_config):
    cfg = replace(base_config, sizing=replace(base_config.sizing, method="atr_risk"))
    entry = EntryDecision(should_enter=True, reasons=("entry_confirmed",), initial_stop=95.0)
    shares = size_position(market, empty_portfolio, entry, cfg)
    # risk budget = 1% of 1_000_000 = 10_000; stop distance = 5.0 → 2000 shares
    # cap = 12% of 1_000_000 / 100 = 1200 → cap binds.
    assert shares == 1200


def test_returns_zero_when_entry_rejected(market, empty_portfolio, base_config):
    entry = EntryDecision(should_enter=False, reasons=("not_stage_2",))
    assert size_position(market, empty_portfolio, entry, base_config) == 0


def test_returns_zero_when_no_equity(market, base_config):
    from ai_trading_system.domains.risk import PortfolioSnapshot

    portfolio = PortfolioSnapshot(cash=0.0, equity=0.0)
    entry = EntryDecision(should_enter=True, reasons=("entry_confirmed",), initial_stop=95.0)
    assert size_position(market, portfolio, entry, base_config) == 0
