"""Shared fixtures for risk-engine tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from ai_trading_system.domains.risk import (
    CandidateSignal,
    MarketSnapshot,
    PortfolioSnapshot,
    PositionSnapshot,
    RiskPolicyConfig,
)


@pytest.fixture
def base_config() -> RiskPolicyConfig:
    return RiskPolicyConfig(name="test")


@pytest.fixture
def candidate() -> CandidateSignal:
    return CandidateSignal(
        symbol_id="ACME",
        exchange="NSE",
        rank=5,
        composite_score=82.5,
        is_stage_2=True,
        sector="TECH",
        sector_strength=0.8,
        watchlist_bucket="TRIGGERED_TODAY",
    )


@pytest.fixture
def market() -> MarketSnapshot:
    return MarketSnapshot(
        symbol_id="ACME",
        exchange="NSE",
        date=date(2026, 5, 11),
        close=100.0,
        atr_14=2.5,
        sma_11=98.0,
        sma_20=97.0,
        sma_50=92.0,
        sma_200=85.0,
        volume_ratio_20=2.1,
        delivery_pct=55.0,
        sector_delivery_median=50.0,
        swing_low_20=94.0,
        breakout_candle_low=96.5,
    )


@pytest.fixture
def empty_portfolio() -> PortfolioSnapshot:
    return PortfolioSnapshot(cash=1_000_000.0, equity=1_000_000.0, positions=(), sector_exposure={})


@pytest.fixture
def position() -> PositionSnapshot:
    return PositionSnapshot(
        symbol_id="ACME",
        exchange="NSE",
        entry_date=date(2026, 4, 1),
        entry_price=95.0,
        shares=100,
        sector="TECH",
        stop_price=88.0,
        stop_method="atr",
        rank_at_entry=3,
        score_at_entry=85.0,
        bars_held=10,
        rank_above_threshold_streak=0,
        score_below_threshold_streak=0,
    )


@pytest.fixture
def make_market(market):
    """Return a factory that produces a market snapshot with overrides."""

    def _factory(**overrides) -> MarketSnapshot:
        return replace(market, **overrides)

    return _factory


@pytest.fixture
def make_position(position):
    def _factory(**overrides) -> PositionSnapshot:
        return replace(position, **overrides)

    return _factory


@pytest.fixture
def make_candidate(candidate):
    def _factory(**overrides) -> CandidateSignal:
        return replace(candidate, **overrides)

    return _factory
