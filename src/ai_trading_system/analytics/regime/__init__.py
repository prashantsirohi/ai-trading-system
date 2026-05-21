"""Regime analytics built on market breadth."""

from ai_trading_system.analytics.regime.breadth import (
    MarketRegimeSnapshot,
    classify_regime,
    confirmed_regime,
    compute_market_regime_snapshot,
    regime_disagreement,
    resolve_previous_regime,
)
from ai_trading_system.analytics.regime.direction import build_market_direction
from ai_trading_system.analytics.regime.profiles import RegimeProfile, load_regime_profile
from ai_trading_system.analytics.regime.regime_phase import (
    RegimePhase,
    RegimePhaseResult,
    compute_regime_phase,
)

__all__ = [
    "MarketRegimeSnapshot",
    "RegimePhase",
    "RegimePhaseResult",
    "RegimeProfile",
    "build_market_direction",
    "classify_regime",
    "confirmed_regime",
    "compute_market_regime_snapshot",
    "compute_regime_phase",
    "load_regime_profile",
    "regime_disagreement",
    "resolve_previous_regime",
]
