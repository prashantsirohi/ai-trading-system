"""Shared trading-rule engine consumed by research backtesting and paper trading.

Pure-domain module: data in, decision out. No I/O, no DB, no order placement.
Backtesters and the paper-trade execution path both import from here so that
entry / exit / stop / sizing rules have a single source of truth.
"""

from ai_trading_system.domains.risk.contracts import (
    CandidateSignal,
    EntryDecision,
    ExitDecision,
    MarketSnapshot,
    PortfolioSnapshot,
    PositionSnapshot,
    RiskOrderIntent,
)
from ai_trading_system.domains.risk.config import RiskPolicyConfig, load_profile
from ai_trading_system.domains.risk.rule_engine import TradingRuleEngine

__all__ = [
    "CandidateSignal",
    "EntryDecision",
    "ExitDecision",
    "MarketSnapshot",
    "PortfolioSnapshot",
    "PositionSnapshot",
    "RiskOrderIntent",
    "RiskPolicyConfig",
    "TradingRuleEngine",
    "load_profile",
]
