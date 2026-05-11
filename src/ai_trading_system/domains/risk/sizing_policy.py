"""Position sizing."""

from __future__ import annotations

from ai_trading_system.domains.risk.config import RiskPolicyConfig
from ai_trading_system.domains.risk.contracts import (
    EntryDecision,
    MarketSnapshot,
    PortfolioSnapshot,
)


def size_position(
    market: MarketSnapshot,
    portfolio: PortfolioSnapshot,
    entry: EntryDecision,
    config: RiskPolicyConfig,
) -> int:
    """Return whole-share size for a confirmed entry; 0 if no viable size."""
    if not entry.should_enter or market.close <= 0:
        return 0

    equity = max(float(portfolio.equity), 0.0)
    if equity <= 0:
        return 0

    method = config.sizing.method
    max_position_value = equity * config.constraints.max_stock_weight_pct / 100.0

    if method == "equal_weight":
        slot_value = equity / max(config.constraints.max_concurrent_positions, 1)
        target_value = min(slot_value, max_position_value)
        shares = int(target_value // market.close)
        return max(shares, 0)

    if method == "atr_risk":
        if entry.initial_stop is None:
            return 0
        stop_distance = market.close - entry.initial_stop
        if stop_distance <= 0:
            return 0
        risk_budget = equity * config.sizing.risk_per_trade_pct / 100.0
        risk_shares = int(risk_budget // stop_distance)
        cap_shares = int(max_position_value // market.close)
        return max(min(risk_shares, cap_shares), 0)

    return 0
