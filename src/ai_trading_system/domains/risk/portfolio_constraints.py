"""Portfolio-level constraint checks (post-sizing)."""

from __future__ import annotations

from ai_trading_system.domains.risk.config import RiskPolicyConfig
from ai_trading_system.domains.risk.contracts import (
    CandidateSignal,
    MarketSnapshot,
    PortfolioSnapshot,
)


def check_constraints(
    candidate: CandidateSignal,
    market: MarketSnapshot,
    shares: int,
    portfolio: PortfolioSnapshot,
    config: RiskPolicyConfig,
) -> tuple[bool, list[str]]:
    """Return ``(allowed, reasons)`` after sizing is known."""
    reasons: list[str] = []
    equity = max(float(portfolio.equity), 0.0)
    if equity <= 0:
        return False, ["zero_equity"]

    if portfolio.open_positions_count >= config.constraints.max_concurrent_positions:
        reasons.append("max_positions_reached")

    position_value = market.close * shares
    weight_pct = (position_value / equity) * 100.0 if equity > 0 else 0.0
    if weight_pct > config.constraints.max_stock_weight_pct:
        reasons.append("max_stock_weight_exceeded")

    existing_sector_pct = float(portfolio.sector_exposure.get(candidate.sector, 0.0)) * 100.0
    new_sector_pct = existing_sector_pct + weight_pct
    if new_sector_pct > config.constraints.max_sector_exposure_pct:
        reasons.append("max_sector_exposure_exceeded")

    return (not reasons), reasons
