"""Entry-gate evaluation. Returns a reason-list-style decision."""

from __future__ import annotations

from ai_trading_system.domains.risk.config import RiskPolicyConfig
from ai_trading_system.domains.risk.contracts import (
    CandidateSignal,
    EntryDecision,
    MarketSnapshot,
    PortfolioSnapshot,
)
from ai_trading_system.domains.risk.stop_policy import (
    StopMethodUnavailable,
    calculate_initial_stop,
)


def evaluate_entry(
    candidate: CandidateSignal,
    market: MarketSnapshot,
    portfolio: PortfolioSnapshot,
    config: RiskPolicyConfig,
) -> EntryDecision:
    reasons: list[str] = []

    if config.entry.require_stage_2 and not candidate.is_stage_2:
        reasons.append("not_stage_2")

    if config.entry.require_price_above_sma200:
        if market.sma_200 is None or market.close < market.sma_200:
            reasons.append("below_sma200")

    if config.entry.require_sector_positive and candidate.sector_strength <= 0:
        reasons.append("weak_sector")

    min_vol = config.entry.min_volume_ratio
    if min_vol is not None and min_vol > 0:
        if market.volume_ratio_20 is None or market.volume_ratio_20 < min_vol:
            reasons.append("volume_not_confirmed")

    if config.entry.require_delivery_above_sector_median:
        if (
            market.delivery_pct is None
            or market.sector_delivery_median is None
            or market.delivery_pct < market.sector_delivery_median
        ):
            reasons.append("delivery_below_sector_median")

    if portfolio.open_positions_count >= config.constraints.max_concurrent_positions:
        reasons.append("portfolio_full")

    if portfolio.holds(candidate.symbol_id):
        reasons.append("already_held")

    if reasons:
        return EntryDecision(should_enter=False, reasons=tuple(reasons))

    try:
        stop_price, method_label = calculate_initial_stop(market, config.stop)
    except StopMethodUnavailable as exc:
        return EntryDecision(
            should_enter=False,
            reasons=(f"stop_unavailable:{exc}",),
        )

    return EntryDecision(
        should_enter=True,
        reasons=("entry_confirmed",),
        initial_stop=stop_price,
        stop_method=method_label,
    )
