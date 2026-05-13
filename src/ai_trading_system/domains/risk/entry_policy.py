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

    if config.entry.require_price_above_sma50:
        if market.sma_50 is None or market.close <= market.sma_50:
            reasons.append("below_sma50")

    if config.entry.require_price_above_ema20:
        if market.ema_20 is None or market.close <= market.ema_20:
            reasons.append("below_ema20")

    if config.entry.require_sma50_above_sma200_or_rising_20d:
        sma50_above_sma200 = (
            market.sma_50 is not None
            and market.sma_200 is not None
            and market.sma_50 > market.sma_200
        )
        if not (sma50_above_sma200 or market.sma50_rising_20d is True):
            reasons.append("sma50_not_above_sma200_or_rising")

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

    min_close_to_high = config.entry.min_close_to_52w_high
    if min_close_to_high is not None:
        if market.high_52w is None or market.high_52w <= 0 or market.close < market.high_52w * min_close_to_high:
            reasons.append("not_near_52w_high")

    min_return_20 = config.entry.min_return_20_pct
    if min_return_20 is not None:
        if market.return_20_pct is None or market.return_20_pct <= min_return_20:
            reasons.append("return_20_too_low")

    min_return_50 = config.entry.min_return_50_pct
    if min_return_50 is not None:
        if market.return_50_pct is None or market.return_50_pct <= min_return_50:
            reasons.append("return_50_too_low")

    max_drawdown = config.entry.max_drawdown_from_recent_high_pct
    if max_drawdown is not None:
        if market.drawdown_from_recent_high_pct is None or market.drawdown_from_recent_high_pct >= max_drawdown:
            reasons.append("drawdown_too_deep")

    max_below_ema20_days = config.entry.max_below_ema20_days_20
    if max_below_ema20_days is not None:
        if market.below_ema20_days_20 is None or market.below_ema20_days_20 > max_below_ema20_days:
            reasons.append("too_many_closes_below_ema20")

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
