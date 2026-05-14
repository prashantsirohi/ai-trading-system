"""Exit-condition evaluation, priority-ordered."""

from __future__ import annotations

from ai_trading_system.domains.risk.config import RiskPolicyConfig
from ai_trading_system.domains.risk.contracts import (
    CandidateSignal,
    ExitDecision,
    MarketSnapshot,
    PositionSnapshot,
)

PRIORITY_HOLD = 999


def _dma_value(market: MarketSnapshot, window: int | None) -> float | None:
    if window == 11:
        return market.sma_11
    if window == 20:
        return market.sma_20
    if window == 50:
        return market.sma_50
    if window == 200:
        return market.sma_200
    return None


def evaluate_exit(
    position: PositionSnapshot,
    market: MarketSnapshot,
    latest_signal: CandidateSignal | None,
    config: RiskPolicyConfig,
) -> ExitDecision:
    """Return the highest-priority exit (lowest priority number wins)."""
    candidates: list[ExitDecision] = []

    # 0 — hard stop. Fire on intrabar low when available, else close-only.
    if position.stop_price is not None:
        breach_price = market.low if market.low is not None else market.close
        if breach_price < position.stop_price:
            candidates.append(
                ExitDecision(
                    should_exit=True,
                    reason="hard_stop",
                    priority=0,
                    stop_line=position.stop_price,
                )
            )

    buffer = 1.0 - config.exit.dma_whipsaw_buffer_pct / 100.0

    # 1 — emergency 200-DMA exit
    if config.exit.emergency_exit_below_sma200 and market.sma_200 is not None:
        if market.close < market.sma_200 * buffer:
            candidates.append(
                ExitDecision(
                    should_exit=True,
                    reason="close_below_200dma",
                    priority=1,
                    stop_line=market.sma_200,
                )
            )

    # 2 — trailing DMA exit
    if config.exit.dma_exit_window is not None:
        dma_val = _dma_value(market, config.exit.dma_exit_window)
        if dma_val is not None and market.close < dma_val * buffer:
            candidates.append(
                ExitDecision(
                    should_exit=True,
                    reason=f"close_below_{config.exit.dma_exit_window}dma",
                    priority=2,
                    stop_line=dma_val,
                )
            )

    # 3 — rank deterioration streak
    if config.exit.exit_on_rank_deterioration:
        if position.rank_above_threshold_streak >= config.exit.rank_deterioration_bars:
            candidates.append(
                ExitDecision(
                    should_exit=True,
                    reason="rank_deterioration_streak",
                    priority=3,
                    current_rank=latest_signal.rank if latest_signal else None,
                )
            )

    # 4 — score deterioration streak
    if config.exit.exit_on_score_deterioration:
        if position.score_below_threshold_streak >= config.exit.score_deterioration_bars:
            candidates.append(
                ExitDecision(
                    should_exit=True,
                    reason="score_deterioration_streak",
                    priority=4,
                    current_score=latest_signal.composite_score if latest_signal else None,
                )
            )

    # 5 — time stop
    if config.exit.time_stop_days is not None:
        if position.bars_held >= config.exit.time_stop_days:
            candidates.append(
                ExitDecision(
                    should_exit=True,
                    reason="time_stop",
                    priority=5,
                )
            )

    if not candidates:
        return ExitDecision(should_exit=False, reason="hold", priority=PRIORITY_HOLD)

    candidates.sort(key=lambda d: (d.priority, d.reason))
    return candidates[0]
