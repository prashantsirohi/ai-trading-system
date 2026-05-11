"""Initial-stop calculation for new entries."""

from __future__ import annotations

from ai_trading_system.domains.risk.config import StopConfig
from ai_trading_system.domains.risk.contracts import MarketSnapshot


class StopMethodUnavailable(ValueError):
    """Raised when the configured stop method needs data the snapshot lacks."""


def calculate_initial_stop(
    market: MarketSnapshot,
    config: StopConfig,
) -> tuple[float, str]:
    """Return ``(stop_price, method_label)`` for an entry at ``market.close``."""
    method = config.method

    if method == "atr":
        if market.atr_14 is None or market.atr_14 <= 0:
            raise StopMethodUnavailable("atr stop requires atr_14 > 0")
        return market.close - market.atr_14 * config.atr_multiple, "atr"

    if method == "percent":
        return market.close * (1.0 - config.stop_pct), "percent"

    if method == "swing_low":
        if market.swing_low_20 is None:
            raise StopMethodUnavailable("swing_low stop requires swing_low_20")
        return float(market.swing_low_20), "swing_low"

    if method == "breakout_candle_low":
        if market.breakout_candle_low is None:
            raise StopMethodUnavailable("breakout_candle_low stop requires breakout_candle_low")
        return float(market.breakout_candle_low), "breakout_candle_low"

    if method == "hybrid":
        if market.swing_low_20 is None or market.atr_14 is None or market.atr_14 <= 0:
            raise StopMethodUnavailable("hybrid stop requires both swing_low_20 and atr_14")
        atr_stop = market.close - market.atr_14 * config.hybrid_atr_multiple
        return max(float(market.swing_low_20), atr_stop), "hybrid"

    raise ValueError(f"Unknown stop method: {method}")
