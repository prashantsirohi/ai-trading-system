"""Strategy router: translates market_stage → StrategyConfig.

The router provides a single immutable configuration object for each
market regime, ensuring a consistent mapping between breadth-derived
regime labels and downstream execution parameters.

Usage
-----
    from ai_trading_system.domains.ranking.strategy_router import route

    cfg = route("S4")
    # cfg.rank_mode            == "watchlist"
    # cfg.weekly_stage_gate    == False
    # cfg.breakout_active      == False
    # cfg.position_multiplier  == 0.5
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyConfig:
    """Immutable bundle of strategy parameters for one market regime."""

    market_stage: str
    """The regime label this config was derived from (S1, S2, S3, S4, MIXED)."""

    rank_mode: str
    """Passed to ``ranker.rank_all(rank_mode=...)``.

    One of: ``stage2_breakout``, ``defensive``, ``watchlist``, ``default``.
    """

    weekly_stage_gate: bool
    """Whether to gate rank candidates to weekly-S2 symbols only."""

    breakout_active: bool
    """Whether to run the breakout scanner at all."""

    position_regime: str
    """Label forwarded as ``execution_regime`` to the execution layer.

    One of: ``TREND``, ``BEARISH_MIXED``, ``STRONG_BEAR_TREND``.
    """

    position_multiplier: float
    """Scalar forwarded as ``execution_regime_multiplier`` (0.5–1.0)."""

    breakout_bias_allowlist: str
    """Comma-separated market-bias values that qualify a breakout setup.

    Empty string when ``breakout_active=False``.
    """

    breakout_min_breadth: float
    """Minimum breadth score required to qualify a breakout setup.

    0.0 when ``breakout_active=False``.
    """


# ── Routing table ─────────────────────────────────────────────────────────────

_CONFIGS: dict[str, StrategyConfig] = {
    "S2": StrategyConfig(
        market_stage="S2",
        rank_mode="stage2_breakout",
        weekly_stage_gate=True,
        breakout_active=True,
        position_regime="TREND",
        position_multiplier=1.0,
        breakout_bias_allowlist="BULLISH,NEUTRAL",
        breakout_min_breadth=45.0,
    ),
    "S3": StrategyConfig(
        market_stage="S3",
        rank_mode="defensive",
        weekly_stage_gate=False,
        breakout_active=True,
        position_regime="BEARISH_MIXED",
        position_multiplier=0.7,
        breakout_bias_allowlist="NEUTRAL",
        breakout_min_breadth=55.0,
    ),
    "S4": StrategyConfig(
        market_stage="S4",
        rank_mode="watchlist",
        weekly_stage_gate=False,
        breakout_active=False,
        position_regime="STRONG_BEAR_TREND",
        position_multiplier=0.5,
        breakout_bias_allowlist="",
        breakout_min_breadth=0.0,
    ),
    # S1 and MIXED both map to the default (neutral) configuration.
    "S1": StrategyConfig(
        market_stage="S1",
        rank_mode="default",
        weekly_stage_gate=False,
        breakout_active=True,
        position_regime="TREND",
        position_multiplier=1.0,
        breakout_bias_allowlist="BULLISH,NEUTRAL",
        breakout_min_breadth=45.0,
    ),
    "MIXED": StrategyConfig(
        market_stage="MIXED",
        rank_mode="default",
        weekly_stage_gate=False,
        breakout_active=True,
        position_regime="TREND",
        position_multiplier=1.0,
        breakout_bias_allowlist="BULLISH,NEUTRAL",
        breakout_min_breadth=45.0,
    ),
}


def route(market_stage: str) -> StrategyConfig:
    """Return the ``StrategyConfig`` for the given market stage.

    Unknown labels fall back to the ``MIXED`` (neutral default) config so
    the system degrades gracefully rather than crashing.
    """
    return _CONFIGS.get(market_stage, _CONFIGS["MIXED"])
