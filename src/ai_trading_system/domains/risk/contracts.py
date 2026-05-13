"""Frozen dataclasses exchanged between callers and the trading-rule engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal


@dataclass(frozen=True)
class CandidateSignal:
    """One ranking row prepared for risk evaluation."""

    symbol_id: str
    exchange: str
    rank: int
    composite_score: float
    is_stage_2: bool
    sector: str
    sector_strength: float
    watchlist_bucket: str | None = None


@dataclass(frozen=True)
class MarketSnapshot:
    """Per-symbol market state on the evaluation bar."""

    symbol_id: str
    exchange: str
    date: date
    close: float
    atr_14: float
    sma_11: float | None = None
    sma_20: float | None = None
    sma_50: float | None = None
    sma_200: float | None = None
    ema_20: float | None = None
    high_52w: float | None = None
    return_20_pct: float | None = None
    return_50_pct: float | None = None
    sma50_rising_20d: bool | None = None
    drawdown_from_recent_high_pct: float | None = None
    below_ema20_days_20: int | None = None
    volume_ratio_20: float | None = None
    delivery_pct: float | None = None
    sector_delivery_median: float | None = None
    swing_low_20: float | None = None
    breakout_candle_low: float | None = None


@dataclass(frozen=True)
class PositionSnapshot:
    """Open position state needed for exit evaluation.

    Distinct from ``ai_trading_system.domains.execution.portfolio.PositionSnapshot``
    which is derived from the fills ledger; this carries the extra fields the
    engine needs (stop, streaks, rank/score history) without coupling the risk
    domain to the execution store.
    """

    symbol_id: str
    exchange: str
    entry_date: date
    entry_price: float
    shares: int
    sector: str
    stop_price: float | None = None
    stop_method: str | None = None
    rank_at_entry: int | None = None
    score_at_entry: float | None = None
    bars_held: int = 0
    rank_above_threshold_streak: int = 0
    score_below_threshold_streak: int = 0


@dataclass(frozen=True)
class PortfolioSnapshot:
    """Aggregate portfolio state at the evaluation bar."""

    cash: float
    equity: float
    positions: tuple[PositionSnapshot, ...] = ()
    sector_exposure: dict[str, float] = field(default_factory=dict)

    def holds(self, symbol_id: str) -> bool:
        return any(p.symbol_id == symbol_id for p in self.positions)

    @property
    def open_positions_count(self) -> int:
        return len(self.positions)


@dataclass(frozen=True)
class EntryDecision:
    """Outcome of evaluating one candidate at one bar."""

    should_enter: bool
    reasons: tuple[str, ...]
    initial_stop: float | None = None
    stop_method: str | None = None


@dataclass(frozen=True)
class ExitDecision:
    """Outcome of evaluating one open position at one bar."""

    should_exit: bool
    reason: str
    priority: int
    stop_line: float | None = None
    current_rank: int | None = None
    current_score: float | None = None


@dataclass(frozen=True)
class RiskOrderIntent:
    """Engine-emitted order intent. Adapter converts to execution.OrderIntent."""

    symbol_id: str
    exchange: str
    side: Literal["BUY", "SELL"]
    quantity: int
    intent_kind: Literal["entry", "exit"]
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)
