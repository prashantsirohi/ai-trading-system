"""Per-bar engine-driven backtest.

Consumes the same data shape paper trading sees (one ranked DataFrame per date),
runs ``TradingRuleEngine.generate_order_intents`` against an in-memory portfolio,
and records full trade rows with ``entry_reason``, ``exit_reason``, ``stop_price``,
``rank_at_entry``, ``rank_at_exit``, ``score_at_entry``, ``score_at_exit``, etc.

Intentionally does **not** import from ``rank_backtester.py`` — that legacy module
is retained for back-compat and the engine path is the new single source of truth
shared with paper trading.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Iterable, Mapping

import pandas as pd

from ai_trading_system.domains.risk import (
    PortfolioSnapshot,
    PositionSnapshot,
    RiskPolicyConfig,
    TradingRuleEngine,
)
from ai_trading_system.domains.risk.adapters import (
    candidate_from_row,
    market_from_row,
)


@dataclass
class BacktestTrade:
    """One round-trip trade with full provenance."""

    symbol_id: str
    exchange: str
    entry_date: date
    entry_price: float
    entry_reason: str
    stop_price: float | None
    stop_method: str | None
    rank_at_entry: int | None
    score_at_entry: float | None
    sector: str
    shares: int
    exit_date: date | None = None
    exit_price: float | None = None
    exit_reason: str | None = None
    rank_at_exit: int | None = None
    score_at_exit: float | None = None
    dma_exit_line: float | None = None
    bars_held: int = 0
    pnl: float | None = None
    pnl_pct: float | None = None


@dataclass
class BacktestResult:
    trades: list[BacktestTrade] = field(default_factory=list)
    equity_curve: list[dict] = field(default_factory=list)
    config_name: str = ""

    def to_trades_df(self) -> pd.DataFrame:
        if not self.trades:
            return pd.DataFrame()
        return pd.DataFrame([asdict(t) for t in self.trades])

    def to_equity_df(self) -> pd.DataFrame:
        if not self.equity_curve:
            return pd.DataFrame()
        return pd.DataFrame(self.equity_curve)


class EngineBacktestRunner:
    """Walk per-date ranked snapshots through the shared engine.

    Inputs
    ------
    ranked_by_date : dict[date, pd.DataFrame]
        One ranked DataFrame per trading day. Same schema paper trading consumes
        (columns: symbol_id, exchange, close, composite_score, eligible_rank,
        is_stage2_uptrend, sector_name, sector_strength_score, sma_20, sma_50,
        sma_200, sma_11, atr_14, volume_ratio_20, swing_low_20, ...).
    risk_config : RiskPolicyConfig
        Same config object paper trading uses.
    starting_equity : float
        Portfolio equity at t=0. Cash-tracking is approximate (no compounding of
        unrealized P&L into sizing decisions within the loop).
    """

    def __init__(
        self,
        *,
        risk_config: RiskPolicyConfig,
        starting_equity: float = 1_000_000.0,
        commission_bps: float = 10.0,
        slippage_bps: float = 20.0,
        risk_config_by_date: Mapping[date, RiskPolicyConfig] | None = None,
    ):
        self.risk_config = risk_config
        self.starting_equity = float(starting_equity)
        self.commission_bps = float(commission_bps)
        self.slippage_bps = float(slippage_bps)
        self.engine = TradingRuleEngine(risk_config)
        self.risk_config_by_date = dict(risk_config_by_date or {})

    def run(
        self,
        ranked_by_date: Mapping[date, pd.DataFrame],
    ) -> BacktestResult:
        result = BacktestResult(config_name=self.risk_config.name)
        if not ranked_by_date:
            return result

        positions: dict[str, PositionSnapshot] = {}
        # Maintain a "rank threshold streak" counter per held symbol.
        rank_streak: dict[str, int] = {}
        score_streak: dict[str, int] = {}
        # Track open BacktestTrade rows so we can close them on exit.
        open_trades: dict[str, BacktestTrade] = {}
        latest_close_by_symbol: dict[str, float] = {}

        sorted_dates = sorted(ranked_by_date.keys())
        equity = self.starting_equity

        for bar_date in sorted_dates:
            active_config = self.risk_config_by_date.get(bar_date, self.risk_config)
            if active_config is not self.engine.config:
                self.engine = TradingRuleEngine(active_config)
            ranked = ranked_by_date[bar_date]
            rows = ranked.to_dict(orient="records") if ranked is not None and not ranked.empty else []
            row_by_symbol = {str(r.get("symbol_id")): r for r in rows if r.get("symbol_id")}

            # ---- update streaks for every held symbol BEFORE engine call.
            for symbol_id, position in list(positions.items()):
                row = row_by_symbol.get(symbol_id)
                if row is None:
                    rank_streak[symbol_id] = rank_streak.get(symbol_id, 0) + 1
                    score_streak[symbol_id] = score_streak.get(symbol_id, 0) + 1
                else:
                    rank_val = int(row.get("eligible_rank") or row.get("rank") or 0)
                    score_val = float(
                        row.get("composite_score_adjusted") or row.get("composite_score") or 0.0
                    )
                    rank_streak[symbol_id] = (
                        rank_streak.get(symbol_id, 0) + 1
                        if rank_val > active_config.exit.max_hold_rank
                        else 0
                    )
                    score_streak[symbol_id] = (
                        score_streak.get(symbol_id, 0) + 1
                        if score_val < active_config.exit.min_hold_score
                        else 0
                    )

                # Replace position snapshot with refreshed bars_held + streak.
                refreshed = PositionSnapshot(
                    symbol_id=position.symbol_id,
                    exchange=position.exchange,
                    entry_date=position.entry_date,
                    entry_price=position.entry_price,
                    shares=position.shares,
                    sector=position.sector,
                    stop_price=position.stop_price,
                    stop_method=position.stop_method,
                    rank_at_entry=position.rank_at_entry,
                    score_at_entry=position.score_at_entry,
                    bars_held=position.bars_held + 1,
                    rank_above_threshold_streak=rank_streak[symbol_id],
                    score_below_threshold_streak=score_streak[symbol_id],
                )
                positions[symbol_id] = refreshed

            # ---- build candidates + markets.
            candidates = [candidate_from_row(r) for r in rows]
            market_by_symbol = {}
            for r in rows:
                sid = str(r.get("symbol_id") or "")
                if sid:
                    market_by_symbol[sid] = market_from_row(r, as_of=bar_date)
                    latest_close_by_symbol[sid] = market_by_symbol[sid].close

            # For held symbols that fell off ranked: synthesize a minimal market.
            for symbol_id in positions:
                if symbol_id in market_by_symbol:
                    continue
                # Without ranked data, we have to skip exit evaluation this bar.
                # The next bar with ranked data will pick it up.
                pass

            portfolio = _build_portfolio(positions, equity)
            intents = self.engine.generate_order_intents(candidates, market_by_symbol, portfolio)

            # ---- apply intents.
            for intent in intents:
                if intent.intent_kind == "exit":
                    position = positions.pop(intent.symbol_id, None)
                    if position is None:
                        continue
                    rank_streak.pop(intent.symbol_id, None)
                    score_streak.pop(intent.symbol_id, None)
                    market = market_by_symbol.get(intent.symbol_id)
                    raw_exit_price = _exit_fill_price(intent, position, market)
                    # Sell fill suffers slippage (executes below quote).
                    exit_price = raw_exit_price * (1.0 - self.slippage_bps / 10_000.0)
                    trade = open_trades.pop(intent.symbol_id, None)
                    if trade is not None:
                        trade.exit_date = bar_date
                        trade.exit_price = exit_price
                        trade.exit_reason = intent.reason
                        trade.bars_held = position.bars_held
                        trade.dma_exit_line = intent.metadata.get("stop_line")
                        latest = row_by_symbol.get(intent.symbol_id, {})
                        trade.rank_at_exit = (
                            int(latest.get("eligible_rank") or latest.get("rank") or 0)
                            if latest
                            else None
                        )
                        trade.score_at_exit = (
                            float(latest.get("composite_score") or 0.0) if latest else None
                        )
                        gross_pnl = (exit_price - trade.entry_price) * trade.shares
                        commission = (
                            (trade.entry_price + exit_price)
                            * trade.shares
                            * (self.commission_bps / 10_000.0)
                        )
                        trade.pnl = gross_pnl - commission
                        if trade.entry_price > 0:
                            trade.pnl_pct = trade.pnl / (trade.entry_price * trade.shares)
                        equity += trade.pnl
                        result.trades.append(trade)
                else:  # entry
                    market = market_by_symbol.get(intent.symbol_id)
                    if market is None:
                        continue
                    sector = str(intent.metadata.get("sector") or "")
                    # Buy fill suffers slippage (executes above quote).
                    fill_price = market.close * (1.0 + self.slippage_bps / 10_000.0)
                    new_pos = PositionSnapshot(
                        symbol_id=intent.symbol_id,
                        exchange=intent.exchange,
                        entry_date=bar_date,
                        entry_price=fill_price,
                        shares=intent.quantity,
                        sector=sector,
                        stop_price=intent.metadata.get("initial_stop"),
                        stop_method=intent.metadata.get("stop_method"),
                        rank_at_entry=intent.metadata.get("rank_at_entry"),
                        score_at_entry=intent.metadata.get("score_at_entry"),
                        bars_held=0,
                    )
                    positions[intent.symbol_id] = new_pos
                    rank_streak[intent.symbol_id] = 0
                    score_streak[intent.symbol_id] = 0
                    open_trades[intent.symbol_id] = BacktestTrade(
                        symbol_id=new_pos.symbol_id,
                        exchange=new_pos.exchange,
                        entry_date=bar_date,
                        entry_price=new_pos.entry_price,
                        entry_reason=intent.reason,
                        stop_price=new_pos.stop_price,
                        stop_method=new_pos.stop_method,
                        rank_at_entry=new_pos.rank_at_entry,
                        score_at_entry=new_pos.score_at_entry,
                        sector=new_pos.sector,
                        shares=new_pos.shares,
                    )

            result.equity_curve.append(
                {
                    "date": bar_date,
                    "equity": equity,
                    "open_positions": len(positions),
                }
            )

        # ---- close any still-open trades at the last bar (mark-to-market).
        for symbol_id, trade in open_trades.items():
            position = positions.get(symbol_id)
            if position is None:
                continue
            trade.exit_date = sorted_dates[-1]
            raw_exit = latest_close_by_symbol.get(symbol_id, position.entry_price)
            trade.exit_price = raw_exit * (1.0 - self.slippage_bps / 10_000.0)
            trade.exit_reason = "backtest_end"
            trade.bars_held = position.bars_held
            gross_pnl = (trade.exit_price - trade.entry_price) * trade.shares
            commission = (
                (trade.entry_price + trade.exit_price)
                * trade.shares
                * (self.commission_bps / 10_000.0)
            )
            trade.pnl = gross_pnl - commission
            trade.pnl_pct = (
                trade.pnl / (trade.entry_price * trade.shares)
                if trade.entry_price > 0
                else 0.0
            )
            equity += trade.pnl
            result.trades.append(trade)

        if result.equity_curve:
            result.equity_curve[-1]["equity"] = equity

        return result


def _exit_fill_price(intent, position: PositionSnapshot, market) -> float:
    """Fill price for an exit. Hard-stop fills at stop, or open on gap-down."""
    if market is None:
        return position.entry_price
    if intent.reason == "hard_stop" and position.stop_price is not None:
        # Gap-down: bar opens below stop → fill at open (worse than stop).
        if market.open is not None and market.open < position.stop_price:
            return float(market.open)
        return float(position.stop_price)
    return float(market.close)


def _build_portfolio(positions: dict[str, PositionSnapshot], equity: float) -> PortfolioSnapshot:
    pos_tuple = tuple(positions.values())
    sector_exposure: dict[str, float] = {}
    if equity > 0:
        for p in pos_tuple:
            if not p.sector:
                continue
            sector_exposure[p.sector] = (
                sector_exposure.get(p.sector, 0.0)
                + (p.entry_price * p.shares) / equity
            )
    invested = sum(p.entry_price * p.shares for p in pos_tuple)
    return PortfolioSnapshot(
        cash=max(equity - invested, 0.0),
        equity=equity,
        positions=pos_tuple,
        sector_exposure=sector_exposure,
    )
