"""Orchestration: turn candidates + portfolio state into an ordered intent list."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from ai_trading_system.domains.risk.config import RiskPolicyConfig
from ai_trading_system.domains.risk.contracts import (
    CandidateSignal,
    EntryDecision,
    ExitDecision,
    MarketSnapshot,
    PortfolioSnapshot,
    PositionSnapshot,
    RiskOrderIntent,
)
from ai_trading_system.domains.risk.entry_policy import evaluate_entry
from ai_trading_system.domains.risk.exit_policy import evaluate_exit
from ai_trading_system.domains.risk.portfolio_constraints import check_constraints
from ai_trading_system.domains.risk.sizing_policy import size_position


class TradingRuleEngine:
    """Single source of truth for entry/exit/stop/sizing decisions.

    Both research backtesting and paper trading construct an engine bound to one
    ``RiskPolicyConfig`` and ask it ``generate_order_intents`` per bar.
    """

    def __init__(self, config: RiskPolicyConfig):
        self.config = config

    # ----- Single-decision API (useful for tests + downstream adapters) -----

    def evaluate_entry(
        self,
        candidate: CandidateSignal,
        market: MarketSnapshot,
        portfolio: PortfolioSnapshot,
    ) -> EntryDecision:
        return evaluate_entry(candidate, market, portfolio, self.config)

    def evaluate_exit(
        self,
        position: PositionSnapshot,
        market: MarketSnapshot,
        latest_signal: CandidateSignal | None,
    ) -> ExitDecision:
        return evaluate_exit(position, market, latest_signal, self.config)

    def size_position(
        self,
        market: MarketSnapshot,
        portfolio: PortfolioSnapshot,
        entry: EntryDecision,
    ) -> int:
        return size_position(market, portfolio, entry, self.config)

    # ----- Orchestrated API -----

    def generate_order_intents(
        self,
        candidates: Sequence[CandidateSignal],
        market_by_symbol: Mapping[str, MarketSnapshot],
        portfolio: PortfolioSnapshot,
    ) -> list[RiskOrderIntent]:
        """Produce intents with exits first, then entries (so freed slots are reusable)."""
        latest_signal_by_symbol = {c.symbol_id: c for c in candidates}

        intents: list[RiskOrderIntent] = []

        # ----- 1. Exits first.
        surviving_positions: list[PositionSnapshot] = []
        for position in portfolio.positions:
            market = market_by_symbol.get(position.symbol_id)
            if market is None:
                surviving_positions.append(position)
                continue
            decision = self.evaluate_exit(
                position, market, latest_signal_by_symbol.get(position.symbol_id)
            )
            if decision.should_exit:
                intents.append(
                    RiskOrderIntent(
                        symbol_id=position.symbol_id,
                        exchange=position.exchange,
                        side="SELL",
                        quantity=position.shares,
                        intent_kind="exit",
                        reason=decision.reason,
                        metadata={
                            "priority": decision.priority,
                            "stop_line": decision.stop_line,
                            "current_rank": decision.current_rank,
                            "current_score": decision.current_score,
                            "entry_price": position.entry_price,
                            "entry_date": position.entry_date.isoformat(),
                            "rank_at_entry": position.rank_at_entry,
                            "score_at_entry": position.score_at_entry,
                        },
                    )
                )
            else:
                surviving_positions.append(position)

        # ----- 2. Recompute portfolio snapshot after hypothetical exits.
        post_exit_portfolio = PortfolioSnapshot(
            cash=portfolio.cash,
            equity=portfolio.equity,
            positions=tuple(surviving_positions),
            sector_exposure=_recompute_sector_exposure(
                surviving_positions, portfolio.sector_exposure, portfolio.positions
            ),
        )

        # ----- 3. Entries on the post-exit snapshot, highest rank first.
        sorted_candidates = sorted(candidates, key=lambda c: c.rank)
        running_portfolio = post_exit_portfolio
        for candidate in sorted_candidates:
            market = market_by_symbol.get(candidate.symbol_id)
            if market is None:
                continue
            decision = self.evaluate_entry(candidate, market, running_portfolio)
            if not decision.should_enter:
                continue
            shares = self.size_position(market, running_portfolio, decision)
            if shares <= 0:
                continue
            allowed, reasons = check_constraints(
                candidate, market, shares, running_portfolio, self.config
            )
            if not allowed:
                continue
            intents.append(
                RiskOrderIntent(
                    symbol_id=candidate.symbol_id,
                    exchange=candidate.exchange,
                    side="BUY",
                    quantity=shares,
                    intent_kind="entry",
                    reason="entry_confirmed",
                    metadata={
                        "initial_stop": decision.initial_stop,
                        "stop_method": decision.stop_method,
                        "rank_at_entry": candidate.rank,
                        "score_at_entry": candidate.composite_score,
                        "sector": candidate.sector,
                        "watchlist_bucket": candidate.watchlist_bucket,
                    },
                )
            )
            running_portfolio = _add_hypothetical_position(
                running_portfolio, candidate, market, shares, decision
            )

        return intents


def _recompute_sector_exposure(
    surviving: list[PositionSnapshot],
    original: dict[str, float],
    original_positions: tuple[PositionSnapshot, ...],
) -> dict[str, float]:
    """Re-weight sector exposure proportionally after removing exited symbols.

    Approximation good enough for in-bar entry gating; the next tick will load
    a fresh portfolio snapshot from the real ledger.
    """
    if not original_positions:
        return dict(original)
    survivors = {p.symbol_id for p in surviving}
    removed_count = sum(1 for p in original_positions if p.symbol_id not in survivors)
    if removed_count == 0:
        return dict(original)
    scale = len(surviving) / max(len(original_positions), 1)
    return {sector: exposure * scale for sector, exposure in original.items()}


def _add_hypothetical_position(
    portfolio: PortfolioSnapshot,
    candidate: CandidateSignal,
    market: MarketSnapshot,
    shares: int,
    entry: EntryDecision,
) -> PortfolioSnapshot:
    hypothetical = PositionSnapshot(
        symbol_id=candidate.symbol_id,
        exchange=candidate.exchange,
        entry_date=market.date,
        entry_price=market.close,
        shares=shares,
        sector=candidate.sector,
        stop_price=entry.initial_stop,
        stop_method=entry.stop_method,
        rank_at_entry=candidate.rank,
        score_at_entry=candidate.composite_score,
    )
    new_positions = portfolio.positions + (hypothetical,)
    equity = max(portfolio.equity, 1.0)
    weight = (market.close * shares) / equity
    new_sector_exposure = dict(portfolio.sector_exposure)
    new_sector_exposure[candidate.sector] = new_sector_exposure.get(candidate.sector, 0.0) + weight
    return PortfolioSnapshot(
        cash=portfolio.cash - market.close * shares,
        equity=portfolio.equity,
        positions=new_positions,
        sector_exposure=new_sector_exposure,
    )
