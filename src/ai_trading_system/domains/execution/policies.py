"""Strategy policies that convert ranked signals into trade actions."""

from __future__ import annotations

from typing import Dict, Iterable, Optional

import pandas as pd

from ai_trading_system.domains.execution.models import TradeAction
from ai_trading_system.domains.execution.portfolio import PositionSnapshot
from ai_trading_system.domains.risk import (
    RiskOrderIntent,
    RiskPolicyConfig,
    TradingRuleEngine,
)
from ai_trading_system.domains.risk.adapters import (
    candidate_from_row,
    market_from_row,
    portfolio_snapshot,
    position_from_execution_snapshot,
)


SUPPORTED_STRATEGY_MODES = {
    "technical",
    "ml",
    "hybrid_confirm",
    "hybrid_overlay",
}


def compute_atr_position_size(
    capital: float,
    risk_per_trade: float,
    entry_price: float,
    atr: float,
    atr_multiple: float = 2.0,
) -> int:
    """Compute ATR risk-based position size."""
    if capital <= 0 or entry_price <= 0 or atr <= 0:
        return 0
    risk_amount = float(capital) * float(risk_per_trade)
    stop_distance = float(atr) * float(atr_multiple)
    if stop_distance <= 0:
        return 0
    qty = int(risk_amount / stop_distance)
    return max(qty, 0)


def build_trade_actions(
    *,
    ranked_df: pd.DataFrame,
    positions: Dict[str, PositionSnapshot],
    ml_overlay_df: Optional[pd.DataFrame] = None,
    strategy_mode: str = "technical",
    target_position_count: int = 5,
    ml_horizon: int = 5,
    ml_confirm_threshold: float = 0.55,
    risk_config: RiskPolicyConfig | None = None,
    equity: float | None = None,
    stop_records: Dict[str, dict] | None = None,
    market_extras: Dict[str, dict] | None = None,
) -> list[TradeAction]:
    """Build buy/sell actions from the current ranked universe and open positions.

    When ``risk_config`` is supplied, the shared ``TradingRuleEngine`` produces
    the actions (exits first, then entries) and the legacy in-target/out-of-target
    diff is skipped. When omitted, behavior is unchanged from before.
    """
    if risk_config is not None:
        return _build_trade_actions_with_engine(
            ranked_df=ranked_df,
            positions=positions,
            risk_config=risk_config,
            equity=equity,
            stop_records=stop_records or {},
            market_extras=market_extras or {},
            fallback_mode=(strategy_mode or "technical").lower(),
        )

    mode = (strategy_mode or "technical").lower()
    if mode not in SUPPORTED_STRATEGY_MODES:
        raise ValueError(f"Unsupported strategy_mode: {strategy_mode}")

    ranked = ranked_df.copy() if ranked_df is not None else pd.DataFrame()
    overlay = ml_overlay_df.copy() if ml_overlay_df is not None else pd.DataFrame()
    target_symbols = _target_symbols(
        ranked=ranked,
        overlay=overlay,
        strategy_mode=mode,
        target_position_count=int(target_position_count),
        ml_horizon=int(ml_horizon),
        ml_confirm_threshold=float(ml_confirm_threshold),
    )

    ranked_lookup = _records_by_symbol(ranked)
    overlay_lookup = _records_by_symbol(overlay)

    actions: list[TradeAction] = []
    held_symbols = set(positions.keys())

    for symbol_id, position in positions.items():
        if symbol_id not in target_symbols:
            actions.append(
                TradeAction(
                    action="SELL",
                    symbol_id=symbol_id,
                    exchange=position.exchange,
                    side="SELL",
                    quantity=position.quantity,
                    requested_price=_lookup_price(symbol_id, ranked_lookup, overlay_lookup, fallback=position.last_fill_price),
                    strategy_mode=mode,
                    reason="rebalance_out",
                    metadata={"avg_entry_price": position.avg_entry_price},
                )
            )
            continue

        if mode in {"ml", "hybrid_confirm", "hybrid_overlay"} and not _passes_ml_exit_guard(
            overlay_lookup.get(symbol_id, {}),
            ml_horizon=ml_horizon,
            threshold=ml_confirm_threshold,
        ):
            actions.append(
                TradeAction(
                    action="SELL",
                    symbol_id=symbol_id,
                    exchange=position.exchange,
                    side="SELL",
                    quantity=position.quantity,
                    requested_price=_lookup_price(symbol_id, ranked_lookup, overlay_lookup, fallback=position.last_fill_price),
                    strategy_mode=mode,
                    reason="ml_exit_guard",
                    metadata={"avg_entry_price": position.avg_entry_price},
                )
            )

    for symbol_id in target_symbols:
        if symbol_id in held_symbols:
            continue
        row = ranked_lookup.get(symbol_id, {})
        overlay_row = overlay_lookup.get(symbol_id, {})
        actions.append(
            TradeAction(
                action="BUY",
                symbol_id=symbol_id,
                exchange=str(row.get("exchange") or overlay_row.get("exchange") or "NSE"),
                side="BUY",
                quantity=None,
                requested_price=_lookup_price(symbol_id, ranked_lookup, overlay_lookup, fallback=None),
                strategy_mode=mode,
                reason="target_entry",
                metadata={
                    "composite_score": row.get("composite_score"),
                    "ml_probability": overlay_row.get(f"ml_{int(ml_horizon)}d_prob"),
                    "technical_rank": row.get("rank_position") or row.get("technical_rank"),
                },
            )
        )
    return actions


def _target_symbols(
    *,
    ranked: pd.DataFrame,
    overlay: pd.DataFrame,
    strategy_mode: str,
    target_position_count: int,
    ml_horizon: int,
    ml_confirm_threshold: float,
) -> list[str]:
    if target_position_count <= 0:
        return []

    ranked = ranked.copy() if ranked is not None else pd.DataFrame()
    overlay = overlay.copy() if overlay is not None else pd.DataFrame()

    if strategy_mode == "technical":
        if "composite_score" in ranked.columns:
            ranked = ranked.sort_values("composite_score", ascending=False)
        return _symbol_list(ranked.head(target_position_count))

    if overlay.empty:
        return []

    prob_col = f"ml_{int(ml_horizon)}d_prob"
    blend_rank_col = f"blend_{int(ml_horizon)}d_rank"
    blend_score_col = f"blend_{int(ml_horizon)}d_score"
    overlay_ordered = overlay.copy()

    if strategy_mode == "ml":
        sort_col = blend_rank_col if blend_rank_col in overlay_ordered.columns else prob_col
        ascending = sort_col.endswith("_rank")
        overlay_ordered = overlay_ordered.sort_values(sort_col, ascending=ascending)
        return _symbol_list(overlay_ordered.head(target_position_count))

    merged = ranked.merge(
        overlay[[column for column in overlay.columns if column in {"symbol_id", prob_col, blend_rank_col, blend_score_col}]],
        on="symbol_id",
        how="left",
    )
    if prob_col in merged.columns:
        merged = merged[merged[prob_col].fillna(0.0) >= float(ml_confirm_threshold)]
    if strategy_mode == "hybrid_confirm":
        if "composite_score" in merged.columns:
            merged = merged.sort_values("composite_score", ascending=False)
        return _symbol_list(merged.head(target_position_count))

    sort_col = blend_rank_col if blend_rank_col in merged.columns else "composite_score"
    ascending = sort_col.endswith("_rank")
    merged = merged.sort_values(sort_col, ascending=ascending)
    return _symbol_list(merged.head(target_position_count))


def _passes_ml_exit_guard(row: dict, *, ml_horizon: int, threshold: float) -> bool:
    if not row:
        return False
    value = row.get(f"ml_{int(ml_horizon)}d_prob")
    try:
        return float(value) >= float(threshold)
    except (TypeError, ValueError):
        return False


def _records_by_symbol(df: pd.DataFrame) -> dict[str, dict]:
    if df is None or df.empty or "symbol_id" not in df.columns:
        return {}
    records = df.to_dict(orient="records")
    return {str(row["symbol_id"]): row for row in records}


def _lookup_price(
    symbol_id: str,
    ranked_lookup: dict[str, dict],
    overlay_lookup: dict[str, dict],
    *,
    fallback: float | None,
) -> float | None:
    for row in (ranked_lookup.get(symbol_id, {}), overlay_lookup.get(symbol_id, {})):
        for key in ("close", "last_price", "requested_price"):
            value = row.get(key)
            if value not in (None, ""):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
    return fallback


def _symbol_list(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty or "symbol_id" not in df.columns:
        return []
    return [str(value) for value in df["symbol_id"].tolist() if value not in (None, "")]


def _build_trade_actions_with_engine(
    *,
    ranked_df: pd.DataFrame,
    positions: Dict[str, PositionSnapshot],
    risk_config: RiskPolicyConfig,
    equity: float | None,
    stop_records: Dict[str, dict],
    market_extras: Dict[str, dict],
    fallback_mode: str,
) -> list[TradeAction]:
    """Engine-driven action generation. Exits precede entries in the returned list."""
    ranked = ranked_df if ranked_df is not None else pd.DataFrame()
    rows = ranked.to_dict(orient="records") if not ranked.empty else []
    row_by_symbol: Dict[str, dict] = {str(r.get("symbol_id")): r for r in rows if r.get("symbol_id")}

    candidates = [candidate_from_row(r) for r in rows]
    sector_lookup = {c.symbol_id: c.sector for c in candidates}

    # Build market snapshots for both candidates AND held symbols (held symbols
    # need a snapshot so their exits can be evaluated even when they fell off
    # the ranked list).
    market_by_symbol = {}
    for r in rows:
        sid = str(r.get("symbol_id") or "")
        if not sid:
            continue
        market_by_symbol[sid] = market_from_row(r, extra=market_extras.get(sid))

    risk_positions = []
    for symbol_id, exec_pos in positions.items():
        sector = sector_lookup.get(symbol_id, "")
        stop_rec = stop_records.get(symbol_id)
        risk_pos = position_from_execution_snapshot(
            exec_pos,
            sector=sector,
            stop_record=stop_rec,
        )
        risk_positions.append(risk_pos)
        if symbol_id not in market_by_symbol:
            held_row = row_by_symbol.get(symbol_id, {"symbol_id": symbol_id, "exchange": exec_pos.exchange})
            market_by_symbol[symbol_id] = market_from_row(
                held_row, extra=market_extras.get(symbol_id)
            )

    equity_value = float(equity) if equity is not None else 0.0
    if equity_value <= 0:
        # Reasonable default so the engine can still emit relative-weight intents.
        equity_value = sum(
            float(p.entry_price) * int(p.shares) for p in risk_positions if p.shares > 0
        ) or 1_000_000.0

    portfolio = portfolio_snapshot(positions=risk_positions, equity=equity_value)

    engine = TradingRuleEngine(risk_config)
    intents = engine.generate_order_intents(candidates, market_by_symbol, portfolio)

    return [_intent_to_action(intent, fallback_mode, positions) for intent in intents]


def _intent_to_action(
    intent: RiskOrderIntent,
    strategy_mode: str,
    positions: Dict[str, PositionSnapshot],
) -> TradeAction:
    """Translate engine intent → execution TradeAction."""
    requested_price: float | None = None
    if intent.intent_kind == "exit":
        snapshot = positions.get(intent.symbol_id)
        if snapshot is not None:
            requested_price = snapshot.last_fill_price or snapshot.avg_entry_price
    return TradeAction(
        action=intent.side,
        symbol_id=intent.symbol_id,
        exchange=intent.exchange,
        side=intent.side,
        quantity=int(intent.quantity) if intent.quantity else None,
        requested_price=requested_price,
        strategy_mode=strategy_mode,
        reason=intent.reason,
        metadata={
            "intent_kind": intent.intent_kind,
            **intent.metadata,
        },
    )
