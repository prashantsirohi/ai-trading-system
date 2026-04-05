"""Strategy policies that convert ranked signals into trade actions."""

from __future__ import annotations

from typing import Dict, Iterable, Optional

import pandas as pd

from execution.models import TradeAction
from execution.portfolio import PositionSnapshot


SUPPORTED_STRATEGY_MODES = {
    "technical",
    "ml",
    "hybrid_confirm",
    "hybrid_overlay",
}


def build_trade_actions(
    *,
    ranked_df: pd.DataFrame,
    positions: Dict[str, PositionSnapshot],
    ml_overlay_df: Optional[pd.DataFrame] = None,
    strategy_mode: str = "technical",
    target_position_count: int = 5,
    ml_horizon: int = 5,
    ml_confirm_threshold: float = 0.55,
) -> list[TradeAction]:
    """Build buy/sell actions from the current ranked universe and open positions."""
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
