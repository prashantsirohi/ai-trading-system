"""Execution service that wires adapters, storage, and risk sizing together."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, Optional

from execution.adapters.base import ExecutionAdapter
from execution.models import OrderIntent
from execution.policies import compute_atr_position_size
from execution.store import ExecutionStore


class ExecutionService:
    """High-level execution orchestration for signals and manual orders."""

    def __init__(
        self,
        store: ExecutionStore,
        adapter: ExecutionAdapter,
        *,
        default_order_type: str = "MARKET",
        default_product_type: str = "INTRADAY",
        default_validity: str = "DAY",
        risk_manager: Any = None,
    ):
        self.store = store
        self.adapter = adapter
        self.default_order_type = default_order_type
        self.default_product_type = default_product_type
        self.default_validity = default_validity
        self.risk_manager = risk_manager

    def submit_order(
        self,
        intent: OrderIntent,
        *,
        market_price: float | None = None,
    ) -> Dict[str, Any]:
        order, fills = self.adapter.place_order(intent, market_price=market_price)
        self.store.upsert_order(order)
        self.store.append_fills(fills)
        return {
            "status": order.status,
            "order": order.to_dict(),
            "fills": [fill.to_dict() for fill in fills],
        }

    def refresh_order(self, order_id: str, *, market_price: float | None = None) -> Dict[str, Any]:
        order = self.store.get_order(order_id)
        if order is None:
            raise ValueError(f"Unknown order_id: {order_id}")
        refreshed, fills = self.adapter.refresh_order(order, market_price=market_price)
        self.store.upsert_order(refreshed)
        self.store.append_fills(fills)
        return {
            "status": refreshed.status,
            "order": refreshed.to_dict(),
            "fills": [fill.to_dict() for fill in fills],
        }

    def execute_signal(
        self,
        signal: Mapping[str, Any],
        *,
        price: float,
        capital: float = 1_000_000,
        regime: str = "TREND",
        regime_multiplier: float = 1.0,
        risk_manager: Any = None,
    ) -> Dict[str, Any]:
        rm = risk_manager or self.risk_manager
        symbol_id = str(signal.get("symbol_id", "")).strip()
        if not symbol_id:
            return {"status": "REJECTED", "reason": "signal missing symbol_id"}

        exchange = str(signal.get("exchange", "NSE"))
        side = str(signal.get("side") or signal.get("transaction_type") or "BUY").upper()

        risk_payload: Optional[dict] = None
        quantity = int(signal.get("quantity") or signal.get("shares") or 0)
        if rm is not None and quantity <= 0:
            use_atr_position_sizing = bool(signal.get("use_atr_position_sizing", False))
            if use_atr_position_sizing:
                atr = _maybe_float(signal.get("atr_14"))
                atr_multiple = _maybe_float(signal.get("atr_multiple")) or 2.0
                risk_per_trade = _maybe_float(signal.get("risk_per_trade_pct")) or 0.01
                quantity = compute_atr_position_size(
                    capital=float(capital),
                    risk_per_trade=float(risk_per_trade),
                    entry_price=float(price),
                    atr=float(atr or 0.0),
                    atr_multiple=float(atr_multiple),
                )
                risk_payload = {
                    "shares": int(quantity),
                    "atr": atr,
                    "atr_multiple": atr_multiple,
                    "risk_per_trade_pct": risk_per_trade,
                    "sizing_method": "atr_config",
                }
            else:
                risk_payload = rm.compute_position_size(
                    symbol_id,
                    exchange=exchange,
                    capital=capital,
                    regime=regime,
                    regime_multiplier=regime_multiplier,
                )
                quantity = int(risk_payload.get("shares", 0))

        if quantity <= 0:
            return {
                "status": "REJECTED",
                "reason": "position sizing returned zero quantity",
                "risk": risk_payload or {},
            }

        intent = OrderIntent(
            symbol_id=symbol_id,
            exchange=exchange,
            quantity=quantity,
            side=side,
            order_type=str(signal.get("order_type", self.default_order_type)).upper(),
            product_type=str(signal.get("product_type", self.default_product_type)),
            validity=str(signal.get("validity", self.default_validity)),
            limit_price=_maybe_float(signal.get("limit_price")),
            stop_price=_maybe_float(signal.get("stop_price")),
            requested_price=float(price),
            correlation_id=str(signal.get("correlation_id") or "").strip() or None,
            metadata={
                "signal_strength": _maybe_float(signal.get("signal_strength")),
                "strategy": signal.get("strategy"),
                "regime": regime,
            },
        )
        result = self.submit_order(intent, market_price=price)
        if risk_payload is not None:
            result["risk"] = risk_payload
        return result


def _maybe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
