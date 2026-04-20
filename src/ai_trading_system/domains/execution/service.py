"""Execution service that wires adapters, storage, and risk sizing together."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, Optional

from ai_trading_system.domains.execution.adapters.base import ExecutionAdapter
from ai_trading_system.domains.execution.models import OrderIntent
from ai_trading_system.domains.execution.policies import compute_atr_position_size
from ai_trading_system.domains.execution.store import ExecutionStore


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
        return _build_order_result(order, fills)

    def refresh_order(self, order_id: str, *, market_price: float | None = None) -> Dict[str, Any]:
        order = self.store.get_order(order_id)
        if order is None:
            raise ValueError(f"Unknown order_id: {order_id}")
        refreshed, fills = self.adapter.refresh_order(order, market_price=market_price)
        self.store.upsert_order(refreshed)
        self.store.append_fills(fills)
        return _build_order_result(refreshed, fills)

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
        self._persist_stop_on_fill(
            symbol_id=symbol_id,
            exchange=exchange,
            quantity=intent.quantity,
            fill_price=price,
            side=side,
            signal=signal,
        )
        return result

    def _persist_stop_on_fill(
        self,
        *,
        symbol_id: str,
        exchange: str,
        quantity: int,
        fill_price: float,
        side: str,
        signal: dict,
    ) -> None:
        if side.upper() != "BUY" or quantity <= 0:
            return
        atr = _maybe_float(signal.get("atr_14"))
        atr_multiple = _maybe_float(signal.get("atr_multiple")) or _maybe_float(signal.get("exit_atr_multiple")) or 2.0
        if atr is None or atr <= 0:
            return
        stop_price = round(fill_price - (atr_multiple * atr), 4)
        position_key = f"{exchange.upper()}:{symbol_id.upper()}"
        self.store.upsert_position_stop(
            position_key=position_key,
            symbol_id=symbol_id,
            exchange=exchange,
            quantity=quantity,
            entry_price=fill_price,
            stop_price=stop_price,
            atr_multiplier=atr_multiple,
            status="ACTIVE",
            metadata={
                "signal": signal.get("strategy"),
                "regime": signal.get("regime"),
            },
        )

    def check_stop_triggered(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        current_price: float = 0.0,
    ) -> dict:
        position_key = f"{exchange.upper()}:{symbol_id.upper()}"
        stop_record = self.store.get_position_stop(position_key)
        if not stop_record or stop_record.get("status") != "ACTIVE":
            return {"triggered": False, "reason": "no_active_stop"}
        stop_price = float(stop_record.get("stop_price", 0))
        if stop_price <= 0:
            return {"triggered": False, "reason": "invalid_stop_price"}
        if current_price <= 0:
            return {"triggered": False, "reason": "no_market_price"}
        triggered = current_price <= stop_price
        return {
            "triggered": triggered,
            "stop_price": stop_price,
            "current_price": current_price,
            "position_key": position_key,
        }

    def maintain_trailing_stops(
        self,
        *,
        current_prices: Mapping[str, float] | None = None,
        atr_by_symbol: Mapping[str, float] | None = None,
        open_symbols: set[str] | None = None,
    ) -> Dict[str, Any]:
        normalized_prices = {
            str(symbol_id).strip().upper(): float(price)
            for symbol_id, price in (current_prices or {}).items()
            if symbol_id not in (None, "") and _maybe_float(price) is not None
        }
        normalized_atr = {
            str(symbol_id).strip().upper(): float(atr)
            for symbol_id, atr in (atr_by_symbol or {}).items()
            if symbol_id not in (None, "") and _maybe_float(atr) is not None
        }
        normalized_open_symbols = {
            str(symbol_id).strip().upper()
            for symbol_id in (open_symbols or set())
            if symbol_id not in (None, "")
        }

        updated_count = 0
        evaluated_count = 0
        for stop_record in self.store.list_active_stops():
            symbol_id = str(stop_record.get("symbol_id") or "").strip().upper()
            if not symbol_id:
                continue
            if normalized_open_symbols and symbol_id not in normalized_open_symbols:
                continue
            current_price = normalized_prices.get(symbol_id)
            atr = normalized_atr.get(symbol_id)
            if current_price is None or atr is None or atr <= 0:
                continue
            evaluated_count += 1
            existing_stop = float(stop_record.get("stop_price") or 0.0)
            atr_multiplier = float(stop_record.get("atr_multiplier") or 2.0)
            candidate_stop = round(current_price - (atr_multiplier * atr), 4)
            if candidate_stop <= existing_stop:
                continue
            metadata = dict(stop_record.get("metadata") or {})
            metadata.update(
                {
                    "trailing_stop_updated": True,
                    "trailing_reference_price": round(current_price, 4),
                    "trailing_atr_14": round(atr, 4),
                    "prior_stop_price": existing_stop,
                }
            )
            self.store.upsert_position_stop(
                position_key=str(stop_record.get("position_key") or f"{stop_record.get('exchange', 'NSE')}:{symbol_id}"),
                symbol_id=str(stop_record.get("symbol_id") or symbol_id),
                exchange=str(stop_record.get("exchange") or "NSE"),
                quantity=int(stop_record.get("quantity") or 0),
                entry_price=float(stop_record.get("entry_price") or 0.0),
                stop_price=candidate_stop,
                atr_multiplier=atr_multiplier,
                status=str(stop_record.get("status") or "ACTIVE"),
                metadata=metadata,
            )
            updated_count += 1

        return {
            "updated_count": updated_count,
            "evaluated_count": evaluated_count,
        }


def _maybe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_order_result(order: Any, fills: list[Any]) -> Dict[str, Any]:
    return {
        "status": order.status,
        "order": order.to_dict(),
        "fills": [fill.to_dict() for fill in fills],
    }
