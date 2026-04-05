"""Local paper-trading adapter with deterministic fills."""

from __future__ import annotations

import uuid
from dataclasses import replace
from typing import List, Tuple

from execution.adapters.base import ExecutionAdapter
from execution.models import FillRecord, OrderIntent, OrderRecord, utcnow


class PaperExecutionAdapter(ExecutionAdapter):
    """Simple local execution simulator for paper trading and tests."""

    broker_name = "paper"

    def __init__(self, *, slippage_bps: float = 5.0):
        self.slippage_bps = float(slippage_bps)

    def place_order(
        self,
        intent: OrderIntent,
        *,
        market_price: float | None = None,
    ) -> Tuple[OrderRecord, List[FillRecord]]:
        now = utcnow()
        order = OrderRecord(
            order_id=str(uuid.uuid4()),
            broker=self.broker_name,
            symbol_id=intent.symbol_id,
            quantity=int(intent.quantity),
            side=intent.side.upper(),
            exchange=intent.exchange,
            order_type=intent.order_type.upper(),
            product_type=intent.product_type,
            validity=intent.validity,
            status="OPEN",
            submitted_at=now,
            updated_at=now,
            correlation_id=intent.correlation_id,
            limit_price=intent.limit_price,
            stop_price=intent.stop_price,
            requested_price=intent.requested_price,
            metadata={**intent.metadata, "simulator": "paper"},
        )
        return self.refresh_order(order, market_price=market_price)

    def refresh_order(
        self,
        order: OrderRecord,
        *,
        market_price: float | None = None,
    ) -> Tuple[OrderRecord, List[FillRecord]]:
        if order.status not in {"OPEN", "NEW"}:
            return order, []
        if market_price is None:
            return order, []

        now = utcnow()
        can_fill = self._should_fill(order, market_price)
        if not can_fill:
            return replace(order, updated_at=now), []

        fill_price = self._resolve_fill_price(order, market_price)
        fill = FillRecord(
            fill_id=str(uuid.uuid4()),
            order_id=order.order_id,
            broker=self.broker_name,
            symbol_id=order.symbol_id,
            quantity=order.quantity,
            price=fill_price,
            filled_at=now,
            side=order.side,
            exchange=order.exchange,
            metadata={"order_type": order.order_type},
        )
        filled = replace(
            order,
            status="FILLED",
            updated_at=now,
            avg_fill_price=fill_price,
            filled_quantity=order.quantity,
        )
        return filled, [fill]

    def cancel_order(self, order: OrderRecord) -> OrderRecord:
        if order.status != "OPEN":
            return order
        return replace(order, status="CANCELLED", updated_at=utcnow())

    def _should_fill(self, order: OrderRecord, market_price: float) -> bool:
        if order.order_type == "MARKET":
            return True
        if order.order_type == "LIMIT":
            if order.limit_price is None:
                return False
            if order.side == "BUY":
                return market_price <= float(order.limit_price)
            return market_price >= float(order.limit_price)
        return False

    def _resolve_fill_price(self, order: OrderRecord, market_price: float) -> float:
        adjusted = self._apply_slippage(market_price, order.side)
        if order.order_type == "LIMIT" and order.limit_price is not None:
            if order.side == "BUY":
                return round(min(adjusted, float(order.limit_price)), 4)
            return round(max(adjusted, float(order.limit_price)), 4)
        return round(adjusted, 4)

    def _apply_slippage(self, market_price: float, side: str) -> float:
        direction = 1 if side.upper() == "BUY" else -1
        return float(market_price) * (1 + direction * (self.slippage_bps / 10_000.0))
