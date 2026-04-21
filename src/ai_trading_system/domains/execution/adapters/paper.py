"""Local paper-trading adapter with deterministic fills."""

from __future__ import annotations

import uuid
from dataclasses import replace
from typing import List, Tuple

from ai_trading_system.domains.execution.adapters.base import ExecutionAdapter
from ai_trading_system.domains.execution.models import FillRecord, OrderIntent, OrderRecord, utcnow

NSETxnCostConfig = {
    "brokerage_flat": 20.0,
    "gst_rate": 0.18,
    "stt_buy": 0.001,
    "stt_sell": 0.001,
    "exchange_rate": 0.00005,
    "sebi_rate": 0.000001,
    "stamp_duty_rate": 0.0001,
}


class NSETransactionCost:
    """NSE transaction cost calculator."""

    @staticmethod
    def calculate(
        price: float,
        quantity: int,
        side: str,
    ) -> dict:
        turnover = price * quantity
        side_upper = side.upper()

        brokerage = NSETxnCostConfig["brokerage_flat"]
        gst = brokerage * NSETxnCostConfig["gst_rate"]
        stt = turnover * (
            NSETxnCostConfig["stt_buy"] if side_upper == "BUY" else NSETxnCostConfig["stt_sell"]
        )
        exchange_fee = turnover * NSETxnCostConfig["exchange_rate"]
        sebi_charges = turnover * NSETxnCostConfig["sebi_rate"]
        stamp_duty = (
            turnover * NSETxnCostConfig["stamp_duty_rate"] if side_upper == "BUY" else 0.0
        )

        total_cost = brokerage + gst + stt + exchange_fee + sebi_charges + stamp_duty
        bps = (total_cost / turnover * 10_000) if turnover > 0 else 0.0

        return {
            "brokerage": round(brokerage, 2),
            "gst": round(gst, 2),
            "stt": round(stt, 4),
            "exchange_fee": round(exchange_fee, 4),
            "sebi_charges": round(sebi_charges, 4),
            "stamp_duty": round(stamp_duty, 4),
            "total_cost": round(total_cost, 2),
            "bps": round(bps, 2),
            "turnover": round(turnover, 2),
        }


class PaperExecutionAdapter(ExecutionAdapter):
    """Simple local execution simulator for paper trading and tests."""

    broker_name = "paper"

    def __init__(self, *, slippage_bps: float = 5.0, include_costs: bool = True):
        self.slippage_bps = float(slippage_bps)
        self.include_costs = include_costs

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
        fill_metadata = {"order_type": order.order_type}
        if self.include_costs and order.exchange.upper() == "NSE":
            cost_breakdown = NSETransactionCost.calculate(
                price=fill_price,
                quantity=order.quantity,
                side=order.side,
            )
            fill_metadata["transaction_cost"] = cost_breakdown
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
            metadata=fill_metadata,
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
