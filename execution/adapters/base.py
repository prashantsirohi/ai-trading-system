"""Abstract execution adapter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Tuple

from execution.models import FillRecord, OrderIntent, OrderRecord


class ExecutionAdapter(ABC):
    """Broker/paper adapter contract."""

    broker_name: str = "unknown"

    @abstractmethod
    def place_order(
        self,
        intent: OrderIntent,
        *,
        market_price: float | None = None,
    ) -> Tuple[OrderRecord, List[FillRecord]]:
        """Submit a new order."""

    def refresh_order(
        self,
        order: OrderRecord,
        *,
        market_price: float | None = None,
    ) -> Tuple[OrderRecord, List[FillRecord]]:
        """Refresh an open order against current market state."""
        return order, []

    def cancel_order(self, order: OrderRecord) -> OrderRecord:
        """Cancel an order when supported."""
        return order
