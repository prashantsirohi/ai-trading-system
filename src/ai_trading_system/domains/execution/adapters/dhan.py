"""Safe Dhan execution adapter scaffold."""

from __future__ import annotations

import uuid
from typing import List, Tuple

from ai_trading_system.domains.execution.adapters.base import ExecutionAdapter
from ai_trading_system.domains.execution.models import FillRecord, OrderIntent, OrderRecord, utcnow


class DhanExecutionAdapter(ExecutionAdapter):
    """Dhan adapter placeholder with explicit dry-run default."""

    broker_name = "dhan"

    def __init__(
        self,
        *,
        client_id: str,
        access_token: str,
        api_key: str = "",
        dry_run: bool = True,
    ):
        self.client_id = client_id
        self.access_token = access_token
        self.api_key = api_key
        self.dry_run = dry_run

    def place_order(
        self,
        intent: OrderIntent,
        *,
        market_price: float | None = None,
    ) -> Tuple[OrderRecord, List[FillRecord]]:
        now = utcnow()
        if self.dry_run:
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
                status="SIMULATED",
                submitted_at=now,
                updated_at=now,
                correlation_id=intent.correlation_id,
                limit_price=intent.limit_price,
                stop_price=intent.stop_price,
                requested_price=intent.requested_price or market_price,
                metadata={
                    **intent.metadata,
                    "dry_run": True,
                    "note": "Live Dhan execution is intentionally disabled until sandbox/live validation is completed.",
                },
            )
            return order, []

        raise RuntimeError(
            "Live Dhan execution is intentionally disabled. Validate sandbox/live order behavior before enabling broker placement."
        )
