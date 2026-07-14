from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import uuid

import pandas as pd
import pytest

from ai_trading_system.domains.execution import (
    AutoTrader,
    ExecutionService,
    ExecutionStore,
    PaperExecutionAdapter,
    PortfolioManager,
)
from ai_trading_system.domains.execution.models import FillRecord, OrderIntent, OrderRecord, utcnow


class _FixedRiskManager:
    def compute_position_size(self, symbol_id: str, **_: object) -> dict:
        return {"symbol_id": symbol_id, "shares": 10, "risk_amount": 100.0}


class _DeferredFillAdapter:
    broker_name = "deferred-test"

    def place_order(self, intent: OrderIntent, *, market_price: float | None = None):
        now = utcnow()
        return (
            OrderRecord(
                order_id=str(uuid.uuid4()),
                broker=self.broker_name,
                symbol_id=intent.symbol_id,
                quantity=intent.quantity,
                side=intent.side,
                exchange=intent.exchange,
                order_type=intent.order_type,
                product_type=intent.product_type,
                validity=intent.validity,
                status="OPEN",
                submitted_at=now,
                updated_at=now,
                correlation_id=intent.correlation_id,
                requested_price=intent.requested_price,
                filled_quantity=0,
                metadata=dict(intent.metadata),
            ),
            [],
        )

    def refresh_order(self, order: OrderRecord, *, market_price: float | None = None):
        remaining = order.quantity - order.filled_quantity
        fill_quantity = min(4, remaining) if order.filled_quantity == 0 else remaining
        fill = FillRecord(
            fill_id=str(uuid.uuid4()),
            order_id=order.order_id,
            broker=self.broker_name,
            symbol_id=order.symbol_id,
            quantity=fill_quantity,
            price=float(market_price or order.requested_price or 0.0),
            filled_at=utcnow(),
            side=order.side,
            exchange=order.exchange,
        )
        total = order.filled_quantity + fill_quantity
        status = "FILLED" if total == order.quantity else "PARTIALLY_FILLED"
        return replace(order, status=status, filled_quantity=total, updated_at=utcnow()), [fill]

    def cancel_order(self, order: OrderRecord) -> OrderRecord:
        return replace(order, status="CANCELLED", updated_at=utcnow())


class _UnknownOutcomeAdapter(PaperExecutionAdapter):
    def __init__(self):
        super().__init__(slippage_bps=0)
        self.dispatch_count = 0

    def place_order(self, intent: OrderIntent, *, market_price: float | None = None):
        self.dispatch_count += 1
        super().place_order(intent, market_price=market_price)
        raise RuntimeError("connection lost after broker accepted order")


def test_unknown_submission_is_not_redispatched_and_can_be_reconciled(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    adapter = _UnknownOutcomeAdapter()
    service = ExecutionService(store, adapter)
    intent = OrderIntent(
        symbol_id="SAFE",
        quantity=10,
        correlation_id="run-1:SAFE:entry",
    )

    with pytest.raises(RuntimeError, match="connection lost"):
        service.submit_order(intent, market_price=100.0)

    retry = service.submit_order(intent, market_price=100.0)
    reconciled = service.reconcile_submission("run-1:SAFE:entry")

    assert retry["status"] == "RECONCILIATION_REQUIRED"
    assert adapter.dispatch_count == 1
    assert reconciled["status"] == "FILLED"
    assert reconciled["reconciled"] is True
    assert len(store.list_orders()) == 1
    assert len(store.list_fills()) == 1


def test_concurrent_identical_submissions_dispatch_once(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    intent = OrderIntent(
        symbol_id="SAFE",
        quantity=10,
        correlation_id="run-1:SAFE:concurrent-entry",
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda _: service.submit_order(intent, market_price=100.0),
                range(2),
            )
        )

    assert {result["order"]["order_id"] for result in results} == {
        store.list_orders()[0]["order_id"]
    }
    assert len(store.list_orders()) == 1
    assert len(store.list_fills()) == 1


def test_stop_lifecycle_follows_confirmed_partial_buy_fills(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, _DeferredFillAdapter())
    submitted = service.execute_signal(
        {
            "symbol_id": "SAFE",
            "quantity": 10,
            "atr_14": 5.0,
            "atr_multiple": 2.0,
        },
        price=100.0,
    )

    assert submitted["status"] == "OPEN"
    assert store.get_position_stop("NSE:SAFE") is None

    partial = service.refresh_order(submitted["order"]["order_id"], market_price=100.0)
    partial_stop = store.get_position_stop("NSE:SAFE")
    assert partial["status"] == "PARTIALLY_FILLED"
    assert partial_stop["quantity"] == 4
    assert partial_stop["stop_price"] == 90.0

    completed = service.refresh_order(submitted["order"]["order_id"], market_price=101.0)
    completed_stop = store.get_position_stop("NSE:SAFE")
    assert completed["status"] == "FILLED"
    assert completed_stop["quantity"] == 10


def test_partial_sell_keeps_stop_active_for_remaining_position(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    buy_service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    buy_service.execute_signal(
        {"symbol_id": "SAFE", "quantity": 10, "atr_14": 5.0},
        price=100.0,
    )
    sell_service = ExecutionService(store, _DeferredFillAdapter())
    submitted = sell_service.submit_order(
        OrderIntent(symbol_id="SAFE", quantity=10, side="SELL", requested_price=100.0),
    )

    assert store.get_position_stop("NSE:SAFE")["status"] == "ACTIVE"
    partial = sell_service.refresh_order(submitted["order"]["order_id"], market_price=100.0)
    partial_stop = store.get_position_stop("NSE:SAFE")
    assert partial["status"] == "PARTIALLY_FILLED"
    assert partial_stop["status"] == "ACTIVE"
    assert partial_stop["quantity"] == 6

    sell_service.refresh_order(submitted["order"]["order_id"], market_price=100.0)
    assert store.get_position_stop("NSE:SAFE")["status"] == "INACTIVE"


def test_cancel_after_partial_buy_preserves_stop_for_filled_quantity(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, _DeferredFillAdapter())
    submitted = service.execute_signal(
        {"symbol_id": "SAFE", "quantity": 10, "atr_14": 5.0},
        price=100.0,
    )
    service.refresh_order(submitted["order"]["order_id"], market_price=100.0)

    cancelled = service.cancel_order(submitted["order"]["order_id"])
    stop = store.get_position_stop("NSE:SAFE")

    assert cancelled["status"] == "CANCELLED"
    assert stop["status"] == "ACTIVE"
    assert stop["quantity"] == 4


def test_concurrent_batches_share_one_heat_boundary(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)

    def run(ranked: pd.DataFrame) -> dict:
        service = ExecutionService(
            store,
            PaperExecutionAdapter(slippage_bps=0),
            risk_manager=_FixedRiskManager(),
        )
        return AutoTrader(service, PortfolioManager(store)).run(
            ranked_df=ranked,
            target_position_count=len(ranked),
            buy_quantity=10,
            capital=1_000.0,
            heat_gate_threshold=0.15,
        )

    aaa = pd.DataFrame(
        [{"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "atr_14": 5.0}]
    )
    both = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "atr_14": 5.0},
            {"symbol_id": "BBB", "exchange": "NSE", "close": 100.0, "atr_14": 5.0},
        ]
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(run, [aaa, both]))

    heat_ok, heat = PortfolioManager(store).check_heat_gate(
        PortfolioManager(store).open_positions(),
        capital=1_000.0,
        threshold=0.15,
    )
    assert heat_ok is True
    assert heat <= 0.15
    assert len(store.list_fills()) == 1
