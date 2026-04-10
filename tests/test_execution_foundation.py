from __future__ import annotations

from pathlib import Path

from execution import DhanExecutor
from execution.adapters import PaperExecutionAdapter
from execution.models import OrderIntent
from execution.service import ExecutionService
from execution.store import ExecutionStore


class _StaticRiskManager:
    def compute_position_size(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        capital: float = 1_000_000,
        regime: str = "TREND",
        regime_multiplier: float = 1.0,
    ) -> dict:
        return {
            "symbol_id": symbol_id,
            "shares": 25,
            "position_value": 2500.0,
            "risk_amount": 100.0,
            "stop_loss": 94.0,
            "atr": 2.0,
            "close": 100.0,
            "regime": regime,
            "regime_multiplier": regime_multiplier,
        }


def test_paper_execution_market_order_persists_fill(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=10))

    result = service.submit_order(
        OrderIntent(symbol_id="RELIANCE", exchange="NSE", quantity=10, side="BUY"),
        market_price=100.0,
    )

    assert result["status"] == "FILLED"
    assert result["order"]["filled_quantity"] == 10
    assert len(result["fills"]) == 1
    assert store.list_orders()[0]["symbol_id"] == "RELIANCE"
    assert store.list_fills()[0]["order_id"] == result["order"]["order_id"]


def test_paper_execution_limit_order_can_be_refreshed_into_fill(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))

    pending = service.submit_order(
        OrderIntent(
            symbol_id="TCS",
            exchange="NSE",
            quantity=5,
            side="BUY",
            order_type="LIMIT",
            limit_price=100.0,
        ),
        market_price=101.0,
    )

    assert pending["status"] == "OPEN"
    filled = service.refresh_order(pending["order"]["order_id"], market_price=99.5)

    assert filled["status"] == "FILLED"
    assert filled["order"]["avg_fill_price"] == 99.5
    assert len(store.list_fills(order_id=pending["order"]["order_id"])) == 1


def test_dhan_executor_stays_safe_and_returns_simulated_order(tmp_path: Path) -> None:
    executor = DhanExecutor(
        api_key="key",
        client_id="client",
        access_token="token",
        risk_manager=_StaticRiskManager(),
        project_root=tmp_path,
        dry_run=True,
    )

    result = executor.execute_signal(
        {"symbol_id": "INFY", "exchange": "NSE", "strategy": "swing_breakout"},
        price=100.0,
    )

    assert result["status"] == "SIMULATED"
    assert result["order"]["broker"] == "dhan"
    assert result["order"]["filled_quantity"] == 0
    assert result["risk"]["shares"] == 25
