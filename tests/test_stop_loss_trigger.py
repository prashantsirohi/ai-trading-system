from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.execution import AutoTrader, ExecutionService, ExecutionStore, PaperExecutionAdapter, PortfolioManager
from ai_trading_system.domains.execution.models import OrderIntent


def test_autotrader_uses_current_prices_for_stop_trigger_and_deactivates_stop(tmp_path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    portfolio = PortfolioManager(store)

    service.submit_order(
        OrderIntent(symbol_id="TESTSTOCK", exchange="NSE", quantity=10, side="BUY"),
        market_price=100.0,
    )
    store.upsert_position_stop(
        position_key="NSE:TESTSTOCK",
        symbol_id="TESTSTOCK",
        exchange="NSE",
        quantity=10,
        entry_price=100.0,
        stop_price=95.0,
        atr_multiplier=2.0,
        status="ACTIVE",
    )

    result = AutoTrader(service, portfolio).run(
        ranked_df=pd.DataFrame([{"symbol_id": "TESTSTOCK", "exchange": "NSE", "close": 93.0}]),
        current_prices={"TESTSTOCK": 93.0},
        strategy_mode="technical",
        target_position_count=1,
    )

    actions = pd.DataFrame(result["actions"])
    assert not actions.empty
    assert ((actions["symbol_id"] == "TESTSTOCK") & (actions["action"] == "SELL")).any()

    stop_record = store.get_position_stop("NSE:TESTSTOCK")
    assert stop_record is not None
    assert stop_record["status"] == "INACTIVE"


def test_trailing_stop_raises_with_price_and_never_moves_down(tmp_path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))

    service.submit_order(
        OrderIntent(symbol_id="TRENDING", exchange="NSE", quantity=10, side="BUY"),
        market_price=100.0,
    )
    store.upsert_position_stop(
        position_key="NSE:TRENDING",
        symbol_id="TRENDING",
        exchange="NSE",
        quantity=10,
        entry_price=100.0,
        stop_price=95.0,
        atr_multiplier=2.0,
        status="ACTIVE",
    )

    raised = service.maintain_trailing_stops(
        current_prices={"TRENDING": 110.0},
        atr_by_symbol={"TRENDING": 3.0},
        open_symbols={"TRENDING"},
    )
    stop_after_raise = store.get_position_stop("NSE:TRENDING")

    assert raised["updated_count"] == 1
    assert stop_after_raise is not None
    assert stop_after_raise["stop_price"] == 104.0

    unchanged = service.maintain_trailing_stops(
        current_prices={"TRENDING": 101.0},
        atr_by_symbol={"TRENDING": 4.0},
        open_symbols={"TRENDING"},
    )
    stop_after_pullback = store.get_position_stop("NSE:TRENDING")

    assert unchanged["updated_count"] == 0
    assert stop_after_pullback is not None
    assert stop_after_pullback["stop_price"] == 104.0
