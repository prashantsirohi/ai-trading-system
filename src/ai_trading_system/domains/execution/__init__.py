"""Execution domain modules."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ai_trading_system.domains.execution.adapters import (
    DhanExecutionAdapter,
    ExecutionAdapter,
    PaperExecutionAdapter,
)
from ai_trading_system.domains.execution.autotrader import AutoTrader
from ai_trading_system.domains.execution.portfolio import (
    PortfolioManager,
    closed_trade_ref,
    open_position_trade_ref,
)
from ai_trading_system.domains.execution.service import ExecutionService
from ai_trading_system.domains.execution.store import ExecutionStore


class DhanExecutor:
    """Compatibility wrapper that keeps legacy imports working safely."""

    def __init__(
        self,
        *,
        api_key: str,
        client_id: str,
        access_token: str,
        risk_manager: Any = None,
        project_root: Path | str | None = None,
        dry_run: bool = True,
        order_type: str = "MARKET",
        product_type: str = "INTRADAY",
        validity: str = "DAY",
    ):
        root = Path(project_root) if project_root else Path(__file__).resolve().parents[4]
        self.store = ExecutionStore(root)
        self.adapter = DhanExecutionAdapter(
            client_id=client_id,
            access_token=access_token,
            api_key=api_key,
            dry_run=dry_run,
        )
        self.service = ExecutionService(
            self.store,
            self.adapter,
            default_order_type=order_type,
            default_product_type=product_type,
            default_validity=validity,
            risk_manager=risk_manager,
        )

    def execute_signal(
        self,
        signal: Mapping[str, Any],
        *,
        price: float,
        risk_manager: Any = None,
        capital: float = 1_000_000,
        regime: str = "TREND",
        regime_multiplier: float = 1.0,
    ) -> dict:
        return self.service.execute_signal(
            signal,
            price=price,
            capital=capital,
            regime=regime,
            regime_multiplier=regime_multiplier,
            risk_manager=risk_manager,
        )


__all__ = [
    "DhanExecutor",
    "DhanExecutionAdapter",
    "ExecutionAdapter",
    "AutoTrader",
    "ExecutionService",
    "ExecutionStore",
    "PortfolioManager",
    "PaperExecutionAdapter",
    "closed_trade_ref",
    "open_position_trade_ref",
]
