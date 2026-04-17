"""Auto-trading service that turns ranked signals into executed paper orders."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from execution.models import OrderIntent
from execution.policies import build_trade_actions
from execution.portfolio import PortfolioManager, check_portfolio_constraints
from execution.service import ExecutionService
from services.execute.entry_policy import select_entry_policy
from services.execute.exit_policy import build_exit_plan


class AutoTrader:
    """Drive buy/sell actions from ranked universes and current positions."""

    def __init__(self, service: ExecutionService, portfolio: PortfolioManager):
        self.service = service
        self.portfolio = portfolio

    def run(
        self,
        *,
        ranked_df: pd.DataFrame,
        ml_overlay_df: Optional[pd.DataFrame] = None,
        strategy_mode: str = "technical",
        target_position_count: int = 5,
        ml_horizon: int = 5,
        ml_confirm_threshold: float = 0.55,
        buy_quantity: int | None = None,
        capital: float = 1_000_000,
        regime: str = "TREND",
        regime_multiplier: float = 1.0,
        preview_only: bool = False,
        execution_enabled: bool = True,
        entry_policy_name: str = "breakout",
        exit_atr_multiple: float = 2.0,
        exit_max_holding_days: int = 20,
        use_portfolio_constraints: bool = False,
        max_positions: int = 10,
        max_sector_exposure: float = 0.30,
        max_single_stock_weight: float = 0.10,
        use_atr_position_sizing: bool = False,
    ) -> Dict[str, Any]:
        positions_before = self.portfolio.open_positions()
        actions = build_trade_actions(
            ranked_df=ranked_df,
            positions=positions_before,
            ml_overlay_df=ml_overlay_df,
            strategy_mode=strategy_mode,
            target_position_count=target_position_count,
            ml_horizon=ml_horizon,
            ml_confirm_threshold=ml_confirm_threshold,
        )
        executions: list[dict] = []
        preview_payloads: list[dict] = []

        ranked_lookup = {
            str(row["symbol_id"]): row
            for row in ranked_df.to_dict(orient="records")
            if "symbol_id" in row
        } if ranked_df is not None and not ranked_df.empty else {}
        open_count = len(positions_before)

        if not execution_enabled and not preview_only:
            return {
                "actions": [action.to_dict() for action in actions],
                "executions": [],
                "positions_before": [position.to_dict() for position in positions_before.values()],
                "positions_after": [position.to_dict() for position in positions_before.values()],
                "status": "disabled",
            }

        for action in actions:
            if preview_only:
                preview_payloads.append(
                    {
                        "action": action.to_dict(),
                        "result": {
                            "status": "PREVIEW",
                            "order": {
                                "symbol_id": action.symbol_id,
                                "exchange": action.exchange,
                                "side": action.side,
                                "quantity": int(action.quantity or buy_quantity or 0),
                                "requested_price": action.requested_price,
                                "strategy_mode": action.strategy_mode,
                                "reason": action.reason,
                            },
                            "fills": [],
                        },
                    }
                )
                continue
            if action.action == "BUY":
                signal = dict(ranked_lookup.get(action.symbol_id, {}))
                entry_plan = select_entry_policy(signal, policy_name=entry_policy_name)
                exit_plan = build_exit_plan(
                    signal,
                    atr_multiple=exit_atr_multiple,
                    max_holding_days=exit_max_holding_days,
                )
                if use_portfolio_constraints:
                    constraint_result = check_portfolio_constraints(
                        signal,
                        {
                            "open_positions_count": open_count,
                            "sector_exposure": {},
                            "symbol_weights": {},
                        },
                        max_positions=max_positions,
                        max_sector_exposure=max_sector_exposure,
                        max_single_stock_weight=max_single_stock_weight,
                    )
                    if not constraint_result["allowed"]:
                        executions.append(
                            {
                                "action": action.to_dict(),
                                "result": {
                                    "status": "REJECTED",
                                    "reason": "portfolio_constraints_failed",
                                    "constraints": constraint_result,
                                },
                            }
                        )
                        continue
                signal.update(
                    {
                        "symbol_id": action.symbol_id,
                        "exchange": action.exchange,
                        "side": "BUY",
                        "quantity": int(action.quantity or buy_quantity or 0),
                        "strategy": action.strategy_mode,
                        "entry_policy": entry_plan,
                        "exit_plan": exit_plan,
                        "execution_weight": signal.get("execution_weight", 1.0),
                        "use_atr_position_sizing": bool(use_atr_position_sizing),
                        "atr_14": signal.get("atr_14"),
                        "risk_per_trade_pct": signal.get("risk_per_trade_pct", 0.01),
                        "atr_multiple": signal.get("atr_multiple", exit_atr_multiple),
                        "correlation_id": f"{action.strategy_mode}:{action.symbol_id}:{action.reason}",
                    }
                )
                result = self.service.execute_signal(
                    signal,
                    price=float(action.requested_price or signal.get("close") or 0.0),
                    capital=capital,
                    regime=regime,
                    regime_multiplier=regime_multiplier,
                )
                if result.get("status") not in {"REJECTED", "ERROR"}:
                    open_count += 1
            else:
                result = self.service.submit_order(
                    OrderIntent(
                        symbol_id=action.symbol_id,
                        exchange=action.exchange,
                        quantity=int(action.quantity or 0),
                        side="SELL",
                        requested_price=action.requested_price,
                        correlation_id=f"{action.strategy_mode}:{action.symbol_id}:{action.reason}",
                        metadata={"reason": action.reason, **(action.metadata or {})},
                    ),
                    market_price=float(action.requested_price or 0.0),
                )
            executions.append(
                {
                    "action": action.to_dict(),
                    "result": result,
                }
            )

        if preview_only:
            positions_after = positions_before
            executions = preview_payloads
        else:
            positions_after = self.portfolio.open_positions()
        return {
            "actions": [action.to_dict() for action in actions],
            "executions": executions,
            "positions_before": [position.to_dict() for position in positions_before.values()],
            "positions_after": [position.to_dict() for position in positions_after.values()],
            "status": "preview" if preview_only else "completed",
        }
