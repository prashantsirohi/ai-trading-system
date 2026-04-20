"""Auto-trading service that turns ranked signals into executed paper orders."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import pandas as pd

from ai_trading_system.domains.execution.models import OrderIntent
from ai_trading_system.domains.execution.policies import build_trade_actions, compute_atr_position_size
from ai_trading_system.domains.execution.portfolio import PortfolioManager, check_portfolio_constraints
from ai_trading_system.domains.execution.service import ExecutionService
from ai_trading_system.domains.execution.entry_policy import select_entry_policy
from ai_trading_system.domains.execution.exit_policy import build_exit_plan

logger = logging.getLogger(__name__)


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
        current_prices: Optional[dict[str, float]] = None,
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
        max_sector_exposure: float = 0.20,
        max_single_stock_weight: float = 0.10,
        use_atr_position_sizing: bool = False,
        heat_gate_threshold: float = 0.15,
    ) -> Dict[str, Any]:
        positions_before = self.portfolio.open_positions()
        current_prices = {
            str(symbol_id).strip().upper(): float(price)
            for symbol_id, price in (current_prices or {}).items()
            if symbol_id not in (None, "") and price is not None
        }
        heat_gate_ok, open_risk = self.portfolio.check_heat_gate(
            positions=positions_before,
            capital=capital,
            threshold=heat_gate_threshold,
        )

        stop_loss_actions = []
        if execution_enabled and not preview_only:
            for pos_symbol, position in positions_before.items():
                current_price = current_prices.get(str(pos_symbol).strip().upper())
                if current_price is None:
                    logger.debug(
                        "Skipping stop evaluation for %s because no current price was provided",
                        position.symbol_id,
                    )
                    continue
                stop_check = self.service.check_stop_triggered(
                    symbol_id=position.symbol_id,
                    exchange=position.exchange,
                    current_price=current_price,
                )
                if stop_check.get("triggered"):
                    from ai_trading_system.domains.execution.models import TradeAction
                    stop_loss_actions.append(
                        TradeAction(
                            symbol_id=position.symbol_id,
                            exchange=position.exchange,
                            action="SELL",
                            quantity=position.quantity,
                            requested_price=current_price,
                            reason=f"stop_triggered: stop_price={stop_check.get('stop_price')}, current={stop_check.get('current_price')}",
                            metadata={"stop_position_key": stop_check.get("position_key")},
                        )
                    )

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
        sector_lookup = _build_sector_lookup(ranked_df)
        portfolio_state = _build_portfolio_state(
            positions=positions_before,
            capital=capital,
            sector_lookup=sector_lookup,
        )

        if not execution_enabled and not preview_only:
            return {
                "actions": [action.to_dict() for action in actions],
                "executions": [],
                "positions_before": _serialize_positions(positions_before),
                "positions_after": _serialize_positions(positions_before),
                "status": "disabled",
            }

        if stop_loss_actions:
            actions = stop_loss_actions + list(actions)

        for action in actions:
            if preview_only:
                preview_payloads.append(_build_preview_execution(action, buy_quantity=buy_quantity))
                continue
            if action.action == "BUY":
                signal = dict(ranked_lookup.get(action.symbol_id, {}))
                signal.setdefault("symbol_id", action.symbol_id)
                signal.setdefault("exchange", action.exchange)
                signal_sector = _resolve_sector(signal, sector_lookup)
                signal.setdefault("sector_name", signal_sector)
                entry_plan = select_entry_policy(signal, policy_name=entry_policy_name)
                exit_plan = build_exit_plan(
                    signal,
                    atr_multiple=exit_atr_multiple,
                    max_holding_days=exit_max_holding_days,
                )
                if use_portfolio_constraints:
                    requested_price = float(action.requested_price or signal.get("close") or 0.0)
                    requested_quantity = _estimate_buy_quantity(
                        signal=signal,
                        explicit_quantity=int(action.quantity or buy_quantity or 0),
                        requested_price=requested_price,
                        capital=capital,
                        regime=regime,
                        regime_multiplier=regime_multiplier,
                        use_atr_position_sizing=use_atr_position_sizing,
                        risk_manager=self.service.risk_manager,
                    )
                    projected_state = _project_portfolio_state_for_buy(
                        portfolio_state=portfolio_state,
                        candidate=signal,
                        capital=capital,
                        quantity=requested_quantity,
                        requested_price=requested_price,
                        sector_lookup=sector_lookup,
                    )
                    constraint_result = check_portfolio_constraints(
                        signal,
                        projected_state,
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
                if not heat_gate_ok:
                    executions.append(
                        {
                            "action": action.to_dict(),
                            "result": {
                                "status": "REJECTED",
                                "reason": "heat_gate_exceeded",
                                "open_risk": open_risk,
                                "threshold": heat_gate_threshold,
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
                    portfolio_state = _project_portfolio_state_for_buy(
                        portfolio_state=portfolio_state,
                        candidate=signal,
                        capital=capital,
                        quantity=_estimate_buy_quantity(
                            signal=signal,
                            explicit_quantity=int(action.quantity or buy_quantity or 0),
                            requested_price=float(action.requested_price or signal.get("close") or 0.0),
                            capital=capital,
                            regime=regime,
                            regime_multiplier=regime_multiplier,
                            use_atr_position_sizing=use_atr_position_sizing,
                            risk_manager=self.service.risk_manager,
                        ),
                        requested_price=float(action.requested_price or signal.get("close") or 0.0),
                        sector_lookup=sector_lookup,
                    )
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
                if (
                    action.reason.startswith("stop_triggered:")
                    and result.get("status") not in {"REJECTED", "ERROR"}
                ):
                    position_key = str((action.metadata or {}).get("stop_position_key") or "").strip()
                    if position_key:
                        self.service.store.deactivate_stop(position_key)
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
            "positions_before": _serialize_positions(positions_before),
            "positions_after": _serialize_positions(positions_after),
            "status": "preview" if preview_only else "completed",
        }


def _serialize_positions(positions: dict[str, Any]) -> list[dict]:
    return [position.to_dict() for position in positions.values()]


def _build_sector_lookup(ranked_df: pd.DataFrame | None) -> dict[str, str]:
    if ranked_df is None or ranked_df.empty:
        return {}
    lookup: dict[str, str] = {}
    for row in ranked_df.to_dict(orient="records"):
        symbol_id = str(row.get("symbol_id") or "").strip().upper()
        if not symbol_id:
            continue
        lookup[symbol_id] = str(row.get("sector_name") or row.get("sector") or "UNKNOWN").strip() or "UNKNOWN"
    return lookup


def _resolve_sector(candidate: dict[str, Any], sector_lookup: dict[str, str]) -> str:
    symbol_id = str(candidate.get("symbol_id") or "").strip().upper()
    direct_sector = str(candidate.get("sector_name") or candidate.get("sector") or "").strip()
    if direct_sector:
        return direct_sector
    return sector_lookup.get(symbol_id, "UNKNOWN")


def _build_portfolio_state(
    *,
    positions: dict[str, Any],
    capital: float,
    sector_lookup: dict[str, str],
) -> dict[str, Any]:
    normalized_capital = float(capital or 0.0)
    symbol_weights: dict[str, float] = {}
    sector_exposure: dict[str, float] = {}
    for position in positions.values():
        symbol_id = str(position.symbol_id).strip().upper()
        position_value = float(position.quantity) * float(position.avg_entry_price)
        weight = (position_value / normalized_capital) if normalized_capital > 0 else 0.0
        symbol_weights[symbol_id] = symbol_weights.get(symbol_id, 0.0) + weight
        sector_name = sector_lookup.get(symbol_id, "UNKNOWN")
        sector_exposure[sector_name] = sector_exposure.get(sector_name, 0.0) + weight
    return {
        "open_positions_count": len(positions),
        "symbol_weights": symbol_weights,
        "sector_exposure": sector_exposure,
    }


def _project_portfolio_state_for_buy(
    *,
    portfolio_state: dict[str, Any],
    candidate: dict[str, Any],
    capital: float,
    quantity: int,
    requested_price: float,
    sector_lookup: dict[str, str],
) -> dict[str, Any]:
    next_state = {
        "open_positions_count": int(portfolio_state.get("open_positions_count", 0) or 0),
        "symbol_weights": dict(portfolio_state.get("symbol_weights") or {}),
        "sector_exposure": dict(portfolio_state.get("sector_exposure") or {}),
    }
    symbol_id = str(candidate.get("symbol_id") or "").strip().upper()
    if not symbol_id:
        return next_state
    position_value = max(0.0, float(quantity or 0) * float(requested_price or 0.0))
    weight_delta = (position_value / float(capital)) if float(capital or 0.0) > 0 else 0.0
    if symbol_id not in next_state["symbol_weights"] and position_value > 0:
        next_state["open_positions_count"] += 1
    next_state["symbol_weights"][symbol_id] = next_state["symbol_weights"].get(symbol_id, 0.0) + weight_delta
    sector_name = _resolve_sector(candidate, sector_lookup)
    next_state["sector_exposure"][sector_name] = next_state["sector_exposure"].get(sector_name, 0.0) + weight_delta
    return next_state


def _estimate_buy_quantity(
    *,
    signal: dict[str, Any],
    explicit_quantity: int,
    requested_price: float,
    capital: float,
    regime: str,
    regime_multiplier: float,
    use_atr_position_sizing: bool,
    risk_manager: Any,
) -> int:
    if explicit_quantity > 0:
        return explicit_quantity
    if risk_manager is None:
        return 0
    if use_atr_position_sizing:
        atr = _maybe_float(signal.get("atr_14"))
        atr_multiple = _maybe_float(signal.get("atr_multiple")) or 2.0
        risk_per_trade = _maybe_float(signal.get("risk_per_trade_pct")) or 0.01
        return compute_atr_position_size(
            capital=float(capital),
            risk_per_trade=float(risk_per_trade),
            entry_price=float(requested_price),
            atr=float(atr or 0.0),
            atr_multiple=float(atr_multiple),
        )
    risk_payload = risk_manager.compute_position_size(
        str(signal.get("symbol_id") or "").strip(),
        exchange=str(signal.get("exchange") or "NSE"),
        capital=capital,
        regime=regime,
        regime_multiplier=regime_multiplier,
    )
    return int((risk_payload or {}).get("shares", 0) or 0)


def _maybe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_preview_execution(action: Any, *, buy_quantity: int | None) -> dict:
    return {
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
