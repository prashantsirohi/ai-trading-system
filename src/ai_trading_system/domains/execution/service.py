"""Execution service that wires adapters, storage, and risk sizing together."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
import hashlib
import json
from typing import Any, Dict, Optional

from ai_trading_system.domains.execution.adapters.base import ExecutionAdapter
from ai_trading_system.domains.execution.models import OrderIntent
from ai_trading_system.domains.execution.policies import compute_atr_position_size
from ai_trading_system.domains.execution.store import ExecutionStore


class ExecutionService:
    """High-level execution orchestration for signals and manual orders."""

    def __init__(
        self,
        store: ExecutionStore,
        adapter: ExecutionAdapter,
        *,
        default_order_type: str = "MARKET",
        default_product_type: str = "INTRADAY",
        default_validity: str = "DAY",
        risk_manager: Any = None,
    ):
        self.store = store
        self.adapter = adapter
        self.default_order_type = default_order_type
        self.default_product_type = default_product_type
        self.default_validity = default_validity
        self.risk_manager = risk_manager

    def submit_order(
        self,
        intent: OrderIntent,
        *,
        market_price: float | None = None,
    ) -> Dict[str, Any]:
        correlation_id = str(intent.correlation_id or "").strip()
        if not correlation_id:
            order, fills = self.adapter.place_order(intent, market_price=market_price)
            self.store.upsert_order(order)
            self.store.append_fills(fills)
            self._reconcile_stop_state(order)
            return _build_order_result(order, fills)

        intent = replace(intent, correlation_id=correlation_id)
        payload = _idempotency_payload(intent)
        payload_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        with self.store.submission_lock():
            existing = self.store.get_order_by_correlation_id(correlation_id)
            if existing is not None:
                return self._replay_or_reject(intent, existing)

            reservation, created = self.store.reserve_submission_intent(
                correlation_id=correlation_id,
                payload_hash=payload_hash,
                payload=payload,
            )
            if reservation["payload_hash"] != payload_hash:
                return {
                    "status": "REJECTED",
                    "reason": "idempotency_key_conflict",
                    "correlation_id": correlation_id,
                    "order": None,
                    "fills": [],
                    "idempotent_replay": False,
                }
            if not created:
                order_id = reservation.get("order_id")
                if order_id:
                    persisted = self.store.get_order(str(order_id))
                    if persisted is not None:
                        return self._replay_or_reject(intent, persisted)
                return {
                    "status": "RECONCILIATION_REQUIRED",
                    "reason": "submission_outcome_unknown",
                    "correlation_id": correlation_id,
                    "order": None,
                    "fills": [],
                    "idempotent_replay": True,
                }

            try:
                order, fills = self.adapter.place_order(intent, market_price=market_price)
                self.store.complete_submission(
                    correlation_id=correlation_id,
                    order=order,
                    fills=fills,
                )
                self._reconcile_stop_state(order)
            except Exception as exc:
                self.store.mark_submission_reconciliation_required(correlation_id, str(exc))
                raise
        result = _build_order_result(order, fills)
        result["idempotent_replay"] = False
        return result

    def _replay_or_reject(self, intent: OrderIntent, existing: Any) -> Dict[str, Any]:
        if not _intent_matches_order(intent, existing):
            return {
                "status": "REJECTED",
                "reason": "idempotency_key_conflict",
                "correlation_id": intent.correlation_id,
                "order": existing.to_dict(),
                "fills": self.store.list_fills(order_id=existing.order_id),
                "idempotent_replay": False,
            }
        return {
            "status": existing.status,
            "order": existing.to_dict(),
            "fills": self.store.list_fills(order_id=existing.order_id),
            "idempotent_replay": True,
        }

    def reconcile_submission(self, correlation_id: str) -> Dict[str, Any]:
        """Recover an unknown submission outcome without dispatching a new order."""
        normalized = str(correlation_id or "").strip()
        reservation = self.store.get_submission_intent(normalized)
        if reservation is None:
            return {"status": "NOT_FOUND", "correlation_id": normalized}
        if reservation.get("order_id"):
            order = self.store.get_order(str(reservation["order_id"]))
            if order is not None:
                return self._replay_or_reject(
                    _intent_from_payload(json.loads(reservation["payload_json"]), normalized),
                    order,
                )
        resolver = getattr(self.adapter, "find_order_by_correlation_id", None)
        recovered = resolver(normalized) if callable(resolver) else None
        if recovered is None:
            return {
                "status": "RECONCILIATION_REQUIRED",
                "reason": "broker_outcome_unavailable",
                "correlation_id": normalized,
            }
        order, fills = recovered
        self.store.complete_submission(
            correlation_id=normalized,
            order=order,
            fills=fills,
        )
        self._reconcile_stop_state(order)
        result = _build_order_result(order, fills)
        result["reconciled"] = True
        return result

    def refresh_order(self, order_id: str, *, market_price: float | None = None) -> Dict[str, Any]:
        order = self.store.get_order(order_id)
        if order is None:
            raise ValueError(f"Unknown order_id: {order_id}")
        refreshed, fills = self.adapter.refresh_order(order, market_price=market_price)
        self.store.upsert_order(refreshed)
        self.store.append_fills(fills)
        self._reconcile_stop_state(refreshed)
        return _build_order_result(refreshed, fills)

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        order = self.store.get_order(order_id)
        if order is None:
            raise ValueError(f"Unknown order_id: {order_id}")
        cancelled = self.adapter.cancel_order(order)
        self.store.upsert_order(cancelled)
        self._reconcile_stop_state(cancelled)
        return _build_order_result(cancelled, [])

    def execute_signal(
        self,
        signal: Mapping[str, Any],
        *,
        price: float,
        capital: float = 1_000_000,
        regime: str = "TREND",
        regime_multiplier: float = 1.0,
        risk_manager: Any = None,
    ) -> Dict[str, Any]:
        rm = risk_manager or self.risk_manager
        symbol_id = str(signal.get("symbol_id", "")).strip()
        if not symbol_id:
            return {"status": "REJECTED", "reason": "signal missing symbol_id"}

        exchange = str(signal.get("exchange", "NSE"))
        side = str(signal.get("side") or signal.get("transaction_type") or "BUY").upper()

        risk_payload: Optional[dict] = None
        quantity = int(signal.get("quantity") or signal.get("shares") or 0)
        if rm is not None and quantity <= 0:
            use_atr_position_sizing = bool(signal.get("use_atr_position_sizing", False))
            if use_atr_position_sizing:
                atr = _maybe_float(signal.get("atr_14"))
                atr_multiple = _maybe_float(signal.get("atr_multiple")) or 2.0
                risk_per_trade = _maybe_float(signal.get("risk_per_trade_pct")) or 0.01
                quantity = compute_atr_position_size(
                    capital=float(capital),
                    risk_per_trade=float(risk_per_trade),
                    entry_price=float(price),
                    atr=float(atr or 0.0),
                    atr_multiple=float(atr_multiple),
                )
                risk_payload = {
                    "shares": int(quantity),
                    "atr": atr,
                    "atr_multiple": atr_multiple,
                    "risk_per_trade_pct": risk_per_trade,
                    "sizing_method": "atr_config",
                }
            else:
                risk_payload = rm.compute_position_size(
                    symbol_id,
                    exchange=exchange,
                    capital=capital,
                    regime=regime,
                    regime_multiplier=regime_multiplier,
                )
                quantity = int(risk_payload.get("shares", 0))

        if quantity <= 0:
            return {
                "status": "REJECTED",
                "reason": "position sizing returned zero quantity",
                "risk": risk_payload or {},
            }

        intent = OrderIntent(
            symbol_id=symbol_id,
            exchange=exchange,
            quantity=quantity,
            side=side,
            order_type=str(signal.get("order_type", self.default_order_type)).upper(),
            product_type=str(signal.get("product_type", self.default_product_type)),
            validity=str(signal.get("validity", self.default_validity)),
            limit_price=_maybe_float(signal.get("limit_price")),
            stop_price=_maybe_float(signal.get("stop_price")),
            requested_price=float(price),
            correlation_id=str(signal.get("correlation_id") or "").strip() or None,
            metadata={
                "signal_strength": _maybe_float(signal.get("signal_strength")),
                "strategy": signal.get("strategy"),
                "regime": regime,
                # engine-emitted fields (None when legacy path)
                "intent_kind": signal.get("intent_kind"),
                "reason": signal.get("reason"),
                "initial_stop": _maybe_float(signal.get("initial_stop")),
                "stop_method": signal.get("stop_method"),
                "rank_at_entry": signal.get("rank_at_entry"),
                "score_at_entry": signal.get("score_at_entry"),
                "atr_14": _maybe_float(signal.get("atr_14")),
                "atr_multiple": _maybe_float(signal.get("atr_multiple")),
                "exit_atr_multiple": _maybe_float(signal.get("exit_atr_multiple")),
                "sector": signal.get("sector"),
            },
        )
        result = self.submit_order(intent, market_price=price)
        if risk_payload is not None:
            result["risk"] = risk_payload
        return result

    def _reconcile_stop_state(self, order: Any) -> None:
        from ai_trading_system.domains.execution.portfolio import PortfolioManager

        position_key = f"{order.exchange.upper()}:{order.symbol_id.upper()}"
        position = PortfolioManager(self.store).open_positions().get(order.symbol_id)
        existing = self.store.get_position_stop(position_key)
        if position is None:
            if existing is not None:
                self.store.deactivate_stop(position_key)
            return
        if order.side.upper() != "BUY":
            if existing is not None:
                self.store.upsert_position_stop(
                    position_key=position_key,
                    symbol_id=order.symbol_id,
                    exchange=order.exchange,
                    quantity=position.quantity,
                    entry_price=float(existing["entry_price"]),
                    stop_price=float(existing["stop_price"]),
                    atr_multiplier=float(existing.get("atr_multiplier") or 0.0),
                    status="ACTIVE",
                    metadata=_json_dict(existing.get("metadata_json")),
                )
            return

        signal = dict(order.metadata or {})
        # Prefer the engine-emitted stop when present so backtest and paper agree.
        engine_stop = _maybe_float(signal.get("initial_stop"))
        engine_method = signal.get("stop_method")
        if engine_stop is not None and engine_stop > 0:
            stop_price = round(engine_stop, 4)
            atr_multiple = _maybe_float(signal.get("atr_multiple")) or 0.0
        else:
            atr = _maybe_float(signal.get("atr_14"))
            atr_multiple = (
                _maybe_float(signal.get("atr_multiple"))
                or _maybe_float(signal.get("exit_atr_multiple"))
                or 2.0
            )
            if atr is None or atr <= 0:
                if existing is not None:
                    self.store.upsert_position_stop(
                        position_key=position_key,
                        symbol_id=order.symbol_id,
                        exchange=order.exchange,
                        quantity=position.quantity,
                        entry_price=position.avg_entry_price,
                        stop_price=float(existing["stop_price"]),
                        atr_multiplier=float(existing.get("atr_multiplier") or 0.0),
                        status="ACTIVE",
                        metadata=_json_dict(existing.get("metadata_json")),
                    )
                return
            stop_price = round(position.avg_entry_price - (atr_multiple * atr), 4)
        self.store.upsert_position_stop(
            position_key=position_key,
            symbol_id=order.symbol_id,
            exchange=order.exchange,
            quantity=position.quantity,
            entry_price=position.avg_entry_price,
            stop_price=stop_price,
            atr_multiplier=atr_multiple,
            status="ACTIVE",
            metadata={
                "signal": signal.get("strategy"),
                "regime": signal.get("regime"),
                "reason": signal.get("reason"),
                "stop_method": engine_method,
                "rank_at_entry": signal.get("rank_at_entry"),
                "score_at_entry": signal.get("score_at_entry"),
                "sector": signal.get("sector"),
                # Streak counters maintained by AutoTrader between ticks.
                "rank_above_threshold_streak": int(signal.get("rank_above_threshold_streak") or 0),
                "score_below_threshold_streak": int(signal.get("score_below_threshold_streak") or 0),
                "bars_held": int(signal.get("bars_held") or 0),
            },
        )

    def check_stop_triggered(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        current_price: float = 0.0,
    ) -> dict:
        position_key = f"{exchange.upper()}:{symbol_id.upper()}"
        stop_record = self.store.get_position_stop(position_key)
        if not stop_record or stop_record.get("status") != "ACTIVE":
            return {"triggered": False, "reason": "no_active_stop"}
        stop_price = float(stop_record.get("stop_price", 0))
        if stop_price <= 0:
            return {"triggered": False, "reason": "invalid_stop_price"}
        if current_price <= 0:
            return {"triggered": False, "reason": "no_market_price"}
        triggered = current_price <= stop_price
        return {
            "triggered": triggered,
            "stop_price": stop_price,
            "current_price": current_price,
            "position_key": position_key,
        }

    def maintain_trailing_stops(
        self,
        *,
        current_prices: Mapping[str, float] | None = None,
        atr_by_symbol: Mapping[str, float] | None = None,
        open_symbols: set[str] | None = None,
    ) -> Dict[str, Any]:
        normalized_prices = {
            str(symbol_id).strip().upper(): float(price)
            for symbol_id, price in (current_prices or {}).items()
            if symbol_id not in (None, "") and _maybe_float(price) is not None
        }
        normalized_atr = {
            str(symbol_id).strip().upper(): float(atr)
            for symbol_id, atr in (atr_by_symbol or {}).items()
            if symbol_id not in (None, "") and _maybe_float(atr) is not None
        }
        normalized_open_symbols = {
            str(symbol_id).strip().upper()
            for symbol_id in (open_symbols or set())
            if symbol_id not in (None, "")
        }

        updated_count = 0
        evaluated_count = 0
        for stop_record in self.store.list_active_stops():
            symbol_id = str(stop_record.get("symbol_id") or "").strip().upper()
            if not symbol_id:
                continue
            if normalized_open_symbols and symbol_id not in normalized_open_symbols:
                continue
            current_price = normalized_prices.get(symbol_id)
            atr = normalized_atr.get(symbol_id)
            if current_price is None or atr is None or atr <= 0:
                continue
            evaluated_count += 1
            existing_stop = float(stop_record.get("stop_price") or 0.0)
            atr_multiplier = float(stop_record.get("atr_multiplier") or 2.0)
            candidate_stop = round(current_price - (atr_multiplier * atr), 4)
            if candidate_stop <= existing_stop:
                continue
            metadata = dict(stop_record.get("metadata") or {})
            metadata.update(
                {
                    "trailing_stop_updated": True,
                    "trailing_reference_price": round(current_price, 4),
                    "trailing_atr_14": round(atr, 4),
                    "prior_stop_price": existing_stop,
                }
            )
            self.store.upsert_position_stop(
                position_key=str(stop_record.get("position_key") or f"{stop_record.get('exchange', 'NSE')}:{symbol_id}"),
                symbol_id=str(stop_record.get("symbol_id") or symbol_id),
                exchange=str(stop_record.get("exchange") or "NSE"),
                quantity=int(stop_record.get("quantity") or 0),
                entry_price=float(stop_record.get("entry_price") or 0.0),
                stop_price=candidate_stop,
                atr_multiplier=atr_multiplier,
                status=str(stop_record.get("status") or "ACTIVE"),
                metadata=metadata,
            )
            updated_count += 1

        return {
            "updated_count": updated_count,
            "evaluated_count": evaluated_count,
        }


def _maybe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_order_result(order: Any, fills: list[Any]) -> Dict[str, Any]:
    return {
        "status": order.status,
        "order": order.to_dict(),
        "fills": [fill.to_dict() for fill in fills],
    }


def _idempotency_payload(intent: OrderIntent) -> dict[str, Any]:
    return {
        "symbol_id": str(intent.symbol_id).strip().upper(),
        "quantity": int(intent.quantity),
        "side": str(intent.side).strip().upper(),
        "exchange": str(intent.exchange).strip().upper(),
        "order_type": str(intent.order_type).strip().upper(),
        "product_type": str(intent.product_type).strip().upper(),
        "validity": str(intent.validity).strip().upper(),
        "limit_price": _maybe_float(intent.limit_price),
        "stop_price": _maybe_float(intent.stop_price),
        "requested_price": _maybe_float(intent.requested_price),
    }


def _intent_from_payload(payload: dict[str, Any], correlation_id: str) -> OrderIntent:
    return OrderIntent(
        symbol_id=str(payload["symbol_id"]),
        quantity=int(payload["quantity"]),
        side=str(payload["side"]),
        exchange=str(payload["exchange"]),
        order_type=str(payload["order_type"]),
        product_type=str(payload["product_type"]),
        validity=str(payload["validity"]),
        limit_price=_maybe_float(payload.get("limit_price")),
        stop_price=_maybe_float(payload.get("stop_price")),
        requested_price=_maybe_float(payload.get("requested_price")),
        correlation_id=correlation_id,
    )


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _intent_matches_order(intent: OrderIntent, order: Any) -> bool:
    """Return whether an idempotent retry carries the original order payload."""
    return (
        str(intent.symbol_id).strip().upper() == str(order.symbol_id).strip().upper()
        and int(intent.quantity) == int(order.quantity)
        and str(intent.side).strip().upper() == str(order.side).strip().upper()
        and str(intent.exchange).strip().upper() == str(order.exchange).strip().upper()
        and str(intent.order_type).strip().upper() == str(order.order_type).strip().upper()
        and str(intent.product_type).strip().upper()
        == str(order.product_type).strip().upper()
        and str(intent.validity).strip().upper() == str(order.validity).strip().upper()
        and _maybe_float(intent.limit_price) == _maybe_float(order.limit_price)
        and _maybe_float(intent.stop_price) == _maybe_float(order.stop_price)
        and _maybe_float(intent.requested_price) == _maybe_float(order.requested_price)
    )
