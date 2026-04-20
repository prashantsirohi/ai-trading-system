"""Portfolio state derived from persisted execution fills."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List

from ai_trading_system.domains.execution.store import ExecutionStore

POSITION_STATES = [
    "candidate",
    "active",
    "partial",
    "exit",
]


def open_position_trade_ref(symbol_id: str, exchange: str = "NSE") -> str:
    """Stable journal reference for an active position."""
    return f"open:{str(exchange or 'NSE').upper()}:{str(symbol_id or '').upper()}"


def closed_trade_ref(fill_id: str) -> str:
    """Stable journal reference for a realized trade row."""
    return f"closed:{str(fill_id or '').strip()}"


def check_portfolio_constraints(
    candidate: dict,
    portfolio_state: dict,
    *,
    max_positions: int = 10,
    max_sector_exposure: float = 0.20,
    max_single_stock_weight: float = 0.10,
) -> dict:
    """Evaluate basic portfolio limits for a candidate order."""
    reasons = []

    if int(portfolio_state.get("open_positions_count", 0) or 0) >= int(max_positions):
        reasons.append("max_positions_reached")

    candidate_sector = str(candidate.get("sector_name") or candidate.get("sector") or "").strip()
    if candidate_sector:
        sector_exposure = float((portfolio_state.get("sector_exposure") or {}).get(candidate_sector, 0.0) or 0.0)
        if sector_exposure > float(max_sector_exposure):
            reasons.append("max_sector_exposure_exceeded")

    symbol = str(candidate.get("symbol_id") or "").strip()
    if symbol:
        single_weight = float((portfolio_state.get("symbol_weights") or {}).get(symbol, 0.0) or 0.0)
        if single_weight > float(max_single_stock_weight):
            reasons.append("max_single_stock_weight_exceeded")

    return {
        "allowed": len(reasons) == 0,
        "reasons": reasons,
    }


@dataclass(slots=True)
class PositionSnapshot:
    """Current long position state for a symbol."""

    symbol_id: str
    exchange: str
    quantity: int
    avg_entry_price: float
    last_fill_price: float

    def to_dict(self) -> dict:
        return asdict(self)


class PortfolioManager:
    """Derive current open positions from the execution ledger."""

    def __init__(self, store: ExecutionStore):
        self.store = store

    def open_positions(self) -> Dict[str, PositionSnapshot]:
        fills = sorted(
            self.store.list_fills(),
            key=lambda row: (row.get("filled_at") or "", row.get("fill_id") or ""),
        )
        state: Dict[tuple[str, str], dict] = {}
        for fill in fills:
            key = (str(fill["symbol_id"]), str(fill.get("exchange", "NSE")))
            snapshot = state.setdefault(
                key,
                {
                    "symbol_id": key[0],
                    "exchange": key[1],
                    "quantity": 0,
                    "avg_entry_price": 0.0,
                    "last_fill_price": 0.0,
                },
            )
            quantity = int(fill.get("quantity") or 0)
            price = float(fill.get("price") or 0.0)
            side = str(fill.get("side", "BUY")).upper()
            if side == "BUY":
                existing_qty = int(snapshot["quantity"])
                new_qty = existing_qty + quantity
                if new_qty > 0:
                    snapshot["avg_entry_price"] = (
                        (existing_qty * float(snapshot["avg_entry_price"])) + (quantity * price)
                    ) / new_qty
                snapshot["quantity"] = new_qty
            else:
                snapshot["quantity"] = max(0, int(snapshot["quantity"]) - quantity)
                if int(snapshot["quantity"]) == 0:
                    snapshot["avg_entry_price"] = 0.0
            snapshot["last_fill_price"] = price

        positions: Dict[str, PositionSnapshot] = {}
        for snapshot in state.values():
            if int(snapshot["quantity"]) <= 0:
                continue
            position = PositionSnapshot(
                symbol_id=str(snapshot["symbol_id"]),
                exchange=str(snapshot["exchange"]),
                quantity=int(snapshot["quantity"]),
                avg_entry_price=round(float(snapshot["avg_entry_price"]), 4),
                last_fill_price=round(float(snapshot["last_fill_price"]), 4),
            )
            positions[position.symbol_id] = position
        return positions

    def check_heat_gate(
        self,
        positions: Dict[str, PositionSnapshot],
        capital: float,
        threshold: float = 0.15,
    ) -> tuple[bool, float]:
        if not positions:
            return True, 0.0
        active_stops = {
            f"{str(row.get('exchange') or 'NSE').upper()}:{str(row.get('symbol_id') or '').upper()}": row
            for row in self.store.list_active_stops()
        }
        total_risk = 0.0
        for pos in positions.values():
            position_key = f"{str(pos.exchange or 'NSE').upper()}:{str(pos.symbol_id or '').upper()}"
            total_risk += _estimate_position_risk(
                position=pos,
                stop_record=active_stops.get(position_key),
            )
        risk_pct = total_risk / capital if capital > 0 else 0.0
        return risk_pct <= threshold, round(risk_pct, 4)

    def open_positions_frame(self) -> List[dict]:
        rows: list[dict] = []
        for position in self.open_positions().values():
            payload = position.to_dict()
            payload["trade_ref"] = open_position_trade_ref(position.symbol_id, position.exchange)
            rows.append(payload)
        return rows


def _estimate_position_risk(
    *,
    position: PositionSnapshot,
    stop_record: dict | None,
    fallback_risk_pct: float = 0.10,
) -> float:
    if stop_record and str(stop_record.get("status") or "").upper() == "ACTIVE":
        entry_price = float(stop_record.get("entry_price") or position.avg_entry_price or 0.0)
        stop_price = float(stop_record.get("stop_price") or 0.0)
        stop_quantity = int(stop_record.get("quantity") or 0)
        quantity = stop_quantity if stop_quantity > 0 else int(position.quantity)
        stop_distance = max(0.0, entry_price - stop_price)
        return stop_distance * max(quantity, 0)

    fallback_distance = max(0.0, float(position.avg_entry_price) * float(fallback_risk_pct))
    return fallback_distance * max(int(position.quantity), 0)
