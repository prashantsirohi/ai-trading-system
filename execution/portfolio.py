"""Portfolio state derived from persisted execution fills."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List

from execution.store import ExecutionStore

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
    max_sector_exposure: float = 0.30,
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

    def open_positions_frame(self) -> List[dict]:
        rows: list[dict] = []
        for position in self.open_positions().values():
            payload = position.to_dict()
            payload["trade_ref"] = open_position_trade_ref(position.symbol_id, position.exchange)
            rows.append(payload)
        return rows
