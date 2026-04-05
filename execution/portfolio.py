"""Portfolio state derived from persisted execution fills."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List

from execution.store import ExecutionStore


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
        return [position.to_dict() for position in self.open_positions().values()]
