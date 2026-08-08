"""Typed contracts for broker imports and journal results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

FileType = Literal["tradebook", "holdings"]
SnapshotMode = Literal["reconciliation_only", "opening_anchor"]


@dataclass(frozen=True, slots=True)
class RawRow:
    sheet: str
    row_number: int
    values: dict[str, Any]
    row_hash: str


@dataclass(frozen=True, slots=True)
class ParsedFill:
    symbol: str
    isin: str | None
    trade_date: date
    exchange: str
    segment: str
    series: str
    side: Literal["buy", "sell"]
    auction: bool
    quantity: Decimal
    price: Decimal
    trade_id: str
    order_id: str
    executed_at: datetime
    raw_row_number: int


@dataclass(frozen=True, slots=True)
class ParsedHolding:
    instrument: str
    quantity: Decimal
    average_cost: Decimal
    ltp: Decimal
    invested: Decimal
    current_value: Decimal
    pnl: Decimal
    net_change_pct: Decimal
    day_change_pct: Decimal
    raw_row_number: int


@dataclass(frozen=True, slots=True)
class ParseResult:
    file_type: FileType
    format_version: str
    raw_rows: tuple[RawRow, ...]
    records: tuple[ParsedFill | ParsedHolding, ...]
    metadata: dict[str, Any] = field(default_factory=dict)
    issues: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class ImportResult:
    import_id: str
    status: str
    file_sha256: str
    rows_read: int
    rows_normalized: int
    summary: dict[str, Any]
