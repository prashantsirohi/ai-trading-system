"""Detect broad raw-close discontinuities before trusted data is written."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


DEFAULT_BULK_RAW_GAP_PCT = 30.0
DEFAULT_BULK_RAW_GAP_SYMBOL_COUNT = 10


@dataclass(frozen=True)
class BulkRawPriceBasisShift:
    """One date with a suspicious number of simultaneous raw-close gaps."""

    trade_date: str
    symbols: tuple[str, ...]
    median_abs_pct_change: float
    max_abs_pct_change: float

    @property
    def symbol_count(self) -> int:
        return len(self.symbols)

    def to_dict(self) -> dict[str, object]:
        return {
            "trade_date": self.trade_date,
            "symbol_count": self.symbol_count,
            "symbols": list(self.symbols),
            "median_abs_pct_change": self.median_abs_pct_change,
            "max_abs_pct_change": self.max_abs_pct_change,
        }


class BulkRawPriceBasisShiftError(RuntimeError):
    """Raised before a write that would introduce a broad price-basis shift."""

    def __init__(self, operation: str, shifts: list[BulkRawPriceBasisShift]):
        self.operation = operation
        self.shifts = tuple(shifts)
        details = "; ".join(
            f"{shift.trade_date} ({shift.symbol_count} symbols)" for shift in shifts
        )
        super().__init__(
            f"{operation} rejected before write: broad raw-price basis shift detected on {details}."
        )


def detect_bulk_raw_price_basis_shifts(
    rows: pd.DataFrame,
    *,
    gap_pct: float = DEFAULT_BULK_RAW_GAP_PCT,
    symbol_count: int = DEFAULT_BULK_RAW_GAP_SYMBOL_COUNT,
) -> list[BulkRawPriceBasisShift]:
    """Return dates where at least ``symbol_count`` symbols gap by ``gap_pct``.

    ``rows`` must contain ``symbol_id``, ``close``, and either ``timestamp`` or
    ``trade_date``. The caller is responsible for including the immediately
    adjacent retained observations when validating a replacement window.
    """

    if rows is None or rows.empty:
        return []
    date_column = "trade_date" if "trade_date" in rows.columns else "timestamp"
    required = {"symbol_id", "close", date_column}
    missing = required.difference(rows.columns)
    if missing:
        raise ValueError(f"Price-continuity rows missing columns: {sorted(missing)}")

    frame = rows.loc[:, ["symbol_id", date_column, "close"]].copy()
    frame["symbol_id"] = frame["symbol_id"].astype(str)
    frame["trade_date"] = pd.to_datetime(frame[date_column], errors="coerce").dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["symbol_id", "trade_date", "close"])
    frame = frame.sort_values(["symbol_id", "trade_date"]).drop_duplicates(
        ["symbol_id", "trade_date"], keep="last"
    )
    frame["prev_close"] = frame.groupby("symbol_id", sort=False)["close"].shift(1)
    eligible = frame[frame["prev_close"].notna() & frame["prev_close"].ne(0)].copy()
    eligible["abs_pct_change"] = (
        (eligible["close"] / eligible["prev_close"] - 1.0) * 100.0
    ).abs()
    gaps = eligible[eligible["abs_pct_change"] >= float(gap_pct)]

    shifts: list[BulkRawPriceBasisShift] = []
    for trade_date, group in gaps.groupby("trade_date", sort=True):
        symbols = tuple(sorted(set(group["symbol_id"].astype(str))))
        if len(symbols) < int(symbol_count):
            continue
        shifts.append(
            BulkRawPriceBasisShift(
                trade_date=pd.Timestamp(trade_date).date().isoformat(),
                symbols=symbols,
                median_abs_pct_change=round(float(group["abs_pct_change"].median()), 4),
                max_abs_pct_change=round(float(group["abs_pct_change"].max()), 4),
            )
        )
    return shifts


def require_no_bulk_raw_price_basis_shifts(
    rows: pd.DataFrame,
    *,
    operation: str,
    gap_pct: float = DEFAULT_BULK_RAW_GAP_PCT,
    symbol_count: int = DEFAULT_BULK_RAW_GAP_SYMBOL_COUNT,
) -> None:
    """Reject a proposed write when it contains a broad basis discontinuity."""

    shifts = detect_bulk_raw_price_basis_shifts(
        rows,
        gap_pct=gap_pct,
        symbol_count=symbol_count,
    )
    if shifts:
        raise BulkRawPriceBasisShiftError(operation, shifts)
