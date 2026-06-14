"""Small dataframe helpers shared by investigator modules."""

from __future__ import annotations

from typing import Any

import pandas as pd


def symbol_column(frame: pd.DataFrame) -> str | None:
    for column in ("symbol_id", "symbol", "ticker"):
        if column in frame.columns:
            return column
    return None


def as_symbol(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().upper()


def num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(out):
        return default
    return out


def has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def first_existing(row: pd.Series, *columns: str, default: Any = None) -> Any:
    for column in columns:
        if column in row.index and pd.notna(row[column]):
            return row[column]
    return default


def safe_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date
