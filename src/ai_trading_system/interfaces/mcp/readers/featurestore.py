"""Read-only access to the per-symbol Parquet feature store.

``analytics/feature_reader.py`` opens ``ohlcv.duckdb`` read-write to run its
``read_parquet`` scans. Parquet needs no store handle at all, so this reader
scans through an in-memory DuckDB connection instead — it cannot hold a lock on
or write to a live database.

Family and symbol arrive from agent input and become path components, so every
resolved path is checked for containment inside the configured feature store,
mirroring ``FeatureReader._contained_path``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import coerce_date

# The nine technical families the pipeline materializes per symbol.
FEATURE_FAMILIES: tuple[str, ...] = (
    "rsi",
    "adx",
    "sma",
    "ema",
    "macd",
    "atr",
    "bb",
    "roc",
    "supertrend",
)

_KEY_COLUMNS = ("symbol_id", "exchange", "timestamp")


class FeaturePathError(ValueError):
    """A requested family or symbol resolved outside the feature store."""


def resolve_families(families: Iterable[str] | None) -> list[str]:
    """Validate requested family names against the known set."""

    if not families:
        return list(FEATURE_FAMILIES)
    requested = [str(family).strip().lower() for family in families]
    unknown = [family for family in requested if family not in FEATURE_FAMILIES]
    if unknown:
        raise FeaturePathError(
            f"Unknown feature families: {sorted(unknown)}. "
            f"Known families: {list(FEATURE_FAMILIES)}."
        )
    # Preserve the canonical order so output columns are stable.
    return [family for family in FEATURE_FAMILIES if family in set(requested)]


def contained_path(ctx: McpContext, *parts: str) -> Path:
    """Join ``parts`` under the feature store, rejecting any escape."""

    root = ctx.feature_store_dir.expanduser().resolve()
    candidate = root.joinpath(*(str(part) for part in parts)).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise FeaturePathError(
            "Feature path escapes the configured feature store"
        ) from exc
    return candidate


def symbol_feature_path(
    ctx: McpContext, family: str, exchange: str, symbol: str
) -> Path:
    """Path of one symbol's Parquet partition for a feature family."""

    return contained_path(
        ctx,
        str(family).strip().lower(),
        ctx.resolve_exchange(exchange),
        f"{ctx.normalize_symbol(symbol)}.parquet",
    )


def read_symbol_features(
    ctx: McpContext,
    symbol: str,
    *,
    exchange: str = "NSE",
    families: Sequence[str] | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Merge one symbol's feature families into a single wide frame.

    Returns the frame and the list of families that actually contributed;
    missing partitions are skipped so a partial feature store still answers.
    """

    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    selected = resolve_families(families)
    start = coerce_date(from_date)
    end = coerce_date(to_date)

    merged: pd.DataFrame | None = None
    present: list[str] = []

    for family in selected:
        path = symbol_feature_path(ctx, family, exchange_code, symbol_id)
        if not path.exists():
            continue
        frame = pd.read_parquet(path)
        if frame.empty or "timestamp" not in frame.columns:
            continue
        frame = frame.copy()
        frame["timestamp"] = pd.to_datetime(
            frame["timestamp"], errors="coerce"
        ).dt.normalize()
        frame = frame.dropna(subset=["timestamp"])
        if start is not None:
            frame = frame[frame["timestamp"] >= pd.Timestamp(start)]
        if end is not None:
            frame = frame[frame["timestamp"] <= pd.Timestamp(end)]
        if frame.empty:
            continue

        # 'close' and identity columns repeat across families; keep one copy.
        drop = {"date", "close"} if merged is not None else {"date"}
        if merged is not None:
            drop |= {"symbol_id", "exchange"}
        frame = frame[[column for column in frame.columns if column not in drop]]
        frame = frame.drop_duplicates("timestamp", keep="last")

        merged = frame if merged is None else merged.merge(frame, on="timestamp", how="outer")
        present.append(family)

    if merged is None:
        return pd.DataFrame(columns=list(_KEY_COLUMNS)), []

    merged = merged.sort_values("timestamp").reset_index(drop=True)
    if "symbol_id" not in merged.columns:
        merged.insert(0, "symbol_id", symbol_id)
    if "exchange" not in merged.columns:
        merged.insert(1, "exchange", exchange_code)
    return merged, present


def read_latest_cross_section(
    ctx: McpContext,
    family: str,
    *,
    exchange: str = "NSE",
    cutoff: str | date | None = None,
    symbols: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Latest row per symbol for one feature family, at or before ``cutoff``."""

    resolve_families([family])
    directory = contained_path(
        ctx, str(family).strip().lower(), ctx.resolve_exchange(exchange)
    )
    if not directory.is_dir():
        return pd.DataFrame()

    pattern = str(directory / "*.parquet")
    clauses: list[str] = []
    params: list[Any] = [pattern]

    if symbols:
        normalized = [ctx.normalize_symbol(symbol) for symbol in symbols]
        placeholders = ", ".join("?" for _ in normalized)
        clauses.append(f"symbol_id IN ({placeholders})")
        params.extend(normalized)

    cutoff_date = coerce_date(cutoff)
    if cutoff_date is not None:
        clauses.append("CAST(timestamp AS DATE) <= CAST(? AS DATE)")
        params.append(cutoff_date.isoformat())

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT * EXCLUDE (row_number) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY symbol_id ORDER BY timestamp DESC
            ) AS row_number
            FROM read_parquet(?)
            {where}
        ) ranked
        WHERE row_number = 1
    """

    with ctx.parquet() as conn:
        return conn.execute(sql, params).fetchdf()


__all__ = [
    "FEATURE_FAMILIES",
    "FeaturePathError",
    "contained_path",
    "read_latest_cross_section",
    "read_symbol_features",
    "resolve_families",
    "symbol_feature_path",
]
