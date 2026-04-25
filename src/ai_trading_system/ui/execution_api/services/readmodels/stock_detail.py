"""Read models for the per-stock detail workspace.

Backs:

  * ``GET /api/execution/stocks/{symbol}`` — fundamentals, latest quote,
    ranking position, lifecycle (rank → breakout → pattern → execution).
  * ``GET /api/execution/stocks/{symbol}/ohlcv`` — daily candles + delivery.

All functions take a ``project_root`` and read directly from the operational
data sources (``data/ohlcv.duckdb``, ``data/masterdata.db``, latest rank
artifact frames). They never raise on missing inputs — instead they return
``{"available": False}``-style payloads so the UI can render a degraded
state without 5xx noise.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import duckdb
import pandas as pd

from ai_trading_system.ui.execution_api.services.readmodels.latest_operational_snapshot import (
    ExecutionContext,
    LatestOperationalSnapshot,
    get_execution_context,
    load_latest_operational_snapshot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# The set of symbol-master columns we surface in the metadata block. Filtered
# at query time against the actual table columns so older masterdata schemas
# (e.g. fixtures missing some optional columns) don't produce SQL errors.
_SYMBOL_COLUMNS = (
    "symbol_id",
    "security_id",
    "symbol_name",
    "exchange",
    "instrument_type",
    "isin",
    "lot_size",
    "tick_size",
    "sector",
    "industry",
    "nse_symbol",
    "bse_symbol",
    "mcap",
    "last_updated",
)


def _existing_symbol_columns(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("PRAGMA table_info(symbols)").fetchall()
    available = {row[1] for row in rows}
    return [col for col in _SYMBOL_COLUMNS if col in available]


def _scalar_or_none(value: Any) -> Any:
    """Coerce numpy / pandas NA-likes to plain Python or ``None``."""

    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            return value
    return value


def _isoformat(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    try:
        return pd.Timestamp(value).isoformat()
    except Exception:
        return str(value)


def _frame_row_for_symbol(frame: pd.DataFrame, symbol: str) -> Optional[dict[str, Any]]:
    """Return the first row of ``frame`` whose ``symbol_id`` matches ``symbol``."""

    if frame is None or frame.empty or "symbol_id" not in frame.columns:
        return None
    matches = frame.loc[frame["symbol_id"].astype(str) == symbol]
    if matches.empty:
        return None
    record = matches.iloc[0].to_dict()
    return {k: _scalar_or_none(v) for k, v in record.items()}


def _rank_position(frame: pd.DataFrame, symbol: str) -> Optional[int]:
    """1-based rank position of ``symbol`` in ``ranked_signals`` (None if absent)."""

    if frame is None or frame.empty or "symbol_id" not in frame.columns:
        return None
    matches = frame.index[frame["symbol_id"].astype(str) == symbol].tolist()
    if not matches:
        return None
    # ``ranked_signals`` is ordered by composite_score descending in the
    # producer; row position == rank.
    return int(matches[0]) + 1


def _lifecycle(
    *,
    rank_pos: Optional[int],
    universe_size: int,
    breakout_row: Optional[dict[str, Any]],
    pattern_row: Optional[dict[str, Any]],
    stock_scan_row: Optional[dict[str, Any]],
) -> dict[str, str]:
    """Derive the four-stage lifecycle chip values shown in the Canvas UI."""

    if rank_pos is None:
        rank_label = "OUT"
    elif rank_pos <= 5:
        rank_label = "TOP 5"
    elif rank_pos <= 25:
        rank_label = "TOP 25"
    elif rank_pos <= max(universe_size, 1) // 2:
        rank_label = "MID TIER"
    else:
        rank_label = "LOWER TIER"

    breakout_label = "CONFIRMED" if breakout_row is not None else "NONE"
    if pattern_row is not None:
        pattern_label = str(
            pattern_row.get("pattern_type")
            or pattern_row.get("pattern")
            or "DETECTED"
        ).upper()
    else:
        pattern_label = "NONE"

    category = (stock_scan_row or {}).get("category")
    category_str = str(category).upper() if category is not None else ""
    if category_str.startswith("BUY"):
        execution_label = "ELIGIBLE"
    elif category_str.startswith("WATCH"):
        execution_label = "WATCHLIST"
    elif category_str.startswith("BLOCK") or category_str.startswith("REJECT"):
        execution_label = "BLOCKED"
    elif rank_pos is not None and rank_pos <= 25:
        execution_label = "WATCHLIST"
    else:
        execution_label = "OUT"

    return {
        "rank": rank_label,
        "breakout": breakout_label,
        "pattern": pattern_label,
        "execution": execution_label,
    }


# ---------------------------------------------------------------------------
# /stocks/{symbol}
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _StockDataSources:
    ctx: ExecutionContext
    snapshot: LatestOperationalSnapshot


def _load_sources(
    project_root: str | Path | None,
    snapshot: Optional[LatestOperationalSnapshot] = None,
) -> _StockDataSources:
    ctx = get_execution_context(project_root)
    snap = snapshot or load_latest_operational_snapshot(project_root)
    return _StockDataSources(ctx=ctx, snapshot=snap)


def _load_symbol_metadata(ctx: ExecutionContext, symbol: str) -> Optional[dict[str, Any]]:
    if not ctx.master_db.exists():
        return None
    conn = sqlite3.connect(ctx.master_db.as_posix())
    try:
        # PRAGMA gives us the actual column set so we don't have to maintain
        # parallel knowledge about which optional fields a given env has.
        try:
            cols = _existing_symbol_columns(conn)
        except sqlite3.DatabaseError:
            return None
        if not cols:
            return None
        select_clause = ", ".join(cols)
        row = conn.execute(
            f"SELECT {select_clause} FROM symbols WHERE symbol_id = ? LIMIT 1",
            (symbol,),
        ).fetchone()
        if row is None:
            return None
        return {col: _scalar_or_none(value) for col, value in zip(cols, row)}
    finally:
        conn.close()


def _load_latest_quote(ctx: ExecutionContext, symbol: str) -> Optional[dict[str, Any]]:
    if not ctx.ohlcv_db.exists():
        return None
    conn = duckdb.connect(ctx.ohlcv_db.as_posix(), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT
                c.timestamp,
                c.open, c.high, c.low, c.close, c.volume,
                d.delivery_pct
            FROM _catalog c
            LEFT JOIN _delivery d
              ON d.symbol_id = c.symbol_id
             AND d.exchange = c.exchange
             AND d.timestamp = c.timestamp
            WHERE c.symbol_id = ?
            ORDER BY c.timestamp DESC
            LIMIT 1
            """,
            [symbol],
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    timestamp, open_, high, low, close, volume, delivery_pct = row
    return {
        "timestamp": _isoformat(timestamp),
        "open": _scalar_or_none(open_),
        "high": _scalar_or_none(high),
        "low": _scalar_or_none(low),
        "close": _scalar_or_none(close),
        "volume": _scalar_or_none(volume),
        "delivery_pct": _scalar_or_none(delivery_pct),
    }


def get_stock_detail(
    project_root: str | Path | None,
    symbol: str,
    *,
    snapshot: Optional[LatestOperationalSnapshot] = None,
) -> dict[str, Any]:
    """Return the consolidated detail payload for ``symbol``.

    The shape is intentionally permissive — every block can independently
    be ``None`` if its underlying source is missing. The top-level
    ``available`` is ``True`` whenever *any* block is populated, so the UI
    can still render partial views (e.g. show fundamentals when the rank
    pipeline hasn't run yet).
    """

    sources = _load_sources(project_root, snapshot=snapshot)
    ctx = sources.ctx
    snap = sources.snapshot

    metadata = _load_symbol_metadata(ctx, symbol)
    latest_quote = _load_latest_quote(ctx, symbol)

    ranked = snap.frames.get("ranked_signals", pd.DataFrame())
    breakouts = snap.frames.get("breakout_scan", pd.DataFrame())
    patterns = snap.frames.get("pattern_scan", pd.DataFrame())
    stock_scan = snap.frames.get("stock_scan", pd.DataFrame())

    rank_pos = _rank_position(ranked, symbol)
    rank_row = _frame_row_for_symbol(ranked, symbol)
    breakout_row = _frame_row_for_symbol(breakouts, symbol)
    pattern_row = _frame_row_for_symbol(patterns, symbol)
    scan_row = _frame_row_for_symbol(stock_scan, symbol)

    universe_size = int(len(ranked.index)) if ranked is not None else 0

    ranking_block: Optional[dict[str, Any]]
    if rank_row is not None or scan_row is not None:
        ranking_block = {
            "rank_position": rank_pos,
            "universe_size": universe_size,
            "composite_score": _scalar_or_none(
                (rank_row or {}).get("composite_score")
            ),
            "sector_name": _scalar_or_none((rank_row or {}).get("sector_name")),
            "category": _scalar_or_none((scan_row or {}).get("category")),
            "in_breakout_scan": breakout_row is not None,
            "in_pattern_scan": pattern_row is not None,
        }
    else:
        ranking_block = None

    lifecycle = _lifecycle(
        rank_pos=rank_pos,
        universe_size=universe_size,
        breakout_row=breakout_row,
        pattern_row=pattern_row,
        stock_scan_row=scan_row,
    )

    available = any(
        block is not None for block in (metadata, latest_quote, ranking_block)
    )

    return {
        "available": available,
        "symbol": symbol,
        "metadata": metadata,
        "latest_quote": latest_quote,
        "ranking": ranking_block,
        "lifecycle": lifecycle,
    }


# ---------------------------------------------------------------------------
# /stocks/{symbol}/ohlcv
# ---------------------------------------------------------------------------


def get_stock_ohlcv(
    project_root: str | Path | None,
    symbol: str,
    *,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    interval: str = "daily",
    limit: Optional[int] = None,
) -> dict[str, Any]:
    """Return daily OHLCV + delivery candles for ``symbol``.

    Filters:

      * ``from_date``, ``to_date`` — inclusive ISO date strings (``YYYY-MM-DD``).
        Invalid values are silently dropped so the endpoint never returns 4xx
        on parameter shape (the UI just gets the unfiltered series back).
      * ``interval`` — only ``"daily"`` is supported today; any other value
        flows through and surfaces in the response so the UI can warn.
      * ``limit`` — caps the number of candles returned (most-recent first
        on the wire, but candles inside ``candles`` stay in ascending time
        order for chart-friendliness).
    """

    ctx = get_execution_context(project_root)
    if not ctx.ohlcv_db.exists():
        return {
            "available": False,
            "symbol": symbol,
            "interval": interval,
            "candles": [],
        }

    where_clauses = ["c.symbol_id = ?"]
    params: list[Any] = [symbol]

    def _try_parse(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        try:
            return pd.Timestamp(value).date().isoformat()
        except Exception:
            return None

    parsed_from = _try_parse(from_date)
    parsed_to = _try_parse(to_date)
    if parsed_from is not None:
        # ``CAST(? AS DATE)`` survives DuckDB's strict timestamp/varchar
        # binder check while still letting the parameter come over the wire
        # as a string.
        where_clauses.append("c.timestamp >= CAST(? AS DATE)")
        params.append(parsed_from)
    if parsed_to is not None:
        # Inclusive end-of-day: cast to DATE then add a day so the timestamp
        # comparison includes anything stamped on ``parsed_to``.
        where_clauses.append("c.timestamp < CAST(? AS DATE) + INTERVAL 1 DAY")
        params.append(parsed_to)

    where_sql = " AND ".join(where_clauses)
    sql = f"""
        SELECT
            c.timestamp, c.open, c.high, c.low, c.close, c.volume,
            d.delivery_pct
        FROM _catalog c
        LEFT JOIN _delivery d
          ON d.symbol_id = c.symbol_id
         AND d.exchange = c.exchange
         AND d.timestamp = c.timestamp
        WHERE {where_sql}
        ORDER BY c.timestamp ASC
    """

    conn = duckdb.connect(ctx.ohlcv_db.as_posix(), read_only=True)
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    candles: list[dict[str, Any]] = []
    for ts, open_, high, low, close, volume, delivery_pct in rows:
        candles.append(
            {
                "timestamp": _isoformat(ts),
                "open": _scalar_or_none(open_),
                "high": _scalar_or_none(high),
                "low": _scalar_or_none(low),
                "close": _scalar_or_none(close),
                "volume": _scalar_or_none(volume),
                "delivery_pct": _scalar_or_none(delivery_pct),
            }
        )

    if limit is not None and limit >= 0 and len(candles) > limit:
        # Keep the most recent ``limit`` rows but preserve ascending order.
        candles = candles[-limit:]

    return {
        "available": True,
        "symbol": symbol,
        "interval": interval,
        "from": parsed_from,
        "to": parsed_to,
        "count": len(candles),
        "candles": candles,
    }


__all__ = [
    "get_stock_detail",
    "get_stock_ohlcv",
]
