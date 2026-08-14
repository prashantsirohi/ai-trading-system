"""One-call symbol overview, composed from the point-in-time tools.

Deliberately *not* built on ``readmodels/stock_detail.py::get_stock_detail``.
That helper resolves symbol aliases over a read-write SQLite handle, fetches
the latest quote by ``symbol_id`` alone with no ``exchange`` and no cutoff, and
blends latest CSV artifacts with persisted history inside one payload — so a
historical or BSE profile could mix the wrong listing, an unadjusted price, and
facts carrying different dates.

Here every block is produced by the MCP's own reader under one shared
``as_of``, keeps its own ``as_of_effective``/``as_of_status``/``source``, and is
left empty rather than filled from a different date. ``alignment`` then reports
how far apart the blocks actually are, using the same vocabulary as
``DecisionOperatorReadService.data_freshness_status``.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
    coerce_date,
    envelope,
)
from ai_trading_system.interfaces.mcp.readers import master
from ai_trading_system.interfaces.mcp.tools import fundamentals as fundamentals_tool
from ai_trading_system.interfaces.mcp.tools import prices as prices_tool
from ai_trading_system.interfaces.mcp.tools import rank as rank_tool
from ai_trading_system.interfaces.mcp.tools import stage as stage_tool

ALIGNED = "ALIGNED"
PARTIALLY_STALE = "PARTIALLY_STALE"
STALE = "STALE"
INCOMPLETE = "INCOMPLETE"

# Blocks expected to carry a date; identity has none by nature.
_DATED_BLOCKS = ("quote", "stage", "rank", "fundamentals")


def _has_content(value: Any) -> bool:
    """True when a block carries actual evidence.

    A block such as ``fundamentals`` is a dict of sub-blocks that are all
    ``None`` when nothing was published yet — truthy as a container, empty as
    an answer. Truthiness alone would report a historical profile as answered.
    """

    if value is None:
        return False
    if isinstance(value, dict):
        return any(_has_content(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_has_content(item) for item in value)
    return True


def _alignment(
    block_dates: dict[str, date | None], *, tolerance_days: int = 1
) -> tuple[str, int | None]:
    """Classify how far apart the dated blocks are."""

    observed = [value for value in block_dates.values() if value is not None]
    if len(observed) < len(_DATED_BLOCKS):
        return INCOMPLETE, (
            (max(observed) - min(observed)).days if len(observed) > 1 else None
        )
    spread = (max(observed) - min(observed)).days
    if spread == 0:
        return ALIGNED, 0
    if spread <= tolerance_days:
        return PARTIALLY_STALE, spread
    return STALE, spread


def get_symbol_profile(
    ctx: McpContext,
    symbol: str,
    *,
    exchange: str = "NSE",
    as_of: str | date | None = None,
    statement_basis: str = "standalone",
) -> dict[str, Any]:
    """Return identity, quote, stage, rank and fundamentals for one symbol."""

    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)

    identity = master.get_symbol_record(ctx, symbol_id, exchange_code)

    quote = prices_tool.get_ohlcv(
        ctx, symbol_id, exchange=exchange_code, as_of=as_of, limit=1
    )
    stage = stage_tool.get_stage_history(
        ctx, symbol_id, exchange=exchange_code, as_of=as_of, limit=1
    )
    rank = rank_tool.get_rank_detail(
        ctx, symbol_id, exchange=exchange_code, as_of=as_of
    )
    fundamentals = fundamentals_tool.get_fundamentals(
        ctx, symbol_id, statement_basis=statement_basis, as_of=as_of
    )

    blocks: dict[str, Any] = {
        "identity": identity,
        "quote": quote["data"][0] if quote["data"] else None,
        "stage": stage["data"][-1] if stage["data"] else None,
        "rank": rank["data"],
        "fundamentals": (
            fundamentals["data"] if _has_content(fundamentals["data"]) else None
        ),
    }

    sources = {
        "quote": quote["meta"],
        "stage": stage["meta"],
        "rank": rank["meta"],
        "fundamentals": fundamentals["meta"],
    }
    block_meta = {
        name: {
            "as_of_status": meta["as_of_status"],
            "as_of_effective": meta["as_of_effective"],
            "source": meta["source"],
            "notes": meta["notes"],
        }
        for name, meta in sources.items()
    }
    block_meta["identity"] = {
        "as_of_status": "LATEST",
        "as_of_effective": None,
        "source": ctx.store_label(ctx.master_db, master.TABLE),
        "notes": []
        if identity
        else [
            f"No master row for {symbol_id} on {exchange_code}; "
            "resolve_symbol will show the available listings."
        ],
    }

    block_dates = {
        name: coerce_date(sources[name]["as_of_effective"]) for name in _DATED_BLOCKS
    }
    alignment, spread = _alignment(block_dates)

    # Identity is deliberately excluded: the symbol master is current-state
    # with no history, so a populated identity block says nothing about
    # whether the requested date can be answered.
    populated = [name for name in _DATED_BLOCKS if _has_content(blocks[name])]

    notes: list[str] = []
    if not populated:
        notes.append(
            f"No dated evidence exists for {symbol_id} on {exchange_code} at "
            "the requested date. The identity block reflects the current "
            "symbol master, which has no history."
        )
    empty_dated = [name for name in _DATED_BLOCKS if block_dates[name] is None]
    if empty_dated and populated:
        notes.append(
            "These blocks have no data at the requested date and were left "
            f"empty rather than filled from another date: {empty_dated}."
        )
    if alignment == STALE:
        notes.append(
            f"Block dates span {spread} days; treat cross-block comparisons "
            "with care."
        )

    effective = max(
        (value for value in block_dates.values() if value is not None), default=None
    )

    if as_of is None:
        status = AS_OF_LATEST
    elif populated:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    return envelope(
        blocks,
        source="composed",
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        notes=notes,
        symbol=symbol_id,
        exchange=exchange_code,
        blocks=block_meta,
        alignment=alignment,
        max_block_spread_days=spread,
        statement_basis=statement_basis,
        data_domain=ctx.paths.domain,
    )


__all__ = [
    "ALIGNED",
    "INCOMPLETE",
    "PARTIALLY_STALE",
    "STALE",
    "get_symbol_profile",
]
