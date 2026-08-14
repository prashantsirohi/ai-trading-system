"""Symbol identity resolution.

Deliberately returns *candidates* rather than a single answer. Identity in this
system is ``(symbol_id, exchange)``, and a name or ISIN can legitimately match
more than one listing. Collapsing that to one row would silently pick an
exchange on the agent's behalf, which is how a BSE listing ends up answering an
NSE question.
"""

from __future__ import annotations

from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_LATEST,
    AS_OF_UNSUPPORTED,
    clamp_limit,
    envelope,
    json_safe,
)
from ai_trading_system.interfaces.mcp.readers import master


def resolve_symbol(
    ctx: McpContext,
    query: str,
    *,
    as_of: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Resolve a symbol, name, ISIN or security id to master candidates.

    The symbol master is a current-state table with no history, so it cannot
    answer "what was this symbol called in January". A request with ``as_of``
    therefore returns no rows rather than presenting today's master as though
    it were historical.
    """

    row_limit = clamp_limit(limit, default=10, maximum=100)

    if as_of is not None:
        return envelope(
            [],
            source=ctx.store_label(ctx.master_db, master.TABLE),
            as_of_status=AS_OF_UNSUPPORTED,
            as_of_requested=as_of,
            notes=[
                "The symbol master stores current state only and has no "
                "effective-dated history, so it cannot be read as of a past "
                "date. Re-run without 'as_of' to resolve against the current "
                "master."
            ],
            query=str(query or "").strip(),
            ambiguous=False,
            candidate_count=0,
        )

    candidates = [
        {key: json_safe(value) for key, value in row.items()}
        for row in master.search_symbols(ctx, query, limit=row_limit)
    ]

    best_tier = candidates[0]["match_type"] if candidates else None
    at_best_tier = [row for row in candidates if row["match_type"] == best_tier]
    ambiguous = len(at_best_tier) > 1

    notes: list[str] = []
    if not candidates:
        notes.append(
            f"No master row matches {str(query or '').strip()!r}. Try the ISIN, "
            "the security id, or part of the company name."
        )
    elif ambiguous:
        listings = ", ".join(
            f"{row['symbol_id']}@{row.get('exchange')}" for row in at_best_tier
        )
        notes.append(
            f"{len(at_best_tier)} listings match equally well ({listings}). "
            "Pass an explicit 'exchange' to the follow-up tool."
        )

    return envelope(
        candidates,
        source=ctx.store_label(ctx.master_db, master.TABLE),
        as_of_status=AS_OF_LATEST,
        notes=notes,
        query=str(query or "").strip(),
        ambiguous=ambiguous,
        candidate_count=len(candidates),
        best_match_type=best_tier,
        data_domain=ctx.paths.domain,
    )


__all__ = ["resolve_symbol"]
