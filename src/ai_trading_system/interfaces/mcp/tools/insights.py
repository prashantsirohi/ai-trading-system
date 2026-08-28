"""Bounded composed explanations and market summaries for MCP agents."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext, StoreBusyError
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
    coerce_date,
    envelope,
    json_safe,
)
from ai_trading_system.interfaces.mcp.readers import decisions, master
from ai_trading_system.interfaces.mcp.tools import fundamentals as fundamentals_tool
from ai_trading_system.interfaces.mcp.tools import (
    fundamental_discovery as fundamental_discovery_tool,
)
from ai_trading_system.interfaces.mcp.tools import governance as governance_tool
from ai_trading_system.interfaces.mcp.tools import lifecycle as lifecycle_tool
from ai_trading_system.interfaces.mcp.tools import patterns as patterns_tool
from ai_trading_system.interfaces.mcp.tools import profile as profile_tool
from ai_trading_system.interfaces.mcp.tools import screen as screen_tool
from ai_trading_system.interfaces.mcp.tools import (
    sector_leadership as sector_leadership_tool,
)
from ai_trading_system.interfaces.mcp.tools import stage as stage_tool

MAX_COMPARE_SYMBOLS = 10
SOURCE = "composed MCP evidence"


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return [value]
        return parsed if isinstance(parsed, list) else [parsed]
    return [value]


def _has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        return any(_has_content(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_has_content(item) for item in value)
    return True


def _rank_rows(
    ctx: McpContext,
    *,
    symbol: str,
    exchange: str,
    universe_id: str,
    as_of: str | date | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[str]]:
    notes: list[str] = []
    rows: list[dict[str, Any] | None] = []
    with ctx.control_plane() as conn:
        for table in (decisions.RANK_UNIVERSE_TABLE, decisions.RANK_TABLE):
            try:
                row = decisions.latest_row(
                    conn,
                    table,
                    symbol_id=symbol,
                    exchange=exchange,
                    as_of=as_of,
                    extra_clauses=("universe_id = ?",),
                    extra_params=(universe_id,),
                )
            except decisions.DecisionVersionUnavailable as exc:
                row = None
                notes.append(f"{table} could not be version-pinned: {exc}")
            rows.append(row)
    return rows[0], rows[1], notes


def _selection_explanation(
    full_rank: dict[str, Any] | None,
    shortlist_rank: dict[str, Any] | None,
) -> dict[str, Any]:
    if full_rank is None:
        return {
            "in_full_universe": False,
            "in_shortlist": False,
            "reasons": ["NO_FULL_UNIVERSE_RANK_EVIDENCE"],
        }

    full_date = coerce_date(full_rank.get("trade_date"))
    shortlist_date = coerce_date((shortlist_rank or {}).get("trade_date"))
    in_shortlist = shortlist_rank is not None and shortlist_date == full_date
    reasons = [str(value) for value in _as_list(full_rank.get("rejection_reasons"))]

    if (
        json_safe(full_rank.get("rank_eligible")) is False
        and "RANK_INELIGIBLE" not in reasons
    ):
        reasons.append("RANK_INELIGIBLE")
    rank_position = full_rank.get("rank_position")
    effective_top_n = full_rank.get("effective_top_n")
    if (
        not in_shortlist
        and rank_position is not None
        and effective_top_n is not None
        and int(rank_position) > int(effective_top_n)
    ):
        reasons.append("BELOW_EFFECTIVE_TOP_N")
    score = full_rank.get("composite_score")
    minimum = full_rank.get("effective_min_score")
    if (
        not in_shortlist
        and score is not None
        and minimum is not None
        and float(score) < float(minimum)
    ):
        reasons.append("BELOW_EFFECTIVE_MIN_SCORE")
    if not in_shortlist and not reasons:
        reasons.append("NOT_SELECTED_BY_RECORDED_POLICY")

    return {
        "in_full_universe": True,
        "in_shortlist": in_shortlist,
        "reasons": list(dict.fromkeys(reasons)),
    }


def _profile_evidence(
    ctx: McpContext,
    *,
    symbol: str,
    exchange: str,
    as_of: str | date | None,
    statement_basis: str,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Use the normal profile, but retain non-price evidence during a writer lock."""

    try:
        profile = profile_tool.get_symbol_profile(
            ctx,
            symbol,
            exchange=exchange,
            as_of=as_of,
            statement_basis=statement_basis,
        )
        return profile["data"], profile["meta"], []
    except StoreBusyError as exc:
        stage = stage_tool.get_stage_history(
            ctx, symbol, exchange=exchange, as_of=as_of, limit=1
        )
        pattern = patterns_tool.get_pattern_detail(
            ctx, symbol, exchange=exchange, as_of=as_of
        )
        fundamentals = fundamentals_tool.get_fundamentals(
            ctx, symbol, statement_basis=statement_basis, as_of=as_of
        )
        thesis = fundamental_discovery_tool.get_fundamental_thesis(
            ctx, symbol, exchange=exchange, as_of=as_of
        )
        blocks = {
            "identity": master.get_symbol_record(ctx, symbol, exchange),
            "quote": None,
            "stage": stage["data"][-1] if stage["data"] else None,
            "rank": None,
            "pattern": pattern["data"] or None,
            "fundamentals": (
                fundamentals["data"] if _has_content(fundamentals["data"]) else None
            ),
            "fundamental_thesis": thesis["data"],
        }
        observed = [
            value
            for value in (
                coerce_date(stage["meta"]["as_of_effective"]),
                coerce_date(pattern["meta"]["as_of_effective"]),
                coerce_date(fundamentals["meta"]["as_of_effective"]),
                coerce_date(thesis["meta"]["as_of_effective"]),
            )
            if value is not None
        ]
        spread = (max(observed) - min(observed)).days if len(observed) > 1 else None
        meta = {
            "alignment": "INCOMPLETE",
            "max_block_spread_days": spread,
            "as_of_effective": max(observed, default=None),
        }
        return (
            blocks,
            meta,
            [
                f"Quote evidence was unavailable during a store writer lock: {exc} "
                "Other read-only evidence was retained."
            ],
        )


def explain_symbol(
    ctx: McpContext,
    symbol: str,
    *,
    exchange: str = "NSE",
    universe_id: str = screen_tool.DEFAULT_UNIVERSE_ID,
    as_of: str | date | None = None,
    statement_basis: str = "standalone",
) -> dict[str, Any]:
    """Explain recorded evidence and selection state without recommending a trade."""

    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    blocks, profile_meta, profile_notes = _profile_evidence(
        ctx,
        symbol=symbol_id,
        exchange=exchange_code,
        as_of=as_of,
        statement_basis=statement_basis,
    )
    full_rank, shortlist_rank, rank_notes = _rank_rows(
        ctx,
        symbol=symbol_id,
        exchange=exchange_code,
        universe_id=universe_id,
        as_of=as_of,
    )
    candidate = lifecycle_tool.get_candidate_status(
        ctx, symbol_id, exchange=exchange_code, as_of=as_of
    )
    freshness = governance_tool.get_data_freshness(ctx, as_of=as_of)

    selection = _selection_explanation(full_rank, shortlist_rank)
    missing = [
        name
        for name in (
            "quote",
            "stage",
            "pattern",
            "fundamentals",
            "fundamental_thesis",
        )
        if not _has_content(blocks.get(name))
    ]
    stale = [
        row["surface"]
        for row in freshness["data"]
        if row.get("freshness_status") in {"STALE", "MISSING"}
    ]
    decision_date = coerce_date((full_rank or shortlist_rank or {}).get("trade_date"))
    data = {
        "symbol_id": symbol_id,
        "exchange": exchange_code,
        "decision_date": decision_date.isoformat() if decision_date else None,
        "identity": blocks.get("identity"),
        "selection": selection,
        "full_universe_rank": json_safe(full_rank),
        "shortlist_rank": json_safe(shortlist_rank)
        if selection["in_shortlist"]
        else None,
        "quote": blocks.get("quote"),
        "stage": blocks.get("stage"),
        "pattern": blocks.get("pattern"),
        "fundamental_thesis": blocks.get("fundamental_thesis"),
        "candidate": candidate["data"],
        "evidence_quality": {
            "profile_alignment": profile_meta["alignment"],
            "max_block_spread_days": profile_meta["max_block_spread_days"],
            "missing_blocks": missing,
            "stale_or_missing_surfaces": stale,
        },
    }
    effective = decision_date or coerce_date(profile_meta["as_of_effective"])
    status = (
        AS_OF_LATEST if as_of is None else (AS_OF_EXACT if effective else AS_OF_NO_DATA)
    )
    return envelope(
        data,
        source=SOURCE,
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        notes=[
            *rank_notes,
            *profile_notes,
            "Evidence explanation only; this tool does not recommend or execute trades.",
        ],
        symbol=symbol_id,
        exchange=exchange_code,
        universe_id=universe_id,
        evidence_only=True,
        data_domain=ctx.paths.domain,
    )


def compare_symbols(
    ctx: McpContext,
    symbols: list[str],
    *,
    exchange: str = "NSE",
    universe_id: str = screen_tool.DEFAULT_UNIVERSE_ID,
    as_of: str | date | None = None,
    statement_basis: str = "standalone",
) -> dict[str, Any]:
    """Compare up to ten symbols using the same evidence fields and cutoff."""

    normalized = list(dict.fromkeys(ctx.normalize_symbol(value) for value in symbols))
    if not normalized:
        raise ValueError("Provide at least one symbol")
    if len(normalized) > MAX_COMPARE_SYMBOLS:
        raise ValueError(
            f"compare_symbols accepts at most {MAX_COMPARE_SYMBOLS} symbols"
        )

    rows: list[dict[str, Any]] = []
    for symbol_id in normalized:
        explanation = explain_symbol(
            ctx,
            symbol_id,
            exchange=exchange,
            universe_id=universe_id,
            as_of=as_of,
            statement_basis=statement_basis,
        )["data"]
        full_rank = explanation.get("full_universe_rank") or {}
        quote = explanation.get("quote") or {}
        pattern_rows = explanation.get("pattern") or []
        pattern = pattern_rows[0] if pattern_rows else {}
        thesis = explanation.get("fundamental_thesis") or {}
        classification = thesis.get("classification") or {}
        projection = thesis.get("projection") or {}
        candidate = explanation.get("candidate") or {}
        rows.append(
            {
                "symbol_id": symbol_id,
                "exchange": explanation["exchange"],
                "decision_date": explanation.get("decision_date"),
                "close": quote.get("close"),
                "quote_date": quote.get("date"),
                "rank_position": full_rank.get("rank_position"),
                "composite_score": full_rank.get("composite_score"),
                "in_shortlist": explanation["selection"]["in_shortlist"],
                "selection_reasons": explanation["selection"]["reasons"],
                "stage_label": (explanation.get("stage") or {}).get("stage_label"),
                "pattern_family": pattern.get("pattern_family"),
                "pattern_score": pattern.get("pattern_score"),
                "primary_thesis": classification.get("primary_thesis"),
                "fundamental_admission_eligible": projection.get("admission_eligible"),
                "fundamental_blockers": projection.get("blockers"),
                "candidate_status": (candidate.get("episode") or {}).get(
                    "episode_status"
                ),
                "missing_blocks": explanation["evidence_quality"]["missing_blocks"],
            }
        )
    rows.sort(
        key=lambda row: (
            row.get("rank_position") is None,
            row.get("rank_position") or 0,
            row["symbol_id"],
        )
    )
    observed_dates = [
        value
        for value in (coerce_date(row.get("decision_date")) for row in rows)
        if value is not None
    ]
    effective = max(observed_dates, default=None)
    return envelope(
        rows,
        source=SOURCE,
        as_of_status=(
            AS_OF_LATEST
            if as_of is None
            else (AS_OF_EXACT if effective else AS_OF_NO_DATA)
        ),
        as_of_requested=as_of,
        as_of_effective=effective,
        date_fields=("decision_date", "quote_date"),
        notes=["Evidence comparison only; rows are not trade recommendations."],
        exchange=ctx.resolve_exchange(exchange),
        universe_id=universe_id,
        compared_count=len(rows),
        maximum_symbols=MAX_COMPARE_SYMBOLS,
        evidence_only=True,
        data_domain=ctx.paths.domain,
    )


def summarize_universe(
    ctx: McpContext,
    *,
    exchange: str = "NSE",
    universe_id: str = screen_tool.DEFAULT_UNIVERSE_ID,
    scope: str = "full_universe",
    stage_label: str | None = None,
    stage_family_filter: str | None = None,
    sector: str | None = None,
    min_composite_score: float | None = None,
    max_rank_position: int | None = None,
    stage2_only: bool = False,
    max_bars_in_stage: int | None = None,
    max_stage_age_days: int | None = None,
    pattern_family: str | None = None,
    pattern_state: str | None = None,
    min_pattern_score: float | None = None,
    max_pivot_distance: float | None = None,
    min_rs_score: float | None = None,
    min_trend_score: float | None = None,
    min_liquidity_score: float | None = None,
    min_delivery_pct: float | None = None,
    primary_thesis: str | None = None,
    admission_eligible: bool | None = None,
    fundamental_blocker: str | None = None,
    fundamental_tier: str | None = None,
    hard_red_flag: bool | None = None,
    as_of: str | date | None = None,
) -> dict[str, Any]:
    """Aggregate a filtered shortlist or full universe without returning rows."""

    screened = screen_tool.screen_universe(
        ctx,
        exchange=exchange,
        universe_id=universe_id,
        scope=scope,
        stage_label=stage_label,
        stage_family_filter=stage_family_filter,
        sector=sector,
        min_composite_score=min_composite_score,
        max_rank_position=max_rank_position,
        stage2_only=stage2_only,
        max_bars_in_stage=max_bars_in_stage,
        max_stage_age_days=max_stage_age_days,
        pattern_family=pattern_family,
        pattern_state=pattern_state,
        min_pattern_score=min_pattern_score,
        max_pivot_distance=max_pivot_distance,
        min_rs_score=min_rs_score,
        min_trend_score=min_trend_score,
        min_liquidity_score=min_liquidity_score,
        min_delivery_pct=min_delivery_pct,
        primary_thesis=primary_thesis,
        admission_eligible=admission_eligible,
        fundamental_blocker=fundamental_blocker,
        fundamental_tier=fundamental_tier,
        hard_red_flag=hard_red_flag,
        include_fundamental_thesis=True,
        as_of=as_of,
        limit=1,
    )
    meta = screened["meta"]
    data = {
        **meta["summary"],
        "scope": meta["scope"],
        "trade_date": meta["as_of_effective"],
        "full_universe_size": meta["full_universe_size"],
        "shortlist_size": meta["shortlist_size"],
        "selection_policy": meta["selection_policy"],
        "effective_min_score": meta["effective_min_score"],
        "effective_top_n": meta["effective_top_n"],
        "market_regime": meta["market_regime"],
        "regime_as_of": meta["regime_as_of"],
        "regime_age_days": meta["regime_age_days"],
        "regime_freshness_status": meta["regime_freshness_status"],
    }
    return envelope(
        data,
        source=meta["source"],
        as_of_status=meta["as_of_status"],
        as_of_requested=as_of,
        as_of_effective=meta["as_of_effective"],
        notes=meta["notes"],
        exchange=meta["exchange"],
        universe_id=universe_id,
        scope=scope,
        filters=meta["filters"],
        aggregation_complete=True,
        evidence_only=True,
        data_domain=ctx.paths.domain,
    )


def get_market_snapshot(
    ctx: McpContext,
    *,
    exchange: str = "NSE",
    universe_id: str = screen_tool.DEFAULT_UNIVERSE_ID,
    as_of: str | date | None = None,
    sector_limit: int = 10,
) -> dict[str, Any]:
    """Return one bounded market orientation composed from governed MCP reads."""

    universe = summarize_universe(
        ctx,
        exchange=exchange,
        universe_id=universe_id,
        scope="full_universe",
        as_of=as_of,
    )
    shortlist = summarize_universe(
        ctx,
        exchange=exchange,
        universe_id=universe_id,
        scope="shortlist",
        as_of=as_of,
    )
    fundamentals = fundamental_discovery_tool.get_fundamental_lane_overview(
        ctx, exchange=exchange, as_of=as_of
    )
    sectors = sector_leadership_tool.get_sector_leadership(
        ctx, exchange=exchange, as_of=as_of, limit=sector_limit
    )
    freshness = governance_tool.get_data_freshness(ctx, as_of=as_of)
    runs = governance_tool.get_pipeline_run(ctx, as_of=as_of, limit=1)
    latest_run = runs["data"][0] if runs["data"] else None
    dq = governance_tool.get_data_quality_status(
        ctx,
        run_id=(latest_run or {}).get("run_id"),
        as_of=as_of,
        limit=100,
    )
    dq_issues = [
        row
        for row in dq["data"]
        if str(row.get("status") or "").upper() not in {"PASS", "PASSED"}
    ]
    data = {
        "universe": universe["data"],
        "shortlist": shortlist["data"],
        "fundamental_lane": fundamentals["data"],
        "sector_leadership": sectors["data"],
        "freshness": freshness["data"],
        "latest_pipeline_run": latest_run,
        "data_quality": {
            "result_count": len(dq["data"]),
            "issue_count": len(dq_issues),
            "issues": dq_issues,
        },
    }
    dates = [
        coerce_date(response["meta"].get("as_of_effective"))
        for response in (universe, shortlist, fundamentals, freshness, runs, dq)
    ]
    effective = max((value for value in dates if value is not None), default=None)
    status = (
        AS_OF_LATEST if as_of is None else (AS_OF_EXACT if effective else AS_OF_NO_DATA)
    )
    notes = []
    if sectors["meta"]["as_of_status"] == "AS_OF_UNSUPPORTED":
        notes.append(
            "Historical sector leadership is unavailable and was not replaced "
            "with latest data."
        )
    return envelope(
        data,
        source=SOURCE,
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        notes=notes,
        exchange=ctx.resolve_exchange(exchange),
        universe_id=universe_id,
        blocks={
            "universe": universe["meta"],
            "shortlist": shortlist["meta"],
            "fundamental_lane": fundamentals["meta"],
            "sector_leadership": sectors["meta"],
            "freshness": freshness["meta"],
            "pipeline_run": runs["meta"],
            "data_quality": dq["meta"],
        },
        evidence_only=True,
        data_domain=ctx.paths.domain,
    )


__all__ = [
    "MAX_COMPARE_SYMBOLS",
    "compare_symbols",
    "explain_symbol",
    "get_market_snapshot",
    "summarize_universe",
]
