"""Cross-sectional screening over the ranked universe.

Joins the version-pinned rank cross-section at a date with governed stage
structure and master sector classification, so an agent can ask "Stage 2 names
in Capital Goods above composite 70" in one call instead of pulling every
symbol and filtering client-side.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from ai_trading_system.domains.opportunities.contracts import (
    is_transition,
    legacy_code_for,
    normalize_stage,
    stage_family,
)
from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
    clamp_limit,
    coerce_date,
    envelope,
    json_safe,
)
from ai_trading_system.interfaces.mcp.readers import decisions, governed_stage, master
from ai_trading_system.interfaces.mcp.tools import (
    fundamental_discovery as fundamental_discovery_tool,
)

DEFAULT_UNIVERSE_ID = "NSE_OPERATIONAL"
DEFAULT_LIMIT = 50
MAX_LIMIT = 500

DATE_FIELDS = ("trade_date", "stage_as_of")

SORT_FIELDS = (
    "rank_position",
    "composite_score",
    "symbol_id",
)

SCOPES = ("shortlist", "full_universe")

_GOVERNED_TABLE = "weekly_stock_stage_history"


def _governed_stage_at(
    conn: Any, exchange: str, cutoff: date | None
) -> dict[str, dict[str, Any]]:
    """Latest governed stage observation per symbol at or before ``cutoff``."""

    result: dict[str, dict[str, Any]] = {}
    for record in governed_stage.snapshot(conn, exchange=exchange, cutoff=cutoff):
        symbol = str(record.get("symbol_id", "")).upper()
        observed = coerce_date(record.get("as_of"))
        stage = normalize_stage(record.get("effective_stage"))
        result[symbol] = {
            "stage_as_of": observed.isoformat() if observed else None,
            "stage_label": stage.value,
            "stage_label_legacy": legacy_code_for(stage),
            "stage_family": stage_family(stage),
            "is_transition": is_transition(stage),
            "stage_status": json_safe(record.get("stage_status")),
            "sector_name": json_safe(record.get("sector_name")),
            "bars_in_stage": json_safe(
                record.get("bars_in_stage")
                or (record.get("observation_json") or {}).get("bars_in_stage")
                if isinstance(record.get("observation_json"), dict)
                else record.get("bars_in_stage")
            ),
        }
    return result


def _fundamental_scores_at(
    ctx: McpContext, exchange: str, cutoff: date | None
) -> dict[str, dict[str, Any]]:
    """Latest score-tier snapshot; the legacy score table is NSE-symbol keyed."""

    if exchange != "NSE" or not ctx.fundamentals_db.exists():
        return {}
    with ctx.fundamentals() as conn:
        if not decisions.table_exists(conn, "fundamental_scores"):
            return {}
        if cutoff:
            effective = conn.execute(
                "SELECT MAX(CAST(snapshot_date AS DATE)) FROM fundamental_scores "
                "WHERE CAST(snapshot_date AS DATE) <= CAST(? AS DATE)",
                [cutoff.isoformat()],
            ).fetchone()[0]
        else:
            effective = conn.execute(
                "SELECT MAX(CAST(snapshot_date AS DATE)) FROM fundamental_scores"
            ).fetchone()[0]
        if effective is None:
            return {}
        frame = conn.execute(
            "SELECT symbol, fundamental_tier, hard_red_flag, red_flags "
            "FROM fundamental_scores WHERE CAST(snapshot_date AS DATE)=CAST(? AS DATE)",
            [str(effective)[:10]],
        ).fetchdf()
    return {
        str(row.get("symbol") or "").upper(): row
        for row in frame.to_dict(orient="records")
    }


def _cross_section_size(
    conn: Any, table: str, *, exchange: str, universe_id: str,
    cutoff: date | None,
) -> int | None:
    if not decisions.table_exists(conn, table):
        return None
    effective = decisions.latest_trade_date(conn, table, as_of=cutoff)
    if effective is None:
        return 0
    version = decisions.approved_version(conn, table, effective)
    return int(
        conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE trade_date=CAST(? AS DATE) "
            "AND exchange=? AND universe_id=? "
            "AND rank_model_version=? AND rank_config_hash=?",
            [
                effective.isoformat(), exchange, universe_id,
                version.model_version, version.config_hash,
            ],
        ).fetchone()[0]
    )


def screen_universe(
    ctx: McpContext,
    *,
    exchange: str = "NSE",
    universe_id: str = DEFAULT_UNIVERSE_ID,
    scope: str = "shortlist",
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
    fundamental_tier: str | None = None,
    hard_red_flag: bool | None = None,
    primary_thesis: str | None = None,
    admission_eligible: bool | None = None,
    fundamental_blocker: str | None = None,
    as_of: str | date | None = None,
    sort_by: str = "rank_position",
    limit: int | None = None,
) -> dict[str, Any]:
    """Filter the ranked universe cross-section at a point in time.

    ``stage_label`` accepts either vocabulary (``S2`` or ``stage_2_advancing``)
    and matches exactly; ``stage_family_filter`` matches the structural family,
    so ``stage_2`` also admits ``transition_2_to_3``.
    """

    exchange_code = ctx.resolve_exchange(exchange)
    scope_key = str(scope or "shortlist").strip().lower()
    if scope_key not in SCOPES:
        raise ValueError(f"Unknown scope: {scope!r} (expected one of {list(SCOPES)})")
    rank_table = (
        decisions.RANK_TABLE
        if scope_key == "shortlist"
        else decisions.RANK_UNIVERSE_TABLE
    )
    row_limit = clamp_limit(limit, default=DEFAULT_LIMIT, maximum=MAX_LIMIT)
    cutoff = coerce_date(as_of)

    sort_key = str(sort_by or "rank_position").strip().lower()
    if sort_key not in SORT_FIELDS:
        raise ValueError(
            f"Unknown sort_by: {sort_by!r} (expected one of {list(SORT_FIELDS)})"
        )

    wanted_stage = normalize_stage(stage_label).value if stage_label else None
    wanted_family = (
        str(stage_family_filter).strip().lower() if stage_family_filter else None
    )

    notes: list[str] = []
    records: list[dict[str, Any]] = []
    stages: dict[str, dict[str, Any]] = {}
    patterns: dict[str, dict[str, Any]] = {}
    effective: date | None = None
    shortlist_size: int | None = None
    full_universe_size: int | None = None

    with ctx.control_plane() as conn:
        if decisions.table_exists(conn, rank_table):
            effective = decisions.latest_trade_date(
                conn, rank_table, as_of=cutoff
            )
            try:
                records = decisions.latest_rows(
                    conn,
                    rank_table,
                    exchange=exchange_code,
                    as_of=cutoff,
                    extra_clauses=["universe_id = ?"],
                    extra_params=[universe_id],
                    limit=MAX_LIMIT * 20,
                )
            except decisions.DecisionVersionUnavailable as exc:
                notes.append(
                    f"Rank cross-section could not be version-pinned: {exc} "
                    "Returning no rows rather than mixing rank model versions."
                )
        if decisions.table_exists(conn, decisions.PATTERN_TABLE):
            try:
                for pattern in decisions.latest_rows(
                    conn, decisions.PATTERN_TABLE, exchange=exchange_code,
                    as_of=cutoff, limit=MAX_LIMIT * 20,
                ):
                    symbol_key = str(pattern.get("symbol_id") or "").upper()
                    current = patterns.get(symbol_key)
                    if current is None or float(pattern.get("pattern_score") or 0) > float(current.get("pattern_score") or 0):
                        patterns[symbol_key] = pattern
            except decisions.DecisionVersionUnavailable as exc:
                notes.append(f"Pattern cross-section unavailable: {exc}")
        stages = _governed_stage_at(conn, exchange_code, cutoff)
        try:
            shortlist_size = _cross_section_size(
                conn, decisions.RANK_TABLE, exchange=exchange_code,
                universe_id=universe_id, cutoff=cutoff,
            )
            full_universe_size = _cross_section_size(
                conn, decisions.RANK_UNIVERSE_TABLE, exchange=exchange_code,
                universe_id=universe_id, cutoff=cutoff,
            )
        except decisions.DecisionVersionUnavailable as exc:
            notes.append(f"Universe sizes could not be version-pinned: {exc}")

    fundamental_map: dict[str, dict[str, Any]] = {}
    if primary_thesis or admission_eligible is not None or fundamental_blocker:
        fundamental_map = fundamental_discovery_tool.load_fundamental_screen_map(
            ctx,
            exchange=exchange_code,
            as_of=as_of,
            primary_thesis=primary_thesis,
            admission_eligible=admission_eligible,
            blocker=fundamental_blocker,
        )
    score_map = (
        _fundamental_scores_at(ctx, exchange_code, cutoff)
        if fundamental_tier or hard_red_flag is not None
        else {}
    )

    # The symbol master is current-state only. It may fill missing sector names
    # for a latest query, but must never be projected into a historical answer.
    sector_by_symbol = (
        master.sector_by_symbol(ctx, exchange=exchange_code) if as_of is None else {}
    )

    rows: list[dict[str, Any]] = []
    for record in records:
        symbol = str(record.get("symbol_id", "")).upper()
        trade_date = coerce_date(record.get("trade_date"))
        stage = stages.get(symbol, {})
        pattern = patterns.get(symbol, {})
        fundamental = fundamental_map.get(symbol)
        fundamental_score = score_map.get(symbol, {})
        row: dict[str, Any] = {
            "symbol_id": symbol,
            "exchange": json_safe(record.get("exchange")),
            "trade_date": trade_date.isoformat() if trade_date else None,
            "rank_position": json_safe(record.get("rank_position")),
            "composite_score": json_safe(record.get("composite_score")),
            "sector_name": stage.get("sector_name") or sector_by_symbol.get(symbol),
            "stage_as_of": stage.get("stage_as_of"),
            "stage_label": stage.get("stage_label"),
            "stage_label_legacy": stage.get("stage_label_legacy"),
            "stage_family": stage.get("stage_family"),
            "is_transition": stage.get("is_transition"),
            "bars_in_stage": stage.get("bars_in_stage"),
            "stage_age_days": (
                (trade_date - coerce_date(stage.get("stage_as_of"))).days
                if trade_date and coerce_date(stage.get("stage_as_of")) else None
            ),
            "rs_score": json_safe(record.get("rs_score")),
            "trend_score": json_safe(record.get("trend_score")),
            "liquidity_score": json_safe(record.get("liquidity_score")),
            "delivery_pct_20d_avg": json_safe(record.get("delivery_pct_20d_avg")),
            "rank_confidence": json_safe(record.get("rank_confidence")),
            "rank_eligible": json_safe(record.get("rank_eligible")),
            "rejection_reasons": json_safe(record.get("rejection_reasons")),
            "pattern_family": json_safe(pattern.get("pattern_family")),
            "pattern_state": json_safe(pattern.get("pattern_state")),
            "pattern_score": json_safe(pattern.get("pattern_score")),
            "distance_to_pivot_pct": json_safe(pattern.get("distance_to_pivot_pct")),
            "primary_thesis": fundamental["classification"]["primary_thesis"] if fundamental else None,
            "fundamental_admission_eligible": fundamental["projection"]["admission_eligible"] if fundamental else None,
            "fundamental_blockers": fundamental["projection"]["blockers"] if fundamental else None,
            "fundamental_tier": json_safe(fundamental_score.get("fundamental_tier")),
            "hard_red_flag": json_safe(fundamental_score.get("hard_red_flag")),
            "fundamental_red_flags": json_safe(fundamental_score.get("red_flags")),
        }

        if wanted_stage and row["stage_label"] != wanted_stage:
            continue
        if wanted_family and row["stage_family"] != wanted_family:
            continue
        if sector and str(row["sector_name"] or "").lower() != sector.strip().lower():
            continue
        if min_composite_score is not None and (
            row["composite_score"] is None
            or row["composite_score"] < float(min_composite_score)
        ):
            continue
        if max_rank_position is not None and (
            row["rank_position"] is None
            or row["rank_position"] > int(max_rank_position)
        ):
            continue
        if stage2_only and row["stage_family"] != "stage_2":
            continue
        if max_bars_in_stage is not None and (
            row["bars_in_stage"] is None or row["bars_in_stage"] > int(max_bars_in_stage)
        ):
            continue
        if max_stage_age_days is not None and (
            row["stage_age_days"] is None
            or row["stage_age_days"] > int(max_stage_age_days)
        ):
            continue
        if pattern_family and str(row["pattern_family"] or "").lower() != pattern_family.lower():
            continue
        if pattern_state and str(row["pattern_state"] or "").lower() != pattern_state.lower():
            continue
        if min_pattern_score is not None and (row["pattern_score"] is None or row["pattern_score"] < float(min_pattern_score)):
            continue
        if max_pivot_distance is not None and (row["distance_to_pivot_pct"] is None or row["distance_to_pivot_pct"] > float(max_pivot_distance)):
            continue
        if any(threshold is not None and (row[field] is None or row[field] < float(threshold)) for field, threshold in (("rs_score", min_rs_score), ("trend_score", min_trend_score), ("liquidity_score", min_liquidity_score), ("delivery_pct_20d_avg", min_delivery_pct))):
            continue
        if primary_thesis and str(row["primary_thesis"] or "").upper() != primary_thesis.upper():
            continue
        if admission_eligible is not None and row["fundamental_admission_eligible"] is not admission_eligible:
            continue
        if fundamental_blocker and fundamental_blocker.upper() not in {str(value).upper() for value in (row["fundamental_blockers"] or [])}:
            continue
        if fundamental_tier and str(row["fundamental_tier"] or "").upper() != fundamental_tier.upper():
            continue
        if hard_red_flag is not None and row["hard_red_flag"] is not hard_red_flag:
            continue
        rows.append(row)

    if sort_key == "composite_score":
        rows.sort(
            key=lambda item: (
                item.get(sort_key) is None,
                -(float(item[sort_key])) if item.get(sort_key) is not None else 0.0,
            )
        )
    else:
        rows.sort(
            key=lambda item: (
                item.get(sort_key) is None,
                item.get(sort_key) if item.get(sort_key) is not None else 0,
            )
        )

    matched = len(rows)
    truncated = matched > row_limit
    if truncated:
        rows = rows[:row_limit]
        notes.append(
            f"{matched} symbols matched; showing {row_limit}. Tighten the "
            "filters or raise 'limit' (max 500)."
        )

    if not records and not notes:
        notes.append(
            f"No ranked rows for universe {universe_id!r} on {exchange_code} at "
            "the requested date."
        )
    if records and not stages:
        notes.append(
            "No governed stage observations were available, so stage fields are "
            "null and stage filters would exclude everything."
        )

    if as_of is None:
        status = AS_OF_LATEST
    elif rows:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    return envelope(
        rows,
        source=(
            f"control_plane.duckdb:{rank_table} + "
            f"control_plane.duckdb:{_GOVERNED_TABLE}"
        ),
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=effective,
        date_fields=DATE_FIELDS,
        notes=notes,
        exchange=exchange_code,
        universe_id=universe_id,
        scope=scope_key,
        stage_vocabulary="WeinsteinStage",
        filters={
            "stage_label": wanted_stage,
            "stage_family": wanted_family,
            "sector": sector,
            "min_composite_score": min_composite_score,
            "max_rank_position": max_rank_position,
            "pattern_family": pattern_family,
            "pattern_state": pattern_state,
            "min_pattern_score": min_pattern_score,
            "max_pivot_distance": max_pivot_distance,
            "primary_thesis": primary_thesis,
            "admission_eligible": admission_eligible,
            "fundamental_blocker": fundamental_blocker,
            "fundamental_tier": fundamental_tier,
            "hard_red_flag": hard_red_flag,
        },
        sort_by=sort_key,
        matched_count=matched,
        truncated=truncated,
        universe_size=len(records),
        full_universe_size=full_universe_size,
        shortlist_size=shortlist_size,
        selection_policy=json_safe(records[0].get("selection_policy")) if records else None,
        effective_min_score=json_safe(records[0].get("effective_min_score")) if records else None,
        effective_top_n=json_safe(records[0].get("effective_top_n")) if records else None,
        market_regime=json_safe(records[0].get("market_regime")) if records else None,
        regime_as_of=json_safe(records[0].get("regime_as_of")) if records else None,
        regime_age_days=json_safe(records[0].get("regime_age_days")) if records else None,
        regime_freshness_status=json_safe(records[0].get("regime_freshness_status")) if records else None,
        regime_freshness_policy_version=json_safe(records[0].get("regime_freshness_policy_version")) if records else None,
        data_domain=ctx.paths.domain,
    )


__all__ = ["DEFAULT_UNIVERSE_ID", "SCOPES", "SORT_FIELDS", "screen_universe"]
