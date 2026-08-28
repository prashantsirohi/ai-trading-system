"""Transport-free, read-only tools over one configured trade-journal account."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from typing import Any

from ai_trading_system.interfaces.journal_mcp.context import JournalMcpContext
from ai_trading_system.interfaces.journal_mcp.schema_catalog import describe_schema
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
    assert_not_future,
    clamp_limit,
    envelope,
    json_safe,
)


def _known_cutoff(value: str | datetime | date | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return (
            value.astimezone(timezone.utc).replace(tzinfo=None)
            if value.tzinfo is not None
            else value
        )
    if isinstance(value, date):
        return datetime.combine(value, time.max)
    text = str(value).strip()
    try:
        if len(text) == 10:
            return datetime.combine(date.fromisoformat(text), time.max)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return (
            parsed.astimezone(timezone.utc).replace(tzinfo=None)
            if parsed.tzinfo is not None
            else parsed
        )
    except ValueError as exc:
        raise ValueError("known_at must be an ISO date or timestamp") from exc


def _portfolio_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError("portfolio_as_of must be an ISO date") from exc


def _rows(cursor: Any) -> list[dict[str, Any]]:
    names = [item[0] for item in (cursor.description or [])]
    return [
        {name: json_safe(value) for name, value in zip(names, row, strict=True)}
        for row in cursor.fetchall()
    ]


def _meta(ctx: JournalMcpContext, known_at: datetime | None) -> dict[str, Any]:
    return {
        "account_scope": ctx.account_scope,
        "known_at_requested": known_at.isoformat() if known_at else None,
        "known_at_status": "EXACT" if known_at else "LATEST",
        "privacy": "account_pinned",
    }


def _analysis_run(
    ctx: JournalMcpContext,
    analysis_type: str,
    *,
    known_at: datetime | None,
    portfolio_as_of: date | None = None,
) -> dict[str, Any] | None:
    params: list[Any] = [ctx.account_ref, analysis_type]
    clauses = ["a.account_ref=?", "a.analysis_type=?", "a.status='COMPLETED'"]
    if known_at is not None:
        clauses.append("a.completed_at<=?")
        params.append(known_at)
    if portfolio_as_of is not None and analysis_type == "reconstruction":
        clauses.append(
            "EXISTS (SELECT 1 FROM portfolio_reconstruction p "
            "WHERE p.analysis_run_id=a.analysis_run_id)"
        )
        clauses.append(
            "NOT EXISTS (SELECT 1 FROM portfolio_reconstruction p "
            "WHERE p.analysis_run_id=a.analysis_run_id AND CAST(p.as_of_at AS DATE)>?)"
        )
        params.append(portfolio_as_of)
    with ctx.reader() as conn:
        result = _rows(conn.execute(
            f"""SELECT a.analysis_run_id,a.logic_version,a.started_at,a.completed_at
                FROM journal_analysis_run a WHERE {' AND '.join(clauses)}
                ORDER BY a.completed_at DESC,a.started_at DESC LIMIT 1""",
            params,
        ))
    return result[0] if result else None


def _response(
    ctx: JournalMcpContext,
    data: Any,
    *,
    source: str,
    portfolio_as_of: date | None,
    known_at: datetime | None,
    effective: date | None = None,
    date_fields: tuple[str, ...] = (),
    **meta: Any,
) -> dict[str, Any]:
    status = (
        AS_OF_LATEST if portfolio_as_of is None
        else AS_OF_EXACT if data
        else AS_OF_NO_DATA
    )
    return envelope(
        data,
        source=source,
        as_of_status=status,
        as_of_requested=portfolio_as_of,
        as_of_effective=effective,
        date_fields=date_fields,
        **_meta(ctx, known_at),
        **meta,
    )


def get_journal_overview(
    ctx: JournalMcpContext,
    portfolio_as_of: str | date | None = None,
    known_at: str | datetime | date | None = None,
) -> dict[str, Any]:
    cutoff = _known_cutoff(known_at)
    effective = _portfolio_date(portfolio_as_of)
    reconstruction = _analysis_run(
        ctx, "reconstruction", known_at=cutoff, portfolio_as_of=effective
    )
    analysis = _analysis_run(ctx, "point_in_time_analysis", known_at=cutoff)
    data: dict[str, Any] = {
        "scope": "securities_only",
        "reconstruction": reconstruction,
        "analysis": analysis,
        "position_count": 0,
        "open_episode_count": 0,
        "dq": {"open": 0, "blocking": 0},
    }
    with ctx.reader() as conn:
        if reconstruction:
            run_id = reconstruction["analysis_run_id"]
            data["position_count"] = conn.execute(
                "SELECT count(*) FROM portfolio_reconstruction WHERE analysis_run_id=? AND quantity<>0",
                [run_id],
            ).fetchone()[0]
            data["open_episode_count"] = conn.execute(
                "SELECT count(*) FROM trade_episode WHERE analysis_run_id=? AND status='OPEN'",
                [run_id],
            ).fetchone()[0]
        dq_clauses = ["account_ref=?", "lifecycle_status='OPEN'"]
        dq_params: list[Any] = [ctx.account_ref]
        if cutoff is not None:
            dq_clauses.append("created_at<=?")
            dq_params.append(cutoff)
        open_count, blocking_count = conn.execute(
            f"""SELECT count(*),count(*) FILTER (WHERE severity='BLOCKING')
                FROM journal_dq_issue WHERE {' AND '.join(dq_clauses)}""",
            dq_params,
        ).fetchone()
        data["dq"] = {"open": open_count, "blocking": blocking_count}
    return _response(
        ctx, data, source="trade_journal.duckdb:journal_analysis_run",
        portfolio_as_of=effective, known_at=cutoff, effective=effective,
    )


def get_journal_positions(
    ctx: JournalMcpContext,
    portfolio_as_of: str | date | None = None,
    known_at: str | datetime | date | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    cutoff = _known_cutoff(known_at)
    effective = _portfolio_date(portfolio_as_of)
    run = _analysis_run(ctx, "reconstruction", known_at=cutoff, portfolio_as_of=effective)
    if run is None:
        return _response(ctx, [], source="trade_journal.duckdb:portfolio_reconstruction",
                         portfolio_as_of=effective, known_at=cutoff)
    bounded = clamp_limit(limit, default=100, maximum=500)
    alias_cutoff = cutoff or datetime.max
    valid_date = effective or date.max
    with ctx.reader() as conn:
        rows = _rows(conn.execute(
            """SELECT p.instrument_id,
                      (SELECT a.symbol FROM instrument_alias a
                       WHERE a.instrument_id=p.instrument_id AND a.created_at<=?
                         AND (a.valid_from IS NULL OR a.valid_from<=?)
                         AND (a.valid_to IS NULL OR a.valid_to>=?)
                       ORDER BY a.valid_from DESC NULLS LAST,a.created_at DESC LIMIT 1) symbol,
                      p.as_of_at,p.quantity,p.fifo_cost,p.weighted_average_cost,
                      p.trust_status,p.generated_at
               FROM portfolio_reconstruction p
               WHERE p.analysis_run_id=? AND p.account_ref=? AND p.quantity<>0
               ORDER BY symbol,p.instrument_id LIMIT ?""",
            [alias_cutoff, valid_date, valid_date, run["analysis_run_id"], ctx.account_ref, bounded + 1],
        ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    observed = max((date.fromisoformat(row["as_of_at"][:10]) for row in rows), default=effective)
    return _response(
        ctx, rows, source="trade_journal.duckdb:portfolio_reconstruction",
        portfolio_as_of=effective, known_at=cutoff, effective=observed,
        date_fields=("as_of_at",), scope="securities_only",
        analysis_run_id=run["analysis_run_id"], logic_version=run["logic_version"],
        truncated=truncated,
    )


def list_trade_episodes(
    ctx: JournalMcpContext,
    symbol: str | None = None,
    status: str | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
    portfolio_as_of: str | date | None = None,
    known_at: str | datetime | date | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    cutoff = _known_cutoff(known_at)
    effective = _portfolio_date(portfolio_as_of)
    start, end = _portfolio_date(from_date), _portfolio_date(to_date)
    run = _analysis_run(ctx, "reconstruction", known_at=cutoff, portfolio_as_of=effective)
    if run is None:
        return _response(ctx, [], source="trade_journal.duckdb:trade_episode",
                         portfolio_as_of=effective, known_at=cutoff)
    clauses = ["e.analysis_run_id=?", "e.account_ref=?"]
    params: list[Any] = [run["analysis_run_id"], ctx.account_ref]
    if symbol:
        clauses.append("EXISTS (SELECT 1 FROM episode_fill_link l JOIN journal_fill f USING(fill_id) WHERE l.episode_id=e.episode_id AND l.analysis_run_id=e.analysis_run_id AND f.symbol=?)")
        params.append(str(symbol).strip().upper())
    if status:
        clauses.append("e.status=?")
        params.append(str(status).strip().upper())
    if start:
        clauses.append("CAST(e.opened_at AS DATE)>=?")
        params.append(start)
    if end:
        clauses.append("CAST(e.opened_at AS DATE)<=?")
        params.append(end)
    if effective:
        clauses.append("CAST(e.opened_at AS DATE)<=?")
        params.append(effective)
    bounded = clamp_limit(limit, default=100, maximum=500)
    params.append(bounded + 1)
    with ctx.reader() as conn:
        rows = _rows(conn.execute(
            f"""SELECT e.episode_id,e.instrument_id,
                       (SELECT f.symbol FROM episode_fill_link l JOIN journal_fill f USING(fill_id)
                        WHERE l.episode_id=e.episode_id AND l.analysis_run_id=e.analysis_run_id
                        ORDER BY f.executed_at LIMIT 1) symbol,
                       e.opened_at,e.closed_at,e.status,e.realised_gross_pnl,
                       e.trust_status,e.generated_at
                FROM trade_episode e WHERE {' AND '.join(clauses)}
                ORDER BY e.opened_at DESC,e.episode_id DESC LIMIT ?""",
            params,
        ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    assert_not_future(rows, effective, ("opened_at",))
    return _response(
        ctx, rows, source="trade_journal.duckdb:trade_episode",
        portfolio_as_of=effective, known_at=cutoff, effective=effective,
        date_fields=("opened_at",), scope="gross", analysis_run_id=run["analysis_run_id"],
        truncated=truncated,
    )


def get_trade_episode(
    ctx: JournalMcpContext,
    episode_id: str,
    known_at: str | datetime | date | None = None,
    include_fills: bool = True,
    include_annotations: bool = False,
    fill_limit: int = 250,
) -> dict[str, Any]:
    cutoff = _known_cutoff(known_at)
    clauses = ["e.episode_id=?", "e.account_ref=?", "a.status='COMPLETED'"]
    params: list[Any] = [episode_id, ctx.account_ref]
    if cutoff:
        clauses.extend(["a.completed_at<=?", "e.generated_at<=?"])
        params.extend([cutoff, cutoff])
    with ctx.reader() as conn:
        episodes = _rows(conn.execute(
            f"""SELECT e.episode_id,e.analysis_run_id,e.instrument_id,e.opened_at,e.closed_at,
                       e.status,e.realised_gross_pnl,e.trust_status,e.generated_at,a.logic_version
                FROM trade_episode e JOIN journal_analysis_run a USING(analysis_run_id)
                WHERE {' AND '.join(clauses)}
                ORDER BY a.completed_at DESC,e.generated_at DESC LIMIT 1""",
            params,
        ))
        if not episodes:
            return _response(ctx, {}, source="trade_journal.duckdb:trade_episode",
                             portfolio_as_of=None, known_at=cutoff)
        episode = episodes[0]
        fills: list[dict[str, Any]] = []
        fills_truncated = False
        if include_fills:
            bounded_fills = clamp_limit(fill_limit, default=250, maximum=500)
            fills = _rows(conn.execute(
                """SELECT f.fill_id,f.symbol,f.exchange,f.trade_date,f.executed_at,
                          f.side,f.auction,f.quantity,f.price,f.trust_status,l.link_type
                   FROM episode_fill_link l JOIN journal_fill f USING(fill_id)
                   WHERE l.episode_id=? AND l.analysis_run_id=? AND f.account_ref=?
                   ORDER BY f.executed_at,f.fill_id LIMIT ?""",
                [episode_id, episode["analysis_run_id"], ctx.account_ref, bounded_fills + 1],
            ))
            fills_truncated = len(fills) > bounded_fills
            fills = fills[:bounded_fills]
        annotations: list[dict[str, Any]] = []
        if include_annotations:
            annotation_clauses = ["episode_id=?"]
            annotation_params: list[Any] = [episode_id]
            if cutoff:
                annotation_clauses.append("created_at<=?")
                annotation_params.append(cutoff)
            annotations = _rows(conn.execute(
                f"""SELECT revision,thesis,setup,intended_stop,target,exit_reason,
                           lesson,tags_json,created_at
                    FROM journal_annotation WHERE {' AND '.join(annotation_clauses)}
                    ORDER BY revision LIMIT 101""",
                annotation_params,
            ))
            annotations = annotations[:100]
            for row in annotations:
                row["tags"] = json.loads(row.pop("tags_json"))
    return _response(
        ctx, {"scope": "gross", "episode": episode, "fills": fills, "annotations": annotations},
        source="trade_journal.duckdb:trade_episode+journal_fill",
        portfolio_as_of=None, known_at=cutoff,
        analysis_run_id=episode["analysis_run_id"], logic_version=episode["logic_version"],
        fills_truncated=fills_truncated,
    )


def get_trade_evaluations(
    ctx: JournalMcpContext,
    symbol: str | None = None,
    evaluation_type: str | None = None,
    known_at: str | datetime | date | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    cutoff = _known_cutoff(known_at)
    run = _analysis_run(ctx, "point_in_time_analysis", known_at=cutoff)
    if run is None:
        return _response(ctx, [], source="trade_journal.duckdb:trade_evaluation",
                         portfolio_as_of=None, known_at=cutoff)
    clauses = ["e.analysis_run_id=?"]
    params: list[Any] = [run["analysis_run_id"]]
    if symbol:
        clauses.append("f.symbol=?")
        params.append(str(symbol).strip().upper())
    if evaluation_type:
        clauses.append("e.evaluation_type=?")
        params.append(str(evaluation_type).strip().upper())
    clauses.append("(f.account_ref IS NULL OR f.account_ref=?)")
    params.append(ctx.account_ref)
    bounded = clamp_limit(limit, default=100, maximum=500)
    params.append(bounded + 1)
    with ctx.reader() as conn:
        rows = _rows(conn.execute(
            f"""SELECT e.evaluation_id,e.episode_id,e.fill_id,e.evaluation_type,
                       e.score,e.score_status,e.components_json,e.classification,e.confidence,
                       e.logic_version,e.generated_at,f.symbol,f.exchange,f.trade_date,f.side
                FROM trade_evaluation e LEFT JOIN journal_fill f USING(fill_id)
                WHERE {' AND '.join(clauses)}
                ORDER BY e.generated_at DESC,e.evaluation_id DESC LIMIT ?""",
            params,
        ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    for row in rows:
        row["components"] = json.loads(row.pop("components_json"))
    return _response(
        ctx, rows, source="trade_journal.duckdb:trade_evaluation",
        portfolio_as_of=None, known_at=cutoff, scope="gross",
        analysis_run_id=run["analysis_run_id"], logic_version=run["logic_version"],
        truncated=truncated,
    )


def get_portfolio_journal_series(
    ctx: JournalMcpContext,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
    known_at: str | datetime | date | None = None,
    limit: int = 250,
) -> dict[str, Any]:
    cutoff = _known_cutoff(known_at)
    start, end = _portfolio_date(from_date), _portfolio_date(to_date)
    run = _analysis_run(ctx, "point_in_time_analysis", known_at=cutoff)
    if run is None:
        return _response(ctx, [], source="trade_journal.duckdb:portfolio_risk_snapshot",
                         portfolio_as_of=end, known_at=cutoff)
    clauses = ["analysis_run_id=?", "account_ref=?"]
    params: list[Any] = [run["analysis_run_id"], ctx.account_ref]
    if start:
        clauses.append("as_of_date>=?")
        params.append(start)
    if end:
        clauses.append("as_of_date<=?")
        params.append(end)
    bounded = clamp_limit(limit, default=250, maximum=1000)
    params.append(bounded + 1)
    with ctx.reader() as conn:
        rows = _rows(conn.execute(
            f"""SELECT as_of_date,scope_label,metrics_json,logic_version,generated_at
                FROM portfolio_risk_snapshot WHERE {' AND '.join(clauses)}
                ORDER BY as_of_date DESC LIMIT ?""",
            params,
        ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    for row in rows:
        row.update(json.loads(row.pop("metrics_json")))
    rows.reverse()
    effective = date.fromisoformat(rows[-1]["as_of_date"]) if rows else end
    return _response(
        ctx, rows, source="trade_journal.duckdb:portfolio_risk_snapshot",
        portfolio_as_of=end, known_at=cutoff, effective=effective,
        date_fields=("as_of_date",), scope="holdings_only",
        analysis_run_id=run["analysis_run_id"], logic_version=run["logic_version"],
        truncated=truncated,
    )


def get_journal_reconciliation(
    ctx: JournalMcpContext,
    reconciliation_id: str | None = None,
    portfolio_as_of: str | date | None = None,
    known_at: str | datetime | date | None = None,
    limit: int = 250,
) -> dict[str, Any]:
    cutoff = _known_cutoff(known_at)
    effective = _portfolio_date(portfolio_as_of)
    clauses = ["account_ref=?"]
    params: list[Any] = [ctx.account_ref]
    if reconciliation_id:
        clauses.append("reconciliation_id=?")
        params.append(reconciliation_id)
    if effective:
        clauses.append("CAST(as_of_at AS DATE)<=?")
        params.append(effective)
    if cutoff:
        clauses.append("generated_at<=?")
        params.append(cutoff)
    with ctx.reader() as conn:
        rows = _rows(conn.execute(
            f"""SELECT reconciliation_id,analysis_run_id,snapshot_id,as_of_at,status,
                       matched_count,issue_count,trust_status,generated_at
                FROM portfolio_reconciliation WHERE {' AND '.join(clauses)}
                ORDER BY generated_at DESC,reconciliation_id DESC LIMIT 1""",
            params,
        ))
        if not rows:
            return _response(ctx, {}, source="trade_journal.duckdb:portfolio_reconciliation",
                             portfolio_as_of=effective, known_at=cutoff)
        reconciliation = rows[0]
        bounded = clamp_limit(limit, default=250, maximum=500)
        items = _rows(conn.execute(
            """SELECT instrument_id,instrument,classification,broker_quantity,
                      ledger_quantity,broker_cost,fifo_cost,weighted_average_cost,
                      quantity_difference,cost_difference
               FROM portfolio_reconciliation_item WHERE reconciliation_id=?
               ORDER BY classification,instrument LIMIT ?""",
            [reconciliation["reconciliation_id"], bounded + 1],
        ))
    truncated = len(items) > bounded
    items = items[:bounded]
    observed = date.fromisoformat(reconciliation["as_of_at"][:10])
    return _response(
        ctx, {"scope": "securities_only", "reconciliation": reconciliation, "items": items},
        source="trade_journal.duckdb:portfolio_reconciliation",
        portfolio_as_of=effective, known_at=cutoff, effective=observed,
        truncated=truncated,
    )


def get_journal_data_quality(
    ctx: JournalMcpContext,
    severity: str | None = None,
    lifecycle_status: str | None = None,
    known_at: str | datetime | date | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    cutoff = _known_cutoff(known_at)
    clauses = ["account_ref=?"]
    params: list[Any] = [ctx.account_ref]
    if severity:
        clauses.append("severity=?")
        params.append(str(severity).strip().upper())
    if lifecycle_status:
        clauses.append("lifecycle_status=?")
        params.append(str(lifecycle_status).strip().upper())
    if cutoff:
        clauses.append("created_at<=?")
        params.append(cutoff)
    bounded = clamp_limit(limit, default=100, maximum=500)
    params.append(bounded + 1)
    with ctx.reader() as conn:
        rows = _rows(conn.execute(
            f"""SELECT issue_id,analysis_run_id,severity,issue_type,entity_type,
                       entity_id,lifecycle_status,created_at,resolved_at,
                       evidence_json<>'{{}}' AS evidence_available
                FROM journal_dq_issue WHERE {' AND '.join(clauses)}
                ORDER BY created_at DESC,issue_id DESC LIMIT ?""",
            params,
        ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    return _response(
        ctx, rows, source="trade_journal.duckdb:journal_dq_issue",
        portfolio_as_of=None, known_at=cutoff, scope="account_private",
        truncated=truncated,
        notes=["Raw import rows and DQ evidence payloads are intentionally excluded."],
    )


__all__ = [
    "describe_schema", "get_journal_data_quality", "get_journal_overview",
    "get_journal_positions", "get_journal_reconciliation",
    "get_portfolio_journal_series", "get_trade_episode", "get_trade_evaluations",
    "list_trade_episodes",
]
