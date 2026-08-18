"""Read-only API for the operational shadow fundamental-discovery lane."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from ai_trading_system.domains.fundamentals.contracts import FundamentalThesisFamily
from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT, AS_OF_LATEST, AS_OF_NO_DATA, clamp_limit, coerce_date,
    envelope, json_safe, assert_not_future,
)

CLASSIFICATION_TABLE = "fundamental_thesis_classification"
PROJECTION_TABLE = "fundamental_thesis_projection"
SOURCE = (
    "fundamentals.duckdb:fundamental_thesis_projection + "
    "fundamentals.duckdb:fundamental_thesis_classification"
)
DEFAULT_LIMIT = 250
MAX_LIMIT = 2000
DATE_FIELDS = (
    "projection.projection_date", "classification.classification_date",
    "classification.source_report_date", "classification.source_available_at",
)


def _assert_cutoff(rows: list[dict[str, Any]], cutoff: date | None) -> None:
    assert_not_future(
        [
            {
                "projection_date": row.get("as_of"),
                "classification_date": row.get("classification_as_of"),
                "source_report_date": row.get("source_report_date"),
                "source_available_at": row.get("source_available_at"),
                "projection_created_at": row.get("created_at"),
                "classification_created_at": row.get("classification_created_at"),
            }
            for row in rows
        ],
        cutoff,
        (
            "projection_date", "classification_date", "source_report_date",
            "source_available_at", "projection_created_at",
            "classification_created_at",
        ),
    )


def _exists(conn: Any, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone() is not None


def _json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return default


def _iso(value: Any) -> str | None:
    parsed = coerce_date(value)
    return parsed.isoformat() if parsed else None


def _select_rows(
    conn: Any, *, exchange: str, symbol: str | None, cutoff: date | None,
    projection_date: date | None = None, limit: int = MAX_LIMIT,
) -> list[dict[str, Any]]:
    clauses = ["p.exchange = ?"]
    params: list[Any] = [exchange]
    if symbol:
        clauses.append("UPPER(p.symbol_id) = ?")
        params.append(symbol)
    if cutoff:
        clauses.extend([
            "p.as_of <= CAST(? AS DATE)",
            "CAST(p.created_at AS DATE) <= CAST(? AS DATE)",
            "(c.source_available_at IS NULL OR c.source_available_at <= CAST(? AS DATE))",
            "CAST(c.created_at AS DATE) <= CAST(? AS DATE)",
        ])
        params.extend([cutoff.isoformat()] * 4)
    if projection_date:
        clauses.append("p.as_of = CAST(? AS DATE)")
        params.append(projection_date.isoformat())
    frame = conn.execute(
        f"""
        SELECT p.*, c.classification_id, c.as_of AS classification_as_of,
               c.statement_basis, c.source_report_date, c.source_available_at,
               c.classification_status, c.evaluations_json, c.evidence_json,
               c.semantic_payload_hash AS classification_payload_hash,
               c.created_at AS classification_created_at
        FROM {PROJECTION_TABLE} p
        JOIN {CLASSIFICATION_TABLE} c
          ON c.symbol_id = p.symbol_id AND c.exchange = p.exchange
         AND c.source_data_hash = p.source_data_hash
         AND c.taxonomy_version = p.taxonomy_version
         AND c.rule_version = p.rule_version
        WHERE {' AND '.join(clauses)}
        ORDER BY p.as_of DESC, p.symbol_id, p.created_at DESC
        LIMIT ?
        """,
        [*params, int(limit)],
    ).fetchdf()
    return frame.to_dict(orient="records")


def _previous_change(conn: Any, record: dict[str, Any], cutoff: date | None) -> dict[str, Any] | None:
    clauses = [
        "symbol_id = ?", "exchange = ?", "source_data_hash <> ?",
        "taxonomy_version = ?", "rule_version = ?",
    ]
    params: list[Any] = [
        record["symbol_id"], record["exchange"], record["source_data_hash"],
        record["taxonomy_version"], record["rule_version"],
    ]
    current_available = coerce_date(
        record.get("source_available_at") or record.get("classification_as_of")
    )
    if current_available:
        clauses.append("COALESCE(source_available_at, as_of) < CAST(? AS DATE)")
        params.append(current_available.isoformat())
    if cutoff:
        clauses.extend([
            "as_of <= CAST(? AS DATE)",
            "(source_available_at IS NULL OR source_available_at <= CAST(? AS DATE))",
            "CAST(created_at AS DATE) <= CAST(? AS DATE)",
        ])
        params.extend([cutoff.isoformat()] * 3)
    row = conn.execute(
        f"""SELECT primary_thesis, source_data_hash, as_of, source_available_at
              FROM {CLASSIFICATION_TABLE}
             WHERE {' AND '.join(clauses)}
             ORDER BY COALESCE(source_available_at, as_of) DESC, created_at DESC
             LIMIT 1""",
        params,
    ).fetchone()
    if not row:
        return None
    changed = row[0] != record.get("primary_thesis") or row[1] != record.get("source_data_hash")
    return {
        "changed": changed,
        "previous_primary_thesis": json_safe(row[0]),
        "previous_source_data_hash": json_safe(row[1]),
        "previous_classification_date": _iso(row[2]),
        "previous_source_available_at": _iso(row[3]),
    }


def _shape(conn: Any, record: dict[str, Any], *, include_evaluations: bool, cutoff: date | None) -> dict[str, Any]:
    blockers = _json(record.get("admission_blockers_json"), [])
    result = {
        "symbol_id": json_safe(record.get("symbol_id")),
        "exchange": json_safe(record.get("exchange")),
        "classification": {
            "classification_id": json_safe(record.get("classification_id")),
            "classification_date": _iso(record.get("classification_as_of")),
            "primary_thesis": json_safe(record.get("primary_thesis")),
            "secondary_theses": _json(record.get("secondary_theses_json"), []),
            "classification_status": json_safe(record.get("classification_status")),
            "statement_basis": json_safe(record.get("statement_basis")),
            "source_report_date": _iso(record.get("source_report_date")),
            "source_available_at": _iso(record.get("source_available_at")),
            "source_data_hash": json_safe(record.get("source_data_hash")),
            "taxonomy_version": json_safe(record.get("taxonomy_version")),
            "rule_version": json_safe(record.get("rule_version")),
            "evidence": _json(record.get("evidence_json"), {}),
        },
        "projection": {
            "projection_id": json_safe(record.get("projection_id")),
            "projection_date": _iso(record.get("as_of")),
            "structural_stage": json_safe(record.get("structural_stage")),
            "admission_eligible": bool(record.get("admission_eligible")),
            "blockers": blockers,
            "daily_context": _json(record.get("daily_context_json"), {}),
            "admission_policy_version": json_safe(record.get("admission_version")),
        },
        "evaluations": _json(record.get("evaluations_json"), []) if include_evaluations else None,
        "change": _previous_change(conn, record, cutoff),
    }
    return result


def get_fundamental_thesis(
    ctx: McpContext, symbol: str, *, exchange: str = "NSE",
    as_of: str | date | None = None, include_evaluations: bool = True,
) -> dict[str, Any]:
    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    cutoff = coerce_date(as_of)
    records: list[dict[str, Any]] = []
    notes: list[str] = []
    with ctx.fundamentals() as conn:
        if _exists(conn, CLASSIFICATION_TABLE) and _exists(conn, PROJECTION_TABLE):
            records = _select_rows(conn, exchange=exchange_code, symbol=symbol_id, cutoff=cutoff, limit=1)
            _assert_cutoff(records, cutoff)
            data = _shape(conn, records[0], include_evaluations=include_evaluations, cutoff=cutoff) if records else None
        else:
            data = None
            notes.append("Fundamental discovery tables are unavailable; no schema was created.")
    effective = _iso(records[0].get("as_of")) if records else None
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if data else AS_OF_NO_DATA)
    return envelope(
        data, source=SOURCE, as_of_status=status, as_of_requested=as_of,
        as_of_effective=effective, date_fields=DATE_FIELDS, notes=notes,
        symbol=symbol_id, exchange=exchange_code, lane="fundamental_discovery",
        shadow_only=True, data_domain=ctx.paths.domain,
    )


def get_fundamental_thesis_history(
    ctx: McpContext, symbol: str, *, exchange: str = "NSE",
    from_date: str | date | None = None, to_date: str | date | None = None,
    as_of: str | date | None = None, limit: int | None = None,
) -> dict[str, Any]:
    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    row_limit = clamp_limit(limit, default=DEFAULT_LIMIT, maximum=MAX_LIMIT)
    bounds = [value for value in (coerce_date(to_date), coerce_date(as_of)) if value]
    cutoff = min(bounds) if bounds else None
    start = coerce_date(from_date)
    notes: list[str] = []
    with ctx.fundamentals() as conn:
        if _exists(conn, CLASSIFICATION_TABLE) and _exists(conn, PROJECTION_TABLE):
            records = _select_rows(conn, exchange=exchange_code, symbol=symbol_id, cutoff=cutoff, limit=row_limit)
            _assert_cutoff(records, cutoff)
            if start:
                records = [row for row in records if coerce_date(row.get("as_of")) >= start]
            records.reverse()
            data = [_shape(conn, row, include_evaluations=True, cutoff=cutoff) for row in records]
        else:
            records, data = [], []
            notes.append("Fundamental discovery tables are unavailable; no schema was created.")
    effective = max((_iso(row.get("as_of")) for row in records), default=None)
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if data else AS_OF_NO_DATA)
    return envelope(
        data, source=SOURCE, as_of_status=status, as_of_requested=as_of,
        as_of_effective=effective, date_fields=DATE_FIELDS, notes=notes,
        symbol=symbol_id, exchange=exchange_code, lane="fundamental_discovery",
        shadow_only=True, truncated=len(records) >= row_limit,
        data_domain=ctx.paths.domain,
    )


def screen_fundamental_theses(
    ctx: McpContext, *, exchange: str = "NSE", as_of: str | date | None = None,
    primary_thesis: str | None = None, classification_status: str | None = None,
    admission_eligible: bool | None = None, blocker: str | None = None,
    statement_basis: str | None = None, limit: int | None = 50,
) -> dict[str, Any]:
    exchange_code = ctx.resolve_exchange(exchange)
    cutoff = coerce_date(as_of)
    row_limit = clamp_limit(limit, default=50, maximum=500)
    wanted_thesis = primary_thesis.upper() if primary_thesis else None
    allowed = {item.value for item in FundamentalThesisFamily}
    if wanted_thesis and wanted_thesis not in allowed:
        raise ValueError(f"Unknown primary_thesis: {primary_thesis!r}")
    notes: list[str] = []
    with ctx.fundamentals() as conn:
        if _exists(conn, CLASSIFICATION_TABLE) and _exists(conn, PROJECTION_TABLE):
            if cutoff:
                effective_row = conn.execute(
                    f"SELECT MAX(as_of) FROM {PROJECTION_TABLE} WHERE exchange=? AND as_of<=CAST(? AS DATE) AND CAST(created_at AS DATE)<=CAST(? AS DATE)",
                    [exchange_code, cutoff.isoformat(), cutoff.isoformat()],
                ).fetchone()
            else:
                effective_row = conn.execute(
                    f"SELECT MAX(as_of) FROM {PROJECTION_TABLE} WHERE exchange=?", [exchange_code]
                ).fetchone()
            effective = coerce_date(effective_row[0]) if effective_row else None
            records = _select_rows(conn, exchange=exchange_code, symbol=None, cutoff=cutoff, projection_date=effective, limit=MAX_LIMIT) if effective else []
            _assert_cutoff(records, cutoff)
            shaped = [_shape(conn, row, include_evaluations=False, cutoff=cutoff) for row in records]
        else:
            effective, shaped = None, []
            notes.append("Fundamental discovery tables are unavailable; no schema was created.")
    def keep(row: dict[str, Any]) -> bool:
        classification, projection = row["classification"], row["projection"]
        blockers = {str(value).upper() for value in projection["blockers"]}
        return not (
            (wanted_thesis and classification["primary_thesis"] != wanted_thesis)
            or (classification_status and str(classification["classification_status"]).upper() != classification_status.upper())
            or (admission_eligible is not None and projection["admission_eligible"] is not admission_eligible)
            or (blocker and blocker.upper() not in blockers)
            or (statement_basis and str(classification["statement_basis"]).lower() != statement_basis.lower())
        )
    matched = [row for row in shaped if keep(row)]
    data = matched[:row_limit]
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if data else AS_OF_NO_DATA)
    return envelope(
        data, source=SOURCE, as_of_status=status, as_of_requested=as_of,
        as_of_effective=effective, date_fields=DATE_FIELDS, notes=notes,
        exchange=exchange_code, lane="fundamental_discovery", shadow_only=True,
        projection_date=effective.isoformat() if effective else None,
        matched_count=len(matched), truncated=len(matched) > row_limit,
        data_domain=ctx.paths.domain,
    )


def load_fundamental_screen_map(
    ctx: McpContext, *, exchange: str, as_of: str | date | None,
    primary_thesis: str | None = None,
    admission_eligible: bool | None = None,
    blocker: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Uncapped internal join map for the bounded rank-screen response."""

    exchange_code = ctx.resolve_exchange(exchange)
    cutoff = coerce_date(as_of)
    with ctx.fundamentals() as conn:
        if not (_exists(conn, CLASSIFICATION_TABLE) and _exists(conn, PROJECTION_TABLE)):
            return {}
        if cutoff:
            effective_row = conn.execute(
                f"SELECT MAX(as_of) FROM {PROJECTION_TABLE} WHERE exchange=? AND as_of<=CAST(? AS DATE) AND CAST(created_at AS DATE)<=CAST(? AS DATE)",
                [exchange_code, cutoff.isoformat(), cutoff.isoformat()],
            ).fetchone()
        else:
            effective_row = conn.execute(
                f"SELECT MAX(as_of) FROM {PROJECTION_TABLE} WHERE exchange=?",
                [exchange_code],
            ).fetchone()
        effective = coerce_date(effective_row[0]) if effective_row else None
        if effective is None:
            return {}
        count = int(conn.execute(
            f"SELECT COUNT(*) FROM {PROJECTION_TABLE} WHERE exchange=? AND as_of=CAST(? AS DATE)",
            [exchange_code, effective.isoformat()],
        ).fetchone()[0])
        records = _select_rows(
            conn, exchange=exchange_code, symbol=None, cutoff=cutoff,
            projection_date=effective, limit=max(count, 1),
        )
        _assert_cutoff(records, cutoff)
        rows = [_shape(conn, record, include_evaluations=False, cutoff=cutoff) for record in records]
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        projection = row["projection"]
        if primary_thesis and row["classification"]["primary_thesis"] != primary_thesis.upper():
            continue
        if admission_eligible is not None and projection["admission_eligible"] is not admission_eligible:
            continue
        if blocker and blocker.upper() not in {str(value).upper() for value in projection["blockers"]}:
            continue
        output[str(row["symbol_id"]).upper()] = row
    return output


def get_fundamental_lane_overview(
    ctx: McpContext, *, exchange: str = "NSE", as_of: str | date | None = None,
) -> dict[str, Any]:
    exchange_code = ctx.resolve_exchange(exchange)
    cutoff = coerce_date(as_of)
    notes: list[str] = []
    with ctx.fundamentals() as conn:
        if _exists(conn, CLASSIFICATION_TABLE) and _exists(conn, PROJECTION_TABLE):
            if cutoff:
                effective_row = conn.execute(
                    f"SELECT MAX(as_of) FROM {PROJECTION_TABLE} WHERE exchange=? AND as_of<=CAST(? AS DATE) AND CAST(created_at AS DATE)<=CAST(? AS DATE)",
                    [exchange_code, cutoff.isoformat(), cutoff.isoformat()],
                ).fetchone()
            else:
                effective_row = conn.execute(
                    f"SELECT MAX(as_of) FROM {PROJECTION_TABLE} WHERE exchange=?",
                    [exchange_code],
                ).fetchone()
            effective = coerce_date(effective_row[0]) if effective_row else None
            count = int(conn.execute(
                f"SELECT COUNT(*) FROM {PROJECTION_TABLE} WHERE exchange=? AND as_of=CAST(? AS DATE)",
                [exchange_code, effective.isoformat()],
            ).fetchone()[0]) if effective else 0
            records = _select_rows(
                conn, exchange=exchange_code, symbol=None, cutoff=cutoff,
                projection_date=effective, limit=max(count, 1),
            ) if effective else []
            _assert_cutoff(records, cutoff)
            rows = [_shape(conn, record, include_evaluations=False, cutoff=cutoff) for record in records]
        else:
            effective, rows = None, []
            notes.append("Fundamental discovery tables are unavailable; no schema was created.")
    families: dict[str, int] = {item.value: 0 for item in FundamentalThesisFamily}
    statuses: dict[str, int] = {}
    blockers: dict[str, int] = {}
    eligible = 0
    for row in rows:
        family = row["classification"]["primary_thesis"]
        if family:
            families[family] = families.get(family, 0) + 1
        status = str(row["classification"]["classification_status"])
        statuses[status] = statuses.get(status, 0) + 1
        eligible += int(row["projection"]["admission_eligible"])
        for blocker in row["projection"]["blockers"]:
            blockers[str(blocker)] = blockers.get(str(blocker), 0) + 1
    data = {
        "projection_date": effective.isoformat() if effective else None,
        "symbols_observed": len(rows), "admission_eligible": eligible,
        "admission_ineligible": len(rows) - eligible,
        "primary_thesis_counts": families,
        "classification_status_counts": statuses,
        "blocker_counts": blockers,
    }
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if rows else AS_OF_NO_DATA)
    return envelope(
        data, source=SOURCE, as_of_status=status, as_of_requested=as_of,
        as_of_effective=effective, notes=notes, exchange=exchange_code,
        lane="fundamental_discovery", shadow_only=True,
        observed_symbols=len(rows), data_domain=ctx.paths.domain,
    )


__all__ = [
    "get_fundamental_lane_overview", "get_fundamental_thesis",
    "get_fundamental_thesis_history", "screen_fundamental_theses",
    "load_fundamental_screen_map",
]
