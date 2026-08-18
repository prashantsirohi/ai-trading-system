"""Read-only pipeline, DQ, artifact-lineage and freshness surfaces."""

from __future__ import annotations

from datetime import date
from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext, StoreUnavailableError
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT, AS_OF_LATEST, AS_OF_NO_DATA, clamp_limit, coerce_date,
    envelope, json_safe,
)
from ai_trading_system.interfaces.mcp.readers import decisions


def _exists(conn: Any, table: str) -> bool:
    return conn.execute("SELECT 1 FROM information_schema.tables WHERE table_name=?", [table]).fetchone() is not None


def _records(frame: Any) -> list[dict[str, Any]]:
    return [{str(k): json_safe(v) for k, v in row.items()} for row in frame.to_dict(orient="records")]


def get_pipeline_run(
    ctx: McpContext, *, run_id: str | None = None,
    as_of: str | date | None = None, limit: int | None = 25,
) -> dict[str, Any]:
    cutoff = coerce_date(as_of)
    row_limit = clamp_limit(limit, default=25, maximum=250)
    with ctx.control_plane() as conn:
        if not _exists(conn, "pipeline_run"):
            rows = []
        else:
            clauses, params = [], []
            if run_id:
                clauses.append("r.run_id=?")
                params.append(run_id)
            if cutoff:
                clauses.append("r.run_date<=CAST(? AS DATE)")
                params.append(cutoff.isoformat())
            where = "WHERE " + " AND ".join(clauses) if clauses else ""
            rows = _records(conn.execute(
                f"""SELECT r.*,
                    (SELECT COUNT(*) FROM pipeline_stage_run s WHERE s.run_id=r.run_id) AS stage_attempt_count,
                    (SELECT COUNT(*) FROM pipeline_artifact a WHERE a.run_id=r.run_id) AS artifact_count,
                    (SELECT COUNT(*) FROM dq_result d WHERE d.run_id=r.run_id AND d.status NOT IN ('passed','PASS','PASSED')) AS dq_issue_count
                    FROM pipeline_run r {where}
                    ORDER BY r.run_date DESC, r.started_at DESC LIMIT ?""",
                [*params, row_limit],
            ).fetchdf())
            if cutoff:
                for row in rows:
                    ended = coerce_date(row.get("ended_at"))
                    if ended and ended > cutoff:
                        row["status"] = "RUNNING_AS_OF"
                        row["ended_at"] = None
                        row["error_class"] = None
                        row["error_message"] = None
                        row["current_stage"] = None
                        row["metadata_json"] = None
    effective = max((coerce_date(row.get("run_date")) for row in rows), default=None)
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if rows else AS_OF_NO_DATA)
    return envelope(rows, source="control_plane.duckdb:pipeline_run", as_of_status=status,
                    as_of_requested=as_of, as_of_effective=effective, date_fields=("run_date",),
                    run_id=run_id, truncated=len(rows) >= row_limit, data_domain=ctx.paths.domain)


def get_data_quality_status(
    ctx: McpContext, *, run_id: str | None = None, stage: str | None = None,
    severity: str | None = None, as_of: str | date | None = None,
    limit: int | None = 250,
) -> dict[str, Any]:
    cutoff = coerce_date(as_of)
    row_limit = clamp_limit(limit, default=250, maximum=2000)
    with ctx.control_plane() as conn:
        if not _exists(conn, "dq_result"):
            rows = []
        else:
            clauses, params = [], []
            if run_id:
                clauses.append("d.run_id=?")
                params.append(run_id)
            if stage:
                clauses.append("d.stage_name=?")
                params.append(stage)
            if severity:
                clauses.append("UPPER(d.severity)=?")
                params.append(severity.upper())
            if cutoff:
                clauses.append("CAST(d.created_at AS DATE)<=CAST(? AS DATE)")
                params.append(cutoff.isoformat())
            where = "WHERE " + " AND ".join(clauses) if clauses else ""
            rows = _records(conn.execute(
                f"SELECT d.* FROM dq_result d {where} ORDER BY d.created_at DESC LIMIT ?",
                [*params, row_limit],
            ).fetchdf())
    effective = max((coerce_date(row.get("created_at")) for row in rows), default=None)
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if rows else AS_OF_NO_DATA)
    return envelope(rows, source="control_plane.duckdb:dq_result", as_of_status=status,
                    as_of_requested=as_of, as_of_effective=effective, date_fields=("created_at",),
                    run_id=run_id, stage=stage, severity=severity,
                    truncated=len(rows) >= row_limit, data_domain=ctx.paths.domain)


def get_artifact_lineage(
    ctx: McpContext, *, run_id: str | None = None,
    artifact_type: str | None = None, as_of: str | date | None = None,
    limit: int | None = 250,
) -> dict[str, Any]:
    cutoff = coerce_date(as_of)
    row_limit = clamp_limit(limit, default=250, maximum=2000)
    with ctx.control_plane() as conn:
        if not _exists(conn, "pipeline_artifact"):
            rows = []
        else:
            clauses = ["s.status='completed'"]
            params: list[Any] = []
            if run_id:
                clauses.append("a.run_id=?")
                params.append(run_id)
            if artifact_type:
                clauses.append("a.artifact_type=?")
                params.append(artifact_type)
            if cutoff:
                clauses.append("CAST(a.created_at AS DATE)<=CAST(? AS DATE)")
                params.append(cutoff.isoformat())
                clauses.append("CAST(s.ended_at AS DATE)<=CAST(? AS DATE)")
                params.append(cutoff.isoformat())
            lifecycle = "a.lifecycle_status" if _exists(conn, "pipeline_artifact") and any(row[1] == "lifecycle_status" for row in conn.execute("PRAGMA table_info('pipeline_artifact')").fetchall()) else "NULL"
            rows = _records(conn.execute(
                f"""SELECT a.artifact_id, a.run_id, a.stage_name, a.attempt_number,
                    a.artifact_type, a.uri, a.content_hash, a.row_count, a.created_at,
                    a.metadata_json, {lifecycle} AS lifecycle_status,
                    s.status AS producer_status, s.ended_at AS producer_completed_at
                    FROM pipeline_artifact a JOIN pipeline_stage_run s
                      ON s.run_id=a.run_id AND s.stage_name=a.stage_name
                     AND s.attempt_number=a.attempt_number
                    WHERE {' AND '.join(clauses)}
                    ORDER BY a.created_at DESC LIMIT ?""",
                [*params, row_limit],
            ).fetchdf())
    effective = max((coerce_date(row.get("created_at")) for row in rows), default=None)
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if rows else AS_OF_NO_DATA)
    return envelope(rows, source="control_plane.duckdb:pipeline_artifact + pipeline_stage_run",
                    as_of_status=status, as_of_requested=as_of, as_of_effective=effective,
                    date_fields=("created_at", "producer_completed_at"), run_id=run_id,
                    artifact_type=artifact_type, truncated=len(rows) >= row_limit,
                    data_domain=ctx.paths.domain)


def get_data_freshness(
    ctx: McpContext, *, as_of: str | date | None = None,
) -> dict[str, Any]:
    cutoff = coerce_date(as_of)
    rows: list[dict[str, Any]] = []
    with ctx.control_plane() as conn:
        for surface, table, column in (
            ("rank_shortlist", decisions.RANK_TABLE, "trade_date"),
            ("rank_full_universe", decisions.RANK_UNIVERSE_TABLE, "trade_date"),
            ("operational_pattern", decisions.PATTERN_TABLE, "trade_date"),
            ("weekly_stage", "weekly_stock_stage_history", "as_of"),
        ):
            if not _exists(conn, table):
                rows.append({"surface": surface, "latest_date": None, "freshness_status": "MISSING"})
                continue
            if cutoff:
                value = conn.execute(f"SELECT MAX({column}) FROM {table} WHERE {column}<=CAST(? AS DATE)", [cutoff.isoformat()]).fetchone()[0]
            else:
                value = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()[0]
            observed = coerce_date(value)
            age = ((cutoff or date.today()) - observed).days if observed else None
            rows.append({"surface": surface, "latest_date": observed.isoformat() if observed else None,
                         "age_days": age, "freshness_status": "CURRENT" if age is not None and age <= 7 else ("STALE" if age is not None else "MISSING")})
    try:
        with ctx.fundamentals() as conn:
            if _exists(conn, "fundamental_thesis_projection"):
                if cutoff:
                    value = conn.execute("SELECT MAX(as_of) FROM fundamental_thesis_projection WHERE as_of<=CAST(? AS DATE) AND CAST(created_at AS DATE)<=CAST(? AS DATE)", [cutoff.isoformat(), cutoff.isoformat()]).fetchone()[0]
                else:
                    value = conn.execute("SELECT MAX(as_of) FROM fundamental_thesis_projection").fetchone()[0]
                observed = coerce_date(value)
                age = ((cutoff or date.today()) - observed).days if observed else None
                rows.append({"surface": "fundamental_discovery", "latest_date": observed.isoformat() if observed else None,
                             "age_days": age, "freshness_status": "CURRENT" if age is not None and age <= 7 else ("STALE" if age is not None else "MISSING")})
    except StoreUnavailableError:
        rows.append({"surface": "fundamental_discovery", "latest_date": None, "freshness_status": "MISSING"})
    observed_dates = [
        value
        for value in (coerce_date(row.get("latest_date")) for row in rows)
        if value is not None
    ]
    effective = max(observed_dates, default=None)
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if effective else AS_OF_NO_DATA)
    return envelope(rows, source="composed freshness read", as_of_status=status,
                    as_of_requested=as_of, as_of_effective=effective, date_fields=("latest_date",),
                    policy_version="mcp-data-freshness-v1", data_domain=ctx.paths.domain)


__all__ = ["get_artifact_lineage", "get_data_freshness", "get_data_quality_status", "get_pipeline_run"]
