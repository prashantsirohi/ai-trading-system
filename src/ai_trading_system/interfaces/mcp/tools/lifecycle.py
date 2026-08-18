"""Read-only candidate, Investigator and opportunity-episode surfaces."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT, AS_OF_LATEST, AS_OF_NO_DATA, clamp_limit, coerce_date,
    envelope, json_safe,
)


def _exists(conn: Any, table: str) -> bool:
    return conn.execute("SELECT 1 FROM information_schema.tables WHERE table_name=?", [table]).fetchone() is not None


def _columns(conn: Any, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()}


def _rows(frame: Any) -> list[dict[str, Any]]:
    output = []
    for raw in frame.to_dict(orient="records"):
        row = {str(k): json_safe(v) for k, v in raw.items()}
        for key, value in list(row.items()):
            if key.endswith("_json") and isinstance(value, str):
                try:
                    row[key[:-5]] = json.loads(value)
                except (TypeError, ValueError):
                    pass
        output.append(row)
    return output


def _episode_filter(symbol: str | None, exchange: str, candidate_id: str | None) -> tuple[list[str], list[Any]]:
    clauses, params = ["exchange=?"], [exchange]
    if candidate_id:
        clauses.append("candidate_id=?")
        params.append(candidate_id)
    if symbol:
        clauses.append("UPPER(symbol_id)=?")
        params.append(symbol)
    return clauses, params


def _episode_as_of(record: dict[str, Any], cutoff: date | None) -> dict[str, Any]:
    if cutoff is None:
        return record
    closed = coerce_date(record.get("episode_closed_at"))
    if closed and closed > cutoff:
        record = dict(record)
        record["episode_closed_at"] = None
        record["closed_run_id"] = None
        record["closed_stage"] = None
        record["closing_reason"] = None
        record["episode_status"] = "OPEN"
    return record


def get_candidate_status(
    ctx: McpContext, symbol: str, *, exchange: str = "NSE",
    as_of: str | date | None = None,
) -> dict[str, Any]:
    symbol_id, exchange_code, cutoff = ctx.normalize_symbol(symbol), ctx.resolve_exchange(exchange), coerce_date(as_of)
    with ctx.control_plane() as conn:
        if not _exists(conn, "candidate_episode"):
            data = None
        else:
            clauses, params = _episode_filter(symbol_id, exchange_code, None)
            if cutoff:
                clauses.extend(["CAST(episode_started_at AS DATE)<=CAST(? AS DATE)", "CAST(created_at AS DATE)<=CAST(? AS DATE)"])
                params.extend([cutoff.isoformat(), cutoff.isoformat()])
            episode = conn.execute(
                f"SELECT * FROM candidate_episode WHERE {' AND '.join(clauses)} ORDER BY episode_started_at DESC, created_at DESC LIMIT 1", params
            ).fetchdf()
            episode_rows = _rows(episode)
            if not episode_rows:
                data = None
            else:
                data = {"episode": _episode_as_of(episode_rows[0], cutoff), "latest_snapshot": None}
                candidate_id = episode_rows[0]["candidate_id"]
                if _exists(conn, "candidate_snapshot"):
                    snap_clauses, snap_params = ["candidate_id=?"], [candidate_id]
                    if cutoff:
                        snap_clauses.extend(["CAST(as_of AS DATE)<=CAST(? AS DATE)", "CAST(created_at AS DATE)<=CAST(? AS DATE)"])
                        snap_params.extend([cutoff.isoformat(), cutoff.isoformat()])
                    snapshots = _rows(conn.execute(
                        f"SELECT * FROM candidate_snapshot WHERE {' AND '.join(snap_clauses)} ORDER BY as_of DESC, observed_at DESC, created_at DESC LIMIT 1", snap_params
                    ).fetchdf())
                    data["latest_snapshot"] = snapshots[0] if snapshots else None
    effective = coerce_date((data or {}).get("latest_snapshot", {}).get("as_of") or (data or {}).get("episode", {}).get("episode_started_at"))
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if data else AS_OF_NO_DATA)
    return envelope(data, source="control_plane.duckdb:candidate_episode + candidate_snapshot",
                    as_of_status=status, as_of_requested=as_of, as_of_effective=effective,
                    symbol=symbol_id, exchange=exchange_code, data_domain=ctx.paths.domain)


def get_candidate_history(
    ctx: McpContext, *, symbol: str | None = None, candidate_id: str | None = None,
    exchange: str = "NSE", as_of: str | date | None = None,
    limit: int | None = 250,
) -> dict[str, Any]:
    if not symbol and not candidate_id:
        raise ValueError("Provide symbol or candidate_id")
    symbol_id = ctx.normalize_symbol(symbol) if symbol else None
    exchange_code, cutoff = ctx.resolve_exchange(exchange), coerce_date(as_of)
    row_limit = clamp_limit(limit, default=250, maximum=2000)
    with ctx.control_plane() as conn:
        if not _exists(conn, "candidate_episode"):
            rows = []
        else:
            episode_clauses, episode_params = _episode_filter(symbol_id, exchange_code, candidate_id)
            if cutoff:
                episode_clauses.append("CAST(episode_started_at AS DATE)<=CAST(? AS DATE)")
                episode_params.append(cutoff.isoformat())
                episode_clauses.append("CAST(created_at AS DATE)<=CAST(? AS DATE)")
                episode_params.append(cutoff.isoformat())
            episodes = _rows(conn.execute(
                f"SELECT * FROM candidate_episode WHERE {' AND '.join(episode_clauses)} ORDER BY episode_started_at LIMIT ?", [*episode_params, row_limit]
            ).fetchdf())
            rows = []
            for episode in episodes:
                item = {"episode": _episode_as_of(episode, cutoff), "transitions": [], "snapshots": []}
                cid = episode["candidate_id"]
                for table, key, order, target in (
                    ("candidate_transition", "transitioned_at", "transitioned_at", "transitions"),
                    ("candidate_snapshot", "as_of", "as_of, observed_at", "snapshots"),
                ):
                    if not _exists(conn, table):
                        continue
                    clauses, params = ["candidate_id=?"], [cid]
                    if cutoff:
                        clauses.append(f"CAST({key} AS DATE)<=CAST(? AS DATE)")
                        params.append(cutoff.isoformat())
                        if "created_at" in _columns(conn, table):
                            clauses.append("CAST(created_at AS DATE)<=CAST(? AS DATE)")
                            params.append(cutoff.isoformat())
                    item[target] = _rows(conn.execute(
                        f"SELECT * FROM {table} WHERE {' AND '.join(clauses)} ORDER BY {order} LIMIT ?", [*params, row_limit]
                    ).fetchdf())
                rows.append(item)
    effective = max((coerce_date(item["episode"].get("episode_started_at")) for item in rows), default=None)
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if rows else AS_OF_NO_DATA)
    return envelope(rows, source="control_plane.duckdb:canonical candidate registry",
                    as_of_status=status, as_of_requested=as_of, as_of_effective=effective,
                    symbol=symbol_id, candidate_id=candidate_id, exchange=exchange_code,
                    truncated=len(rows) >= row_limit, data_domain=ctx.paths.domain)


def get_investigator_evidence(
    ctx: McpContext, *, symbol: str | None = None, candidate_id: str | None = None,
    exchange: str = "NSE", as_of: str | date | None = None,
    limit: int | None = 250,
) -> dict[str, Any]:
    if not symbol and not candidate_id:
        raise ValueError("Provide symbol or candidate_id")
    symbol_id = ctx.normalize_symbol(symbol) if symbol else None
    exchange_code, cutoff = ctx.resolve_exchange(exchange), coerce_date(as_of)
    row_limit = clamp_limit(limit, default=250, maximum=2000)
    with ctx.control_plane() as conn:
        if not _exists(conn, "candidate_evidence_observation") or not _exists(conn, "candidate_episode"):
            rows = []
        else:
            clauses, params = ["e.exchange=?"], [exchange_code]
            if candidate_id:
                clauses.append("o.candidate_id=?")
                params.append(candidate_id)
            if symbol_id:
                clauses.append("UPPER(e.symbol_id)=?")
                params.append(symbol_id)
            clauses.append("(UPPER(o.evidence_type) LIKE '%INVESTIGATOR%' OR UPPER(o.source_module) LIKE '%INVESTIGATOR%')")
            if cutoff:
                clauses.extend(["CAST(o.as_of AS DATE)<=CAST(? AS DATE)", "CAST(o.created_at AS DATE)<=CAST(? AS DATE)"])
                params.extend([cutoff.isoformat(), cutoff.isoformat()])
            rows = _rows(conn.execute(
                f"""SELECT o.*, e.symbol_id, e.exchange, e.episode_type, e.setup_family
                    FROM candidate_evidence_observation o JOIN candidate_episode e USING(candidate_id)
                    WHERE {' AND '.join(clauses)} ORDER BY o.as_of DESC LIMIT ?""",
                [*params, row_limit],
            ).fetchdf())
    effective = max((coerce_date(row.get("as_of")) for row in rows), default=None)
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if rows else AS_OF_NO_DATA)
    return envelope(rows, source="control_plane.duckdb:candidate_evidence_observation",
                    as_of_status=status, as_of_requested=as_of, as_of_effective=effective,
                    symbol=symbol_id, candidate_id=candidate_id, exchange=exchange_code,
                    truncated=len(rows) >= row_limit, data_domain=ctx.paths.domain)


def get_opportunity_episode(
    ctx: McpContext, candidate_id: str, *, as_of: str | date | None = None,
    limit: int | None = 250,
) -> dict[str, Any]:
    cutoff = coerce_date(as_of)
    row_limit = clamp_limit(limit, default=250, maximum=2000)
    table_specs = (
        ("candidate_snapshot", "as_of", "snapshots"),
        ("candidate_transition", "transitioned_at", "transitions"),
        ("candidate_stage_observation", "as_of", "stage_observations"),
        ("candidate_evidence_observation", "as_of", "evidence_observations"),
        ("candidate_opportunity_observation", "as_of", "rank_observations"),
        ("candidate_fundamental_observation", "observed_at", "fundamental_observations"),
    )
    with ctx.control_plane() as conn:
        if not _exists(conn, "candidate_episode"):
            data = None
        else:
            clauses, params = ["candidate_id=?"], [candidate_id]
            if cutoff:
                clauses.extend([
                    "CAST(episode_started_at AS DATE)<=CAST(? AS DATE)",
                    "CAST(created_at AS DATE)<=CAST(? AS DATE)",
                ])
                params.extend([cutoff.isoformat(), cutoff.isoformat()])
            episode = _rows(conn.execute(
                f"SELECT * FROM candidate_episode WHERE {' AND '.join(clauses)}",
                params,
            ).fetchdf())
            if not episode:
                data = None
            else:
                data = {"episode": _episode_as_of(episode[0], cutoff)}
                for table, date_column, target in table_specs:
                    data[target] = []
                    if not _exists(conn, table):
                        continue
                    clauses, params = ["candidate_id=?"], [candidate_id]
                    if cutoff:
                        clauses.append(f"CAST({date_column} AS DATE)<=CAST(? AS DATE)")
                        params.append(cutoff.isoformat())
                        if "created_at" in _columns(conn, table):
                            clauses.append("CAST(created_at AS DATE)<=CAST(? AS DATE)")
                            params.append(cutoff.isoformat())
                    data[target] = _rows(conn.execute(
                        f"SELECT * FROM {table} WHERE {' AND '.join(clauses)} ORDER BY {date_column} LIMIT ?", [*params, row_limit]
                    ).fetchdf())
    effective = coerce_date((data or {}).get("episode", {}).get("episode_started_at"))
    status = AS_OF_LATEST if as_of is None else (AS_OF_EXACT if data else AS_OF_NO_DATA)
    return envelope(data, source="control_plane.duckdb:opportunity episode aggregate",
                    as_of_status=status, as_of_requested=as_of, as_of_effective=effective,
                    candidate_id=candidate_id, includes_fundamental_observations=True,
                    data_domain=ctx.paths.domain)


__all__ = ["get_candidate_history", "get_candidate_status", "get_investigator_evidence", "get_opportunity_episode"]
