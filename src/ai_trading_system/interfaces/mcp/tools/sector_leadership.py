"""Latest-only sector leadership composed from promoted rank and fundamental evidence."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_LATEST, AS_OF_UNSUPPORTED, envelope, json_safe,
)


def _exists(conn: Any, table: str) -> bool:
    return conn.execute("SELECT 1 FROM information_schema.tables WHERE table_name=?", [table]).fetchone() is not None


def _key(value: Any) -> str:
    return " ".join(str(value or "").upper().split())


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [{str(k): json_safe(v) for k, v in row.items()} for row in frame.to_dict(orient="records")]


def _promoted_sector_artifacts(ctx: McpContext) -> tuple[list[dict[str, Any]], list[str]]:
    rows: dict[str, dict[str, Any]] = {}
    notes: list[str] = []
    with ctx.control_plane() as conn:
        if not (_exists(conn, "pipeline_artifact") and _exists(conn, "pipeline_stage_run")):
            return [], ["Pipeline artifact registry is unavailable."]
        columns = {row[1] for row in conn.execute("PRAGMA table_info('pipeline_artifact')").fetchall()}
        lifecycle = "AND a.lifecycle_status='promoted'" if "lifecycle_status" in columns else ""
        artifacts = conn.execute(
            f"""SELECT a.artifact_type, a.uri, a.created_at
                FROM pipeline_artifact a JOIN pipeline_stage_run s
                  ON s.run_id=a.run_id AND s.stage_name=a.stage_name AND s.attempt_number=a.attempt_number
                WHERE s.status='completed' {lifecycle}
                  AND a.artifact_type IN ('sector_dashboard','sector_rotation')
                QUALIFY ROW_NUMBER() OVER (PARTITION BY a.artifact_type ORDER BY a.created_at DESC)=1"""
        ).fetchall()
    for artifact_type, uri, _created in artifacts:
        path = Path(str(uri))
        if not path.is_file():
            notes.append(f"Registered {artifact_type} artifact is unavailable at its recorded path.")
            continue
        try:
            frame = pd.read_csv(path, nrows=10000)
        except Exception as exc:
            notes.append(f"Could not read registered {artifact_type}: {exc}")
            continue
        sector_column = next((name for name in ("sector_name", "sector", "sector_id") if name in frame.columns), None)
        if not sector_column:
            notes.append(f"Registered {artifact_type} has no sector identity column.")
            continue
        for record in _records(frame):
            key = _key(record.get(sector_column))
            if not key:
                continue
            target = rows.setdefault(key, {"sector_name": record.get(sector_column)})
            target[artifact_type] = record
    return list(rows.values()), notes


def get_sector_leadership(
    ctx: McpContext, *, exchange: str = "NSE",
    as_of: str | date | None = None, limit: int = 100,
) -> dict[str, Any]:
    exchange_code = ctx.resolve_exchange(exchange)
    if as_of is not None:
        return envelope(
            [], source="latest promoted sector artifacts + fundamentals.duckdb",
            as_of_status=AS_OF_UNSUPPORTED, as_of_requested=as_of,
            notes=["Sector RS/momentum/rotation artifacts have no publication history; latest data was not substituted."],
            exchange=exchange_code, data_domain=ctx.paths.domain,
        )
    artifact_rows, notes = _promoted_sector_artifacts(ctx)
    merged = {_key(row["sector_name"]): row for row in artifact_rows}
    with ctx.fundamentals() as conn:
        if _exists(conn, "sector_earnings_leadership"):
            earnings = _records(conn.execute(
                """SELECT * FROM sector_earnings_leadership
                   WHERE report_date=(SELECT MAX(report_date) FROM sector_earnings_leadership)
                   ORDER BY sector_fundamental_score DESC"""
            ).fetchdf())
            for record in earnings:
                target = merged.setdefault(_key(record.get("sector_name")), {"sector_name": record.get("sector_name")})
                target["earnings"] = record
        if _exists(conn, "valuation_cycle_features"):
            valuation = _records(conn.execute(
                """SELECT * FROM valuation_cycle_features
                   WHERE UPPER(entity_type)='SECTOR'
                     AND date=(SELECT MAX(date) FROM valuation_cycle_features WHERE UPPER(entity_type)='SECTOR')"""
            ).fetchdf())
            for record in valuation:
                target = merged.setdefault(_key(record.get("entity_id")), {"sector_name": record.get("entity_id")})
                target["valuation"] = record
    rows = list(merged.values())[: max(1, min(int(limit), 500))]
    for row in rows:
        dashboard = row.get("sector_dashboard") or {}
        rotation = row.get("sector_rotation") or {}
        row["relative_strength"] = {k: v for k, v in dashboard.items() if "rs" in k.lower() or "relative" in k.lower()}
        row["momentum"] = {k: v for k, v in dashboard.items() if "momentum" in k.lower()}
        row["quadrant"] = next((v for k, v in {**dashboard, **rotation}.items() if "quadrant" in k.lower()), None)
    return envelope(
        rows, source="latest promoted sector_dashboard/sector_rotation + fundamentals.duckdb:sector_earnings_leadership/valuation_cycle_features",
        as_of_status=AS_OF_LATEST, notes=notes, exchange=exchange_code,
        latest_only=True, truncated=len(merged) > len(rows), data_domain=ctx.paths.domain,
    )


__all__ = ["get_sector_leadership"]
