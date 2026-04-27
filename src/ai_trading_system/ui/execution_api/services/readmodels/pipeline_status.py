"""Read models for operational health, trust, and summary surfaces."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb
import pandas as pd

from ai_trading_system.analytics.data_trust import load_data_trust_summary
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.ui.execution_api.services.control_center import get_recent_runs
from ai_trading_system.ui.execution_api.services.readmodels.latest_operational_snapshot import (
    LatestOperationalSnapshot,
    ExecutionContext,
    get_execution_context,
    load_latest_operational_snapshot,
)

_SCHEMA_REPAIR_HINT = "Run `ai-trading-repair-ingest-schema --apply`."


def _schema_check_detail(table_name: str, swapped_rows: int) -> str:
    detail = f"Swapped {table_name} rows: {swapped_rows}"
    if swapped_rows > 0:
        return f"{detail}. {_SCHEMA_REPAIR_HINT}"
    return detail


def get_execution_db_stats(project_root: str | Path | None = None) -> dict[str, Any]:
    ctx = get_execution_context(project_root)
    try:
        conn = duckdb.connect(str(ctx.ohlcv_db), read_only=True)
        try:
            total_rows = conn.execute("SELECT COUNT(*) FROM _catalog").fetchone()[0]
            total_syms = conn.execute(
                "SELECT COUNT(DISTINCT symbol_id) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
            latest = conn.execute(
                "SELECT MAX(timestamp) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
        finally:
            conn.close()
        return {
            "rows": int(total_rows),
            "symbols": int(total_syms),
            "latest_date": str(latest)[:10] if latest else None,
        }
    except Exception as exc:
        return {"rows": 0, "symbols": 0, "latest_date": None, "error": str(exc)}


def get_execution_health(
    project_root: str | Path | None = None,
    *,
    snapshot: Optional[LatestOperationalSnapshot] = None,
) -> dict[str, Any]:
    current_snapshot = snapshot or load_latest_operational_snapshot(project_root)
    ctx = current_snapshot.context
    payload = current_snapshot.payload
    checks: list[dict[str, object]] = []
    summary: dict[str, object] = {}

    pending_symbols = 0
    unexpected_symbols = 0
    conn = duckdb.connect(str(ctx.ohlcv_db), read_only=True)
    try:
        latest_ohlcv = conn.execute(
            "SELECT MAX(CAST(timestamp AS DATE)) FROM _catalog WHERE exchange = 'NSE'"
        ).fetchone()[0]
        latest_delivery = conn.execute(
            "SELECT MAX(CAST(timestamp AS DATE)) FROM _delivery WHERE exchange = 'NSE'"
        ).fetchone()[0]
        swapped_catalog = conn.execute(
            "SELECT COUNT(*) FROM _catalog WHERE symbol_id IN ('NSE','BSE') AND exchange NOT IN ('NSE','BSE')"
        ).fetchone()[0]
        swapped_delivery = conn.execute(
            "SELECT COUNT(*) FROM _delivery WHERE symbol_id IN ('NSE','BSE') AND exchange NOT IN ('NSE','BSE')"
        ).fetchone()[0]
        catalog_symbols = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT symbol_id FROM _catalog WHERE exchange = 'NSE'"
            ).fetchall()
        }
    finally:
        conn.close()

    try:
        master_conn = sqlite3.connect(ctx.master_db.as_posix())
        try:
            master_symbols = {
                row[0]
                for row in master_conn.execute(
                    "SELECT DISTINCT symbol_id FROM symbols WHERE exchange = 'NSE'"
                ).fetchall()
            }
        finally:
            master_conn.close()
        pending_symbols = len(master_symbols - catalog_symbols)
        unexpected_symbols = len(catalog_symbols - master_symbols)
    except Exception as exc:
        checks.append({"name": "universe_alignment", "status": "error", "detail": str(exc)})

    latest_payload_path = payload.get("_artifact_path")
    payload_age_minutes = None
    if latest_payload_path and Path(latest_payload_path).exists():
        latest_payload_time = datetime.fromtimestamp(Path(latest_payload_path).stat().st_mtime)
        payload_age_minutes = round((datetime.now() - latest_payload_time).total_seconds() / 60, 1)

    delivery_lag_days = None
    if latest_ohlcv and latest_delivery:
        delivery_lag_days = (pd.Timestamp(latest_ohlcv) - pd.Timestamp(latest_delivery)).days

    checks.extend(
        [
            {
                "name": "pipeline_payload",
                "status": "ok" if latest_payload_path else "warn",
                "detail": latest_payload_path or "No dashboard payload found",
            },
            {
                "name": "delivery_freshness",
                "status": "ok" if delivery_lag_days is not None and delivery_lag_days <= 3 else "warn",
                "detail": f"Delivery lag: {delivery_lag_days} day(s)" if delivery_lag_days is not None else "No delivery data",
            },
            {
                "name": "catalog_schema",
                "status": "ok" if swapped_catalog == 0 else "error",
                "detail": _schema_check_detail("_catalog", int(swapped_catalog)),
            },
            {
                "name": "delivery_schema",
                "status": "ok" if swapped_delivery == 0 else "error",
                "detail": _schema_check_detail("_delivery", int(swapped_delivery)),
            },
            {
                "name": "universe_alignment",
                "status": "ok" if pending_symbols == 0 and unexpected_symbols == 0 else "warn",
                "detail": f"Pending symbols: {pending_symbols}, unexpected symbols: {unexpected_symbols}",
            },
        ]
    )

    overall_status = "ok"
    if any(check["status"] == "error" for check in checks):
        overall_status = "error"
    elif any(check["status"] == "warn" for check in checks):
        overall_status = "warn"

    summary.update(
        {
            "latest_ohlcv_date": str(latest_ohlcv) if latest_ohlcv else None,
            "latest_delivery_date": str(latest_delivery) if latest_delivery else None,
            "delivery_lag_days": delivery_lag_days,
            "payload_age_minutes": payload_age_minutes,
            "pending_symbol_count": int(pending_symbols),
            "unexpected_symbol_count": int(unexpected_symbols),
        }
    )
    return {"status": overall_status, "summary": summary, "checks": checks}


def get_execution_ops_health_snapshot(
    project_root: str | Path | None = None,
    stale_threshold_hours: dict[str, float] | None = None,
) -> dict[str, Any]:
    ctx = get_execution_context(project_root)
    thresholds = stale_threshold_hours or {
        "ingest": 36.0,
        "features": 36.0,
        "rank": 24.0,
        "execute": 24.0,
        "publish": 48.0,
    }
    db_path = ctx.project_root / "data" / "control_plane.duckdb"
    if not db_path.exists():
        return {"available": False, "error": "control_plane.duckdb missing"}

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        stage_rows = conn.execute(
            """
            WITH latest AS (
                SELECT
                    stage_name,
                    run_id,
                    started_at,
                    ended_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY stage_name
                        ORDER BY COALESCE(ended_at, started_at) DESC
                    ) AS rn
                FROM pipeline_stage_run
                WHERE status = 'completed'
                  AND stage_name IN ('ingest', 'features', 'rank', 'execute', 'publish')
            )
            SELECT stage_name, run_id, started_at, ended_at
            FROM latest
            WHERE rn = 1
            """
        ).fetchall()
    finally:
        conn.close()

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    stage_names = ("ingest", "features", "rank", "execute", "publish")
    stages: dict[str, dict[str, object]] = {
        stage_name: {
            "stage_name": stage_name,
            "run_id": None,
            "ended_at": None,
            "age_hours": None,
            "stale": True,
        }
        for stage_name in stage_names
    }
    stale_stages: list[str] = []
    for stage_name, run_id, started_at, ended_at in stage_rows:
        end_ts = ended_at or started_at
        if end_ts is None:
            continue
        age_hours = max((now_utc - end_ts).total_seconds() / 3600.0, 0.0)
        stale = age_hours > float(thresholds.get(stage_name, 24.0))
        stages[str(stage_name)] = {
            "stage_name": stage_name,
            "run_id": run_id,
            "ended_at": end_ts.isoformat() if hasattr(end_ts, "isoformat") else str(end_ts),
            "age_hours": round(age_hours, 2),
            "stale": stale,
        }
        if stale:
            stale_stages.append(str(stage_name))

    latest_rank_run = stages.get("rank", {}).get("run_id")
    dq_summary = {"run_id": latest_rank_run, "failed_by_severity": {}, "total_failed": 0}
    if latest_rank_run:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            dq_rows = conn.execute(
                """
                SELECT severity, status, COUNT(*) AS cnt
                FROM dq_result
                WHERE run_id = ?
                GROUP BY severity, status
                """,
                [latest_rank_run],
            ).fetchall()
        finally:
            conn.close()
        failed_by_severity: dict[str, int] = {}
        total_failed = 0
        for severity, status, cnt in dq_rows:
            if status != "failed":
                continue
            failed_by_severity[str(severity)] = int(cnt)
            total_failed += int(cnt)
        dq_summary = {
            "run_id": latest_rank_run,
            "failed_by_severity": failed_by_severity,
            "total_failed": total_failed,
        }

    return {
        "available": True,
        "stages": stages,
        "stale_stages": stale_stages,
        "dq_summary": dq_summary,
        "generated_at": now_utc.isoformat(),
    }


def get_execution_data_trust_snapshot(project_root: str | Path | None = None) -> dict[str, Any]:
    ctx = get_execution_context(project_root)
    summary = load_data_trust_summary(ctx.ohlcv_db)
    registry = RegistryStore(ctx.project_root)
    summary["latest_repair_run"] = registry.get_latest_data_repair_run("NSE")
    return summary


def get_execution_summary_read_model(
    project_root: str | Path,
    *,
    tasks: list[dict[str, Any]],
) -> dict[str, Any]:
    root = Path(project_root)
    snapshot = load_latest_operational_snapshot(root)
    health = get_execution_health(root, snapshot=snapshot)
    db_stats = get_execution_db_stats(root)
    recent_runs = get_recent_runs(root, limit=1)
    latest_run = recent_runs[0] if recent_runs else None
    return {
        "db_stats": db_stats,
        "health": health,
        "latest_run": latest_run,
        "active_task_count": len([row for row in tasks if row.get("status") == "running"]),
        "task_count": len(tasks),
        "payload": {
            "artifact_path": snapshot.payload.get("_artifact_path"),
            "summary": snapshot.payload.get("summary", {}),
            "metadata": snapshot.payload.get("metadata", {}),
        },
    }
