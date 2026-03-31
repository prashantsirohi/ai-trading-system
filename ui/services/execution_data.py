"""Shared read services for execution-facing UI surfaces."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import duckdb
import pandas as pd

from analytics.registry import RegistryStore
from core.paths import get_domain_paths


@dataclass(frozen=True)
class ExecutionContext:
    project_root: Path
    ohlcv_db: Path
    master_db: Path
    pipeline_runs_dir: Path


def get_execution_context(project_root: str | Path | None = None) -> ExecutionContext:
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[2]
    paths = get_domain_paths(root, "operational")
    return ExecutionContext(
        project_root=root,
        ohlcv_db=paths.ohlcv_db_path,
        master_db=paths.master_db_path,
        pipeline_runs_dir=paths.pipeline_runs_dir,
    )


def get_execution_db_stats(project_root: str | Path | None = None) -> Dict:
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


def _load_latest_payload_path(ctx: ExecutionContext) -> Optional[Path]:
    runs_dir = ctx.pipeline_runs_dir
    if not runs_dir.exists():
        return None
    candidates = sorted(
        runs_dir.glob("*/rank/attempt_*/dashboard_payload.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None

    control_plane_db = ctx.project_root / "data" / "control_plane.duckdb"
    run_metadata: dict[str, dict] = {}
    if control_plane_db.exists():
        conn = duckdb.connect(str(control_plane_db), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT run_id, metadata_json
                FROM pipeline_run
                """
            ).fetchall()
            for run_id, metadata_json in rows:
                try:
                    run_metadata[run_id] = json.loads(metadata_json) if metadata_json else {}
                except Exception:
                    run_metadata[run_id] = {}
        finally:
            conn.close()

    def _is_live_operational_payload(path: Path) -> bool:
        run_id = path.parts[-4]
        metadata = run_metadata.get(run_id, {})
        params = metadata.get("params", {}) if isinstance(metadata, dict) else {}
        if params.get("smoke") is True:
            return False
        if params.get("canary") is True:
            return False
        return True

    for candidate in candidates:
        if _is_live_operational_payload(candidate):
            return candidate
    return candidates[0]


def _load_latest_rank_attempt_dir(ctx: ExecutionContext) -> Optional[Path]:
    payload_path = _load_latest_payload_path(ctx)
    if payload_path is None:
        return None
    return payload_path.parent


def load_execution_payload(project_root: str | Path | None = None) -> Dict:
    ctx = get_execution_context(project_root)
    payload_path = _load_latest_payload_path(ctx)
    if payload_path is None:
        return {}
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["_artifact_path"] = str(payload_path)
    return payload


def load_latest_rank_frames(project_root: str | Path | None = None) -> Dict[str, pd.DataFrame]:
    ctx = get_execution_context(project_root)
    rank_dir = _load_latest_rank_attempt_dir(ctx)
    frame_names = {
        "ranked_signals": "ranked_signals.csv",
        "breakout_scan": "breakout_scan.csv",
        "stock_scan": "stock_scan.csv",
        "sector_dashboard": "sector_dashboard.csv",
    }
    frames: Dict[str, pd.DataFrame] = {}
    if rank_dir is None:
        return {key: pd.DataFrame() for key in frame_names}
    for key, filename in frame_names.items():
        path = rank_dir / filename
        if not path.exists():
            frames[key] = pd.DataFrame()
            continue
        try:
            frames[key] = pd.read_csv(path)
        except Exception:
            frames[key] = pd.DataFrame()
    return frames


def get_execution_health(project_root: str | Path | None = None, payload: Optional[Dict] = None) -> Dict:
    ctx = get_execution_context(project_root)
    checks: list[dict[str, object]] = []
    summary: dict[str, object] = {}
    payload = payload or {}

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
                    "SELECT DISTINCT Symbol FROM stock_details WHERE exchange = 'NSE'"
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
                "detail": f"Swapped _catalog rows: {swapped_catalog}",
            },
            {
                "name": "delivery_schema",
                "status": "ok" if swapped_delivery == 0 else "error",
                "detail": f"Swapped _delivery rows: {swapped_delivery}",
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


def load_shadow_overlay_frame(project_root: str | Path | None = None) -> pd.DataFrame:
    registry = RegistryStore(Path(project_root) if project_root else Path(__file__).resolve().parents[2])
    rows = registry.get_shadow_overlay()
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame["prediction_date"] = pd.to_datetime(frame["prediction_date"])
    return frame


def load_shadow_summary_frame(
    grain: str,
    horizon: int,
    *,
    periods: int = 12,
    project_root: str | Path | None = None,
) -> pd.DataFrame:
    registry = RegistryStore(Path(project_root) if project_root else Path(__file__).resolve().parents[2])
    rows = registry.get_shadow_period_summary(grain=grain, horizon=horizon, periods=periods)
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame["period_start"] = pd.to_datetime(frame["period_start"])
    return frame


def pivot_shadow_summary_frame(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df
    pivoted = summary_df.pivot(index="period_start", columns="variant", values=["picks", "hit_rate", "avg_return"])
    pivoted.columns = [f"{metric}_{variant}" for metric, variant in pivoted.columns]
    return pivoted.reset_index().sort_values("period_start", ascending=False)
