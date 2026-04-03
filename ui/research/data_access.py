"""Query/data-access helpers for the research Streamlit dashboard."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import duckdb
import pandas as pd
import streamlit as st
from core.paths import get_domain_paths


STAGE_NAMES = ("ingest", "features", "rank", "publish")


def _safe_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _parse_run_id_from_path(path: Path) -> str:
    parts = path.parts
    if len(parts) < 5:
        return ""
    try:
        return parts[-4]
    except Exception:
        return ""


def _run_date_from_run_id(run_id: str) -> str | None:
    match = re.match(r"^pipeline-(\d{4}-\d{2}-\d{2})-", str(run_id))
    return match.group(1) if match else None


def _get_pipeline_runs_dir(project_root: str) -> Path:
    paths = get_domain_paths(project_root, "operational")
    return Path(paths.pipeline_runs_dir)


def _load_latest_payload_path(project_root: str) -> Path | None:
    runs_dir = _get_pipeline_runs_dir(project_root)
    if not runs_dir.exists():
        return None

    candidates = sorted(
        runs_dir.glob("*/rank/attempt_*/dashboard_payload.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None

    control_plane_db = Path(project_root) / "data" / "control_plane.duckdb"
    run_metadata: dict[str, dict] = {}
    if control_plane_db.exists():
        conn = duckdb.connect(str(control_plane_db), read_only=True)
        try:
            rows = conn.execute("SELECT run_id, metadata_json FROM pipeline_run").fetchall()
            for run_id, metadata_json in rows:
                try:
                    run_metadata[str(run_id)] = json.loads(metadata_json) if metadata_json else {}
                except Exception:
                    run_metadata[str(run_id)] = {}
        finally:
            conn.close()

    def _is_live_operational_payload(path: Path) -> bool:
        run_id = _parse_run_id_from_path(path)
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


@st.cache_data(show_spinner=False, ttl=60 * 3)
def load_latest_rank_frames(project_root: str) -> Dict[str, pd.DataFrame]:
    """Load latest rank-stage CSV artifacts without importing ui.services package."""
    payload_path = _load_latest_payload_path(project_root)
    rank_dir: Path | None = payload_path.parent if payload_path is not None else None

    frame_names = {
        "ranked_signals": "ranked_signals.csv",
        "breakout_scan": "breakout_scan.csv",
        "stock_scan": "stock_scan.csv",
        "sector_dashboard": "sector_dashboard.csv",
    }
    if rank_dir is None:
        runs_dir = _get_pipeline_runs_dir(project_root)
        ranked_candidates = sorted(
            runs_dir.glob("*/rank/attempt_*/ranked_signals.csv"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        rank_dir = ranked_candidates[0].parent if ranked_candidates else None

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


@st.cache_data(show_spinner=False, ttl=60 * 5)
def load_recent_rank_paths(pipeline_runs_dir: str, max_runs: int = 40) -> List[str]:
    """Return recent ranked_signals artifact paths (one per run)."""
    runs_dir = Path(pipeline_runs_dir)
    if not runs_dir.exists():
        return []

    candidates = sorted(
        runs_dir.glob("*/rank/attempt_*/ranked_signals.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return []

    selected: list[str] = []
    seen_runs: set[str] = set()
    for path in candidates:
        run_id = _parse_run_id_from_path(path)
        if not run_id or run_id in seen_runs:
            continue
        selected.append(path.as_posix())
        seen_runs.add(run_id)
        if len(selected) >= max_runs:
            break
    return selected


@st.cache_data(show_spinner=False, ttl=60 * 5)
def load_rank_history_for_symbols(
    pipeline_runs_dir: str,
    symbols: Iterable[str],
    max_runs: int = 40,
) -> pd.DataFrame:
    """Load rank-position history for the requested symbols across recent rank runs."""
    symbol_list = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if not symbol_list:
        return pd.DataFrame(
            columns=["run_id", "symbol_id", "composite_score", "rank_position", "run_order", "run_date"]
        )

    path_strings = load_recent_rank_paths(pipeline_runs_dir, max_runs=max_runs)
    if not path_strings:
        return pd.DataFrame(
            columns=["run_id", "symbol_id", "composite_score", "rank_position", "run_order", "run_date"]
        )

    path_sql = "[" + ", ".join(f"'{_safe_sql_literal(path)}'" for path in path_strings) + "]"
    symbols_sql = "(" + ", ".join(f"'{_safe_sql_literal(symbol)}'" for symbol in symbol_list) + ")"
    query = f"""
        WITH raw AS (
            SELECT
                filename,
                symbol_id,
                composite_score
            FROM read_csv_auto({path_sql}, filename = true)
            WHERE symbol_id IN {symbols_sql}
        ),
        ranked AS (
            SELECT
                regexp_extract(filename, '/pipeline_runs/([^/]+)/rank/', 1) AS run_id,
                symbol_id,
                composite_score,
                ROW_NUMBER() OVER (
                    PARTITION BY regexp_extract(filename, '/pipeline_runs/([^/]+)/rank/', 1)
                    ORDER BY composite_score DESC NULLS LAST
                ) AS rank_position
            FROM raw
        )
        SELECT run_id, symbol_id, composite_score, rank_position
        FROM ranked
        WHERE run_id IS NOT NULL AND run_id <> ''
    """

    conn = duckdb.connect()
    try:
        history_df = conn.execute(query).fetchdf()
    finally:
        conn.close()

    if history_df.empty:
        return pd.DataFrame(
            columns=["run_id", "symbol_id", "composite_score", "rank_position", "run_order", "run_date"]
        )

    run_id_to_order: dict[str, int] = {}
    for idx, path_string in enumerate(reversed(path_strings)):
        run_id = _parse_run_id_from_path(Path(path_string))
        if run_id and run_id not in run_id_to_order:
            run_id_to_order[run_id] = idx

    history_df["symbol_id"] = history_df["symbol_id"].astype(str).str.upper()
    history_df["run_order"] = history_df["run_id"].map(run_id_to_order).fillna(-1).astype(int)
    history_df["run_date"] = history_df["run_id"].map(_run_date_from_run_id)
    history_df["run_date"] = pd.to_datetime(history_df["run_date"], errors="coerce")
    history_df = history_df.sort_values(["run_order", "run_id", "rank_position"]).reset_index(drop=True)
    return history_df


@st.cache_data(show_spinner=False, ttl=60 * 5)
def load_ops_health_snapshot(
    project_root: str,
    stale_threshold_hours: dict[str, float] | None = None,
) -> Dict[str, object]:
    """Read control-plane stage freshness + DQ summary for top-of-page ribbon."""
    thresholds = stale_threshold_hours or {
        "ingest": 36.0,
        "features": 36.0,
        "rank": 24.0,
        "publish": 48.0,
    }
    db_path = Path(project_root) / "data" / "control_plane.duckdb"
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
                  AND stage_name IN ('ingest', 'features', 'rank', 'publish')
            )
            SELECT stage_name, run_id, started_at, ended_at
            FROM latest
            WHERE rn = 1
            """
        ).fetchall()
    finally:
        conn.close()

    now_utc = datetime.utcnow()
    stages: dict[str, dict[str, object]] = {}
    stale_stages: list[str] = []
    for stage_name in STAGE_NAMES:
        stages[stage_name] = {
            "stage_name": stage_name,
            "run_id": None,
            "ended_at": None,
            "age_hours": None,
            "stale": True,
        }

    for stage_name, run_id, started_at, ended_at in stage_rows:
        end_ts = ended_at or started_at
        if end_ts is None:
            continue
        age_hours = max((now_utc - end_ts).total_seconds() / 3600.0, 0.0)
        stale = age_hours > float(thresholds.get(stage_name, 24.0))
        stages[stage_name] = {
            "stage_name": stage_name,
            "run_id": run_id,
            "ended_at": end_ts,
            "age_hours": round(age_hours, 2),
            "stale": stale,
        }
        if stale:
            stale_stages.append(stage_name)

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
