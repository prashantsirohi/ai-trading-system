"""Readmodels for fundamentals pipeline artifacts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from pandas.errors import EmptyDataError


def get_latest_fundamentals(project_root: Path, *, limit: int = 25) -> dict[str, Any]:
    summary_path = _latest_summary_from_registry(project_root) or _latest_summary_from_disk(project_root)
    if summary_path is None:
        return {
            "snapshot_date": None,
            "stale_days": None,
            "tier_counts": {},
            "top_watchlist": [],
            "summary": {},
            "source_path": None,
            "generated_at": None,
            "industry_status": None,
            "industry_snapshot_date": None,
            "industry_label_counts": {},
            "industry_rows_scored": None,
            "industry_trend_status": None,
            "industry_trend_label_counts": {},
        }

    summary = _read_json(summary_path)
    watchlist_path = summary_path.parent / "watchlist_candidates.csv"
    top_watchlist = _read_top_watchlist(watchlist_path, limit=limit)
    generated_at = summary.get("generated_at") or _mtime_iso(summary_path)
    return {
        "snapshot_date": summary.get("snapshot_date"),
        "stale_days": summary.get("stale_days"),
        "tier_counts": summary.get("tier_counts") or {},
        "top_watchlist": top_watchlist,
        "summary": summary,
        "source_path": str(summary_path),
        "generated_at": generated_at,
        "industry_status": summary.get("industry_status"),
        "industry_snapshot_date": summary.get("industry_snapshot_date"),
        "industry_label_counts": summary.get("industry_label_counts") or {},
        "industry_rows_scored": summary.get("industry_rows_scored"),
        "industry_trend_status": summary.get("industry_trend_status"),
        "industry_trend_label_counts": summary.get("industry_trend_label_counts") or {},
    }


def _latest_summary_from_registry(project_root: Path) -> Path | None:
    db_path = project_root / "data" / "control_plane.duckdb"
    if not db_path.exists():
        return None
    conn = duckdb.connect(str(db_path))
    try:
        row = conn.execute(
            """
            SELECT a.uri
            FROM pipeline_artifact a
            JOIN pipeline_run r ON r.run_id = a.run_id
            WHERE a.stage_name = 'fundamentals'
              AND a.artifact_type = 'fundamental_summary'
            ORDER BY r.started_at DESC NULLS LAST, a.created_at DESC NULLS LAST
            LIMIT 1
            """
        ).fetchone()
    except duckdb.Error:
        return None
    finally:
        conn.close()
    if not row:
        return None
    path = Path(str(row[0]))
    return path if path.exists() else None


def _latest_summary_from_disk(project_root: Path) -> Path | None:
    candidates = list((project_root / "data" / "pipeline_runs").glob("*/fundamentals/attempt_*/fundamental_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_top_watchlist(path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        frame = pd.read_csv(path)
    except (EmptyDataError, OSError):
        return []
    if frame.empty:
        return []
    bucket = frame.get("watchlist_bucket", pd.Series("", index=frame.index)).astype(str)
    priority = bucket.ne("ADD_TO_WATCHLIST").astype(int)
    score = pd.to_numeric(frame.get("final_watchlist_score", pd.Series(0, index=frame.index)), errors="coerce").fillna(0)
    ordered = frame.assign(_priority=priority, _score=score).sort_values(
        ["_priority", "_score"],
        ascending=[True, False],
        kind="stable",
    )
    return ordered.drop(columns=["_priority", "_score"], errors="ignore").head(int(limit)).to_dict(orient="records")


def _mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
