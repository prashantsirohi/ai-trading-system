"""Read model for the stock investigator endpoint."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths, resolve_artifact_path


def get_investigator_snapshot(project_root: Path) -> dict[str, Any]:
    artifacts = _latest_artifacts(project_root)
    frames = {
        "today_gainers": _read_csv(artifacts.get("daily_gainer_log")),
        "scores": _read_csv(artifacts.get("investigator_scores")),
        "repeat_tracker": _read_csv(artifacts.get("repeat_tracker")),
        "trap_log": _read_csv(artifacts.get("trap_log")),
        "active_watchlist": _read_csv(artifacts.get("active_watchlist")),
        "archive": _read_csv(artifacts.get("archived_investigator")),
    }
    summary = _read_json(artifacts.get("investigator_summary"))
    scores = frames["scores"]
    high = (
        scores.loc[scores["verdict"].astype(str).eq("HIGH_CONVICTION")].copy()
        if not scores.empty and "verdict" in scores.columns
        else pd.DataFrame()
    )
    archive = frames["archive"]
    return {
        "summary": summary,
        "today_gainers": _records(frames["today_gainers"]),
        "high_conviction": _records(high),
        "repeat_tracker": _records(_sort_repeat_tracker(frames["repeat_tracker"])),
        "trap_log": _records(frames["trap_log"]),
        "active_watchlist": _records(_sort_active_watchlist(frames["active_watchlist"])),
        "archive_summary": {
            "count": int(len(archive)),
            "by_reason": _counts(archive, "drop_reason"),
            "rows": _records(archive, limit=100),
        },
        "source_artifacts": {key: str(value) for key, value in artifacts.items() if value is not None},
    }


def _latest_artifacts(project_root: Path) -> dict[str, Path | None]:
    out: dict[str, Path | None] = {}
    for artifact_type in (
        "daily_gainer_log",
        "investigator_scores",
        "repeat_tracker",
        "active_watchlist",
        "trap_log",
        "archived_investigator",
        "investigator_summary",
    ):
        out[artifact_type] = _latest_artifact_from_registry(project_root, artifact_type) or _latest_artifact_from_disk(
            project_root,
            artifact_type,
        )
    return out


def _latest_artifact_from_registry(project_root: Path, artifact_type: str) -> Path | None:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    db_path = paths.root_dir / "control_plane.duckdb"
    if not db_path.exists():
        return None
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        row = conn.execute(
            """
            SELECT a.uri
            FROM pipeline_artifact a
            JOIN pipeline_run r ON r.run_id = a.run_id
            WHERE a.stage_name = 'investigator'
              AND a.artifact_type = ?
              AND r.status = 'completed'
            ORDER BY r.started_at DESC NULLS LAST, a.created_at DESC NULLS LAST
            LIMIT 1
            """,
            [artifact_type],
        ).fetchone()
    except Exception:
        return None
    finally:
        if "conn" in locals():
            conn.close()
    if not row:
        return None
    path = resolve_artifact_path(str(row[0]), project_root=project_root)
    return path if path.exists() else None


def _latest_artifact_from_disk(project_root: Path, artifact_type: str) -> Path | None:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    filename = "investigator_summary.json" if artifact_type == "investigator_summary" else f"{artifact_type}.csv"
    candidates = list(paths.pipeline_runs_dir.glob(f"*/investigator/attempt_*/{filename}"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _records(frame: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    safe = frame.head(limit).copy() if limit else frame.copy()
    safe = safe.where(safe.notna(), None)
    return safe.to_dict(orient="records")


def _counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].fillna("").astype(str).value_counts().to_dict().items()}


def _sort_repeat_tracker(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    safe = frame.copy()
    priority_sort = (
        safe.get("high_priority_repeat", pd.Series(False, index=safe.index))
        .fillna(False)
        .astype(str)
        .str.lower()
        .isin({"true", "1", "yes"})
        .astype(int)
    )
    for column in ("repeat_score", "appearance_count_20d", "price_progression_pct"):
        safe.loc[:, column] = pd.to_numeric(safe[column], errors="coerce") if column in safe.columns else pd.NA
    safe = safe.assign(_priority_sort=priority_sort)
    safe = safe.sort_values(
        ["_priority_sort", "repeat_score", "appearance_count_20d", "price_progression_pct", "symbol_id"],
        ascending=[False, False, False, False, True],
        na_position="last",
        kind="stable",
    )
    return safe.drop(columns=["_priority_sort"]).reset_index(drop=True)


def _sort_active_watchlist(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    safe = frame.copy()
    verdict_order = {
        "HIGH_CONVICTION": 0,
        "MEDIUM_CONVICTION": 1,
        "WATCH_ONLY": 2,
        "NOISE_TRAP": 3,
    }
    verdict_sort = safe.get("verdict", pd.Series("", index=safe.index)).fillna("").astype(str).str.upper().map(verdict_order).fillna(99)
    for column in ("score_current", "score_peak", "appearance_count_20d"):
        safe.loc[:, column] = pd.to_numeric(safe[column], errors="coerce") if column in safe.columns else pd.NA
    safe = safe.assign(_verdict_sort=verdict_sort)
    safe = safe.sort_values(
        ["_verdict_sort", "score_current", "score_peak", "appearance_count_20d", "symbol_id"],
        ascending=[True, False, False, False, True],
        na_position="last",
        kind="stable",
    )
    return safe.drop(columns=["_verdict_sort"]).reset_index(drop=True)
