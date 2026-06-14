"""Controlled artifact loaders for the latest operational rank snapshot."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import find_latest_pipeline_artifact, get_domain_paths


@dataclass(frozen=True)
class ExecutionContext:
    project_root: Path
    ohlcv_db: Path
    master_db: Path
    pipeline_runs_dir: Path
    control_plane_db: Path | None = None


@dataclass(frozen=True)
class LatestOperationalSnapshot:
    context: ExecutionContext
    payload_path: Path | None
    rank_attempt_dir: Path | None
    payload: dict
    frames: dict[str, pd.DataFrame]


def get_execution_context(project_root: str | Path | None = None) -> ExecutionContext:
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[6]
    paths = get_domain_paths(root, "operational")
    return ExecutionContext(
        project_root=root,
        ohlcv_db=paths.ohlcv_db_path,
        master_db=paths.master_db_path,
        control_plane_db=paths.root_dir / "control_plane.duckdb",
        pipeline_runs_dir=paths.pipeline_runs_dir,
    )


def _load_latest_payload_path(ctx: ExecutionContext) -> Optional[Path]:
    runs_dir = ctx.pipeline_runs_dir
    if not runs_dir.exists():
        return None
    latest_disk = find_latest_pipeline_artifact(
        project_root=ctx.project_root,
        data_domain="operational",
        stage_name="rank",
        filename="dashboard_payload.json",
    )
    candidates = [latest_disk[1]] if latest_disk is not None else []
    candidates.extend(
        path
        for path in sorted(
            runs_dir.glob("*/rank/attempt_*/dashboard_payload.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if path not in candidates
    )
    if not candidates:
        return None

    control_plane_db = ctx.control_plane_db or (ctx.ohlcv_db.parent / "control_plane.duckdb")
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


def _load_payload(payload_path: Path | None) -> dict:
    if payload_path is None:
        return {}
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["_artifact_path"] = str(payload_path)
    return payload


def _load_frames(rank_dir: Path | None) -> dict[str, pd.DataFrame]:
    frame_names = {
        "ranked_signals": "ranked_signals.csv",
        "ranked_universe": "ranked_universe.csv",
        "breakout_scan": "breakout_scan.csv",
        "pattern_scan": "pattern_scan.csv",
        "stock_scan": "stock_scan.csv",
        "sector_dashboard": "sector_dashboard.csv",
        "sector_rotation": "sector_rotation.csv",
        "stock_rotation": "stock_rotation.csv",
        "accumulation_distribution": "accumulation_distribution.csv",
        "sector_custom_indices": "sector_custom_indices.csv",
    }
    if rank_dir is None:
        frames = {key: pd.DataFrame() for key in frame_names}
        frames["watchlist_candidates"] = pd.DataFrame()
        frames["candidate_tracker_current"] = pd.DataFrame()
        return frames

    frames: dict[str, pd.DataFrame] = {}
    for key, filename in frame_names.items():
        path = rank_dir / filename
        if not path.exists():
            frames[key] = pd.DataFrame()
            continue
        try:
            frames[key] = pd.read_csv(path)
        except Exception:
            frames[key] = pd.DataFrame()
    frames["watchlist_candidates"] = _load_same_run_fundamentals_watchlist(rank_dir)
    frames["candidate_tracker_current"] = _load_same_run_candidate_tracker(rank_dir)
    return frames


def _load_same_run_fundamentals_watchlist(rank_dir: Path) -> pd.DataFrame:
    try:
        run_dir = rank_dir.parents[1]
    except IndexError:
        return pd.DataFrame()
    candidates = sorted(
        (run_dir / "fundamentals").glob("attempt_*/watchlist_candidates.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        try:
            return pd.read_csv(path)
        except Exception:
            continue
    return pd.DataFrame()


def _load_same_run_candidate_tracker(rank_dir: Path) -> pd.DataFrame:
    try:
        run_dir = rank_dir.parents[1]
    except IndexError:
        return pd.DataFrame()
    candidates = sorted(
        (run_dir / "candidate_tracker").glob("attempt_*/candidate_tracker_current.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        try:
            return pd.read_csv(path)
        except Exception:
            continue
    return pd.DataFrame()


def load_latest_operational_snapshot(project_root: str | Path | None = None) -> LatestOperationalSnapshot:
    ctx = get_execution_context(project_root)
    payload_path = _load_latest_payload_path(ctx)
    rank_attempt_dir = payload_path.parent if payload_path is not None else _load_latest_rank_attempt_dir(ctx)
    payload = _load_payload(payload_path)
    frames = _load_frames(rank_attempt_dir)
    return LatestOperationalSnapshot(
        context=ctx,
        payload_path=payload_path,
        rank_attempt_dir=rank_attempt_dir,
        payload=payload,
        frames=frames,
    )


def load_execution_payload(project_root: str | Path | None = None) -> dict:
    return load_latest_operational_snapshot(project_root).payload


def load_latest_rank_frames(project_root: str | Path | None = None) -> dict[str, pd.DataFrame]:
    return load_latest_operational_snapshot(project_root).frames
