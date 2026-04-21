"""Controlled artifact loaders for the latest operational rank snapshot."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths


@dataclass(frozen=True)
class ExecutionContext:
    project_root: Path
    ohlcv_db: Path
    master_db: Path
    pipeline_runs_dir: Path


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
        pipeline_runs_dir=paths.pipeline_runs_dir,
    )


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


def _load_payload(payload_path: Path | None) -> dict:
    if payload_path is None:
        return {}
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["_artifact_path"] = str(payload_path)
    return payload


def _load_frames(rank_dir: Path | None) -> dict[str, pd.DataFrame]:
    frame_names = {
        "ranked_signals": "ranked_signals.csv",
        "breakout_scan": "breakout_scan.csv",
        "pattern_scan": "pattern_scan.csv",
        "stock_scan": "stock_scan.csv",
        "sector_dashboard": "sector_dashboard.csv",
    }
    if rank_dir is None:
        return {key: pd.DataFrame() for key in frame_names}

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
    return frames


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
