"""Read model for the latest event-aware insight artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_trading_system.platform.db.paths import get_domain_paths


def get_latest_insight(project_root: str | Path | None = None) -> dict[str, Any]:
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[6]
    paths = get_domain_paths(root, "operational")
    runs_dir = paths.pipeline_runs_dir
    if not runs_dir.exists():
        return {"status": "missing", "detail": "pipeline_runs directory not found"}
    candidates = sorted(
        list(runs_dir.glob("*/insight/attempt_*/daily_insight.json"))
        + list(runs_dir.glob("*/insight/attempt_*/weekly_insight.json")),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return {"status": "missing", "detail": "no insight artifacts found"}
    path = candidates[0]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"status": "error", "detail": str(exc), "artifact_path": str(path)}
    payload["status"] = payload.get("status") or "available"
    payload["artifact_path"] = str(path)
    payload["run_id"] = payload.get("run_id") or path.parts[-4]
    return payload
