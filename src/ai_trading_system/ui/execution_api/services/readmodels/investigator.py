"""Read model for the stock investigator endpoint."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.analytics.registry import RegistryStore


def get_investigator_snapshot(project_root: Path) -> dict[str, Any]:
    registry = RegistryStore(project_root)
    artifacts = _latest_artifacts(registry)
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
        "repeat_tracker": _records(frames["repeat_tracker"]),
        "trap_log": _records(frames["trap_log"]),
        "active_watchlist": _records(frames["active_watchlist"]),
        "archive_summary": {
            "count": int(len(archive)),
            "by_reason": _counts(archive, "drop_reason"),
            "rows": _records(archive, limit=100),
        },
        "source_artifacts": {key: str(value) for key, value in artifacts.items() if value is not None},
    }


def _latest_artifacts(registry: RegistryStore) -> dict[str, Path | None]:
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
        latest = registry.get_latest_artifact(stage_name="investigator", artifact_type=artifact_type, limit=1)
        out[artifact_type] = Path(latest[0].uri) if latest else None
    return out


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
