"""Payload assembly helpers for publish stage channel delivery."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict

import pandas as pd

from run.stages.base import StageArtifact


def build_publish_datasets(
    *,
    context_artifact_for: Callable[[str], StageArtifact | None],
    read_artifact: Callable[[StageArtifact], pd.DataFrame],
    read_json_artifact: Callable[[StageArtifact], Dict[str, Any]],
    ranked_signals_artifact: StageArtifact,
) -> Dict[str, Any]:
    """Load publish datasets from rank-stage artifacts with compatibility defaults."""
    scan_artifact = context_artifact_for("stock_scan")
    breakout_artifact = context_artifact_for("breakout_scan")
    dashboard_artifact = context_artifact_for("sector_dashboard")
    dashboard_payload_artifact = context_artifact_for("dashboard_payload")

    ranked_df = read_artifact(ranked_signals_artifact)
    return {
        "ranked_signals": ranked_df,
        "breakout_scan": read_artifact(breakout_artifact) if breakout_artifact else pd.DataFrame(),
        "stock_scan": read_artifact(scan_artifact) if scan_artifact else pd.DataFrame(),
        "sector_dashboard": read_artifact(dashboard_artifact) if dashboard_artifact else pd.DataFrame(),
        "dashboard_payload": read_json_artifact(dashboard_payload_artifact) if dashboard_payload_artifact else {},
    }


def build_publish_metadata(
    *,
    rank_artifact: StageArtifact,
    ranked_df: pd.DataFrame,
    targets: list[dict[str, Any]],
) -> Dict[str, Any]:
    """Build publish stage metadata summary from delivery outcomes."""
    return {
        "rank_artifact_uri": rank_artifact.uri,
        "rank_artifact_hash": rank_artifact.content_hash,
        "targets": targets,
        "top_symbol": ranked_df.iloc[0]["symbol_id"] if not ranked_df.empty else None,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
