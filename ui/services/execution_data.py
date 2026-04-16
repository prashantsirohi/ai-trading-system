"""Legacy wrappers around execution read models."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

from analytics.registry import RegistryStore
from ui.services.readmodels.latest_operational_snapshot import (
    ExecutionContext,
    get_execution_context,
    load_execution_payload,
    load_latest_rank_frames,
)
from ui.services.readmodels.pipeline_status import (
    get_execution_data_trust_snapshot,
    get_execution_db_stats,
    get_execution_health,
    get_execution_ops_health_snapshot,
)


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
