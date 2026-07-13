"""Legacy wrappers around execution read models."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.ui.execution_api.services.readmodels.latest_operational_snapshot import (
    get_execution_context,
    load_execution_payload,
    load_latest_rank_frames,
)
from ai_trading_system.ui.execution_api.services.readmodels.pipeline_status import (
    get_execution_db_stats,
    get_execution_health,
)

__all__ = [
    "get_execution_context",
    "get_execution_db_stats",
    "get_execution_health",
    "load_execution_payload",
    "load_latest_rank_frames",
    "load_shadow_overlay_frame",
    "load_shadow_summary_frame",
    "pivot_shadow_summary_frame",
]


def load_shadow_overlay_frame(project_root: str | Path | None = None) -> pd.DataFrame:
    registry = RegistryStore(
        Path(project_root) if project_root else Path(__file__).resolve().parents[5],
        initialize=False,
    )
    rows = registry.get_shadow_overlay()
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame.loc[:, "prediction_date"] = pd.to_datetime(frame["prediction_date"])
    return frame


def load_shadow_summary_frame(
    grain: str,
    horizon: int,
    *,
    periods: int = 12,
    project_root: str | Path | None = None,
) -> pd.DataFrame:
    registry = RegistryStore(
        Path(project_root) if project_root else Path(__file__).resolve().parents[5],
        initialize=False,
    )
    grain_aliases = {
        "weekly": "week",
        "week": "week",
        "monthly": "month",
        "month": "month",
    }
    normalized_grain = grain_aliases.get(grain, grain)
    rows = registry.get_shadow_period_summary(grain=normalized_grain, horizon=horizon, periods=periods)
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame.loc[:, "period_start"] = pd.to_datetime(frame["period_start"])
    return frame


def pivot_shadow_summary_frame(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df
    pivoted = summary_df.pivot(index="period_start", columns="variant", values=["picks", "hit_rate", "avg_return"])
    pivoted.columns = [f"{metric}_{variant}" for metric, variant in pivoted.columns]
    return pivoted.reset_index().sort_values("period_start", ascending=False)
