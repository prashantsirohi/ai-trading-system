"""Load rank-stage artifacts for the weekly PDF report.

Phase 1: read directly from the in-memory `datasets` dict that the
PublishStage already builds, plus pull `pattern_scan` and `rank_summary`
from the StageContext (these are not in the default datasets dict).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext


@dataclass
class WeeklyReportData:
    run_id: str
    run_date: str
    ranked_signals: pd.DataFrame
    breakout_scan: pd.DataFrame
    pattern_scan: pd.DataFrame
    sector_dashboard: pd.DataFrame
    stock_scan: pd.DataFrame
    dashboard_payload: Dict[str, Any] = field(default_factory=dict)
    rank_summary: Dict[str, Any] = field(default_factory=dict)
    trust_status: str = "unknown"


def _read_csv_artifact(artifact: Optional[StageArtifact]) -> pd.DataFrame:
    if artifact is None:
        return pd.DataFrame()
    path = Path(artifact.uri)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _read_json_artifact(artifact: Optional[StageArtifact]) -> Dict[str, Any]:
    if artifact is None:
        return {}
    path = Path(artifact.uri)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_report_data(
    context: StageContext,
    datasets: Dict[str, Any],
) -> WeeklyReportData:
    """Assemble report inputs from PublishStage datasets + raw rank artifacts."""

    ranked_signals = datasets.get("ranked_signals")
    if not isinstance(ranked_signals, pd.DataFrame):
        ranked_signals = pd.DataFrame()

    breakout_scan = datasets.get("breakout_scan")
    if not isinstance(breakout_scan, pd.DataFrame):
        breakout_scan = pd.DataFrame()

    sector_dashboard = datasets.get("sector_dashboard")
    if not isinstance(sector_dashboard, pd.DataFrame):
        sector_dashboard = pd.DataFrame()

    stock_scan = datasets.get("stock_scan")
    if not isinstance(stock_scan, pd.DataFrame):
        stock_scan = pd.DataFrame()

    pattern_scan = _read_csv_artifact(context.artifact_for("rank", "pattern_scan"))
    rank_summary = _read_json_artifact(context.artifact_for("rank", "rank_summary"))
    dashboard_payload = datasets.get("dashboard_payload") or {}

    trust_status = (
        datasets.get("publish_trust_status")
        or rank_summary.get("data_trust_status")
        or "unknown"
    )

    return WeeklyReportData(
        run_id=context.run_id,
        run_date=context.run_date,
        ranked_signals=ranked_signals,
        breakout_scan=breakout_scan,
        pattern_scan=pattern_scan,
        sector_dashboard=sector_dashboard,
        stock_scan=stock_scan,
        dashboard_payload=dict(dashboard_payload),
        rank_summary=dict(rank_summary),
        trust_status=str(trust_status),
    )
