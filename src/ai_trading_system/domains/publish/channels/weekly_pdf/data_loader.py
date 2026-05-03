"""Load rank-stage artifacts for the weekly PDF report.

Phase 1: read directly from the in-memory `datasets` dict that the
PublishStage already builds, plus pull `pattern_scan` and `rank_summary`
from the StageContext (these are not in the default datasets dict).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from ai_trading_system.domains.publish.channels.weekly_pdf import history
from ai_trading_system.domains.publish.channels.weekly_pdf.breadth import compute_market_breadth
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext

logger = logging.getLogger(__name__)


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
    prior_run_id: Optional[str] = None
    prior_run_date: Optional[str] = None
    prior_ranked_signals: pd.DataFrame = field(default_factory=pd.DataFrame)
    prior_sector_dashboard: pd.DataFrame = field(default_factory=pd.DataFrame)
    prior_breakouts_per_run: list = field(default_factory=list)
    market_breadth: pd.DataFrame = field(default_factory=pd.DataFrame)
    market_events_snapshot: Dict[str, Any] = field(default_factory=dict)
    enriched_event_signals: list = field(default_factory=list)


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
    market_events_snapshot = datasets.get("market_events_snapshot") or {}
    enriched_event_signals = list(datasets.get("enriched_event_signals") or [])

    trust_status = (
        datasets.get("publish_trust_status")
        or rank_summary.get("data_trust_status")
        or "unknown"
    )

    data = WeeklyReportData(
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
        market_events_snapshot=dict(market_events_snapshot),
        enriched_event_signals=enriched_event_signals,
    )
    _attach_phase2_inputs(context, data)
    return data


def _attach_phase2_inputs(context: StageContext, data: WeeklyReportData) -> None:
    """Best-effort load of prior-week snapshot, lookback breakouts, and breadth.

    Failures here degrade gracefully — Phase 2 sections are optional.
    """
    project_root = getattr(context, "project_root", None)
    if project_root is None:
        return
    data_domain = (context.params or {}).get("data_domain", "operational")
    pipeline_runs_dir = history.resolve_pipeline_runs_dir(project_root, data_domain)

    current_date = history.parse_run_date(context.run_id) or _safe_date(context.run_date)
    if current_date is None:
        logger.info("weekly_pdf: could not parse run_date from run_id=%s", context.run_id)

    if current_date is not None:
        try:
            prior = history.find_prior_run(
                pipeline_runs_dir,
                current_run_id=context.run_id,
                current_run_date=current_date,
                target_days_back=7,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("weekly_pdf: prior-run discovery failed: %s", exc)
            prior = None
        if prior is not None:
            try:
                prior_arts = history.load_prior_artifacts(prior)
            except Exception as exc:  # noqa: BLE001
                logger.warning("weekly_pdf: failed to load prior artifacts: %s", exc)
                prior_arts = {}
            data.prior_run_id = prior.run_id
            data.prior_run_date = prior.run_date.isoformat()
            data.prior_ranked_signals = prior_arts.get("ranked_signals", pd.DataFrame())
            data.prior_sector_dashboard = prior_arts.get("sector_dashboard", pd.DataFrame())

        try:
            recent = history.find_recent_runs_for_failed_breakout(
                pipeline_runs_dir, context.run_id, current_date, lookback_days=10
            )
            data.prior_breakouts_per_run = [
                (r.run_id, _safe_read_csv(r.breakout_scan_path)) for r in recent
            ]
        except Exception as exc:  # noqa: BLE001
            logger.warning("weekly_pdf: lookback breakout discovery failed: %s", exc)
            data.prior_breakouts_per_run = []

        ohlcv_path = project_root / "data" / "ohlcv.duckdb"
        try:
            data.market_breadth = compute_market_breadth(
                ohlcv_path, end_date=current_date, weeks=26
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("weekly_pdf: market breadth computation failed: %s", exc)
            data.market_breadth = pd.DataFrame()


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _safe_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
