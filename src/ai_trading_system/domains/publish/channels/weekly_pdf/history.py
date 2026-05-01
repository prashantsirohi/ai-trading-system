"""Discover prior pipeline runs for week-over-week comparisons."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_RUN_DIR_RE = re.compile(r"^pipeline-(\d{4}-\d{2}-\d{2})-[0-9a-f]+$")


@dataclass
class PriorRun:
    run_id: str
    run_date: date
    rank_attempt_dir: Path

    @property
    def ranked_signals_path(self) -> Path:
        return self.rank_attempt_dir / "ranked_signals.csv"

    @property
    def breakout_scan_path(self) -> Path:
        return self.rank_attempt_dir / "breakout_scan.csv"

    @property
    def sector_dashboard_path(self) -> Path:
        return self.rank_attempt_dir / "sector_dashboard.csv"


def _parse_run_dir(run_dir: Path) -> Optional[tuple[str, date]]:
    match = _RUN_DIR_RE.match(run_dir.name)
    if not match:
        return None
    try:
        run_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None
    return run_dir.name, run_date


def _latest_attempt_dir(run_dir: Path, stage: str) -> Optional[Path]:
    stage_dir = run_dir / stage
    if not stage_dir.is_dir():
        return None
    attempts = sorted(
        (p for p in stage_dir.iterdir() if p.is_dir() and p.name.startswith("attempt_")),
        key=lambda p: int(p.name.split("_", 1)[1]) if p.name.split("_", 1)[1].isdigit() else 0,
    )
    return attempts[-1] if attempts else None


def list_pipeline_runs(pipeline_runs_dir: Path) -> List[PriorRun]:
    """Enumerate runs with a rank attempt directory present."""
    if not pipeline_runs_dir.is_dir():
        return []
    out: List[PriorRun] = []
    for entry in pipeline_runs_dir.iterdir():
        if not entry.is_dir():
            continue
        parsed = _parse_run_dir(entry)
        if parsed is None:
            continue
        run_id, run_date = parsed
        rank_dir = _latest_attempt_dir(entry, "rank")
        if rank_dir is None:
            continue
        out.append(PriorRun(run_id=run_id, run_date=run_date, rank_attempt_dir=rank_dir))
    out.sort(key=lambda r: (r.run_date, r.run_id))
    return out


def find_prior_run(
    pipeline_runs_dir: Path,
    current_run_id: str,
    current_run_date: date,
    target_days_back: int = 7,
    tolerance_days: int = 3,
) -> Optional[PriorRun]:
    """Pick the run closest to (current_run_date - target_days_back), within tolerance."""
    candidates = [
        r for r in list_pipeline_runs(pipeline_runs_dir) if r.run_id != current_run_id
    ]
    if not candidates:
        return None
    target = current_run_date - timedelta(days=target_days_back)
    earlier = [r for r in candidates if r.run_date < current_run_date]
    if not earlier:
        return None
    best = min(earlier, key=lambda r: abs((r.run_date - target).days))
    if abs((best.run_date - target).days) > tolerance_days + target_days_back:
        return None
    return best


def find_recent_runs_for_failed_breakout(
    pipeline_runs_dir: Path,
    current_run_id: str,
    current_run_date: date,
    lookback_days: int = 10,
) -> List[PriorRun]:
    """Return runs strictly between (today - lookback_days) and yesterday, oldest first."""
    runs = list_pipeline_runs(pipeline_runs_dir)
    cutoff = current_run_date - timedelta(days=lookback_days)
    return [
        r for r in runs
        if r.run_id != current_run_id and cutoff <= r.run_date < current_run_date
    ]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def load_prior_artifacts(prior: PriorRun) -> Dict[str, pd.DataFrame]:
    return {
        "ranked_signals": _read_csv(prior.ranked_signals_path),
        "breakout_scan": _read_csv(prior.breakout_scan_path),
        "sector_dashboard": _read_csv(prior.sector_dashboard_path),
    }


def resolve_pipeline_runs_dir(project_root: Path, data_domain: str = "operational") -> Path:
    """Mirror the path-resolution used by StageContext.output_dir."""
    try:
        from ai_trading_system.platform.db.paths import ensure_domain_layout

        paths = ensure_domain_layout(project_root=project_root, data_domain=data_domain)
        return paths.pipeline_runs_dir
    except Exception:  # noqa: BLE001 — fall back to the default layout
        return project_root / "data" / "pipeline_runs"


def parse_run_date(run_id: str) -> Optional[date]:
    match = _RUN_DIR_RE.match(run_id)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


__all__ = [
    "PriorRun",
    "find_prior_run",
    "find_recent_runs_for_failed_breakout",
    "list_pipeline_runs",
    "load_prior_artifacts",
    "parse_run_date",
    "resolve_pipeline_runs_dir",
]
