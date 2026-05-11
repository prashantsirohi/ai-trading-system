"""Walk historical ``data/pipeline_runs/*`` and produce the
``dict[date, ranked_df]`` shape that ``EngineBacktestRunner`` consumes.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

# Directory names look like: pipeline-2026-03-31-d89f79d5
_DIR_RE = re.compile(r"^pipeline-(\d{4}-\d{2}-\d{2})-([0-9a-f]+)$")


def _parse_run_date(name: str) -> date | None:
    match = _DIR_RE.match(name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def _latest_attempt(run_dir: Path) -> Path | None:
    """Return the highest-numbered ``rank/attempt_N/ranked_signals.csv`` path."""
    rank_dir = run_dir / "rank"
    if not rank_dir.is_dir():
        return None
    best: tuple[int, Path] | None = None
    for attempt in rank_dir.glob("attempt_*"):
        m = re.match(r"attempt_(\d+)", attempt.name)
        if not m:
            continue
        n = int(m.group(1))
        ranked = attempt / "ranked_signals.csv"
        if ranked.exists() and (best is None or n > best[0]):
            best = (n, ranked)
    return best[1] if best else None


def discover_runs(pipeline_runs_dir: Path | str) -> list[tuple[date, Path]]:
    """Return ``(date, ranked_signals.csv path)`` for every viable run, deduped by date."""
    base = Path(pipeline_runs_dir)
    if not base.is_dir():
        return []
    by_date: dict[date, Path] = {}
    for run_dir in base.iterdir():
        if not run_dir.is_dir():
            continue
        run_date = _parse_run_date(run_dir.name)
        if run_date is None:
            continue
        path = _latest_attempt(run_dir)
        if path is None:
            continue
        # Keep the most-recent file when multiple runs share a date.
        existing = by_date.get(run_date)
        if existing is None or path.stat().st_mtime > existing.stat().st_mtime:
            by_date[run_date] = path
    return sorted(by_date.items())


def load_ranked_by_date(
    pipeline_runs_dir: Path | str,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    symbols: Iterable[str] | None = None,
) -> dict[date, pd.DataFrame]:
    """Build the ``dict[date, ranked_df]`` consumed by ``EngineBacktestRunner.run()``."""
    symbol_filter = {str(s).strip().upper() for s in symbols} if symbols else None
    out: dict[date, pd.DataFrame] = {}
    for run_date, csv_path in discover_runs(pipeline_runs_dir):
        if from_date and run_date < from_date:
            continue
        if to_date and run_date > to_date:
            continue
        df = pd.read_csv(csv_path)
        if symbol_filter is not None and "symbol_id" in df.columns:
            df = df[df["symbol_id"].astype(str).str.upper().isin(symbol_filter)]
        if df.empty:
            continue
        out[run_date] = df.reset_index(drop=True)
    return out
