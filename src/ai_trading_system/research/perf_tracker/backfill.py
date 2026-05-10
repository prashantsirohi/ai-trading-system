"""Backfill rank_cohort_performance from historical pipeline_runs.

Walks every ``data/pipeline_runs/<run-id>/rank/attempt_*/ranked_signals.csv``,
extracts (date, symbol, rank, composite, factor scores, sector), joins
``watchlist_buckets.csv`` from the publish stage when present, computes forward
5/10/20/60-day returns, and upserts into the tracker table.

Idempotent: re-running over the same data produces identical row counts (rows
keyed on run_date+symbol+exchange are replaced wholesale by the latest
attempt's data).
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.research.perf_tracker.forward_returns import compute_forward_returns
from ai_trading_system.research.perf_tracker.schema import open_research_db

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")

# Map ranked_signals columns -> rank_cohort_performance columns. The tracker
# table holds a stable subset; everything else stays in the CSVs.
RANKED_TO_TRACKER = {
    "symbol_id": "symbol_id",
    "exchange": "exchange",
    "composite_score": "composite_score",
    "composite_score_adjusted": "composite_score_adjusted",
    "rank_mode": "rank_mode",
    "sector_name": "sector_name",
    "rel_strength_score": "factor_rs",
    "vol_intensity_score": "factor_vol",
    "trend_score_score": "factor_trend",
    "prox_high_score": "factor_prox",
    "delivery_pct_score": "factor_deliv",
    "sector_strength_score": "factor_sector",
    "momentum_acceleration_score": "factor_momentum_accel",
}


def _latest_attempt_per_date(
    pipeline_runs_dir: Path,
) -> dict[str, dict[str, Path | None]]:
    """For each calendar date with a ranked_signals.csv, pick the freshest run.

    Returns ``{date_str: {"ranked": Path, "buckets": Path | None}}``. We pick
    the run whose ranked_signals.csv was modified last on disk — that's the
    canonical attempt the publish stage would have shipped that day.
    """
    by_date: dict[str, list[tuple[float, Path]]] = defaultdict(list)
    for run_dir in sorted(pipeline_runs_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        match = DATE_RE.search(run_dir.name)
        if not match:
            continue
        d = match.group(1)
        for attempt_dir in run_dir.glob("rank/attempt_*"):
            ranked = attempt_dir / "ranked_signals.csv"
            if ranked.exists():
                by_date[d].append((ranked.stat().st_mtime, ranked))

    result: dict[str, dict[str, Path | None]] = {}
    for d, candidates in by_date.items():
        latest = max(candidates)[1]  # mtime tiebreak
        run_dir = latest.parent.parent.parent  # ranked_signals -> attempt -> rank -> run
        attempt_num = latest.parent.name
        bucket_csv = run_dir / "publish" / attempt_num / "watchlist_buckets.csv"
        if not bucket_csv.exists():
            # Phase 5 may have shipped under a different attempt number; fall
            # back to any publish/attempt_*/watchlist_buckets.csv in same run.
            buckets_glob = list(run_dir.glob("publish/attempt_*/watchlist_buckets.csv"))
            bucket_csv = buckets_glob[0] if buckets_glob else None
        result[d] = {"ranked": latest, "buckets": bucket_csv}
    return result


def _read_ranked(path: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        return None
    if df.empty or "symbol_id" not in df.columns or "composite_score" not in df.columns:
        return None
    return df


def _build_rows_for_date(
    run_date: str,
    ranked_path: Path,
    buckets_path: Path | None,
) -> pd.DataFrame | None:
    ranked = _read_ranked(ranked_path)
    if ranked is None:
        return None

    # Re-rank by composite_score_adjusted if present, else composite_score.
    # Don't trust file order — different rank_modes sort differently.
    score_col = (
        "composite_score_adjusted"
        if "composite_score_adjusted" in ranked.columns
        and ranked["composite_score_adjusted"].notna().any()
        else "composite_score"
    )
    ranked = ranked.sort_values(score_col, ascending=False).reset_index(drop=True)

    cols_present = {src: dst for src, dst in RANKED_TO_TRACKER.items() if src in ranked.columns}
    out = ranked[list(cols_present.keys())].rename(columns=cols_present).copy()
    out.insert(0, "run_date", run_date)
    out.insert(3, "rank_position", out.index + 1)
    if "exchange" not in out.columns:
        out["exchange"] = "NSE"

    # Attach watchlist_bucket if a publish-stage CSV exists for this date.
    if buckets_path is not None and buckets_path.exists():
        try:
            buckets = pd.read_csv(buckets_path)
            if not buckets.empty and "symbol_id" in buckets.columns:
                bucket_col = (
                    buckets[["symbol_id", "watchlist_bucket"]]
                    .drop_duplicates("symbol_id", keep="first")
                )
                out = out.merge(bucket_col, on="symbol_id", how="left")
        except (pd.errors.EmptyDataError, FileNotFoundError):
            pass
    if "watchlist_bucket" not in out.columns:
        out["watchlist_bucket"] = pd.NA

    # Ensure all schema columns exist before insert; missing ones become NULL.
    for col in (
        "composite_score_adjusted",
        "rank_mode",
        "sector_name",
        "factor_rs",
        "factor_vol",
        "factor_trend",
        "factor_prox",
        "factor_deliv",
        "factor_sector",
        "factor_momentum_accel",
    ):
        if col not in out.columns:
            out[col] = pd.NA
    out["config_id"] = pd.NA  # populated post-Phase-1
    return out


def run_backfill(
    *,
    project_root: str | Path | None = None,
    only_dates: list[str] | None = None,
) -> dict[str, int]:
    """Walk all pipeline_runs and upsert rank cohort rows.

    Parameters
    ----------
    only_dates
        If provided, restrict to these run_date strings (YYYY-MM-DD).
        Useful for incremental top-up runs (e.g. yesterday only).

    Returns ``{"dates_processed": N, "rows_upserted": M}``.
    """
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    by_date = _latest_attempt_per_date(paths.pipeline_runs_dir)
    if only_dates:
        by_date = {d: v for d, v in by_date.items() if d in set(only_dates)}

    if not by_date:
        logger.info("perf_tracker backfill: no ranked_signals.csv files found")
        return {"dates_processed": 0, "rows_upserted": 0}

    frames: list[pd.DataFrame] = []
    for run_date in sorted(by_date.keys()):
        files = by_date[run_date]
        rows = _build_rows_for_date(
            run_date=run_date,
            ranked_path=files["ranked"],   # type: ignore[arg-type]
            buckets_path=files["buckets"],  # type: ignore[arg-type]
        )
        if rows is None or rows.empty:
            continue
        frames.append(rows)

    if not frames:
        return {"dates_processed": 0, "rows_upserted": 0}

    combined = pd.concat(frames, ignore_index=True)
    logger.info("perf_tracker backfill: %d dates, %d raw rows", len(frames), len(combined))

    # Compute forward returns in one pass (helper batches by symbol internally).
    enriched = compute_forward_returns(combined, project_root=project_root)

    # Align to schema column order before insert.
    schema_cols = [
        "run_date", "symbol_id", "exchange", "rank_position",
        "composite_score", "composite_score_adjusted", "rank_mode", "watchlist_bucket",
        "config_id",
        "fwd_5d_return", "fwd_10d_return", "fwd_20d_return", "fwd_60d_return",
        "fwd_5d_matured_at", "fwd_10d_matured_at", "fwd_20d_matured_at", "fwd_60d_matured_at",
        "factor_rs", "factor_vol", "factor_trend", "factor_prox", "factor_deliv",
        "factor_sector", "factor_momentum_accel",
        "sector_name",
    ]
    for col in schema_cols:
        if col not in enriched.columns:
            enriched[col] = pd.NA
    enriched = enriched[schema_cols]

    # DELETE + INSERT keyed by run_date for idempotency. Bulk-load via
    # DuckDB's pandas registration.
    dates_to_replace = sorted(enriched["run_date"].astype(str).unique())
    with open_research_db(project_root=project_root) as con:
        date_list = ",".join(f"'{d}'" for d in dates_to_replace)
        con.execute(f"DELETE FROM rank_cohort_performance WHERE CAST(run_date AS VARCHAR) IN ({date_list})")
        con.register("incoming_rows", enriched)
        con.execute(
            "INSERT INTO rank_cohort_performance "
            "SELECT *, CURRENT_TIMESTAMP AS inserted_at FROM incoming_rows"
        )
        con.unregister("incoming_rows")
        total = con.execute("SELECT COUNT(*) FROM rank_cohort_performance").fetchone()[0]

    logger.info("perf_tracker backfill complete: %d dates, %d rows in table", len(dates_to_replace), total)
    return {"dates_processed": len(dates_to_replace), "rows_upserted": int(len(enriched))}


def main() -> None:  # pragma: no cover - CLI entry
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import argparse

    parser = argparse.ArgumentParser(description="Backfill rank_cohort_performance from pipeline_runs.")
    parser.add_argument("--date", action="append", help="Restrict to this date (repeatable, YYYY-MM-DD).")
    args = parser.parse_args()
    result = run_backfill(only_dates=args.date or None)
    print(f"Done: {result}")


if __name__ == "__main__":  # pragma: no cover
    main()
