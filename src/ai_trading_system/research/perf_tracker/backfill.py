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
from ai_trading_system.research.perf_tracker.constants import (
    FACTOR_COLUMNS,
    RANKED_TO_TRACKER,
)
from ai_trading_system.research.perf_tracker.forward_returns import compute_forward_returns
from ai_trading_system.research.perf_tracker.schema import open_research_db

logger = logging.getLogger(__name__)

PIPELINE_RUN_RE = re.compile(r"^pipeline-(\d{4}-\d{2}-\d{2})-.+$")
FIXTURE_SYMBOL_RE = re.compile(r"^(?:BASE|DRIFT|REC|OLD|R|T|SYM)\d+$|^(?:AAA|BBB|CCC)$")


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
        match = PIPELINE_RUN_RE.fullmatch(run_dir.name)
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


def _operational_trading_dates(ohlcv_db_path: Path) -> set[str]:
    """Return exchange dates present in the operational OHLCV catalog."""
    import duckdb

    con = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT DISTINCT CAST(timestamp AS DATE)
            FROM _catalog
            WHERE timestamp IS NOT NULL
              AND close IS NOT NULL
              AND close > 0
              AND DAYOFWEEK(CAST(timestamp AS DATE)) NOT IN (0, 6)
            """
        ).fetchall()
    finally:
        con.close()
    return {str(row[0]) for row in rows}


def _read_ranked(path: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        return None
    if df.empty or "symbol_id" not in df.columns or "composite_score" not in df.columns:
        return None
    return df


def build_rows_from_ranked_frame(
    run_date: str,
    ranked: pd.DataFrame,
    *,
    buckets: pd.DataFrame | None = None,
    source_type: str = "unknown",
    source_run_id: str | None = None,
    source_artifact_path: str | None = None,
) -> pd.DataFrame:
    """Convert a ranked-signals DataFrame into rank_cohort_performance row form.

    Pure transform — no I/O. Shared by both backfill paths (operational CSVs
    via _build_rows_for_date, and historical research-loader frames via
    historical_backfill).
    """
    # Re-rank by composite_score_adjusted if present, else composite_score.
    # Don't trust input order — different rank_modes sort differently.
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

    if buckets is not None and not buckets.empty and "symbol_id" in buckets.columns:
        bucket_col = (
            buckets[["symbol_id", "watchlist_bucket"]]
            .drop_duplicates("symbol_id", keep="first")
        )
        out = out.merge(bucket_col, on="symbol_id", how="left")
    if "watchlist_bucket" not in out.columns:
        out["watchlist_bucket"] = pd.NA

    # Ensure all schema columns exist before insert; missing ones become NULL.
    for col in ("composite_score_adjusted", "rank_mode", "sector_name", *FACTOR_COLUMNS):
        if col not in out.columns:
            out[col] = pd.NA
    out["config_id"] = pd.NA  # populated post-Phase-1
    out["source_type"] = source_type
    out["source_run_id"] = source_run_id
    out["source_artifact_path"] = source_artifact_path
    out["data_quality_status"] = "trusted"
    return out


def _build_rows_for_date(
    run_date: str,
    ranked_path: Path,
    buckets_path: Path | None,
) -> pd.DataFrame | None:
    ranked = _read_ranked(ranked_path)
    if ranked is None:
        return None
    buckets_df: pd.DataFrame | None = None
    if buckets_path is not None and buckets_path.exists():
        try:
            buckets_df = pd.read_csv(buckets_path)
        except (pd.errors.EmptyDataError, FileNotFoundError):
            buckets_df = None
    return build_rows_from_ranked_frame(
        run_date,
        ranked,
        buckets=buckets_df,
        source_type="pipeline",
        source_run_id=ranked_path.parent.parent.parent.name,
        source_artifact_path=str(ranked_path),
    )


def _validate_operational_rows(rows: pd.DataFrame) -> None:
    """Reject fixture-like or duplicate rows before they reach the live table."""
    keys = ["run_date", "symbol_id", "exchange"]
    duplicates = rows.duplicated(keys, keep=False)
    if duplicates.any():
        sample = rows.loc[duplicates, keys].head(5).to_dict("records")
        raise ValueError(f"perf_tracker duplicate cohort keys: {sample}")

    symbols = rows["symbol_id"].astype(str)
    fixture_like = symbols.str.match(FIXTURE_SYMBOL_RE)
    if fixture_like.any():
        sample = sorted(symbols.loc[fixture_like].unique())[:10]
        raise ValueError(f"perf_tracker fixture-like operational symbols: {sample}")

    run_dates = pd.to_datetime(rows["run_date"], errors="coerce")
    weekend_dates = sorted({
        d.date().isoformat() for d in run_dates if pd.notna(d) and d.weekday() >= 5
    })
    if weekend_dates:
        logger.warning("perf_tracker operational cohorts include weekend dates: %s", weekend_dates)

    daily_sizes = rows.groupby("run_date").size()
    if len(daily_sizes) >= 2:
        median_size = float(daily_sizes.median())
        if median_size > 0:
            sharp_drops = daily_sizes[daily_sizes < median_size * 0.20]
            if not sharp_drops.empty:
                logger.warning(
                    "perf_tracker cohort-size drop below 20%% of median %.1f: %s",
                    median_size,
                    sharp_drops.to_dict(),
                )


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
    trading_dates = _operational_trading_dates(paths.ohlcv_db_path)
    skipped_dates = sorted(set(by_date) - trading_dates)
    if skipped_dates:
        logger.warning("perf_tracker skipped non-trading artifact dates: %s", skipped_dates)
    by_date = {d: value for d, value in by_date.items() if d in trading_dates}
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

    frames = [frame.dropna(axis=1, how="all") for frame in frames if not frame.empty]
    combined = pd.concat(frames, ignore_index=True)
    logger.info("perf_tracker backfill: %d dates, %d raw rows", len(frames), len(combined))

    # Compute forward returns in one pass (helper batches by symbol internally).
    enriched = compute_forward_returns(combined, project_root=project_root)
    _validate_operational_rows(enriched)

    # Align to schema column order before insert.
    schema_cols = [
        "run_date", "symbol_id", "exchange", "rank_position",
        "composite_score", "composite_score_adjusted", "rank_mode", "watchlist_bucket",
        "config_id",
        "fwd_5d_return", "fwd_10d_return", "fwd_20d_return", "fwd_60d_return",
        "fwd_5d_matured_at", "fwd_10d_matured_at", "fwd_20d_matured_at", "fwd_60d_matured_at",
        *FACTOR_COLUMNS,
        "sector_name",
        "fwd_5d_anomaly", "fwd_return_anomaly", "source_type", "source_run_id", "source_artifact_path",
        "data_quality_status",
    ]
    missing_cols = {col: pd.NA for col in schema_cols if col not in enriched.columns}
    if missing_cols:
        enriched = enriched.assign(**missing_cols)
    enriched = enriched[schema_cols]

    # DELETE + INSERT keyed by run_date for idempotency. Bulk-load via
    # DuckDB's pandas registration.
    dates_to_replace = sorted(enriched["run_date"].astype(str).unique())
    with open_research_db(project_root=project_root) as con:
        if dates_to_replace:
            placeholders = ",".join("?" for _ in dates_to_replace)
            con.execute(
                f"DELETE FROM rank_cohort_performance WHERE CAST(run_date AS VARCHAR) IN ({placeholders})",
                list(dates_to_replace),
            )
        con.register("incoming_rows", enriched)
        # Match by column name, not position — protects against on-disk column
        # order drift when ADD COLUMN appends to the end (e.g. factor_above_200dma
        # is at position 26 on older DBs but appears mid-list in schema_cols).
        con.execute(
            "INSERT INTO rank_cohort_performance BY NAME "
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
