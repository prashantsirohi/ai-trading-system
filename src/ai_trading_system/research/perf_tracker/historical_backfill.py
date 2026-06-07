"""Historical backfill of rank_cohort_performance from the research scoring path.

The original ``backfill.py`` reads ranked_signals.csv files under
``pipeline_runs_dir`` — fine for current/recent dates that have pipeline runs
on disk, but pre-2026 dates have no such CSVs. To populate the table for
historical dates we re-score them via ``load_research_ranked_by_date`` (which
produces the same per-factor score columns as the live ranker) and pipe the
resulting frames through the same column-mapping helper.

Process:
  1. Enumerate trading dates in [from_date, to_date] at the requested
     ``frequency`` (calendar source: ``_catalog`` in research_ohlcv.duckdb).
  2. Batch-by-year for the loader: one ``load_research_ranked_by_date(year_start,
     year_end)`` call amortises the warmup window across the whole year.
  3. For each per-date frame, call ``build_rows_from_ranked_frame`` to convert
     to the rank_cohort_performance row schema.
  4. Run ``compute_forward_returns`` against the research OHLCV catalog
     (default operational catalog only goes back to 2025).
  5. Upsert (DELETE + INSERT) keyed by run_date for idempotency.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.backtesting.research_loader import (
    DEFAULT_BENCHMARK_SYMBOL,
    load_research_ranked_by_date,
)
from ai_trading_system.research.perf_tracker.backfill import (
    build_rows_from_ranked_frame,
)
from ai_trading_system.research.perf_tracker.forward_returns import (
    compute_forward_returns,
)
from ai_trading_system.research.perf_tracker.quality import annotate_return_quality
from ai_trading_system.research.perf_tracker.schema import open_research_db

logger = logging.getLogger(__name__)

SCHEMA_COLUMNS: tuple[str, ...] = (
    "run_date", "symbol_id", "exchange", "rank_position",
    "composite_score", "composite_score_adjusted", "rank_mode", "watchlist_bucket",
    "config_id",
    "fwd_5d_return", "fwd_10d_return", "fwd_20d_return", "fwd_60d_return",
    "fwd_5d_matured_at", "fwd_10d_matured_at", "fwd_20d_matured_at", "fwd_60d_matured_at",
    "factor_rs", "factor_vol", "factor_trend", "factor_prox", "factor_deliv",
    "factor_sector", "factor_momentum_accel", "factor_above_200dma",
    "factor_liquidity", "factor_delivery_trend", "sector_name",
    "fwd_5d_anomaly", "fwd_return_anomaly", "source_type", "source_run_id",
    "source_artifact_path", "data_quality_status", "data_quality_reason",
)

VALID_FREQUENCIES = ("daily", "weekly", "quarterly")


def _research_ohlcv_path(project_root: Path | str | None) -> Path:
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    return paths.ohlcv_db_path


def _enumerate_trading_dates(
    from_date: date,
    to_date: date,
    *,
    frequency: str,
    exchange: str,
    research_ohlcv_db: Path,
) -> list[date]:
    """Trading dates between from/to. 'daily' returns every catalog day;
    weekly returns Fridays (or the latest trading day in each calendar week);
    quarterly returns the last trading day of each quarter."""
    if frequency not in VALID_FREQUENCIES:
        raise ValueError(f"frequency={frequency!r} not in {VALID_FREQUENCIES}")
    con = duckdb.connect(str(research_ohlcv_db), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT DISTINCT CAST(timestamp AS DATE) AS d
            FROM _catalog
            WHERE exchange = ? AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
              AND close IS NOT NULL AND close > 0
              AND DAYOFWEEK(CAST(timestamp AS DATE)) NOT IN (0, 6)
            ORDER BY d
            """,
            [exchange, str(from_date), str(to_date)],
        ).fetchall()
    finally:
        con.close()
    all_days = [r[0] for r in rows]
    if not all_days:
        return []

    if frequency == "daily":
        return all_days
    if frequency == "weekly":
        # For each ISO week, pick the latest trading day.
        by_week: dict[tuple[int, int], date] = {}
        for d in all_days:
            iso = d.isocalendar()
            key = (iso[0], iso[1])
            if key not in by_week or d > by_week[key]:
                by_week[key] = d
        return sorted(by_week.values())
    # quarterly: last trading day of each (year, quarter)
    by_q: dict[tuple[int, int], date] = {}
    for d in all_days:
        q = (d.year, (d.month - 1) // 3 + 1)
        if q not in by_q or d > by_q[q]:
            by_q[q] = d
    return sorted(by_q.values())


def _year_batches(dates: Iterable[date]) -> dict[int, list[date]]:
    out: dict[int, list[date]] = defaultdict(list)
    for d in dates:
        out[d.year].append(d)
    return dict(out)


def run_historical_backfill(
    *,
    from_date: date,
    to_date: date,
    project_root: Path | str | None = None,
    frequency: str = "daily",
    exchange: str = "NSE",
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    chunk_log_every: int = 50,
) -> dict[str, int]:
    """Backfill rank_cohort_performance via the research scoring path.

    Returns ``{"dates_processed": N, "rows_upserted": M}``.
    """
    research_ohlcv = _research_ohlcv_path(project_root)
    if not research_ohlcv.exists():
        logger.warning("research OHLCV DB not found at %s", research_ohlcv)
        return {"dates_processed": 0, "rows_upserted": 0}

    target_dates = _enumerate_trading_dates(
        from_date,
        to_date,
        frequency=frequency,
        exchange=exchange,
        research_ohlcv_db=research_ohlcv,
    )
    if not target_dates:
        logger.info("historical backfill: no trading dates in [%s, %s]", from_date, to_date)
        return {"dates_processed": 0, "rows_upserted": 0}

    logger.info(
        "historical backfill: %d dates from %s to %s (%s frequency)",
        len(target_dates),
        target_dates[0],
        target_dates[-1],
        frequency,
    )

    batches = _year_batches(target_dates)
    frames: list[pd.DataFrame] = []
    processed = 0

    for year, dates_in_year in sorted(batches.items()):
        target_set = {d for d in dates_in_year}
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)
        logger.info("year %d: scoring %d dates...", year, len(target_set))
        ranked_by_date = load_research_ranked_by_date(
            project_root or Path.cwd(),
            from_date=year_start,
            to_date=year_end,
            exchange=exchange,
            benchmark_symbol=benchmark_symbol,
        )
        for run_date, ranked_df in sorted(ranked_by_date.items()):
            if run_date not in target_set:
                continue
            rows = build_rows_from_ranked_frame(
                str(run_date),
                ranked_df,
                source_type="historical_research",
                source_run_id=f"historical-{frequency}-{run_date}",
            )
            if rows is None or rows.empty:
                continue
            frames.append(rows)
            processed += 1
            if processed % chunk_log_every == 0:
                logger.info("  processed %d / %d dates", processed, len(target_dates))

    if not frames:
        return {"dates_processed": 0, "rows_upserted": 0}

    combined = pd.concat(frames, ignore_index=True)
    logger.info("historical backfill: %d dates → %d raw rows", processed, len(combined))

    enriched = compute_forward_returns(
        combined,
        project_root=project_root,
        ohlcv_db_path=research_ohlcv,
    )
    enriched = annotate_return_quality(enriched)

    for col in SCHEMA_COLUMNS:
        if col not in enriched.columns:
            enriched[col] = pd.NA
    enriched = enriched[list(SCHEMA_COLUMNS)]

    dates_to_replace = sorted(enriched["run_date"].astype(str).unique())
    with open_research_db(project_root=project_root) as con:
        if dates_to_replace:
            placeholders = ",".join("?" for _ in dates_to_replace)
            con.execute(
                f"DELETE FROM rank_cohort_performance WHERE CAST(run_date AS VARCHAR) IN ({placeholders})",
                list(dates_to_replace),
            )
        con.register("incoming_rows", enriched)
        # Explicit column list — DuckDB ALTER ADD COLUMN places new columns at
        # the end physically, so a positional SELECT * would misalign. Naming
        # the columns insulates us from physical-order drift across migrations.
        col_list = ", ".join(SCHEMA_COLUMNS)
        select_list = ", ".join(f'"{c}"' for c in SCHEMA_COLUMNS)
        con.execute(
            f"INSERT INTO rank_cohort_performance ({col_list}, inserted_at) "
            f"SELECT {select_list}, CURRENT_TIMESTAMP FROM incoming_rows"
        )
        con.unregister("incoming_rows")
        total = con.execute("SELECT COUNT(*) FROM rank_cohort_performance").fetchone()[0]

    logger.info(
        "historical backfill complete: %d dates, %d rows inserted, table now %d rows",
        len(dates_to_replace),
        len(enriched),
        total,
    )
    return {
        "dates_processed": len(dates_to_replace),
        "rows_upserted": int(len(enriched)),
    }
