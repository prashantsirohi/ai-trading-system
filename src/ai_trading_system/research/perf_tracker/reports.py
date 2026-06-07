"""Research-quality diagnostics for rank cohort performance."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd

from ai_trading_system.research.perf_tracker.forward_returns import FORWARD_HORIZONS
from ai_trading_system.research.perf_tracker.health import build_tracker_health
from ai_trading_system.research.perf_tracker.schema import open_research_db, research_db_path


def build_research_quality_reports(
    *,
    project_root: str | Path | None = None,
    recent_days: int = 120,
    repeated_min_n: int = 3,
) -> dict[str, object]:
    """Build summary payload and CSV-ready diagnostics from perf tracker data."""
    db_path = research_db_path(project_root=project_root)
    empty = _empty_reports()
    if not db_path.exists():
        return {
            "summary": _summary_from_frames(build_tracker_health(project_root=project_root), empty),
            "frames": empty,
        }

    with open_research_db(project_root=project_root, read_only=True) as con:
        latest = con.execute("SELECT MAX(run_date) FROM rank_cohort_performance").fetchone()[0]
        recent_cutoff = latest - timedelta(days=recent_days) if latest else None

        rank_bucket = _rank_bucket_performance(con, recent_cutoff=recent_cutoff)
        sector = _sector_performance(con, recent_cutoff=recent_cutoff)
        repeated = _repeated_symbol_performance(con, repeated_min_n=repeated_min_n)
        excluded = con.execute(
            """
            SELECT
                run_date,
                symbol_id,
                exchange,
                rank_position,
                watchlist_bucket,
                sector_name,
                source_type,
                source_run_id,
                fwd_5d_return,
                fwd_10d_return,
                fwd_20d_return,
                fwd_60d_return,
                fwd_5d_anomaly,
                fwd_return_anomaly,
                data_quality_status,
                data_quality_reason
            FROM rank_cohort_performance
            WHERE COALESCE(data_quality_status, 'trusted') <> 'trusted'
               OR COALESCE(fwd_5d_anomaly, FALSE)
               OR COALESCE(fwd_return_anomaly, FALSE)
            ORDER BY run_date DESC, symbol_id
            """
        ).fetchdf()

    frames = {
        "rank_bucket_performance": rank_bucket,
        "sector_performance": sector,
        "repeated_symbol_performance": repeated,
        "excluded_rows": excluded,
    }
    return {
        "summary": _summary_from_frames(build_tracker_health(project_root=project_root), frames),
        "frames": frames,
    }


def _rank_bucket_performance(con, *, recent_cutoff) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in FORWARD_HORIZONS:
        ret = f"fwd_{horizon}d_return"
        for period, where in _period_filters(recent_cutoff):
            frames.append(con.execute(
                f"""
                SELECT
                    ? AS period,
                    ? AS horizon,
                    CASE
                        WHEN rank_position <= 10 THEN 'top-10'
                        WHEN rank_position <= 25 THEN 'rank-11-25'
                        WHEN rank_position <= 50 THEN 'rank-26-50'
                        ELSE 'rank-51-plus'
                    END AS rank_bucket,
                    COUNT(*) AS rows,
                    AVG({ret}) AS avg_return,
                    MEDIAN({ret}) AS median_return,
                    100.0 * AVG(CASE WHEN {ret} > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_pct
                FROM rank_cohort_performance_trusted
                WHERE {ret} IS NOT NULL {where}
                GROUP BY 1, 2, 3
                ORDER BY period, horizon, MIN(rank_position)
                """,
                [period, f"{horizon}d"],
            ).fetchdf())
    return _concat(frames)


def _sector_performance(con, *, recent_cutoff) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in FORWARD_HORIZONS:
        ret = f"fwd_{horizon}d_return"
        for period, where in _period_filters(recent_cutoff):
            frames.append(con.execute(
                f"""
                SELECT
                    ? AS period,
                    ? AS horizon,
                    COALESCE(NULLIF(sector_name, ''), 'unknown') AS sector_name,
                    COUNT(*) AS rows,
                    AVG({ret}) AS avg_return,
                    MEDIAN({ret}) AS median_return,
                    100.0 * AVG(CASE WHEN {ret} > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_pct
                FROM rank_cohort_performance_trusted
                WHERE {ret} IS NOT NULL {where}
                GROUP BY 1, 2, 3
                ORDER BY rows DESC, avg_return DESC
                """,
                [period, f"{horizon}d"],
            ).fetchdf())
    return _concat(frames)


def _repeated_symbol_performance(con, *, repeated_min_n: int) -> pd.DataFrame:
    return con.execute(
        """
        WITH symbol_perf AS (
            SELECT
                symbol_id,
                COALESCE(NULLIF(sector_name, ''), 'unknown') AS sector_name,
                COUNT(*) AS rows,
                AVG(fwd_20d_return) AS avg_20d_return,
                MEDIAN(fwd_20d_return) AS median_20d_return,
                100.0 * AVG(CASE WHEN fwd_20d_return > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_pct
            FROM rank_cohort_performance_trusted
            WHERE fwd_20d_return IS NOT NULL
            GROUP BY 1, 2
            HAVING COUNT(*) >= ?
        ),
        winners AS (
            SELECT 'winner' AS direction, *
            FROM symbol_perf
            ORDER BY avg_20d_return DESC, rows DESC
            LIMIT 25
        ),
        losers AS (
            SELECT 'loser' AS direction, *
            FROM symbol_perf
            ORDER BY avg_20d_return ASC, rows DESC
            LIMIT 25
        )
        SELECT * FROM winners
        UNION ALL
        SELECT * FROM losers
        """,
        [int(repeated_min_n)],
    ).fetchdf()


def _summary_from_frames(health: dict, frames: dict[str, pd.DataFrame]) -> dict:
    rank_frame = frames["rank_bucket_performance"]
    top_20d = []
    if not rank_frame.empty:
        top_20d = rank_frame.loc[
            (rank_frame["period"] == "all") & (rank_frame["horizon"] == "20d")
        ].to_dict("records")
    warnings = list(health.get("warning_reasons") or [])
    if not rank_frame.empty:
        recent_20d = rank_frame.loc[
            (rank_frame["period"].astype(str).str.startswith("recent_"))
            & (rank_frame["horizon"] == "20d")
        ]
        if recent_20d.empty:
            warnings.append("recent 20d cohorts are not mature enough for rank-bucket diagnostics")
    return {
        "status": health.get("status", "unknown"),
        "health": health,
        "warnings": warnings,
        "rank_bucket_20d": top_20d,
        "artifact_rows": {
            name: int(len(frame))
            for name, frame in frames.items()
        },
    }


def _period_filters(recent_cutoff) -> list[tuple[str, str]]:
    filters = [("all", "")]
    if recent_cutoff is not None:
        filters.append(("recent_120d", f"AND run_date >= DATE '{recent_cutoff.isoformat()}'"))
    return filters


def _concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _empty_reports() -> dict[str, pd.DataFrame]:
    return {
        "rank_bucket_performance": pd.DataFrame(),
        "sector_performance": pd.DataFrame(),
        "repeated_symbol_performance": pd.DataFrame(),
        "excluded_rows": pd.DataFrame(),
    }


__all__ = ["build_research_quality_reports"]
