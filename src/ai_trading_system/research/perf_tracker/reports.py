"""Research-quality diagnostics for rank cohort performance."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from ai_trading_system.research.perf_tracker.constants import FACTOR_COLUMNS
from ai_trading_system.research.perf_tracker.forward_returns import FORWARD_HORIZONS
from ai_trading_system.research.perf_tracker.health import build_tracker_health
from ai_trading_system.research.perf_tracker.schema import open_research_db, research_db_path


MIN_FEEDBACK_SAMPLE = 30
IC_POSITIVE_THRESHOLD = 0.02
IC_NEGATIVE_THRESHOLD = -0.02


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


def build_ranking_feedback_summary(
    *,
    project_root: str | Path | None = None,
    lookback_days: int = 180,
) -> dict:
    """Build an observational ranking feedback loop from trusted cohort rows."""
    db_path = research_db_path(project_root=project_root)
    empty = _empty_feedback_summary(lookback_days=lookback_days, status="missing")
    if not db_path.exists():
        empty["warnings"].append("research DuckDB is missing; run perf_tracker backfill first")
        return empty

    with open_research_db(project_root=project_root, read_only=True) as con:
        latest = con.execute("SELECT MAX(run_date) FROM rank_cohort_performance_trusted").fetchone()[0]
        trusted_rows = int(con.execute("SELECT COUNT(*) FROM rank_cohort_performance_trusted").fetchone()[0] or 0)
        if latest is None or trusted_rows == 0:
            empty = _empty_feedback_summary(lookback_days=lookback_days, status="empty")
            empty["warnings"].append("No mature trusted tracker data available")
            return empty

        cutoff = latest - timedelta(days=int(lookback_days)) if lookback_days and lookback_days > 0 else None
        rank_rows = _feedback_rank_bucket_rows(con, cutoff=cutoff)
        factor_rows = _feedback_factor_ic_rows(con, cutoff=cutoff, lookback_days=lookback_days)
        bucket_rows = _feedback_watchlist_bucket_rows(con, cutoff=cutoff)
        drift_rows = _feedback_drift_rows(con, latest=latest, lookback_days=lookback_days)

    warnings = []
    if not rank_rows:
        warnings.append("No mature trusted tracker data available")
    has_sufficient_factor = any(row.get("signal") != "insufficient_sample" for row in factor_rows)
    if not has_sufficient_factor:
        warnings.append("No factor IC rows had enough trusted samples")
    recommendations = _feedback_recommendations(
        rank_rows=rank_rows,
        factor_rows=factor_rows,
        bucket_rows=bucket_rows,
        drift_rows=drift_rows,
    )
    status = "ok" if rank_rows or bucket_rows or has_sufficient_factor else "empty"
    if any(row.get("status") in {"warning", "critical"} for row in drift_rows):
        status = "warning"
    return {
        "status": status,
        "as_of": latest.isoformat() if isinstance(latest, date) else str(latest),
        "lookback_days": int(lookback_days),
        "rank_bucket_rows": rank_rows,
        "factor_ic_rows": factor_rows,
        "bucket_rows": bucket_rows,
        "drift_rows": drift_rows,
        "recommendations": recommendations,
        "warnings": warnings,
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


def _feedback_rank_bucket_rows(con, *, cutoff) -> list[dict]:
    frames: list[pd.DataFrame] = []
    cutoff_sql = _cutoff_sql(cutoff)
    for horizon in FORWARD_HORIZONS:
        ret = f"fwd_{horizon}d_return"
        frames.append(con.execute(
            f"""
            SELECT
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
                100.0 * AVG(CASE WHEN {ret} > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_pct,
                MIN(rank_position) AS sort_rank
            FROM rank_cohort_performance_trusted
            WHERE {ret} IS NOT NULL {cutoff_sql}
            GROUP BY 1, 2
            ORDER BY 1, sort_rank
            """,
            [f"{horizon}d"],
        ).fetchdf())
    frame = _concat(frames)
    if frame.empty:
        return []
    frame = frame.copy()
    frame = frame.drop(columns=["sort_rank"], errors="ignore")
    for column in ("avg_return", "median_return", "win_rate_pct"):
        frame.loc[:, column] = frame[column].map(lambda value: _round(value, 3))
    return frame.to_dict("records")


def _feedback_watchlist_bucket_rows(con, *, cutoff) -> list[dict]:
    frames: list[pd.DataFrame] = []
    cutoff_sql = _cutoff_sql(cutoff)
    for horizon in FORWARD_HORIZONS:
        ret = f"fwd_{horizon}d_return"
        frames.append(con.execute(
            f"""
            SELECT
                ? AS horizon,
                COALESCE(watchlist_bucket, 'unassigned') AS bucket,
                COUNT(*) AS rows,
                AVG({ret}) AS avg_return,
                MEDIAN({ret}) AS median_return,
                100.0 * AVG(CASE WHEN {ret} > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_pct
            FROM rank_cohort_performance_trusted
            WHERE {ret} IS NOT NULL {cutoff_sql}
            GROUP BY 1, 2
            ORDER BY 1, rows DESC, avg_return DESC
            """,
            [f"{horizon}d"],
        ).fetchdf())
    frame = _concat(frames)
    if frame.empty:
        return []
    frame = frame.copy()
    frame.loc[:, "interpretation"] = frame.apply(_bucket_interpretation, axis=1)
    for column in ("avg_return", "median_return", "win_rate_pct"):
        frame.loc[:, column] = frame[column].map(lambda value: _round(value, 3))
    return frame.to_dict("records")


def _feedback_factor_ic_rows(con, *, cutoff, lookback_days: int) -> list[dict]:
    cutoff_sql = _cutoff_sql(cutoff)
    rows: list[dict] = []
    for factor in FACTOR_COLUMNS:
        select_cols = [factor, *(f"fwd_{horizon}d_return" for horizon in FORWARD_HORIZONS)]
        df = con.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM rank_cohort_performance_trusted
            WHERE {factor} IS NOT NULL {cutoff_sql}
            """
        ).fetchdf()
        for horizon in FORWARD_HORIZONS:
            fwd_col = f"fwd_{horizon}d_return"
            valid = df[[factor, fwd_col]].dropna() if not df.empty else pd.DataFrame()
            ic = _spearman_ic(valid[factor], valid[fwd_col]) if len(valid) >= MIN_FEEDBACK_SAMPLE else None
            rows.append({
                "factor": factor.replace("factor_", ""),
                "horizon": f"{horizon}d",
                "window_days": int(lookback_days),
                "rows": int(len(valid)),
                "ic": _round(ic, 4),
                "signal": _ic_signal(ic, int(len(valid))),
            })
    return rows


def _feedback_drift_rows(con, *, latest, lookback_days: int) -> list[dict]:
    recent_cutoff = latest - timedelta(days=30)
    baseline_cutoff = latest - timedelta(days=int(lookback_days)) if lookback_days and lookback_days > 0 else None
    rows = []
    for factor in FACTOR_COLUMNS:
        recent_ic, recent_n = _factor_horizon_ic(con, factor=factor, horizon=20, cutoff=recent_cutoff)
        baseline_ic, baseline_n = _factor_horizon_ic(con, factor=factor, horizon=20, cutoff=baseline_cutoff)
        if recent_ic is None or baseline_ic is None:
            status = "insufficient_sample"
            delta_ic = None
        else:
            delta_ic = recent_ic - baseline_ic
            if baseline_ic >= 0.05 and delta_ic <= -0.03:
                status = "critical"
            elif delta_ic <= -0.02:
                status = "warning"
            else:
                status = "ok"
        rows.append({
            "factor": factor.replace("factor_", ""),
            "horizon": "20d",
            "recent_window_days": 30,
            "baseline_window_days": int(lookback_days),
            "recent_rows": int(recent_n),
            "baseline_rows": int(baseline_n),
            "recent_ic": _round(recent_ic, 4),
            "baseline_ic": _round(baseline_ic, 4),
            "delta_ic": _round(delta_ic, 4),
            "status": status,
        })
    return rows


def _factor_horizon_ic(con, *, factor: str, horizon: int, cutoff) -> tuple[float | None, int]:
    cutoff_sql = _cutoff_sql(cutoff)
    ret = f"fwd_{horizon}d_return"
    df = con.execute(
        f"""
        SELECT {factor}, {ret}
        FROM rank_cohort_performance_trusted
        WHERE {factor} IS NOT NULL
          AND {ret} IS NOT NULL
          {cutoff_sql}
        """
    ).fetchdf()
    if len(df) < MIN_FEEDBACK_SAMPLE:
        return None, int(len(df))
    return _spearman_ic(df[factor], df[ret]), int(len(df))


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


def _cutoff_sql(cutoff) -> str:
    if cutoff is None:
        return ""
    return f"AND run_date >= DATE '{cutoff.isoformat()}'"


def _spearman_ic(x: pd.Series, y: pd.Series) -> float | None:
    x_rank = x.rank()
    y_rank = y.rank()
    if x_rank.nunique(dropna=True) < 2 or y_rank.nunique(dropna=True) < 2:
        return None
    corr = float(x_rank.corr(y_rank))
    return corr if corr == corr else None  # noqa: PLR0124


def _ic_signal(ic: float | None, rows: int) -> str:
    if rows < MIN_FEEDBACK_SAMPLE or ic is None:
        return "insufficient_sample"
    if ic >= IC_POSITIVE_THRESHOLD:
        return "positive"
    if ic <= IC_NEGATIVE_THRESHOLD:
        return "negative"
    return "neutral"


def _bucket_interpretation(row: pd.Series) -> str:
    rows = int(row.get("rows") or 0)
    avg_return = row.get("avg_return")
    win_rate = row.get("win_rate_pct")
    if rows < MIN_FEEDBACK_SAMPLE:
        return "insufficient_sample"
    if avg_return is not None and avg_return < 0 and win_rate is not None and win_rate < 40.0:
        return "weak"
    if avg_return is not None and avg_return > 0 and win_rate is not None and win_rate >= 50.0:
        return "useful"
    return "mixed"


def _feedback_recommendations(
    *,
    rank_rows: list[dict],
    factor_rows: list[dict],
    bucket_rows: list[dict],
    drift_rows: list[dict],
) -> list[dict]:
    recommendations: list[dict] = []
    top10_20d = _find_metric(rank_rows, key="rank_bucket", value="top-10", horizon="20d", metric="avg_return")
    lower_20d = _find_metric(rank_rows, key="rank_bucket", value="rank-51-plus", horizon="20d", metric="avg_return")
    if top10_20d is not None and lower_20d is not None:
        edge = top10_20d - lower_20d
        decision = "backtest_required" if edge > 0 else "reduce_candidate"
        recommendations.append({
            "category": "rank_bucket",
            "subject": "top-10_vs_rank-51-plus_20d",
            "decision": decision,
            "evidence": f"top-10 avg_20d edge is {_round(edge, 3)} pp",
        })

    factor_20d = [row for row in factor_rows if row.get("horizon") == "20d" and row.get("ic") is not None]
    factor_20d = sorted(factor_20d, key=lambda row: float(row.get("ic") or 0), reverse=True)
    for row in factor_20d[:3]:
        if row.get("signal") == "positive":
            recommendations.append({
                "category": "factor",
                "subject": row["factor"],
                "decision": "increase_candidate",
                "evidence": f"20d IC {_round(row.get('ic'), 4)} over {row.get('rows')} trusted rows; backtest before changing weights",
            })
    for row in factor_20d[-3:]:
        if row.get("signal") == "negative":
            recommendations.append({
                "category": "factor",
                "subject": row["factor"],
                "decision": "reduce_candidate",
                "evidence": f"20d IC {_round(row.get('ic'), 4)} over {row.get('rows')} trusted rows",
            })

    for row in bucket_rows:
        if row.get("horizon") == "20d" and row.get("interpretation") == "weak":
            recommendations.append({
                "category": "watchlist_bucket",
                "subject": row["bucket"],
                "decision": "gate_candidate",
                "evidence": f"20d avg {row.get('avg_return')} with win rate {row.get('win_rate_pct')}%",
            })

    for row in drift_rows:
        if row.get("status") in {"warning", "critical"}:
            recommendations.append({
                "category": "drift",
                "subject": row["factor"],
                "decision": "reduce_candidate",
                "evidence": f"recent 20d IC {row.get('recent_ic')} vs baseline {row.get('baseline_ic')}",
            })

    if not recommendations:
        recommendations.append({
            "category": "sample",
            "subject": "ranking_feedback",
            "decision": "insufficient_sample",
            "evidence": "No factor or bucket changes have enough trusted evidence yet",
        })
    return recommendations[:12]


def _find_metric(rows: list[dict], *, key: str, value: str, horizon: str, metric: str) -> float | None:
    for row in rows:
        if row.get(key) == value and row.get("horizon") == horizon:
            metric_value = row.get(metric)
            if metric_value is not None:
                return float(metric_value)
    return None


def _round(value, ndigits: int = 2):
    if value is None or pd.isna(value):
        return None
    return round(float(value), ndigits)


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


def _empty_feedback_summary(*, lookback_days: int, status: str) -> dict:
    return {
        "status": status,
        "as_of": None,
        "lookback_days": int(lookback_days),
        "rank_bucket_rows": [],
        "factor_ic_rows": [],
        "bucket_rows": [],
        "drift_rows": [],
        "recommendations": [],
        "warnings": [],
    }


__all__ = ["build_research_quality_reports", "build_ranking_feedback_summary"]
