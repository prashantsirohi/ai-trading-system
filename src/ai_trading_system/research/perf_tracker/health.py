"""Operational health checks for the performance-tracker store."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.research.perf_tracker.backfill import _latest_attempt_per_date
from ai_trading_system.research.perf_tracker.schema import open_research_db, research_db_path


FIXTURE_SYMBOL_SQL = r"^(BASE|DRIFT|REC|OLD|R|T|SYM)[0-9]+$|^(AAA|BBB|CCC)$"


def build_tracker_health(
    *,
    project_root: str | Path | None = None,
) -> dict:
    """Return a compact health payload without modifying tracker rows."""
    db_path = research_db_path(project_root=project_root)
    if db_path.exists():
        with open_research_db(project_root=project_root, read_only=True) as con:
            (
                raw_rows,
                trusted_rows,
                excluded_rows,
                anomaly_rows,
                reason_rows,
                fixture_rows,
                duplicate_keys,
                latest_date,
                watchlist_bucket_rows,
                known_sector_rows,
                matured_5d_rows,
                matured_10d_rows,
                matured_20d_rows,
                matured_60d_rows,
            ) = con.execute(
                f"""
                SELECT
                    COUNT(*) AS raw_rows,
                    COUNT(*) FILTER (
                        WHERE COALESCE(data_quality_status, 'trusted') = 'trusted'
                          AND NOT COALESCE(fwd_5d_anomaly, FALSE)
                          AND NOT COALESCE(fwd_return_anomaly, FALSE)
                    ) AS trusted_rows,
                    COUNT(*) FILTER (
                        WHERE COALESCE(data_quality_status, 'trusted') <> 'trusted'
                           OR COALESCE(fwd_5d_anomaly, FALSE)
                           OR COALESCE(fwd_return_anomaly, FALSE)
                    ) AS excluded_rows,
                    COUNT(*) FILTER (
                        WHERE COALESCE(fwd_5d_anomaly, FALSE)
                           OR COALESCE(fwd_return_anomaly, FALSE)
                    ) AS anomaly_rows,
                    COUNT(*) FILTER (WHERE data_quality_reason IS NOT NULL AND data_quality_reason <> '') AS reason_rows,
                    COUNT(*) FILTER (
                        WHERE sector_name = 'Test'
                           OR regexp_matches(symbol_id, '{FIXTURE_SYMBOL_SQL}')
                    ) AS fixture_rows,
                    COUNT(*) - COUNT(DISTINCT (run_date, symbol_id, exchange)) AS duplicate_keys,
                    MAX(run_date) AS latest_date,
                    COUNT(*) FILTER (WHERE watchlist_bucket IS NOT NULL AND watchlist_bucket <> '') AS watchlist_bucket_rows,
                    COUNT(*) FILTER (
                        WHERE sector_name IS NOT NULL
                          AND sector_name <> ''
                          AND LOWER(sector_name) <> 'unknown'
                    ) AS known_sector_rows,
                    COUNT(*) FILTER (WHERE fwd_5d_return IS NOT NULL) AS matured_5d_rows,
                    COUNT(*) FILTER (WHERE fwd_10d_return IS NOT NULL) AS matured_10d_rows,
                    COUNT(*) FILTER (WHERE fwd_20d_return IS NOT NULL) AS matured_20d_rows,
                    COUNT(*) FILTER (WHERE fwd_60d_return IS NOT NULL) AS matured_60d_rows
                FROM rank_cohort_performance
                """
            ).fetchone()
            reason_counts = con.execute(
                """
                SELECT COALESCE(data_quality_reason, 'unspecified') AS reason, COUNT(*) AS rows
                FROM rank_cohort_performance
                WHERE COALESCE(data_quality_status, 'trusted') <> 'trusted'
                   OR COALESCE(fwd_5d_anomaly, FALSE)
                   OR COALESCE(fwd_return_anomaly, FALSE)
                GROUP BY 1
                ORDER BY rows DESC, reason
                LIMIT 20
                """
            ).fetchall()
            recent_pipeline_counts = con.execute(
                """
                SELECT run_date, COUNT(*) AS rows
                FROM rank_cohort_performance
                WHERE source_type = 'pipeline'
                GROUP BY run_date
                ORDER BY run_date DESC
                LIMIT 10
                """
            ).fetchall()
    else:
        raw_rows = trusted_rows = excluded_rows = anomaly_rows = reason_rows = fixture_rows = duplicate_keys = 0
        watchlist_bucket_rows = known_sector_rows = 0
        matured_5d_rows = matured_10d_rows = matured_20d_rows = matured_60d_rows = 0
        latest_date = None
        reason_counts = []
        recent_pipeline_counts = []

    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    artifact_dates = sorted(_latest_attempt_per_date(paths.pipeline_runs_dir))
    latest_artifact_date = artifact_dates[-1] if artifact_dates else None
    latest_date_str = latest_date.isoformat() if latest_date else None
    lag_days = None
    if latest_artifact_date and latest_date:
        lag_days = (date.fromisoformat(latest_artifact_date) - latest_date).days

    cohort_drop = False
    cohort_drop_detail = None
    if len(recent_pipeline_counts) >= 4:
        latest_count = int(recent_pipeline_counts[0][1])
        prior = sorted(int(row[1]) for row in recent_pipeline_counts[1:])
        median_prior = float(prior[len(prior) // 2])
        cohort_drop = median_prior > 0 and latest_count < median_prior * 0.20
        cohort_drop_detail = {
            "latest_rows": latest_count,
            "median_prior_rows": median_prior,
        }

    critical_reasons: list[str] = []
    warning_reasons: list[str] = []
    if fixture_rows:
        critical_reasons.append(f"fixture-like rows present: {int(fixture_rows)}")
    if duplicate_keys:
        critical_reasons.append(f"duplicate keys present: {int(duplicate_keys)}")
    if lag_days is not None and lag_days > 0:
        warning_reasons.append(f"tracker lags latest pipeline artifact by {lag_days} days")
    if excluded_rows:
        warning_reasons.append(f"excluded rows retained for inspection: {int(excluded_rows)}")
    if anomaly_rows and reason_rows < anomaly_rows:
        warning_reasons.append("some excluded rows are missing data_quality_reason")
    if raw_rows:
        watchlist_coverage = float(watchlist_bucket_rows or 0) / float(raw_rows)
        sector_coverage = float(known_sector_rows or 0) / float(raw_rows)
        if watchlist_coverage < 0.50:
            warning_reasons.append(f"watchlist bucket coverage is sparse: {watchlist_coverage:.1%}")
        if sector_coverage < 0.50:
            warning_reasons.append(f"known sector coverage is sparse: {sector_coverage:.1%}")
    else:
        watchlist_coverage = 0.0
        sector_coverage = 0.0
    if cohort_drop:
        warning_reasons.append("latest operational cohort is below 20% of recent median")

    status = "critical" if critical_reasons else "warning" if warning_reasons else "ok"
    maturity = {
        "5d": int(matured_5d_rows or 0),
        "10d": int(matured_10d_rows or 0),
        "20d": int(matured_20d_rows or 0),
        "60d": int(matured_60d_rows or 0),
    }
    return {
        "status": status,
        "critical_reasons": critical_reasons,
        "warning_reasons": warning_reasons,
        "raw_rows": int(raw_rows or 0),
        "trusted_rows": int(trusted_rows or 0),
        "excluded_rows": int(excluded_rows or 0),
        "anomaly_rows": int(anomaly_rows or 0),
        "data_quality_reason_rows": int(reason_rows or 0),
        "data_quality_reason_counts": [
            {"reason": str(reason), "rows": int(rows or 0)}
            for reason, rows in reason_counts
        ],
        "fixture_rows": int(fixture_rows or 0),
        "duplicate_keys": int(duplicate_keys or 0),
        "watchlist_bucket_coverage_pct": round(watchlist_coverage * 100.0, 2),
        "known_sector_coverage_pct": round(sector_coverage * 100.0, 2),
        "maturity_rows": maturity,
        "latest_date": latest_date_str,
        "latest_pipeline_artifact_date": latest_artifact_date,
        "lag_days": lag_days,
        "cohort_drop": cohort_drop,
        "cohort_drop_detail": cohort_drop_detail,
    }


__all__ = ["build_tracker_health"]
