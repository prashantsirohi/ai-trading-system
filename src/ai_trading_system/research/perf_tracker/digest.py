"""Weekly performance digest for the rank cohort tracker.

Produces a markdown report covering:

  1. Cohort forward returns       — top-10 / top-50 / top-200 / rest
  2. Bucket attribution           — TRIGGERED_TODAY / CORE_MOMENTUM / EARLY_STAGE2 / AVOID
  3. Factor information coefficient — Spearman correlation of factor scores
                                    with fwd-20d return, rolling 30 / 90 days
  4. Drift watch                  — flag factors whose IC dropped > 30% vs
                                    6-month baseline

The report is written to ``data/research/perf_digests/digest_<YYYY-WW>.md``
and also returned as a string so callers can pipe to Telegram/Slack.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.research.perf_tracker.constants import (
    COMPOSITION_OPTIONAL_COLUMNS,
    CONCENTRATION_STRONG_DELTA,
    CONCENTRATION_WEAK_DELTA,
    COVERAGE_OK_PCT,
    DRIFT_CRITICAL_MIN_BASELINE_IC,
    DRIFT_CRITICAL_MIN_DELTA_IC,
    DRIFT_CRITICAL_MIN_RECENT_N,
    DRIFT_THRESHOLD_PCT,
    DRIFT_WARNING_MIN_RECENT_N,
    FACTOR_COLUMNS,
    FORWARD_RETURN_ANOMALY_5D_PCT,
    MATURATION_WARNING_RATIO,
    SAME_DATE_SMALL_SAMPLE_DAYS,
    SAME_DATE_SMALL_SAMPLE_ROWS,
)
from ai_trading_system.research.perf_tracker.schema import open_research_db

logger = logging.getLogger(__name__)

# Cohort definitions used across cohort + bucket sections.
COHORT_BANDS: tuple[tuple[str, int, int], ...] = (
    ("top-10",   1,    10),
    ("top-50",   1,    50),
    ("top-200",  1,    200),
    ("51-200",   51,   200),
    ("201+",     201,  10_000_000),
)

BUCKET_ORDER_SQL = """
    CASE bucket
        WHEN 'TRIGGERED_TODAY' THEN 1
        WHEN 'CORE_MOMENTUM'   THEN 2
        WHEN 'EARLY_STAGE2'    THEN 3
        WHEN 'AVOID_WEAK_CONFIRMATION' THEN 4
        WHEN 'unassigned' THEN 98
        ELSE 99
    END
"""


@dataclass
class DigestResult:
    """Bundle of artifacts produced by ``build_digest``."""

    markdown: str
    output_path: Path
    section_data: dict[str, pd.DataFrame]


def _cohort_returns(con) -> pd.DataFrame:
    rows = []
    for label, lo, hi in COHORT_BANDS:
        # n_5d / n_20d are the matured-row counts — hit rates must divide by
        # those, not by total row count, otherwise pending rows deflate the
        # ratio. ``COUNT(col)`` skips NULLs in DuckDB; ``AVG`` does too.
        result = con.execute(
            f"""
            SELECT
                COUNT(*)                                                     AS n_total,
                COUNT(fwd_5d_return)                                         AS n_5d,
                COUNT(fwd_20d_return)                                        AS n_20d,
                AVG(fwd_5d_return)                                           AS avg_5d,
                AVG(fwd_10d_return)                                          AS avg_10d,
                AVG(fwd_20d_return)                                          AS avg_20d,
                AVG(fwd_60d_return)                                          AS avg_60d,
                100.0 * SUM(CASE WHEN fwd_5d_return  > 0 THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(fwd_5d_return),  0)                     AS hitrate_5d,
                100.0 * SUM(CASE WHEN fwd_20d_return > 0 THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(fwd_20d_return), 0)                     AS hitrate_20d
            FROM rank_cohort_performance
            WHERE rank_position BETWEEN {lo} AND {hi}
            """
        ).fetchone()
        n_total = int(result[0] or 0)
        n_20d = int(result[2] or 0)
        maturation_20d = (n_20d / n_total) if n_total else None
        rows.append({
            "cohort":      label,
            "n_total":     n_total,
            "n_20d":       n_20d,
            "avg_5d":      _round(result[3]),
            "avg_10d":     _round(result[4]),
            "avg_20d":     _round(result[5]),
            "avg_60d":     _round(result[6]),
            "hitrate_5d":  _round(result[7]),
            "hitrate_20d": _round(result[8]),
            "matured_20d_pct": _round((maturation_20d or 0) * 100.0, 1),
            "low_maturation": bool(
                maturation_20d is not None and maturation_20d < MATURATION_WARNING_RATIO
            ),
        })
    return pd.DataFrame(rows)


def _bucket_attribution(con) -> pd.DataFrame:
    df = con.execute(
        f"""
        SELECT
            COALESCE(watchlist_bucket, 'unassigned') AS bucket,
            COUNT(*) AS n,
            ROUND(AVG(fwd_5d_return),  2) AS avg_5d,
            ROUND(AVG(fwd_10d_return), 2) AS avg_10d,
            ROUND(AVG(fwd_20d_return), 2) AS avg_20d,
            ROUND(100.0 * SUM(CASE WHEN fwd_5d_return  > 0 THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(fwd_5d_return),  0), 1) AS hitrate_5d,
            ROUND(100.0 * SUM(CASE WHEN fwd_20d_return > 0 THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(fwd_20d_return), 0), 1) AS hitrate_20d
        FROM rank_cohort_performance
        GROUP BY bucket
        ORDER BY {BUCKET_ORDER_SQL}
        """
    ).fetchdf()
    return df


def _bucket_coverage(con) -> pd.DataFrame:
    return con.execute(
        f"""
        SELECT
            COALESCE(watchlist_bucket, 'unassigned') AS bucket,
            MIN(run_date) AS first_date,
            MAX(run_date) AS last_date,
            COUNT(*) AS rows,
            COUNT(DISTINCT run_date) AS dates
        FROM rank_cohort_performance
        GROUP BY bucket
        ORDER BY {BUCKET_ORDER_SQL}
        """
    ).fetchdf()


def _same_date_bucket_attribution(con) -> pd.DataFrame:
    control = con.execute(
        """
        WITH bucket_dates AS (
            SELECT DISTINCT run_date
            FROM rank_cohort_performance
            WHERE watchlist_bucket IS NOT NULL
        )
        SELECT
            AVG(fwd_5d_return),
            AVG(fwd_10d_return),
            AVG(fwd_20d_return)
        FROM rank_cohort_performance
        WHERE run_date IN (SELECT run_date FROM bucket_dates)
        """
    ).fetchone()
    control_5d = _round(control[0])
    control_10d = _round(control[1])
    control_20d = _round(control[2])
    df = con.execute(
        f"""
        WITH bucket_dates AS (
            SELECT DISTINCT run_date
            FROM rank_cohort_performance
            WHERE watchlist_bucket IS NOT NULL
        )
        SELECT
            COALESCE(watchlist_bucket, 'unassigned') AS bucket,
            COUNT(*) AS n,
            COUNT(fwd_5d_return) AS n_5d,
            COUNT(fwd_20d_return) AS n_20d,
            AVG(fwd_5d_return) AS avg_5d,
            AVG(fwd_10d_return) AS avg_10d,
            AVG(fwd_20d_return) AS avg_20d,
            100.0 * SUM(CASE WHEN fwd_5d_return  > 0 THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(fwd_5d_return),  0) AS hitrate_5d,
            100.0 * SUM(CASE WHEN fwd_20d_return > 0 THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(fwd_20d_return), 0) AS hitrate_20d
        FROM rank_cohort_performance
        WHERE run_date IN (SELECT run_date FROM bucket_dates)
        GROUP BY bucket
        ORDER BY {BUCKET_ORDER_SQL}
        """
    ).fetchdf()
    if df.empty:
        return df
    df.loc[:, "avg_5d"] = df["avg_5d"].map(_round)
    df.loc[:, "avg_10d"] = df["avg_10d"].map(_round)
    df.loc[:, "avg_20d"] = df["avg_20d"].map(_round)
    df.loc[:, "hitrate_5d"] = df["hitrate_5d"].map(lambda v: _round(v, 1))
    df.loc[:, "hitrate_20d"] = df["hitrate_20d"].map(lambda v: _round(v, 1))
    df["control_avg_5d"] = control_5d
    df["control_avg_10d"] = control_10d
    df["control_avg_20d"] = control_20d
    df.loc[:, "excess_5d"] = df["avg_5d"].map(
        lambda v: _round(v - control_5d) if v is not None and control_5d is not None else None
    )
    df.loc[:, "excess_10d"] = df["avg_10d"].map(
        lambda v: _round(v - control_10d) if v is not None and control_10d is not None else None
    )
    df.loc[:, "excess_20d"] = df["avg_20d"].map(
        lambda v: _round(v - control_20d) if v is not None and control_20d is not None else None
    )
    return df


def _factor_ic(con, *, window_days: int) -> pd.DataFrame:
    """Spearman rank correlation between each factor score and fwd-20d return.

    Computed over the last ``window_days`` trading days. Spearman rather than
    Pearson because we care about ordinal predictive power (does higher factor
    score map to higher forward return), not linear fit.
    """
    available = [
        col for col in FACTOR_COLUMNS
        if con.execute(
            f"SELECT COUNT(*) FROM rank_cohort_performance WHERE {col} IS NOT NULL"
        ).fetchone()[0] > 0
    ]
    if not available:
        return pd.DataFrame(columns=["factor", "n", f"ic_20d_{window_days}d"])

    cols = ", ".join(available)
    df = con.execute(
        f"""
        SELECT {cols}, fwd_20d_return
        FROM rank_cohort_performance
        WHERE fwd_20d_return IS NOT NULL
          AND run_date >= (
              SELECT MAX(run_date) - INTERVAL '{window_days} days'
              FROM rank_cohort_performance
          )
        """
    ).fetchdf()

    if df.empty:
        return pd.DataFrame(columns=["factor", "n", f"ic_20d_{window_days}d"])

    rows = []
    for factor in available:
        valid = df[[factor, "fwd_20d_return"]].dropna()
        ic = _rank_ic(valid)
        rows.append({
            "factor": factor.replace("factor_", ""),
            "n": int(len(valid)),
            f"ic_20d_{window_days}d": _round(ic),
        })
    return pd.DataFrame(rows)


def _rank_ic(valid: pd.DataFrame) -> float | None:
    if len(valid) < 30:
        return None
    # Spearman = Pearson on the ranks. Done by hand to avoid the scipy dependency.
    x_rank = valid.iloc[:, 0].rank()
    y_rank = valid["fwd_20d_return"].rank()
    corr = float(x_rank.corr(y_rank))
    return corr if corr == corr else None  # noqa: PLR0124


def _conditional_factor_ic(con, *, window_days: int) -> pd.DataFrame:
    cohorts = {
        "full_universe": "1=1",
        "top_200_only": "rank_position BETWEEN 1 AND 200",
        "rank_201_plus_only": "rank_position >= 201",
    }
    rows = [{"factor": factor.replace("factor_", "")} for factor in FACTOR_COLUMNS]
    by_factor = {row["factor"]: row for row in rows}
    cols = ", ".join(FACTOR_COLUMNS)
    for cohort, clause in cohorts.items():
        df = con.execute(
            f"""
            SELECT {cols}, fwd_20d_return
            FROM rank_cohort_performance
            WHERE fwd_20d_return IS NOT NULL
              AND {clause}
              AND run_date >= (
                  SELECT MAX(run_date) - INTERVAL '{window_days} days'
                  FROM rank_cohort_performance
              )
            """
        ).fetchdf()
        for factor in FACTOR_COLUMNS:
            valid = df[[factor, "fwd_20d_return"]].dropna() if not df.empty else pd.DataFrame()
            row = by_factor[factor.replace("factor_", "")]
            row[f"ic_20d_{window_days}d_{cohort}"] = _round(_rank_ic(valid))
            row[f"n_{window_days}d_{cohort}"] = int(len(valid))
    return pd.DataFrame(rows)


def _factor_coverage(con) -> pd.DataFrame:
    total = con.execute("SELECT COUNT(*) FROM rank_cohort_performance").fetchone()[0] or 0
    rows = []
    for factor in FACTOR_COLUMNS:
        non_null, first_date, last_date = con.execute(
            f"""
            SELECT
                COUNT({factor}),
                MIN(CASE WHEN {factor} IS NOT NULL THEN run_date END),
                MAX(CASE WHEN {factor} IS NOT NULL THEN run_date END)
            FROM rank_cohort_performance
            """
        ).fetchone()
        non_null_count = int(non_null or 0)
        total_int = int(total)
        null_count = max(total_int - non_null_count, 0)
        if total_int == 0:
            coverage_pct = None
            null_pct = 100.0
        else:
            coverage_pct = 100.0 * non_null_count / total_int
            null_pct = 100.0 * null_count / total_int
        if coverage_pct is None or coverage_pct <= 0:
            status = "not_wired"
        elif coverage_pct < 50.0:
            status = "poor_coverage"
        elif coverage_pct < COVERAGE_OK_PCT:
            status = "partial_coverage"
        else:
            status = "ok"
        rows.append({
            "factor": factor.replace("factor_", ""),
            "total_rows": total_int,
            "non_null_count": non_null_count,
            "null_count": null_count,
            "coverage_pct": _round(coverage_pct, 1),
            "null_pct": _round(null_pct, 1),
            "first_available_date": first_date,
            "last_available_date": last_date,
            "status": status,
        })
    return pd.DataFrame(rows)


def _drift_watch(
    ic_30d: pd.DataFrame,
    ic_180d: pd.DataFrame,
    *,
    coverage: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Flag factors whose recent IC dropped > DRIFT_THRESHOLD_PCT vs baseline."""
    if ic_30d.empty or ic_180d.empty:
        return pd.DataFrame(columns=[
            "factor", "ic_recent", "ic_baseline", "recent_n", "baseline_n",
            "delta_ic", "delta_pct", "status", "alert",
        ])
    recent_col = next(c for c in ic_30d.columns if c.startswith("ic_"))
    base_col = next(c for c in ic_180d.columns if c.startswith("ic_"))
    recent_n_col = next(c for c in ic_30d.columns if c == "n" or c.startswith("n_"))
    base_n_col = next(c for c in ic_180d.columns if c == "n" or c.startswith("n_"))
    recent_frame = ic_30d.rename(columns={recent_col: "ic_recent_raw", recent_n_col: "recent_n_raw"})
    baseline_frame = ic_180d.rename(columns={base_col: "ic_baseline_raw", base_n_col: "baseline_n_raw"})
    merged = recent_frame.merge(baseline_frame, on="factor", how="outer")
    coverage_by_factor: dict[str, float | None] = {}
    if coverage is not None and not coverage.empty and "coverage_pct" in coverage.columns:
        for _, crow in coverage.iterrows():
            coverage_by_factor[crow["factor"]] = crow.get("coverage_pct")
    rows = []
    for _, row in merged.iterrows():
        recent = row["ic_recent_raw"]
        base = row["ic_baseline_raw"]
        recent_n = int(row["recent_n_raw"]) if not pd.isna(row["recent_n_raw"]) else 0
        baseline_n = int(row["baseline_n_raw"]) if not pd.isna(row["baseline_n_raw"]) else 0
        cov_pct = coverage_by_factor.get(row["factor"])
        unreliable = cov_pct is not None and cov_pct < COVERAGE_OK_PCT
        if pd.isna(recent) or pd.isna(base) or abs(base) < 1e-6:
            delta_pct = None
            delta_ic = None
            if unreliable:
                status = "unreliable_coverage"
            elif recent_n < DRIFT_WARNING_MIN_RECENT_N:
                status = "insufficient_sample"
            else:
                status = "no_baseline"
        else:
            delta_ic = recent - base
            delta_pct = delta_ic / abs(base) * 100.0
            if unreliable:
                status = "unreliable_coverage"
            elif recent_n < DRIFT_WARNING_MIN_RECENT_N:
                status = "insufficient_sample"
            elif recent_n < DRIFT_CRITICAL_MIN_RECENT_N:
                # Watch tier: cap below warning/critical.
                status = "watch" if delta_pct < -DRIFT_THRESHOLD_PCT else "ok"
            elif (
                delta_ic <= -DRIFT_CRITICAL_MIN_DELTA_IC
                and base >= DRIFT_CRITICAL_MIN_BASELINE_IC
            ):
                status = "critical"
            elif delta_pct < -DRIFT_THRESHOLD_PCT:
                status = "warning"
            else:
                status = "ok"
        alert = status.upper() if status in {"warning", "critical"} else ""
        rows.append({
            "factor":      row["factor"],
            "ic_recent":   _round(recent),
            "ic_baseline": _round(base),
            "recent_n":    recent_n,
            "baseline_n":  baseline_n,
            "delta_ic":    _round(delta_ic, 3),
            "delta_pct":   _round(delta_pct),
            "status":      status,
            "alert":       alert,
        })
    return pd.DataFrame(rows)


def _anomaly_summary(con) -> dict:
    """Count fwd-return rows whose magnitude is likely a corporate action.

    Forward returns are computed from raw OHLCV close — splits and bonuses
    cause spurious ±50%+ swings that distort cohort aggregations. This
    surfaces the count so users can investigate before relying on a digest
    that includes them.
    """
    threshold = FORWARD_RETURN_ANOMALY_5D_PCT
    row = con.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE fwd_5d_return IS NOT NULL) AS n_matured,
            COUNT(*) FILTER (
                WHERE fwd_5d_return IS NOT NULL AND ABS(fwd_5d_return) > {threshold}
            ) AS n_anomaly
        FROM rank_cohort_performance
        """
    ).fetchone()
    n_matured = int(row[0] or 0)
    n_anomaly = int(row[1] or 0)
    return {
        "threshold_pct": threshold,
        "n_matured": n_matured,
        "n_anomaly": n_anomaly,
        "pct": (100.0 * n_anomaly / n_matured) if n_matured else None,
    }


def _round(value, ndigits: int = 2):
    if value is None or pd.isna(value):
        return None
    return round(float(value), ndigits)


def _table_columns(con) -> set[str]:
    rows = con.execute("PRAGMA table_info('rank_cohort_performance')").fetchall()
    return {str(r[1]) for r in rows}


def _bucket_composition(con) -> pd.DataFrame:
    """Average factor state per bucket, including only columns that exist."""
    present = _table_columns(con)
    select_pieces = ["COALESCE(watchlist_bucket, 'unassigned') AS bucket",
                     "COUNT(*) AS n",
                     "ROUND(AVG(rank_position), 2) AS avg_rank_position"]
    for col in COMPOSITION_OPTIONAL_COLUMNS:
        if col in present:
            select_pieces.append(f"ROUND(AVG({col}), 3) AS avg_{col}")
    df = con.execute(
        f"""
        SELECT {', '.join(select_pieces)}
        FROM rank_cohort_performance
        GROUP BY bucket
        ORDER BY {BUCKET_ORDER_SQL}
        """
    ).fetchdf()
    # Pad missing columns with NaN so the rendered table shows '—'.
    for col in COMPOSITION_OPTIONAL_COLUMNS:
        key = f"avg_{col}"
        if key not in df.columns:
            df[key] = None
    return df


def _bucket_daily(con, *, lookback_days: int = 90) -> pd.DataFrame:
    lb = ""
    if lookback_days and lookback_days > 0:
        lb = (
            " AND run_date >= ("
            "  SELECT MAX(run_date) - INTERVAL '" + str(int(lookback_days)) + " days'"
            "  FROM rank_cohort_performance)"
        )
    return con.execute(
        f"""
        SELECT
            run_date,
            COALESCE(watchlist_bucket, 'unassigned') AS bucket,
            COUNT(*) AS n,
            ROUND(AVG(fwd_5d_return), 2) AS avg_5d,
            ROUND(100.0 * SUM(CASE WHEN fwd_5d_return > 0 THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(fwd_5d_return), 0), 1) AS hitrate_5d
        FROM rank_cohort_performance
        WHERE 1=1
        {lb}
        GROUP BY run_date, bucket
        ORDER BY run_date DESC, {BUCKET_ORDER_SQL}
        """
    ).fetchdf()


def _concentration(con, *, lookback_days: int = 90) -> dict:
    """Concentration diagnostic: top-N band returns + interpretation signal."""
    lb = ""
    if lookback_days and lookback_days > 0:
        lb = (
            " AND run_date >= ("
            "  SELECT MAX(run_date) - INTERVAL '" + str(int(lookback_days)) + " days'"
            "  FROM rank_cohort_performance)"
        )
    rows: list[dict] = []
    by_cohort: dict[str, dict] = {}
    for label, lo, hi in COHORT_BANDS:
        r = con.execute(
            f"""
            SELECT
                COUNT(*),
                AVG(fwd_5d_return),
                AVG(fwd_10d_return),
                AVG(fwd_20d_return),
                100.0 * SUM(CASE WHEN fwd_20d_return > 0 THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(fwd_20d_return), 0)
            FROM rank_cohort_performance
            WHERE rank_position BETWEEN {lo} AND {hi}
            {lb}
            """
        ).fetchone()
        entry = {
            "cohort": label,
            "n": int(r[0] or 0),
            "avg_5d": _round(r[1]),
            "avg_10d": _round(r[2]),
            "avg_20d": _round(r[3]),
            "hitrate_20d": _round(r[4], 1),
        }
        rows.append(entry)
        by_cohort[label] = entry
    top200 = by_cohort.get("top-200", {}).get("avg_20d")
    plus201 = by_cohort.get("201+", {}).get("avg_20d")
    top10 = by_cohort.get("top-10", {}).get("avg_20d")
    for entry in rows:
        a = entry["avg_20d"]
        entry["delta_vs_top_200"] = _round(a - top200) if a is not None and top200 is not None else None
        entry["delta_vs_201_plus"] = _round(a - plus201) if a is not None and plus201 is not None else None
    if top10 is None or top200 is None:
        signal = "unknown"
        message = "Not enough data to assess top-10 vs top-200."
    else:
        delta = top10 - top200
        if delta < CONCENTRATION_WEAK_DELTA:
            signal = "weak"
            message = (
                "Top-10 does not materially outperform top-200. "
                "Treat top-200 as eligible universe rather than top-10 portfolio."
            )
        elif delta >= CONCENTRATION_STRONG_DELTA:
            signal = "strong"
            message = "Top-10 shows meaningful incremental return over top-200."
        else:
            signal = "mixed"
            message = "Top-10 edge over top-200 is small but non-trivial."
    return {
        "cohorts": rows,
        "signal": signal,
        "message": message,
        "top10_avg_20d": top10,
        "top200_avg_20d": top200,
    }


def _df_to_md(df: pd.DataFrame) -> str:
    """Tiny markdown table renderer (avoids tabulate dependency)."""
    if df.empty:
        return "_(no rows)_"
    headers = list(df.columns)
    sep = "| " + " | ".join(headers) + " |\n"
    sep += "|" + "|".join(["---"] * len(headers)) + "|\n"
    for _, row in df.iterrows():
        cells = ["" if pd.isna(v) else str(v) for v in row]
        sep += "| " + " | ".join(cells) + " |\n"
    return sep


def build_digest(
    *,
    project_root: str | Path | None = None,
    as_of: date | None = None,
) -> DigestResult:
    """Generate the weekly digest. Writes to disk + returns the markdown."""
    as_of = as_of or date.today()
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    digest_dir = paths.root_dir / "research" / "perf_digests"
    digest_dir.mkdir(parents=True, exist_ok=True)

    iso_year, iso_week, _ = as_of.isocalendar()
    output_path = digest_dir / f"digest_{iso_year}-W{iso_week:02d}.md"

    sections: dict[str, pd.DataFrame] = {}
    with open_research_db(project_root=project_root, read_only=True) as con:
        meta = con.execute(
            "SELECT MIN(run_date), MAX(run_date), COUNT(DISTINCT run_date), COUNT(*) "
            "FROM rank_cohort_performance"
        ).fetchone()
        first_date, last_date, n_dates, n_rows = meta
        sections["cohorts"] = _cohort_returns(con)
        sections["buckets"] = _bucket_attribution(con)
        sections["bucket_coverage"] = _bucket_coverage(con)
        sections["same_date_buckets"] = _same_date_bucket_attribution(con)
        sections["ic_30d"]  = _factor_ic(con, window_days=30)
        sections["ic_90d"]  = _factor_ic(con, window_days=90)
        sections["ic_180d"] = _factor_ic(con, window_days=180)
        sections["conditional_ic_90d"] = _conditional_factor_ic(con, window_days=90)
        sections["factor_coverage"] = _factor_coverage(con)
        sections["drift"]   = _drift_watch(
            sections["ic_30d"],
            sections["ic_180d"],
            coverage=sections["factor_coverage"],
        )
        sections["bucket_composition"] = _bucket_composition(con)
        sections["bucket_daily"] = _bucket_daily(con, lookback_days=90)
        concentration = _concentration(con, lookback_days=90)
        sections["concentration"] = pd.DataFrame(concentration["cohorts"])
        anomaly_meta = _anomaly_summary(con)
        concentration_meta = {
            "signal": concentration["signal"],
            "message": concentration["message"],
            "top10_avg_20d": concentration["top10_avg_20d"],
            "top200_avg_20d": concentration["top200_avg_20d"],
        }

    md = _render_markdown(
        as_of=as_of,
        first_date=first_date,
        last_date=last_date,
        n_dates=n_dates,
        n_rows=n_rows,
        sections=sections,
        concentration_meta=concentration_meta,
        anomaly_meta=anomaly_meta,
    )
    output_path.write_text(md, encoding="utf-8")
    logger.info("perf_tracker digest written to %s", output_path)
    return DigestResult(markdown=md, output_path=output_path, section_data=sections)


def _render_markdown(
    *,
    as_of: date,
    first_date,
    last_date,
    n_dates: int,
    n_rows: int,
    sections: dict[str, pd.DataFrame],
    concentration_meta: dict | None = None,
    anomaly_meta: dict | None = None,
) -> str:
    parts: list[str] = []
    parts.append(f"# Performance Tracker Digest — {as_of.isoformat()}\n")
    parts.append(
        "_Observational only. Does not change ranking weights, paper-trading rules, "
        "or production configs._\n"
    )
    parts.append(
        f"_Coverage: {first_date} → {last_date} · {n_dates} ranking dates · "
        f"{n_rows:,} (date, symbol) rows_\n"
    )

    # Interpretation header — sets the tone for the rest of the digest.
    if concentration_meta:
        sig = concentration_meta.get("signal", "unknown")
        edge_line = {
            "weak":    "**Top-200 edge does not exist** (signal=weak).",
            "mixed":   "**Top-200 edge exists, but is mixed**.",
            "strong":  "**Top-200 edge exists** (signal=strong).",
            "unknown": "Top-200 edge: insufficient data.",
        }.get(sig, "Top-200 edge: unknown.")
        conc_line = {
            "weak":    "Top-10 concentration **not justified**.",
            "mixed":   "Top-10 concentration **mixed**.",
            "strong":  "Top-10 concentration **justified**.",
            "unknown": "Top-10 concentration: unknown.",
        }.get(sig, "")
        parts.append(f"\n> {edge_line} {conc_line}\n> _{concentration_meta.get('message', '')}_\n")

    # Pending-row maturation warning. When the 20d horizon is unmatured for a
    # large fraction of a cohort, average returns / hit rates are dominated by
    # rows that have only had 20d of price action recorded — readings will
    # swing as more rows mature, so flag them up-front.
    cohorts_df = sections.get("cohorts")
    if cohorts_df is not None and "low_maturation" in getattr(cohorts_df, "columns", []):
        low = cohorts_df[cohorts_df["low_maturation"]]
        if len(low) > 0:
            names = ", ".join(
                f"{r.cohort} ({r.matured_20d_pct or 0:.0f}%)"
                for r in low.itertuples()
            )
            parts.append(
                f"> **Pending-row warning**: cohort(s) with <{int(MATURATION_WARNING_RATIO * 100)}% "
                f"of rows matured at 20d — readings unstable: {names}.\n"
            )

    # Corporate-action anomaly indicator: raw close means split/bonus days
    # produce spurious ±50%+ 5-day returns. We don't drop them silently —
    # surface a count so the user can decide.
    if anomaly_meta and anomaly_meta.get("n_anomaly", 0) > 0:
        pct = anomaly_meta.get("pct")
        pct_str = f"{pct:.2f}%" if pct is not None else "n/a"
        parts.append(
            f"> **Forward-return anomalies**: {anomaly_meta['n_anomaly']:,} rows "
            f"(of {anomaly_meta['n_matured']:,} matured at 5d, {pct_str}) have "
            f"|fwd_5d_return| > {anomaly_meta['threshold_pct']:.0f}% — likely "
            f"corporate actions in the raw-close OHLCV feed.\n"
        )

    # Same-date bias indicator.
    same_date = sections.get("same_date_buckets")
    if same_date is not None and not same_date.empty and "n" in same_date.columns:
        small = same_date[
            (same_date["n"] < SAME_DATE_SMALL_SAMPLE_ROWS)
        ] if "n" in same_date.columns else None
        if small is not None and len(small) > 0:
            parts.append(
                f"> Bucket attribution **appears biased by date coverage** "
                f"({len(small)} bucket(s) below {SAME_DATE_SMALL_SAMPLE_ROWS} rows).\n"
            )
        else:
            parts.append("> Bucket attribution: date coverage looks adequate.\n")

    # Factor coverage interpretation.
    fc = sections.get("factor_coverage")
    if fc is not None and not fc.empty and "status" in fc.columns:
        missing = fc[fc["status"].isin(["not_wired", "poor_coverage"])]
        if len(missing) > 0:
            names = ", ".join(missing["factor"].tolist())
            parts.append(f"> **Factors missing coverage**: {names}.\n")
        partial = fc[fc["status"] == "partial_coverage"]
        if len(partial) > 0:
            names = ", ".join(partial["factor"].tolist())
            parts.append(f"> Partial coverage (excluded from drift): {names}.\n")

    # Drift suppression note.
    drift = sections.get("drift")
    if drift is not None and not drift.empty and "status" in drift.columns:
        n_insufficient = int((drift["status"] == "insufficient_sample").sum())
        n_unreliable = int((drift["status"] == "unreliable_coverage").sum())
        if n_insufficient or n_unreliable:
            parts.append(
                f"> Drift alert suppressed for "
                f"{n_insufficient} factor(s) (insufficient sample), "
                f"{n_unreliable} factor(s) (unreliable coverage).\n"
            )

    if "concentration" in sections:
        parts.append("\n## 1. Rank concentration diagnostic")
        parts.append(
            "Average forward returns and deltas by rank-band. The interpretation "
            "header above summarises the top-10 vs top-200 edge.\n"
        )
        parts.append(_df_to_md(sections["concentration"]))

    parts.append("\n## 2. Cohort forward returns")
    parts.append("Top-N picks should outperform the rest. If `top-10 avg_20d` is "
                 "indistinguishable from `201+ avg_20d`, the ranking isn't "
                 "discriminating.\n")
    parts.append(_df_to_md(sections["cohorts"]))

    parts.append("\n## 2. Bucket attribution (Phase 5 watchlist taxonomy)")
    parts.append("`TRIGGERED_TODAY` 5d return should lead `CORE_MOMENTUM` 5d, "
                 "and `AVOID_WEAK_CONFIRMATION` 5d should be ≤ 0 if the bucket "
                 "label is honest. Empty until Phase 5-aware runs accumulate.\n")
    parts.append(_df_to_md(sections["buckets"]))

    parts.append("\n### Bucket coverage by date")
    parts.append("Use this to separate true bucket underperformance from sparse or partial date coverage.\n")
    parts.append(_df_to_md(sections["bucket_coverage"]))

    parts.append("\n### Same-date bucket attribution")
    parts.append("Bucket returns compared only against rows from dates where any watchlist bucket was assigned.\n")
    parts.append(_df_to_md(sections["same_date_buckets"]))

    if "bucket_composition" in sections:
        parts.append("\n### Bucket composition")
        parts.append(
            "Average factor state at assignment time, per bucket. Use to spot "
            "whether bad buckets are late-stage / weak-trend / overextended "
            "rather than wrongly labelled.\n"
        )
        parts.append(_df_to_md(sections["bucket_composition"]))

    parts.append("\n## 3. Factor information coefficient (Spearman vs fwd_20d)")
    parts.append("Higher IC = factor is doing real predictive work. Compare 30d "
                 "vs 90d vs 180d to spot drift.\n")
    combined = sections["ic_30d"].merge(
        sections["ic_90d"], on="factor", how="outer"
    ).merge(sections["ic_180d"], on="factor", how="outer")
    parts.append(_df_to_md(combined))

    parts.append("\n### Conditional factor IC (90d)")
    parts.append("IC split by full universe, top-200, and rank-201+ cohorts.\n")
    parts.append(_df_to_md(sections["conditional_ic_90d"]))

    parts.append("\n### Factor coverage")
    parts.append("Null-rate diagnostics for every tracked factor column.\n")
    parts.append(_df_to_md(sections["factor_coverage"]))

    parts.append("\n## 4. Drift watch")
    parts.append(
        f"Flags any factor whose 30-day IC dropped > {DRIFT_THRESHOLD_PCT:.0f}% "
        f"vs the 180-day baseline, with alerts suppressed below {DRIFT_WARNING_MIN_RECENT_N:,} "
        "recent samples.\n"
    )
    parts.append(_df_to_md(sections["drift"]))

    return "\n".join(parts) + "\n"


def main() -> None:  # pragma: no cover - CLI entry
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = build_digest()
    print(f"\nDigest written to: {result.output_path}\n")
    print(result.markdown)


if __name__ == "__main__":  # pragma: no cover
    main()
