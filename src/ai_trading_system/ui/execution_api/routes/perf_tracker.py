"""Performance tracker endpoints for the Research page.

Read-only views over ``rank_cohort_performance`` (Phase 0 of the strategy
feedback loop). All endpoints hit ``data/research.duckdb`` via the shared
``open_research_db(read_only=True)`` helper, so concurrent writers from the
pipeline stage don't conflict with API readers.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from fastapi import APIRouter, HTTPException, Query

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
    SAME_DATE_SMALL_SAMPLE_DAYS,
    SAME_DATE_SMALL_SAMPLE_ROWS,
)
from ai_trading_system.research.perf_tracker.schema import open_research_db
from ai_trading_system.ui.execution_api.routes._deps import project_root

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/execution/perf-tracker", tags=["perf-tracker"])


COHORT_BANDS: tuple[tuple[str, int, int], ...] = (
    ("top-10",  1,   10),
    ("top-50",  1,   50),
    ("top-200", 1,   200),
    ("51-200",  51,  200),
    ("201+",    201, 10_000_000),
)

DEFAULT_IC_WINDOWS: tuple[int, ...] = (30, 90, 180)
DEFAULT_IC_HORIZONS: tuple[int, ...] = (5, 10, 20)

# Partial-coverage threshold is route-local (not shared with the digest yet).
COVERAGE_PARTIAL_PCT = 50.0

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


def _round(value, ndigits: int = 2):
    if value is None:
        return None
    try:
        return round(float(value), ndigits)
    except (TypeError, ValueError):
        return None


def _lookback_clause(lookback_days: int | None) -> str:
    """Build a WHERE-fragment limiting rows to the last N days."""
    if not lookback_days or lookback_days <= 0:
        return ""
    return (
        " AND run_date >= ("
        "  SELECT MAX(run_date) - INTERVAL '" + str(int(lookback_days)) + " days'"
        "  FROM rank_cohort_performance"
        ")"
    )


def _rank_ic_xy(x, y) -> float | None:
    """Spearman = Pearson on the ranks; minimum 30 valid pairs."""
    if len(x) < 30:
        return None
    x_rank = x.rank()
    y_rank = y.rank()
    corr = float(x_rank.corr(y_rank))
    return corr if corr == corr else None  # noqa: PLR0124


def _rank_ic(valid) -> float | None:
    """Back-compat shim: first column vs fwd_20d_return."""
    if len(valid) < 30:
        return None
    return _rank_ic_xy(valid.iloc[:, 0], valid["fwd_20d_return"])


def _factor_name(column: str) -> str:
    return column.replace("factor_", "")


def _table_columns(con) -> set[str]:
    """Return the set of columns currently present on rank_cohort_performance."""
    rows = con.execute("PRAGMA table_info('rank_cohort_performance')").fetchall()
    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    return {str(r[1]) for r in rows}


def _coverage_status(coverage_pct: float | None) -> str:
    if coverage_pct is None:
        return "not_wired"
    if coverage_pct <= 0:
        return "not_wired"
    if coverage_pct < COVERAGE_PARTIAL_PCT:
        return "poor_coverage"
    if coverage_pct < COVERAGE_OK_PCT:
        return "partial_coverage"
    return "ok"


def _compute_factor_coverage(con) -> list[dict]:
    """Per-factor coverage rows (used by /factor-coverage and /drift)."""
    total = con.execute(
        "SELECT COUNT(*) FROM rank_cohort_performance"
    ).fetchone()[0] or 0
    total = int(total)
    rows: list[dict] = []
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
        null_count = max(total - non_null_count, 0)
        if total == 0:
            coverage_pct: float | None = None
            null_pct: float | None = 100.0
        else:
            coverage_pct = 100.0 * non_null_count / total
            null_pct = 100.0 * null_count / total
        rows.append({
            "factor": _factor_name(factor),
            "total_rows": total,
            "non_null_rows": non_null_count,
            "null_rows": null_count,
            "non_null_count": non_null_count,
            "coverage_pct": _round(coverage_pct, 1),
            "null_pct": _round(null_pct, 1),
            "first_available_date": first_date.isoformat() if first_date else None,
            "last_available_date": last_date.isoformat() if last_date else None,
            "status": _coverage_status(coverage_pct),
        })
    return rows


@router.get("/coverage")
def coverage():
    """Date-range + row-count summary for the header strip."""
    with open_research_db(project_root=project_root(), read_only=True) as con:
        row = con.execute(
            "SELECT MIN(run_date), MAX(run_date), "
            "COUNT(DISTINCT run_date), COUNT(*) "
            "FROM rank_cohort_performance"
        ).fetchone()
    first_date, last_date, n_dates, n_rows = row
    return {
        "first_date": first_date.isoformat() if first_date else None,
        "last_date":  last_date.isoformat() if last_date else None,
        "dates":      int(n_dates or 0),
        "rows":       int(n_rows or 0),
    }


@router.get("/cohorts")
def cohorts(
    lookback_days: int = Query(
        90, ge=0, le=10_000,
        description="Window for the aggregate (0 = use all rows).",
    ),
):
    """Per-cohort forward returns + hit rates."""
    lb = _lookback_clause(lookback_days)
    rows = []
    with open_research_db(project_root=project_root(), read_only=True) as con:
        for label, lo, hi in COHORT_BANDS:
            result = con.execute(
                f"""
                SELECT
                    COUNT(*),
                    COUNT(fwd_5d_return),
                    COUNT(fwd_20d_return),
                    AVG(fwd_5d_return),
                    AVG(fwd_10d_return),
                    AVG(fwd_20d_return),
                    AVG(fwd_60d_return),
                    100.0 * SUM(CASE WHEN fwd_5d_return  > 0 THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(fwd_5d_return),  0),
                    100.0 * SUM(CASE WHEN fwd_20d_return > 0 THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(fwd_20d_return), 0)
                FROM rank_cohort_performance
                WHERE rank_position BETWEEN {lo} AND {hi}
                {lb}
                """
            ).fetchone()
            rows.append({
                "cohort":      label,
                "n_total":     int(result[0] or 0),
                "n_5d":        int(result[1] or 0),
                "n_20d":       int(result[2] or 0),
                "avg_5d":      _round(result[3]),
                "avg_10d":     _round(result[4]),
                "avg_20d":     _round(result[5]),
                "avg_60d":     _round(result[6]),
                "hitrate_5d":  _round(result[7], 1),
                "hitrate_20d": _round(result[8], 1),
            })
    return {"lookback_days": lookback_days, "cohorts": rows}


@router.get("/buckets")
def buckets(
    lookback_days: int = Query(
        90, ge=0, le=10_000,
        description="Window for the aggregate (0 = use all rows).",
    ),
):
    """Watchlist bucket attribution (TRIGGERED_TODAY / CORE_MOMENTUM / …)."""
    lb = _lookback_clause(lookback_days)
    with open_research_db(project_root=project_root(), read_only=True) as con:
        result = con.execute(
            f"""
            SELECT
                COALESCE(watchlist_bucket, 'unassigned') AS bucket,
                COUNT(*) AS n,
                COUNT(fwd_5d_return)  AS n_5d,
                COUNT(fwd_20d_return) AS n_20d,
                AVG(fwd_5d_return),
                AVG(fwd_10d_return),
                AVG(fwd_20d_return),
                100.0 * SUM(CASE WHEN fwd_5d_return  > 0 THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(fwd_5d_return),  0),
                100.0 * SUM(CASE WHEN fwd_20d_return > 0 THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(fwd_20d_return), 0)
            FROM rank_cohort_performance
            WHERE 1=1
            {lb}
            GROUP BY bucket
            ORDER BY
                {BUCKET_ORDER_SQL}
            """
        ).fetchall()
    rows = [
        {
            "bucket":      r[0],
            "n":           int(r[1] or 0),
            "n_5d":        int(r[2] or 0),
            "n_20d":       int(r[3] or 0),
            "avg_5d":      _round(r[4]),
            "avg_10d":     _round(r[5]),
            "avg_20d":     _round(r[6]),
            "hitrate_5d":  _round(r[7], 1),
            "hitrate_20d": _round(r[8], 1),
        }
        for r in result
    ]
    return {"lookback_days": lookback_days, "buckets": rows}


@router.get("/bucket-coverage")
def bucket_coverage():
    """Date + symbol coverage by watchlist bucket, including unassigned history."""
    with open_research_db(project_root=project_root(), read_only=True) as con:
        result = con.execute(
            f"""
            SELECT
                COALESCE(watchlist_bucket, 'unassigned') AS bucket,
                MIN(run_date) AS first_date,
                MAX(run_date) AS last_date,
                COUNT(*) AS rows,
                COUNT(DISTINCT run_date) AS dates,
                COUNT(DISTINCT symbol_id) AS symbols_count,
                COUNT(*) * 1.0 / NULLIF(SUM(COUNT(*)) OVER (), 0) AS pct_of_all_rows,
                AVG(CASE WHEN fwd_5d_return  IS NOT NULL THEN 1.0 ELSE 0.0 END) AS pct_with_fwd_5d,
                AVG(CASE WHEN fwd_20d_return IS NOT NULL THEN 1.0 ELSE 0.0 END) AS pct_with_fwd_20d
            FROM rank_cohort_performance
            GROUP BY bucket
            ORDER BY {BUCKET_ORDER_SQL}
            """
        ).fetchall()
    return {
        "buckets": [
            {
                "bucket": r[0],
                "first_date": r[1].isoformat() if r[1] else None,
                "last_date": r[2].isoformat() if r[2] else None,
                "rows": int(r[3] or 0),
                "dates": int(r[4] or 0),
                "symbols_count": int(r[5] or 0),
                "pct_of_all_rows": _round(r[6], 4),
                "pct_with_fwd_5d": _round(r[7], 4),
                "pct_with_fwd_20d": _round(r[8], 4),
            }
            for r in result
        ],
    }


@router.get("/buckets/same-date")
def same_date_buckets(
    lookback_days: int = Query(
        90, ge=0, le=10_000,
        description="Window for the aggregate (0 = use all rows).",
    ),
):
    """Bucket attribution against controls from the same bucket-aware dates."""
    lb = _lookback_clause(lookback_days)
    with open_research_db(project_root=project_root(), read_only=True) as con:
        control = con.execute(
            f"""
            WITH bucket_dates AS (
                SELECT DISTINCT run_date
                FROM rank_cohort_performance
                WHERE watchlist_bucket IS NOT NULL
                {lb}
            )
            SELECT
                COUNT(*) AS n,
                COUNT(fwd_5d_return) AS n_5d,
                COUNT(fwd_20d_return) AS n_20d,
                AVG(fwd_5d_return) AS avg_5d,
                AVG(fwd_10d_return) AS avg_10d,
                AVG(fwd_20d_return) AS avg_20d,
                COUNT(DISTINCT run_date) AS trading_days
            FROM rank_cohort_performance
            WHERE run_date IN (SELECT run_date FROM bucket_dates)
            """
        ).fetchone()
        result = con.execute(
            f"""
            WITH bucket_dates AS (
                SELECT DISTINCT run_date
                FROM rank_cohort_performance
                WHERE watchlist_bucket IS NOT NULL
                {lb}
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
                      / NULLIF(COUNT(fwd_20d_return), 0) AS hitrate_20d,
                COUNT(DISTINCT run_date) AS trading_days
            FROM rank_cohort_performance
            WHERE run_date IN (SELECT run_date FROM bucket_dates)
            GROUP BY bucket
            ORDER BY {BUCKET_ORDER_SQL}
            """
        ).fetchall()
    control_avg_5d = _round(control[3])
    control_avg_10d = _round(control[4])
    control_avg_20d = _round(control[5])
    rows = []
    for r in result:
        avg_5d = _round(r[4])
        avg_10d = _round(r[5])
        avg_20d = _round(r[6])
        n = int(r[1] or 0)
        trading_days = int(r[9] or 0)
        rows.append({
            "bucket": r[0],
            "n": n,
            "n_5d": int(r[2] or 0),
            "n_20d": int(r[3] or 0),
            "avg_5d": avg_5d,
            "avg_10d": avg_10d,
            "avg_20d": avg_20d,
            "hitrate_5d": _round(r[7], 1),
            "hitrate_20d": _round(r[8], 1),
            "trading_days": trading_days,
            "small_sample": bool(
                trading_days < SAME_DATE_SMALL_SAMPLE_DAYS
                or n < SAME_DATE_SMALL_SAMPLE_ROWS
            ),
            "control_avg_5d": control_avg_5d,
            "control_avg_10d": control_avg_10d,
            "control_avg_20d": control_avg_20d,
            "excess_5d": _round(avg_5d - control_avg_5d) if avg_5d is not None and control_avg_5d is not None else None,
            "excess_10d": _round(avg_10d - control_avg_10d) if avg_10d is not None and control_avg_10d is not None else None,
            "excess_20d": _round(avg_20d - control_avg_20d) if avg_20d is not None and control_avg_20d is not None else None,
        })
    return {
        "lookback_days": lookback_days,
        "control": {
            "n": int(control[0] or 0),
            "n_5d": int(control[1] or 0),
            "n_20d": int(control[2] or 0),
            "avg_5d": control_avg_5d,
            "avg_10d": control_avg_10d,
            "avg_20d": control_avg_20d,
            "trading_days": int(control[6] or 0),
        },
        "buckets": rows,
    }


def _parse_windows(raw: str | None) -> tuple[int, ...]:
    if not raw:
        return DEFAULT_IC_WINDOWS
    try:
        parts = [int(p.strip()) for p in raw.split(",") if p.strip()]
    except ValueError:
        raise HTTPException(status_code=400, detail="windows must be comma-separated ints")
    parts = [p for p in parts if 1 <= p <= 10_000]
    if not parts:
        raise HTTPException(status_code=400, detail="windows must have ≥1 positive int")
    return tuple(parts)


def _factor_ic_for_window(con, window_days: int, available: Iterable[str]) -> dict[str, dict]:
    """Compute Spearman IC vs fwd_20d for each factor over the last N days."""
    available = list(available)
    if not available:
        return {}
    cols = ", ".join(available)
    df = con.execute(
        f"""
        SELECT {cols}, fwd_20d_return
        FROM rank_cohort_performance
        WHERE fwd_20d_return IS NOT NULL
          AND run_date >= (
              SELECT MAX(run_date) - INTERVAL '{int(window_days)} days'
              FROM rank_cohort_performance
          )
        """
    ).fetchdf()
    out: dict[str, dict] = {}
    if df.empty:
        return out
    for factor in available:
        valid = df[[factor, "fwd_20d_return"]].dropna()
        ic = _rank_ic(valid)
        out[factor] = {"n": int(len(valid)), "ic": _round(ic, 3)}
    return out


def _available_factors(con) -> list[str]:
    return [
        col for col in FACTOR_COLUMNS
        if con.execute(
            f"SELECT COUNT(*) FROM rank_cohort_performance WHERE {col} IS NOT NULL"
        ).fetchone()[0] > 0
    ]


def _conditional_ic_for_window(
    con,
    window_days: int,
    cohort_clause: str,
    horizons: Iterable[int],
) -> dict[str, dict[int, dict]]:
    """For one window+cohort, compute IC for each factor across all horizons.

    Returns ``{factor: {horizon: {"n": int, "ic": float|None}}}``.
    """
    horizons = list(horizons)
    fwd_cols = [f"fwd_{h}d_return" for h in horizons]
    cols = ", ".join((*FACTOR_COLUMNS, *fwd_cols))
    df = con.execute(
        f"""
        SELECT {cols}
        FROM rank_cohort_performance
        WHERE {cohort_clause}
          AND run_date >= (
              SELECT MAX(run_date) - INTERVAL '{int(window_days)} days'
              FROM rank_cohort_performance
          )
        """
    ).fetchdf()
    out: dict[str, dict[int, dict]] = {}
    for factor in FACTOR_COLUMNS:
        per_horizon: dict[int, dict] = {}
        for h in horizons:
            fcol = f"fwd_{h}d_return"
            if df.empty:
                per_horizon[h] = {"n": 0, "ic": None}
                continue
            valid = df[[factor, fcol]].dropna()
            if len(valid) < 30:
                per_horizon[h] = {"n": int(len(valid)), "ic": None}
                continue
            ic = _rank_ic_xy(valid[factor], valid[fcol])
            per_horizon[h] = {"n": int(len(valid)), "ic": _round(ic, 3)}
        out[factor] = per_horizon
    return out


@router.get("/factor-ic")
def factor_ic(
    windows: str | None = Query(
        None,
        description="Comma-separated lookbacks in days (default: 30,90,180).",
    ),
):
    """Spearman rank correlation of each factor score vs fwd_20d return."""
    wins = _parse_windows(windows)
    with open_research_db(project_root=project_root(), read_only=True) as con:
        available = _available_factors(con)
        rows = []
        per_window = {w: _factor_ic_for_window(con, w, available) for w in wins}
        for factor in available:
            entry: dict = {"factor": _factor_name(factor)}
            for w in wins:
                cell = per_window[w].get(factor, {"n": 0, "ic": None})
                entry[f"ic_{w}d"] = cell["ic"]
                entry[f"n_{w}d"] = cell["n"]
            rows.append(entry)
    return {"windows": list(wins), "factors": rows}


@router.get("/factor-ic/conditional")
def conditional_factor_ic(
    windows: str | None = Query(
        None,
        description="Comma-separated lookbacks in days (default: 30,90,180).",
    ),
):
    """Spearman IC for every factor within rank cohorts, across multiple horizons."""
    wins = _parse_windows(windows)
    horizons = DEFAULT_IC_HORIZONS
    cohorts = {
        "full_universe": "1=1",
        "top_200_only": "rank_position BETWEEN 1 AND 200",
        "rank_201_plus_only": "rank_position >= 201",
    }
    with open_research_db(project_root=project_root(), read_only=True) as con:
        per_window = {
            (w, cohort): _conditional_ic_for_window(con, w, clause, horizons)
            for w in wins
            for cohort, clause in cohorts.items()
        }
    rows = []
    for factor in FACTOR_COLUMNS:
        entry: dict = {"factor": _factor_name(factor)}
        for w in wins:
            for cohort in cohorts:
                cell = per_window[(w, cohort)][factor]
                # Legacy keys (back-compat): ic_{window}d_{cohort} kept against fwd_20d.
                entry[f"ic_{w}d_{cohort}"] = cell[20]["ic"] if 20 in cell else None
                entry[f"n_{w}d_{cohort}"] = cell[20]["n"] if 20 in cell else 0
                for h in horizons:
                    entry[f"ic_{h}d_{w}w_{cohort}"] = cell[h]["ic"]
                    entry[f"n_{h}d_{w}w_{cohort}"] = cell[h]["n"]
        rows.append(entry)
    return {
        "windows": list(wins),
        "horizons": list(horizons),
        "cohorts": list(cohorts),
        "factors": rows,
    }


@router.get("/factor-coverage")
def factor_coverage():
    """Null-rate diagnostics for every tracked factor column."""
    with open_research_db(project_root=project_root(), read_only=True) as con:
        rows = _compute_factor_coverage(con)
        total = rows[0]["total_rows"] if rows else 0
    return {"rows": int(total), "factors": rows}


@router.get("/drift")
def drift(
    recent_window: int = Query(30, ge=1, le=10_000),
    baseline_window: int = Query(180, ge=1, le=10_000),
    threshold_pct: float = Query(DRIFT_THRESHOLD_PCT, ge=0.0, le=1000.0),
):
    """Flag factors whose recent IC has dropped by more than ``threshold_pct`` vs baseline.

    Sample-size guardrails (per spec, Part 7):
      * recent_n < 1500            → ``insufficient_sample`` (no alert)
      * 1500 ≤ recent_n < 3000     → at most ``watch`` (no warning/critical)
      * recent_n ≥ 3000            → warning/critical allowed

    Factors whose coverage_pct is below 80 are tagged ``unreliable_coverage``
    regardless of the IC delta, so the UI can de-emphasise them.
    """
    with open_research_db(project_root=project_root(), read_only=True) as con:
        available = _available_factors(con)
        recent = _factor_ic_for_window(con, recent_window, available)
        baseline = _factor_ic_for_window(con, baseline_window, available)
        coverage_rows = _compute_factor_coverage(con)
    coverage_by_factor = {row["factor"]: row for row in coverage_rows}
    rows = []
    for factor in available:
        fname = _factor_name(factor)
        recent_cell = recent.get(factor, {})
        baseline_cell = baseline.get(factor, {})
        r = recent_cell.get("ic")
        b = baseline_cell.get("ic")
        recent_n = int(recent_cell.get("n", 0) or 0)
        baseline_n = int(baseline_cell.get("n", 0) or 0)
        cov = coverage_by_factor.get(fname, {})
        coverage_pct = cov.get("coverage_pct")
        coverage_status = cov.get("status")
        unreliable = coverage_pct is not None and coverage_pct < COVERAGE_OK_PCT
        if r is None or b is None or abs(b) < 1e-6:
            delta_pct = None
            delta_ic = None
            if unreliable:
                status = "unreliable_coverage"
            elif recent_n < DRIFT_WARNING_MIN_RECENT_N:
                status = "insufficient_sample"
            else:
                status = "no_baseline"
        else:
            delta_ic = r - b
            delta_pct = (r - b) / abs(b) * 100.0
            if unreliable:
                status = "unreliable_coverage"
            elif recent_n < DRIFT_WARNING_MIN_RECENT_N:
                status = "insufficient_sample"
            elif recent_n < DRIFT_CRITICAL_MIN_RECENT_N:
                # Watch tier: cap below warning/critical even if delta is huge.
                if delta_pct < -threshold_pct:
                    status = "watch"
                else:
                    status = "ok"
            else:
                if (
                    delta_ic <= -DRIFT_CRITICAL_MIN_DELTA_IC
                    and b >= DRIFT_CRITICAL_MIN_BASELINE_IC
                ):
                    status = "critical"
                elif delta_pct < -threshold_pct:
                    status = "warning"
                else:
                    status = "ok"
        alert = status in {"warning", "critical"}
        rows.append({
            "factor":         fname,
            "ic_recent":      r,
            "ic_baseline":    b,
            "recent_n":       recent_n,
            "baseline_n":     baseline_n,
            "delta_ic":       _round(delta_ic, 3),
            "delta_pct":      _round(delta_pct, 1),
            "status":         status,
            "alert":          bool(alert),
            "coverage_pct":   coverage_pct,
            "coverage_status": coverage_status,
        })
    flagged = [r for r in rows if r["alert"]]
    return {
        "recent_window":   recent_window,
        "baseline_window": baseline_window,
        "threshold_pct":   threshold_pct,
        "factors":         rows,
        "flagged":         flagged,
    }


# ---------------------------------------------------------------------------
# Diagnostics sprint additions
# ---------------------------------------------------------------------------


@router.get("/buckets/composition")
def buckets_composition():
    """Average factor state at assignment time, per bucket.

    Only emits AVG() for columns that exist on rank_cohort_performance — missing
    columns are reported as ``null`` so the UI can still render the slot.
    """
    with open_research_db(project_root=project_root(), read_only=True) as con:
        present = _table_columns(con)
        select_pieces = ["COALESCE(watchlist_bucket, 'unassigned') AS bucket",
                         "COUNT(*) AS n",
                         "AVG(rank_position) AS avg_rank_position"]
        for col in COMPOSITION_OPTIONAL_COLUMNS:
            if col in present:
                select_pieces.append(f"AVG({col}) AS avg_{col}")
        result = con.execute(
            f"""
            SELECT {', '.join(select_pieces)}
            FROM rank_cohort_performance
            GROUP BY bucket
            ORDER BY {BUCKET_ORDER_SQL}
            """
        ).fetchall()
    rows = []
    for r in result:
        entry: dict = {
            "bucket": r[0],
            "n": int(r[1] or 0),
            "avg_rank_position": _round(r[2], 2),
        }
        idx = 3
        for col in COMPOSITION_OPTIONAL_COLUMNS:
            key = f"avg_{col}"
            if col in present:
                entry[key] = _round(r[idx], 3)
                idx += 1
            else:
                entry[key] = None
        rows.append(entry)
    return {
        "available_columns": sorted(c for c in COMPOSITION_OPTIONAL_COLUMNS if c in present),
        "missing_columns": sorted(c for c in COMPOSITION_OPTIONAL_COLUMNS if c not in present),
        "composition": rows,
    }


@router.get("/buckets/daily")
def buckets_daily(
    lookback_days: int = Query(
        90, ge=0, le=10_000,
        description="Window for the daily aggregate (0 = use all rows).",
    ),
):
    """Per-(run_date, bucket) rows for daily attribution."""
    lb = _lookback_clause(lookback_days)
    with open_research_db(project_root=project_root(), read_only=True) as con:
        result = con.execute(
            f"""
            SELECT
                run_date,
                COALESCE(watchlist_bucket, 'unassigned') AS bucket,
                COUNT(*) AS n,
                AVG(fwd_5d_return) AS avg_5d,
                100.0 * SUM(CASE WHEN fwd_5d_return > 0 THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(fwd_5d_return), 0) AS hitrate_5d
            FROM rank_cohort_performance
            WHERE 1=1
            {lb}
            GROUP BY run_date, bucket
            ORDER BY run_date DESC, {BUCKET_ORDER_SQL}
            """
        ).fetchall()
    return {
        "lookback_days": lookback_days,
        "rows": [
            {
                "run_date": r[0].isoformat() if r[0] else None,
                "bucket": r[1],
                "n": int(r[2] or 0),
                "avg_5d": _round(r[3]),
                "hitrate_5d": _round(r[4], 1),
            }
            for r in result
        ],
    }


@router.get("/concentration")
def concentration(
    lookback_days: int = Query(
        90, ge=0, le=10_000,
        description="Window for the aggregate (0 = use all rows).",
    ),
):
    """Rank-band concentration: top-10/50/200 vs 51-200/201+ deltas.

    Adds an interpretation ``signal`` (weak/strong/mixed) summarising whether
    top-10 picks meaningfully outperform the broader top-200.
    """
    lb = _lookback_clause(lookback_days)
    rows: list[dict] = []
    by_cohort: dict[str, dict] = {}
    with open_research_db(project_root=project_root(), read_only=True) as con:
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

    top200 = by_cohort.get("top-200", {})
    plus201 = by_cohort.get("201+", {})
    for entry in rows:
        a20 = entry.get("avg_20d")
        t200 = top200.get("avg_20d")
        p201 = plus201.get("avg_20d")
        entry["delta_vs_top_200"] = _round(a20 - t200) if a20 is not None and t200 is not None else None
        entry["delta_vs_201_plus"] = _round(a20 - p201) if a20 is not None and p201 is not None else None

    # Concentration signal
    top10_20d = by_cohort.get("top-10", {}).get("avg_20d")
    top200_20d = top200.get("avg_20d")
    if top10_20d is None or top200_20d is None:
        signal = "unknown"
        message = "Not enough data to assess top-10 vs top-200."
    else:
        delta = top10_20d - top200_20d
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
        "lookback_days": lookback_days,
        "cohorts": rows,
        "signal": signal,
        "message": message,
        "top10_avg_20d": top10_20d,
        "top200_avg_20d": top200_20d,
        "top200_minus_201_plus_avg_20d": (
            _round(top200_20d - plus201.get("avg_20d"))
            if top200_20d is not None and plus201.get("avg_20d") is not None
            else None
        ),
    }


# ---------------------------------------------------------------------------
# Digest viewer
# ---------------------------------------------------------------------------


def _digests_dir() -> Path:
    paths = get_domain_paths(project_root=project_root(), data_domain="operational")
    return paths.root_dir / "research" / "perf_digests"


@router.get("/digests")
def list_digests():
    """List markdown digests under data/research/perf_digests/ sorted by mtime desc."""
    digest_dir = _digests_dir()
    if not digest_dir.exists():
        return {"digests": []}
    entries = []
    for path in digest_dir.glob("*.md"):
        try:
            stat = path.stat()
        except OSError:
            continue
        entries.append({
            "filename": path.name,
            "mtime": stat.st_mtime,
            "size_bytes": int(stat.st_size),
        })
    entries.sort(key=lambda e: e["mtime"], reverse=True)
    return {"digests": entries}


@router.get("/digests/{filename}")
def get_digest(filename: str):
    """Return the markdown body of a single digest file (path-traversal guarded)."""
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="invalid filename")
    if not filename.endswith(".md"):
        raise HTTPException(status_code=400, detail="only .md digests are served")
    path = _digests_dir() / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail="digest not found")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"could not read digest: {exc}")
    return {"filename": filename, "markdown": text}
