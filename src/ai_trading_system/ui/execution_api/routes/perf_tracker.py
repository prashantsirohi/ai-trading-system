"""Performance tracker endpoints for the Research page.

Read-only views over ``rank_cohort_performance`` (Phase 0 of the strategy
feedback loop). The five endpoints mirror the four sections of the weekly
digest (plus a coverage strip) so the UI can render the same data the
markdown digest exposes:

  * ``GET /api/execution/perf-tracker/coverage``      — header strip
  * ``GET /api/execution/perf-tracker/cohorts``       — top-N forward returns
  * ``GET /api/execution/perf-tracker/buckets``       — watchlist bucket attribution
  * ``GET /api/execution/perf-tracker/factor-ic``     — Spearman IC per factor × window
  * ``GET /api/execution/perf-tracker/drift``         — factors whose IC has decayed

All queries hit ``data/research.duckdb`` via the shared
``open_research_db(read_only=True)`` helper, so concurrent writers from the
pipeline stage don't conflict with API readers.
"""

from __future__ import annotations

import logging
from typing import Iterable

from fastapi import APIRouter, HTTPException, Query

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

FACTOR_COLUMNS: tuple[str, ...] = (
    "factor_rs",
    "factor_vol",
    "factor_trend",
    "factor_prox",
    "factor_deliv",
    "factor_sector",
    "factor_momentum_accel",
)

DRIFT_THRESHOLD_PCT = 30.0
DEFAULT_IC_WINDOWS: tuple[int, ...] = (30, 90, 180)
DRIFT_WARNING_MIN_RECENT_N = 1500
DRIFT_CRITICAL_MIN_RECENT_N = 3000
DRIFT_CRITICAL_MIN_DELTA_IC = 0.03
DRIFT_CRITICAL_MIN_BASELINE_IC = 0.05

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


def _rank_ic(valid) -> float | None:
    if len(valid) < 30:
        return None
    x_rank = valid.iloc[:, 0].rank()
    y_rank = valid["fwd_20d_return"].rank()
    corr = float(x_rank.corr(y_rank))
    return corr if corr == corr else None  # noqa: PLR0124


def _factor_name(column: str) -> str:
    return column.replace("factor_", "")


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
    """Date coverage by watchlist bucket, including unassigned historical rows."""
    with open_research_db(project_root=project_root(), read_only=True) as con:
        result = con.execute(
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
        ).fetchall()
    return {
        "buckets": [
            {
                "bucket": r[0],
                "first_date": r[1].isoformat() if r[1] else None,
                "last_date": r[2].isoformat() if r[2] else None,
                "rows": int(r[3] or 0),
                "dates": int(r[4] or 0),
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
                AVG(fwd_20d_return) AS avg_20d
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
                      / NULLIF(COUNT(fwd_20d_return), 0) AS hitrate_20d
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
        rows.append({
            "bucket": r[0],
            "n": int(r[1] or 0),
            "n_5d": int(r[2] or 0),
            "n_20d": int(r[3] or 0),
            "avg_5d": avg_5d,
            "avg_10d": avg_10d,
            "avg_20d": avg_20d,
            "hitrate_5d": _round(r[7], 1),
            "hitrate_20d": _round(r[8], 1),
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


def _all_factor_ic_for_window(con, window_days: int, cohort_clause: str = "1=1") -> dict[str, dict]:
    cols = ", ".join(FACTOR_COLUMNS)
    df = con.execute(
        f"""
        SELECT {cols}, fwd_20d_return
        FROM rank_cohort_performance
        WHERE fwd_20d_return IS NOT NULL
          AND {cohort_clause}
          AND run_date >= (
              SELECT MAX(run_date) - INTERVAL '{int(window_days)} days'
              FROM rank_cohort_performance
          )
        """
    ).fetchdf()
    out: dict[str, dict] = {}
    for factor in FACTOR_COLUMNS:
        if df.empty:
            out[factor] = {"n": 0, "ic": None}
            continue
        valid = df[[factor, "fwd_20d_return"]].dropna()
        out[factor] = {"n": int(len(valid)), "ic": _round(_rank_ic(valid), 3)}
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
            entry: dict = {"factor": factor.replace("factor_", "")}
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
    """Spearman IC for every factor within rank cohorts."""
    wins = _parse_windows(windows)
    cohorts = {
        "full_universe": "1=1",
        "top_200_only": "rank_position BETWEEN 1 AND 200",
        "rank_201_plus_only": "rank_position >= 201",
    }
    with open_research_db(project_root=project_root(), read_only=True) as con:
        per_window = {
            (w, cohort): _all_factor_ic_for_window(con, w, clause)
            for w in wins
            for cohort, clause in cohorts.items()
        }
    rows = []
    for factor in FACTOR_COLUMNS:
        entry: dict = {"factor": _factor_name(factor)}
        for w in wins:
            for cohort in cohorts:
                cell = per_window[(w, cohort)][factor]
                entry[f"ic_{w}d_{cohort}"] = cell["ic"]
                entry[f"n_{w}d_{cohort}"] = cell["n"]
        rows.append(entry)
    return {"windows": list(wins), "cohorts": list(cohorts), "factors": rows}


@router.get("/factor-coverage")
def factor_coverage():
    """Null-rate diagnostics for every tracked factor column."""
    with open_research_db(project_root=project_root(), read_only=True) as con:
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
            null_pct = 100.0 if total == 0 else 100.0 * (int(total) - non_null_count) / int(total)
            rows.append({
                "factor": _factor_name(factor),
                "non_null_count": non_null_count,
                "null_pct": _round(null_pct, 1),
                "first_available_date": first_date.isoformat() if first_date else None,
                "last_available_date": last_date.isoformat() if last_date else None,
            })
    return {"rows": int(total), "factors": rows}


@router.get("/drift")
def drift(
    recent_window: int = Query(30, ge=1, le=10_000),
    baseline_window: int = Query(180, ge=1, le=10_000),
    threshold_pct: float = Query(DRIFT_THRESHOLD_PCT, ge=0.0, le=1000.0),
):
    """Flag factors whose recent IC has dropped by more than ``threshold_pct`` vs baseline."""
    with open_research_db(project_root=project_root(), read_only=True) as con:
        available = _available_factors(con)
        recent = _factor_ic_for_window(con, recent_window, available)
        baseline = _factor_ic_for_window(con, baseline_window, available)
    rows = []
    for factor in available:
        recent_cell = recent.get(factor, {})
        baseline_cell = baseline.get(factor, {})
        r = recent_cell.get("ic")
        b = baseline_cell.get("ic")
        recent_n = int(recent_cell.get("n", 0) or 0)
        baseline_n = int(baseline_cell.get("n", 0) or 0)
        if r is None or b is None or abs(b) < 1e-6:
            delta_pct = None
            delta_ic = None
            status = "insufficient_sample" if recent_n < DRIFT_WARNING_MIN_RECENT_N else "no_baseline"
        else:
            delta_ic = r - b
            delta_pct = (r - b) / abs(b) * 100.0
            if recent_n < DRIFT_WARNING_MIN_RECENT_N:
                status = "insufficient_sample"
            elif (
                recent_n >= DRIFT_CRITICAL_MIN_RECENT_N
                and delta_ic <= -DRIFT_CRITICAL_MIN_DELTA_IC
                and b >= DRIFT_CRITICAL_MIN_BASELINE_IC
            ):
                status = "critical"
            elif delta_pct < -threshold_pct:
                status = "warning"
            else:
                status = "ok"
        alert = status in {"warning", "critical"}
        rows.append({
            "factor":      _factor_name(factor),
            "ic_recent":   r,
            "ic_baseline": b,
            "recent_n":    recent_n,
            "baseline_n":  baseline_n,
            "delta_ic":    _round(delta_ic, 3),
            "delta_pct":   _round(delta_pct, 1),
            "status":      status,
            "alert":       bool(alert),
        })
    flagged = [r for r in rows if r["alert"]]
    return {
        "recent_window":   recent_window,
        "baseline_window": baseline_window,
        "threshold_pct":   threshold_pct,
        "factors":         rows,
        "flagged":         flagged,
    }
