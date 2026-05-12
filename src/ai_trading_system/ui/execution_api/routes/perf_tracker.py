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
                CASE bucket
                    WHEN 'TRIGGERED_TODAY' THEN 1
                    WHEN 'CORE_MOMENTUM'   THEN 2
                    WHEN 'EARLY_STAGE2'    THEN 3
                    WHEN 'AVOID_WEAK_CONFIRMATION' THEN 4
                    ELSE 5
                END
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
        if len(valid) < 30:
            ic = None
        else:
            x_rank = valid[factor].rank()
            y_rank = valid["fwd_20d_return"].rank()
            corr = float(x_rank.corr(y_rank))
            # NaN-safe: corr can be NaN when one column is constant.
            ic = corr if corr == corr else None  # noqa: PLR0124
        out[factor] = {"n": int(len(valid)), "ic": _round(ic, 3)}
    return out


def _available_factors(con) -> list[str]:
    return [
        col for col in FACTOR_COLUMNS
        if con.execute(
            f"SELECT COUNT(*) FROM rank_cohort_performance WHERE {col} IS NOT NULL"
        ).fetchone()[0] > 0
    ]


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
        r = recent.get(factor, {}).get("ic")
        b = baseline.get(factor, {}).get("ic")
        if r is None or b is None or abs(b) < 1e-6:
            delta_pct = None
            alert = False
        else:
            delta_pct = (r - b) / abs(b) * 100.0
            alert = delta_pct < -threshold_pct
        rows.append({
            "factor":      factor.replace("factor_", ""),
            "ic_recent":   r,
            "ic_baseline": b,
            "delta_pct":   _round(delta_pct, 1),
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
