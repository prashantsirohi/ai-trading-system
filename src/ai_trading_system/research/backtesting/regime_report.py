"""Regime-stratified backtest validation (Phase 7).

Answers the GO/NO-GO question: does the regime signal predict forward
returns? The roadmap acceptance criterion is that the four-tier ordering
``strong_bull > bull > cautious_bull > neutral > risk_off`` holds on
mean forward returns across 5/10/20/60-day horizons over a multi-decade
window. If it doesn't, the regime framework is broken and Phases 5/6's
higher-risk defaults should not be enabled for live trading.

This module is benchmark-level by design: forward returns are computed
on the UNIV_TOP1000 index (the same series the regime is measured against),
so the report tests the *signal*, not any particular strategy. A full
strategy backtest stratified by regime is a follow-up — without the
signal-level ordering established first, that work is premature.

Output: JSON summary keyed by regime with mean/median/win_rate/sample_size
per horizon, plus a CSV with one row per (regime, horizon) for analysis.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date as date_type
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

from ai_trading_system.analytics.regime.breadth import (
    _load_recent_raw_snapshots,
    classify_regime,
    confirmed_regime,
    load_regime_rules,
)

LOG = logging.getLogger(__name__)

DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20, 60)
DEFAULT_BENCHMARK: str = "UNIV_TOP1000"
# Snapshots-per-day limit. 7000 covers ~27 years of trading days with slack.
_DEFAULT_DAY_LIMIT: int = 7000


@dataclass(frozen=True)
class RegimeReport:
    """Result of build_regime_forward_return_report.

    ``per_regime_horizon`` is the row-oriented CSV-style frame: one row per
    (regime, horizon) with aggregate metrics. ``summary`` is a structured
    dict for JSON output. ``daily`` is the underlying per-day frame so
    callers can drill in.
    """

    daily: pd.DataFrame  # one row per trading day with regime + fwd returns
    per_regime_horizon: pd.DataFrame
    summary: dict[str, Any]


def _resolve_db_path(project_root: Path | str | None, db_path: str | Path | None) -> Path:
    """Prefer caller-supplied path; otherwise default to research_ohlcv.duckdb.

    The research DB covers ~26 years of UNIV_TOP1000 vs the operational
    DB which only has the most recent slice.
    """
    if db_path is not None:
        return Path(db_path)
    root = Path(project_root or ".")
    from ai_trading_system.platform.db.paths import get_domain_paths

    return get_domain_paths(project_root=root, data_domain="research").ohlcv_db_path


def _load_benchmark_closes(
    db_path: Path,
    *,
    index_code: str,
    from_date: str,
    to_date: str,
) -> pd.DataFrame:
    """Return [(date, close)] for the benchmark in the requested range."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            """
            SELECT date AS d, close
            FROM _index_catalog
            WHERE index_code = ?
              AND date BETWEEN ?::DATE AND ?::DATE
              AND close IS NOT NULL
              AND close > 0
            ORDER BY date ASC
            """,
            [index_code, from_date, to_date],
        ).fetchdf()
    finally:
        con.close()
    if df.empty:
        return df
    df = df.assign(d=pd.to_datetime(df["d"]).dt.date)
    return df


def _attach_forward_returns(
    daily: pd.DataFrame,
    benchmark_closes: pd.DataFrame,
    horizons: Iterable[int],
) -> pd.DataFrame:
    """Self-join the benchmark series to compute fwd_N_return per row.

    ``daily`` must have a ``date`` column (date objects). ``benchmark_closes``
    must have ``d`` (date) and ``close``. Returns a copy of ``daily`` with
    new columns ``fwd_<N>_return`` (in percent) per horizon.
    """
    if daily.empty or benchmark_closes.empty:
        return daily.copy()
    bench = benchmark_closes.copy()
    bench = bench.assign(idx=range(len(bench)))
    bench_by_date = bench.set_index("d")["idx"].to_dict()
    bench_closes = bench["close"].to_list()

    out = daily.copy()
    out = out.assign(__idx=out["date"].map(bench_by_date))
    n_rows = len(bench)

    for n in horizons:
        col_return = f"fwd_{n}_return"
        col_matured = f"fwd_{n}_matured_at"
        target_idx = out["__idx"].astype("Int64") + n

        def lookup_close(i: int | None) -> float | None:
            if i is None or pd.isna(i):
                return None
            i_int = int(i)
            if 0 <= i_int < n_rows:
                return bench_closes[i_int]
            return None

        def lookup_date(i: int | None) -> Any:
            if i is None or pd.isna(i):
                return pd.NaT
            i_int = int(i)
            if 0 <= i_int < n_rows:
                return bench["d"].iloc[i_int]
            return pd.NaT

        out.loc[:, col_matured] = target_idx.apply(lookup_date)
        future_close = target_idx.apply(lookup_close).astype("Float64")
        present_close = out["__idx"].apply(lookup_close).astype("Float64")
        out.loc[:, col_return] = (
            (future_close - present_close) / present_close * 100.0
        ).astype("Float64")
    return out.drop(columns=["__idx"])


def _aggregate_by_regime(
    daily: pd.DataFrame,
    horizons: Iterable[int],
) -> pd.DataFrame:
    """Return long-form (regime, horizon, metric...) frame for CSV output."""
    rows: list[dict[str, Any]] = []
    total_days = len(daily)
    for regime, grp in daily.groupby("regime", dropna=False):
        days_in_regime = len(grp)
        pct_of_period = (days_in_regime / total_days * 100.0) if total_days else 0.0
        for n in horizons:
            col = f"fwd_{n}_return"
            if col not in grp.columns:
                continue
            ret = pd.to_numeric(grp[col], errors="coerce").dropna()
            if ret.empty:
                rows.append(
                    {
                        "regime": regime,
                        "horizon_days": n,
                        "days_in_regime": days_in_regime,
                        "pct_of_period": round(pct_of_period, 2),
                        "sample_size": 0,
                        "mean_return_pct": None,
                        "median_return_pct": None,
                        "win_rate_pct": None,
                        "max_drawup_pct": None,
                        "max_drawdown_pct": None,
                    }
                )
                continue
            rows.append(
                {
                    "regime": regime,
                    "horizon_days": n,
                    "days_in_regime": days_in_regime,
                    "pct_of_period": round(pct_of_period, 2),
                    "sample_size": int(len(ret)),
                    "mean_return_pct": round(float(ret.mean()), 4),
                    "median_return_pct": round(float(ret.median()), 4),
                    "win_rate_pct": round(float((ret > 0).mean() * 100.0), 2),
                    "max_drawup_pct": round(float(ret.max()), 4),
                    "max_drawdown_pct": round(float(ret.min()), 4),
                }
            )
    return pd.DataFrame(rows)


def _summary_dict(
    daily: pd.DataFrame,
    per_regime_horizon: pd.DataFrame,
    *,
    horizons: Iterable[int],
    from_date: str,
    to_date: str,
    benchmark: str,
) -> dict[str, Any]:
    regimes = ("risk_off", "neutral", "cautious_bull", "bull", "strong_bull")
    regime_breakdown: dict[str, dict[str, Any]] = {}
    for regime in regimes:
        slice_ = per_regime_horizon[per_regime_horizon["regime"] == regime]
        if slice_.empty:
            continue
        first = slice_.iloc[0]
        per_horizon = {}
        for _, row in slice_.iterrows():
            per_horizon[f"{int(row['horizon_days'])}d"] = {
                "sample_size": int(row["sample_size"]),
                "mean_return_pct": row["mean_return_pct"],
                "median_return_pct": row["median_return_pct"],
                "win_rate_pct": row["win_rate_pct"],
            }
        regime_breakdown[regime] = {
            "days_in_regime": int(first["days_in_regime"]),
            "pct_of_period": float(first["pct_of_period"]),
            "per_horizon": per_horizon,
        }

    # Ordering verdict: per horizon, do mean returns increase along the
    # risk ladder? "Pass" = strictly monotone non-decreasing; "fail"
    # otherwise. A passing report is the Phase-7 acceptance signal.
    ordering: dict[str, dict[str, Any]] = {}
    rank = {"risk_off": 0, "neutral": 1, "cautious_bull": 2, "bull": 3, "strong_bull": 4}
    for n in horizons:
        means: list[tuple[int, str, float]] = []
        for regime in regimes:
            row = per_regime_horizon[
                (per_regime_horizon["regime"] == regime)
                & (per_regime_horizon["horizon_days"] == n)
            ]
            if row.empty or pd.isna(row.iloc[0]["mean_return_pct"]):
                continue
            means.append((rank[regime], regime, float(row.iloc[0]["mean_return_pct"])))
        means.sort(key=lambda r: r[0])
        monotone = all(means[i][2] <= means[i + 1][2] for i in range(len(means) - 1))
        ordering[f"{n}d"] = {
            "monotone_non_decreasing": monotone,
            "by_rank": [{"regime": r, "mean_return_pct": v} for _, r, v in means],
        }

    return {
        "from_date": from_date,
        "to_date": to_date,
        "benchmark": benchmark,
        "total_days": int(len(daily)),
        "horizons_days": list(horizons),
        "regime_breakdown": regime_breakdown,
        "forward_return_ordering": ordering,
    }


def build_regime_forward_return_report(
    *,
    project_root: Path | str | None = None,
    from_date: str,
    to_date: str,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    benchmark: str = DEFAULT_BENCHMARK,
    db_path: str | Path | None = None,
    confirmation_days: int = 3,
) -> RegimeReport:
    """Compute regime classification + forward returns for every trading day.

    Pipeline:
      1. Load the active regime rules (with hysteresis enter/exit) from
         the project root via load_regime_rules.
      2. Pull every trading day's breadth metrics in the requested range
         via _load_recent_raw_snapshots (research DB by default).
      3. Apply 3-of-N confirmation across raw classifications to produce
         the daily confirmed regime — matches live runtime.
      4. Compute forward returns of ``benchmark`` at each horizon.
      5. Aggregate (regime, horizon) → mean/median/win_rate.
      6. Compute the four-tier ordering verdict per horizon.

    Returns a ``RegimeReport`` with daily + aggregate frames + summary
    dict. Callers (CLI / tests) write JSON + CSV.
    """
    resolved_db = _resolve_db_path(project_root, db_path)
    if not resolved_db.exists():
        raise FileNotFoundError(
            f"Backtest source DB not found at {resolved_db}. "
            f"Pass --db or ensure data/research/research_ohlcv.duckdb exists."
        )

    rules_payload = load_regime_rules(project_root or Path("."))
    raw_rules: dict[str, Any] = dict(rules_payload.get("rules") or {})
    priority = rules_payload.get("priority")
    if isinstance(priority, (list, tuple)):
        raw_rules["__priority__"] = list(priority)

    LOG.info("Loading daily breadth snapshots %s → %s …", from_date, to_date)
    snapshots = _load_recent_raw_snapshots(
        resolved_db,
        as_of=str(to_date),
        exchange="NSE",
        index_code=benchmark,
        limit=_DEFAULT_DAY_LIMIT,
        rules=raw_rules,
        previous_regime=None,  # cold start at the oldest day
    )
    if not snapshots:
        raise RuntimeError(
            f"No breadth snapshots returned for {benchmark} between {from_date} and {to_date}."
        )

    from_d = pd.to_datetime(from_date).date()
    to_d = pd.to_datetime(to_date).date()
    daily_rows: list[dict[str, Any]] = []
    raw_seq: list[str] = []
    for snap in snapshots:
        d = pd.to_datetime(snap.date).date()
        if d < from_d or d > to_d:
            # Discard pre-window history. We still need it for the rolling
            # confirmation buffer, but _load_recent_raw_snapshots already
            # returns chronological order so we can skip safely.
            raw_seq.append(snap.regime)
            continue
        raw_seq.append(snap.regime)
        # Re-derive confirmed regime from the trailing N raw days.
        window = raw_seq[-max(confirmation_days, 1):]
        confirmed = confirmed_regime(window, confirmation_days=confirmation_days)
        daily_rows.append(
            {
                "date": d,
                "raw_regime": snap.regime,
                "regime": confirmed,
                "pct_above_200dma": snap.pct_above_200dma,
                "pct_above_50dma": snap.pct_above_50dma,
                "pct_at_52w_high": snap.pct_at_52w_high,
                "regime_score": snap.regime_score,
            }
        )
    daily = pd.DataFrame(daily_rows)
    if daily.empty:
        raise RuntimeError(
            f"No in-window trading days found between {from_date} and {to_date}."
        )

    LOG.info("Attaching forward returns on %s for %d days …", benchmark, len(daily))
    bench_closes = _load_benchmark_closes(
        resolved_db, index_code=benchmark, from_date=from_date, to_date=to_date
    )
    # Extend bench closes a bit past to_date so the longest horizon can mature
    # (the SQL is bounded by to_date — for our forward-window we want futures
    # within the same data, so we relax the upper bound here).
    extend_until = pd.Timestamp(to_d).date()
    bench_full = _load_benchmark_closes(
        resolved_db,
        index_code=benchmark,
        from_date="1995-01-01",
        to_date="2099-12-31",
    )
    if not bench_full.empty:
        bench_full = bench_full[bench_full["d"] >= from_d]
    daily = _attach_forward_returns(daily, bench_full, horizons=horizons)

    per_regime_horizon = _aggregate_by_regime(daily, horizons=horizons)
    summary = _summary_dict(
        daily,
        per_regime_horizon,
        horizons=horizons,
        from_date=str(from_date),
        to_date=str(to_date),
        benchmark=benchmark,
    )
    return RegimeReport(daily=daily, per_regime_horizon=per_regime_horizon, summary=summary)


def write_report(
    report: RegimeReport,
    *,
    out_dir: Path | str,
    stem: str = "regime_validation",
) -> dict[str, Path]:
    """Write JSON summary + CSV per-regime-horizon to ``out_dir``.

    Returns a dict ``{"summary": ..., "csv": ..., "daily": ...}`` of the
    written paths. ``daily`` is the per-day frame (useful for ad-hoc
    drill-down in Jupyter).
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    summary_path = out / f"{stem}.json"
    csv_path = out / f"{stem}_by_regime.csv"
    daily_path = out / f"{stem}_daily.csv"
    summary_path.write_text(
        json.dumps(report.summary, indent=2, default=str), encoding="utf-8"
    )
    report.per_regime_horizon.to_csv(csv_path, index=False)
    report.daily.to_csv(daily_path, index=False)
    return {"summary": summary_path, "csv": csv_path, "daily": daily_path}
