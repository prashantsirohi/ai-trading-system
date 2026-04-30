"""Stage-gate backtest: gated vs baseline ranking comparison.

For each rebalance Friday in the study window:
  - Classifies every symbol's weekly Weinstein stage using only data
    available up to that Friday (no look-ahead).
  - Runs the existing RankBacktester.rank_stocks() for factor scores.
  - Builds two top-N portfolios: baseline (all ranked) vs gated (S2 only).
  - Looks up equal-weight forward returns 4w and 12w later.

Reports per-period hit-rate, median return, win-rate, and a side-by-side
metrics table across both modes.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.domains.ranking.weekly import to_weekly
from ai_trading_system.domains.ranking.stage_classifier import (
    MIN_WEEKS,
    classify_latest,
)

LOG = logging.getLogger(__name__)

FORWARD_WINDOWS = {"4w": 20, "12w": 60}   # trading days
STAGE_MIN_CONF = 0.6


# ── Data loading ──────────────────────────────────────────────────────────────

def load_all_daily(ohlcv_db_path: str, exchange: str = "NSE") -> pd.DataFrame:
    """Pull full daily OHLCV into memory once."""
    conn = duckdb.connect(ohlcv_db_path, read_only=True)
    try:
        df = conn.execute(f"""
            SELECT symbol_id AS symbol,
                   CAST(timestamp AS DATE) AS date,
                   open, high, low, close, volume
            FROM _catalog
            WHERE exchange = '{exchange}'
            ORDER BY symbol, date
        """).fetchdf()
    finally:
        conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_close_pivot(daily: pd.DataFrame) -> pd.DataFrame:
    """Wide pivot: index=date, columns=symbol, values=close."""
    return daily.pivot_table(index="date", columns="symbol", values="close")


def load_weekly_stage_snapshots(
    ohlcv_db_path: str,
    *,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Load persisted weekly stage snapshots for validation reports."""
    conn = duckdb.connect(ohlcv_db_path, read_only=True)
    try:
        exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = 'weekly_stage_snapshot'
            """
        ).fetchone()[0]
        if not exists:
            return pd.DataFrame()
        clauses = []
        params: list[object] = []
        if start:
            clauses.append("week_end_date >= CAST(? AS DATE)")
            params.append(start)
        if end:
            clauses.append("week_end_date <= CAST(? AS DATE)")
            params.append(end)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        snapshots = conn.execute(
            f"""
            SELECT symbol, week_end_date, stage_label, stage_transition,
                   bars_in_stage, stage_confidence
            FROM weekly_stage_snapshot
            {where}
            ORDER BY week_end_date, symbol
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()
    if not snapshots.empty:
        snapshots["week_end_date"] = pd.to_datetime(snapshots["week_end_date"])
    return snapshots


# ── In-memory weekly stage snapshot ──────────────────────────────────────────

def build_stage_snapshot(
    daily: pd.DataFrame,
    asof: pd.Timestamp,
) -> pd.DataFrame:
    """Classify all symbols as of `asof` using data up to that date.

    Returns a frame with columns: symbol, stage_label, stage_confidence.
    """
    subset = daily[daily["date"] <= asof]
    rows = []
    for symbol, grp in subset.groupby("symbol", sort=False):
        sym_daily = grp.drop(columns="symbol").set_index("date").sort_index()
        try:
            weekly = to_weekly(sym_daily)
        except Exception:
            continue
        if len(weekly) < MIN_WEEKS:
            continue
        res = classify_latest(weekly, symbol=symbol)
        rows.append({
            "symbol": symbol,
            "stage_label": res.stage_label,
            "stage_confidence": res.stage_confidence,
        })
    return pd.DataFrame(rows)


def s2_symbols(snapshot: pd.DataFrame, min_conf: float = STAGE_MIN_CONF) -> set[str]:
    """Set of symbols classified as S2 with sufficient confidence."""
    if snapshot.empty:
        return set()
    mask = (snapshot["stage_label"] == "S2") & (snapshot["stage_confidence"] >= min_conf)
    return set(snapshot.loc[mask, "symbol"])


# ── Rebalance date generation ─────────────────────────────────────────────────

def rebalance_fridays(
    daily: pd.DataFrame,
    *,
    start: str,
    end: str,
) -> list[pd.Timestamp]:
    """All Fridays (or nearest trading day) in [start, end]."""
    trading_days = pd.DatetimeIndex(sorted(daily["date"].unique()))
    fridays = [d for d in trading_days
               if pd.Timestamp(start) <= d <= pd.Timestamp(end)
               and d.weekday() == 4]
    if not fridays:
        # Fall back to all available trading days if no Fridays in range.
        fridays = [d for d in trading_days
                   if pd.Timestamp(start) <= d <= pd.Timestamp(end)]
    return sorted(fridays)


# ── Forward return calculation ────────────────────────────────────────────────

def forward_return(
    symbols: list[str],
    entry_date: pd.Timestamp,
    close_pivot: pd.DataFrame,
    fwd_days: int,
) -> Optional[float]:
    """Equal-weight forward return for a symbol basket over fwd_days trading days."""
    if not symbols:
        return None
    dates = close_pivot.index
    entry_loc = dates.searchsorted(entry_date)
    exit_loc = entry_loc + fwd_days
    if exit_loc >= len(dates):
        return None
    entry_prices = close_pivot.iloc[entry_loc].reindex(symbols).dropna()
    exit_prices = close_pivot.iloc[exit_loc].reindex(symbols).dropna()
    valid = entry_prices.index.intersection(exit_prices.index)
    if valid.empty:
        return None
    returns = (exit_prices[valid] - entry_prices[valid]) / entry_prices[valid]
    return float(returns.mean() * 100)   # as percentage


# ── Market-stage breadth (per rebalance date) ─────────────────────────────────

def market_stage_for_date(
    snapshot: pd.DataFrame,
    *,
    s2_bull_threshold: float = 0.40,
    s4_bear_threshold: float = 0.40,
    s3_threshold: float = 0.30,
    min_symbols: int = 100,
) -> str:
    """Breadth-based market stage from a pre-built stage snapshot frame.

    ``snapshot`` is the DataFrame returned by :func:`build_stage_snapshot` —
    it has a ``stage_label`` column with one row per classified symbol.

    Uses the same priority rules as ``market_stage.get_market_stage()``:
        S4% > s4_bear_threshold → 'S4'
        S2% > s2_bull_threshold → 'S2'
        S3% > s3_threshold     → 'S3'
        else                   → 'MIXED'
    """
    if snapshot.empty or "stage_label" not in snapshot.columns:
        return "MIXED"
    valid = snapshot[snapshot["stage_label"].notna() & (snapshot["stage_label"] != "UNDEFINED")]
    total = len(valid)
    if total < min_symbols:
        return "MIXED"
    counts = valid["stage_label"].value_counts()
    s2_pct = counts.get("S2", 0) / total
    s4_pct = counts.get("S4", 0) / total
    s3_pct = counts.get("S3", 0) / total
    if s4_pct > s4_bear_threshold:
        return "S4"
    elif s2_pct > s2_bull_threshold:
        return "S2"
    elif s3_pct > s3_threshold:
        return "S3"
    return "MIXED"


# ── Per-period record ────────────────────────────────────────────────────────

@dataclass
class PeriodResult:
    date: str
    mode: str
    n_candidates: int
    n_s2: int
    top_n: int
    ret_4w: Optional[float]
    ret_12w: Optional[float]
    symbols: list
    market_stage: str = "unknown"


# ── Main comparison engine ────────────────────────────────────────────────────

def run_comparison(
    ohlcv_db_path: str,
    feature_store_dir: str,
    *,
    top_n: int = 20,
    min_score: float = 50.0,
    start: str = "2025-08-01",
    end_4w: str = "2026-03-21",
    end_12w: str = "2026-02-14",
    exchange: str = "NSE",
    stage_min_conf: float = STAGE_MIN_CONF,
    filter_by_market_stage: bool = False,
    market_stage_s2_only: bool = True,
) -> pd.DataFrame:
    """Run gated vs baseline comparison. Returns a long-form results frame."""
    from ai_trading_system.analytics.rank_backtester import RankBacktester

    bt = RankBacktester(
        ohlcv_db_path=ohlcv_db_path,
        feature_store_dir=feature_store_dir,
        top_n=top_n,
        data_domain="operational",
    )

    LOG.info("Loading full daily OHLCV into memory …")
    daily = load_all_daily(ohlcv_db_path, exchange)
    close_pivot = load_close_pivot(daily)
    LOG.info("  %d symbols × %d trading days", len(close_pivot.columns), len(close_pivot))

    # Use the broader end date to generate rebalance dates.
    end = max(end_4w, end_12w)
    dates = rebalance_fridays(daily, start=start, end=end)
    LOG.info("  %d rebalance Fridays from %s to %s", len(dates), start, end)

    records: list[PeriodResult] = []
    snapshot_cache: dict[str, pd.DataFrame] = {}

    for friday in dates:
        date_str = friday.strftime("%Y-%m-%d")
        LOG.info("  [%s] classifying stages …", date_str)

        # Build (or reuse) the stage snapshot for this Friday.
        if date_str not in snapshot_cache:
            snap = build_stage_snapshot(daily, friday)
            snapshot_cache[date_str] = snap
        else:
            snap = snapshot_cache[date_str]

        # Compute breadth-based market stage from this snapshot.
        mstage = market_stage_for_date(snap)

        # Optionally skip weeks where the market itself is not in S2.
        if filter_by_market_stage and market_stage_s2_only and mstage != "S2":
            LOG.info("  [%s] skip — market_stage=%s (filter_by_market_stage=True)", date_str, mstage)
            continue

        s2_set = s2_symbols(snap, min_conf=stage_min_conf)

        # Factor ranking from the existing backtester.
        try:
            ranked = bt.rank_stocks(date_str, exchange=exchange)
        except Exception as exc:
            LOG.warning("  rank_stocks failed for %s: %s", date_str, exc)
            continue

        ranked = ranked[ranked["composite_score"] >= min_score].copy()
        if ranked.empty:
            LOG.warning("  no candidates above min_score=%s on %s", min_score, date_str)
            continue

        # Baseline: top-N by composite score, no stage filter.
        baseline_syms = ranked.head(top_n)["symbol_id"].tolist()

        # Gated: restrict to S2 first, then take top-N by composite score.
        gated_ranked = ranked[ranked["symbol_id"].isin(s2_set)]
        gated_syms = gated_ranked.head(top_n)["symbol_id"].tolist()

        for mode, syms in [("baseline", baseline_syms), ("gated", gated_syms)]:
            # Only compute forward return if the window end is within data.
            r4 = forward_return(syms, friday, close_pivot, FORWARD_WINDOWS["4w"]) \
                if friday <= pd.Timestamp(end_4w) else None
            r12 = forward_return(syms, friday, close_pivot, FORWARD_WINDOWS["12w"]) \
                if friday <= pd.Timestamp(end_12w) else None

            records.append(PeriodResult(
                date=date_str,
                mode=mode,
                n_candidates=len(ranked),
                n_s2=len(s2_set),
                top_n=len(syms),
                ret_4w=r4,
                ret_12w=r12,
                symbols=syms,
                market_stage=mstage,
            ))

    df = pd.DataFrame([asdict(r) for r in records])
    return df


# ── Metrics summary ───────────────────────────────────────────────────────────

def summarise(results: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-period records into a metrics table."""
    rows = []
    for mode in ["baseline", "gated"]:
        sub = results[results["mode"] == mode]

        def _stats(col: str) -> dict:
            vals = sub[col].dropna()
            if vals.empty:
                return {"n": 0, "mean": None, "median": None, "win_rate": None,
                        "p25": None, "p75": None}
            return {
                "n": int(len(vals)),
                "mean": round(float(vals.mean()), 2),
                "median": round(float(vals.median()), 2),
                "win_rate": round(float((vals > 0).mean() * 100), 1),
                "p25": round(float(vals.quantile(0.25)), 2),
                "p75": round(float(vals.quantile(0.75)), 2),
            }

        s4 = _stats("ret_4w")
        s12 = _stats("ret_12w")
        rows.append({
            "mode": mode,
            "periods_4w": s4["n"],
            "mean_ret_4w": s4["mean"],
            "median_ret_4w": s4["median"],
            "win_rate_4w": s4["win_rate"],
            "p25_4w": s4["p25"],
            "p75_4w": s4["p75"],
            "periods_12w": s12["n"],
            "mean_ret_12w": s12["mean"],
            "median_ret_12w": s12["median"],
            "win_rate_12w": s12["win_rate"],
            "p25_12w": s12["p25"],
            "p75_12w": s12["p75"],
            "avg_top_n": round(float(sub["top_n"].mean()), 1),
            "avg_s2_universe": round(float(sub["n_s2"].mean()), 0),
        })
    summary = pd.DataFrame(rows).set_index("mode")
    return summary


def stage2_freshness_bucket(row: pd.Series, *, fresh_bars: int = 8) -> str:
    """Classify a stage snapshot row into the Stage 2 freshness cohorts."""
    transition = str(row.get("stage_transition") or "").upper()
    label = str(row.get("stage_label") or "").upper()
    bars = pd.to_numeric(pd.Series([row.get("bars_in_stage")]), errors="coerce").iloc[0]
    if transition == "S1_TO_S2":
        return "S1_TO_S2"
    if label == "S2":
        if pd.notna(bars) and int(bars) <= int(fresh_bars):
            return "fresh_s2"
        return "mature_s2"
    return "non_s2"


def evaluate_stage2_freshness(
    snapshots: pd.DataFrame,
    close_pivot: pd.DataFrame,
    *,
    fresh_bars: int = 8,
    horizons: dict[str, int] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare forward returns for fresh S2, mature S2, S1_TO_S2, and non-S2."""
    horizons = horizons or FORWARD_WINDOWS
    if snapshots.empty or close_pivot.empty:
        empty_summary = pd.DataFrame(
            columns=["bucket", "horizon", "n", "mean_return", "median_return", "win_rate", "p25", "p75"]
        )
        return pd.DataFrame(), empty_summary

    close_pivot = close_pivot.sort_index()
    rows = []
    for _, snap in snapshots.iterrows():
        symbol = str(snap["symbol"])
        asof = pd.Timestamp(snap["week_end_date"])
        bucket = stage2_freshness_bucket(snap, fresh_bars=fresh_bars)
        if symbol not in close_pivot.columns:
            continue
        dates = close_pivot.index
        entry_loc = dates.searchsorted(asof)
        if entry_loc >= len(dates):
            continue
        entry_price = close_pivot.iloc[entry_loc].get(symbol)
        if pd.isna(entry_price) or entry_price <= 0:
            continue
        record = {
            "date": dates[entry_loc].date().isoformat(),
            "symbol": symbol,
            "bucket": bucket,
            "stage_label": snap.get("stage_label"),
            "stage_transition": snap.get("stage_transition"),
            "bars_in_stage": snap.get("bars_in_stage"),
        }
        for label, days in horizons.items():
            exit_loc = entry_loc + int(days)
            if exit_loc >= len(dates):
                record[f"ret_{label}"] = np.nan
                continue
            exit_price = close_pivot.iloc[exit_loc].get(symbol)
            record[f"ret_{label}"] = (
                float((exit_price / entry_price - 1.0) * 100)
                if pd.notna(exit_price) and exit_price > 0
                else np.nan
            )
        rows.append(record)

    detail = pd.DataFrame(rows)
    summary_rows = []
    for bucket in ["fresh_s2", "mature_s2", "S1_TO_S2", "non_s2"]:
        sub = detail[detail["bucket"] == bucket] if not detail.empty else pd.DataFrame()
        for label in horizons:
            col = f"ret_{label}"
            vals = pd.to_numeric(sub.get(col, pd.Series(dtype=float)), errors="coerce").dropna()
            summary_rows.append(
                {
                    "bucket": bucket,
                    "horizon": label,
                    "n": int(len(vals)),
                    "mean_return": round(float(vals.mean()), 2) if not vals.empty else np.nan,
                    "median_return": round(float(vals.median()), 2) if not vals.empty else np.nan,
                    "win_rate": round(float((vals > 0).mean() * 100), 1) if not vals.empty else np.nan,
                    "p25": round(float(vals.quantile(0.25)), 2) if not vals.empty else np.nan,
                    "p75": round(float(vals.quantile(0.75)), 2) if not vals.empty else np.nan,
                }
            )
    return detail, pd.DataFrame(summary_rows)


def run_stage2_freshness_report(
    ohlcv_db_path: str,
    *,
    exchange: str = "NSE",
    start: str | None = None,
    end: str | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, object]:
    """Generate Stage 2 freshness forward-return detail and summary reports."""
    daily = load_all_daily(ohlcv_db_path, exchange=exchange)
    close_pivot = load_close_pivot(daily)
    snapshots = load_weekly_stage_snapshots(ohlcv_db_path, start=start, end=end)
    detail, summary = evaluate_stage2_freshness(snapshots, close_pivot)
    paths: dict[str, str] = {}
    if output_dir is not None:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        detail_path = out / "stage2_freshness_detail.csv"
        summary_path = out / "stage2_freshness_summary.csv"
        detail.to_csv(detail_path, index=False)
        summary.to_csv(summary_path, index=False)
        paths = {"detail": str(detail_path), "summary": str(summary_path)}
    return {"detail": detail, "summary": summary, "paths": paths}


def print_report(results: pd.DataFrame, summary: pd.DataFrame) -> None:
    """Print a readable side-by-side comparison."""
    print("\n" + "=" * 70)
    print("  WEEKLY STAGE GATE BACKTEST  —  Gated (S2 only) vs Baseline")
    print("=" * 70)
    print(f"\n  Study period : {results['date'].min()} → {results['date'].max()}")
    b = summary.loc["baseline"]
    g = summary.loc["gated"]
    print(f"  Baseline top-N avg size : {b['avg_top_n']:.0f}")
    print(f"  Gated   top-N avg size  : {g['avg_top_n']:.0f}  "
          f"(avg S2 universe: {g['avg_s2_universe']:.0f})")

    print("\n  ── 4-Week Forward Returns ──────────────────────────────────")
    print(f"  {'Metric':<22}  {'Baseline':>10}  {'Gated':>10}  {'Δ':>8}")
    print(f"  {'-'*22}  {'-'*10}  {'-'*10}  {'-'*8}")
    for label, bk, gk in [
        ("Mean return (%)",    "mean_ret_4w",   "mean_ret_4w"),
        ("Median return (%)",  "median_ret_4w", "median_ret_4w"),
        ("Win rate (%)",       "win_rate_4w",   "win_rate_4w"),
        ("P25 return (%)",     "p25_4w",        "p25_4w"),
        ("P75 return (%)",     "p75_4w",        "p75_4w"),
        ("Periods",            "periods_4w",    "periods_4w"),
    ]:
        bval = b.get(bk)
        gval = g.get(gk)
        delta = (gval - bval) if (bval is not None and gval is not None) else None
        bstr = f"{bval:>10.2f}" if bval is not None else f"{'N/A':>10}"
        gstr = f"{gval:>10.2f}" if gval is not None else f"{'N/A':>10}"
        dstr = f"{delta:>+8.2f}" if delta is not None else f"{'N/A':>8}"
        print(f"  {label:<22}  {bstr}  {gstr}  {dstr}")

    print("\n  ── 12-Week Forward Returns ─────────────────────────────────")
    print(f"  {'Metric':<22}  {'Baseline':>10}  {'Gated':>10}  {'Δ':>8}")
    print(f"  {'-'*22}  {'-'*10}  {'-'*10}  {'-'*8}")
    for label, bk, gk in [
        ("Mean return (%)",    "mean_ret_12w",   "mean_ret_12w"),
        ("Median return (%)",  "median_ret_12w", "median_ret_12w"),
        ("Win rate (%)",       "win_rate_12w",   "win_rate_12w"),
        ("P25 return (%)",     "p25_12w",        "p25_12w"),
        ("P75 return (%)",     "p75_12w",        "p75_12w"),
        ("Periods",            "periods_12w",    "periods_12w"),
    ]:
        bval = b.get(bk)
        gval = g.get(gk)
        delta = (gval - bval) if (bval is not None and gval is not None) else None
        bstr = f"{bval:>10.2f}" if bval is not None else f"{'N/A':>10}"
        gstr = f"{gval:>10.2f}" if gval is not None else f"{'N/A':>10}"
        dstr = f"{delta:>+8.2f}" if delta is not None else f"{'N/A':>8}"
        print(f"  {label:<22}  {bstr}  {gstr}  {dstr}")
    print()
