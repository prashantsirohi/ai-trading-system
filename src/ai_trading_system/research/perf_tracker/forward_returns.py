"""Compute forward N-day returns for (symbol, date) rows from OHLCV.

Shared between the perf-tracker backfill, the daily perf-tracker stage, and
(future) Phase 3 forward evaluator.

Math: ``fwd_Nd_return = (close_at_run_date_plus_N - close_at_run_date) / close_at_run_date``
expressed as a percentage. Uses raw close (no split adjustment) — same caveat
as everywhere else in the system.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.research.perf_tracker.constants import (
    FORWARD_RETURN_ANOMALY_5D_PCT,
)

FORWARD_HORIZONS: tuple[int, ...] = (5, 10, 20, 60)


def compute_forward_returns(
    rows: pd.DataFrame,
    *,
    project_root: str | Path | None = None,
    horizons: tuple[int, ...] = FORWARD_HORIZONS,
) -> pd.DataFrame:
    """Attach forward-return columns to (run_date, symbol_id, exchange) rows.

    Parameters
    ----------
    rows
        DataFrame with at least ``run_date``, ``symbol_id``, ``exchange``.
        ``run_date`` may be ``date`` / ``datetime`` / ISO string.
    horizons
        Forward windows in trading-day count (default 5/10/20/60).

    Returns
    -------
    The input ``rows`` with two new columns per horizon:
      ``fwd_<N>d_return``       — percent return (NaN if not yet matured)
      ``fwd_<N>d_matured_at``   — date of the future close used (NaN if pending)

    Pending rows (whose horizon hasn't matured yet) are returned with NaN —
    callers re-run this function periodically to fill them in.
    """
    if rows is None or rows.empty:
        return rows

    work = rows.copy()
    # Use assign() rather than .loc[] to dodge pandas' string-dtype strictness
    # when the input column is a string and we want to overwrite with date objects.
    work = work.assign(run_date=pd.to_datetime(work["run_date"]).dt.date)

    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    con = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    try:
        # Pull every (symbol, date, close) for symbols we care about, ordered,
        # then assign each row a per-symbol trading-day index. Forward returns
        # become a self-join on (symbol, idx + horizon).
        symbols_csv = ",".join(f"'{s}'" for s in work["symbol_id"].unique())
        if not symbols_csv:
            return work
        ohlcv = con.execute(
            f"""
            SELECT symbol_id,
                   exchange,
                   CAST(timestamp AS DATE) AS d,
                   close,
                   ROW_NUMBER() OVER (PARTITION BY symbol_id, exchange ORDER BY timestamp) AS idx
            FROM _catalog
            WHERE symbol_id IN ({symbols_csv})
              AND close > 0
              AND timestamp IS NOT NULL
            """
        ).fetchdf()
    finally:
        con.close()

    if ohlcv.empty:
        for n in horizons:
            work[f"fwd_{n}d_return"] = pd.NA
            work[f"fwd_{n}d_matured_at"] = pd.NaT
        return work

    ohlcv["d"] = pd.to_datetime(ohlcv["d"]).dt.date

    # Map (symbol, exchange, run_date) -> idx_at_run_date
    base_idx = ohlcv.rename(columns={"d": "run_date", "idx": "idx_at_run", "close": "close_at_run"})
    base_idx = base_idx[["symbol_id", "exchange", "run_date", "idx_at_run", "close_at_run"]]
    work = work.merge(base_idx, on=["symbol_id", "exchange", "run_date"], how="left")

    for n in horizons:
        target = ohlcv[["symbol_id", "exchange", "idx", "d", "close"]].copy()
        target.loc[:, "idx_target"] = target["idx"] - n  # so target.idx = idx_at_run + n
        target = target.drop(columns=["idx"]).rename(
            columns={
                "d": f"fwd_{n}d_matured_at",
                "close": f"close_fwd_{n}d",
                "idx_target": "idx_at_run",
            }
        )
        work = work.merge(target, on=["symbol_id", "exchange", "idx_at_run"], how="left")
        close_run = pd.to_numeric(work["close_at_run"], errors="coerce")
        close_fwd = pd.to_numeric(work[f"close_fwd_{n}d"], errors="coerce")
        work[f"fwd_{n}d_return"] = (close_fwd - close_run) / close_run.replace(0, pd.NA) * 100.0
        work = work.drop(columns=[f"close_fwd_{n}d"])

    work = work.drop(columns=["idx_at_run", "close_at_run"], errors="ignore")

    # Anomaly flag: a |5-day return| above the threshold is almost certainly
    # a corporate action (split/bonus) showing up in raw close, not real
    # alpha. We don't drop these (callers may want to inspect them) but we
    # surface a boolean column so digest/cohort aggregations can exclude them.
    if "fwd_5d_return" in work.columns:
        r5 = pd.to_numeric(work["fwd_5d_return"], errors="coerce")
        work["fwd_5d_anomaly"] = (r5.abs() > FORWARD_RETURN_ANOMALY_5D_PCT).fillna(False)
    else:
        work["fwd_5d_anomaly"] = False

    return work
