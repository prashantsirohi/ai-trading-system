"""Compute forward N-day returns for (symbol, date) rows from OHLCV.

Shared between the perf-tracker backfill, the daily perf-tracker stage, and
(future) Phase 3 forward evaluator.

Math: ``fwd_Nd_return = (close_at_run_date_plus_N - close_at_run_date) / close_at_run_date``
expressed as a percentage, using adjusted close and exchange-wide catalog
session dates. Missing adjusted endpoints are quarantined, never raw fallbacks.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import numpy as np

from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.research.perf_tracker.constants import (
    FORWARD_RETURN_ANOMALY_5D_PCT,
    FORWARD_RETURN_ANOMALY_THRESHOLDS,
)

FORWARD_HORIZONS: tuple[int, ...] = (5, 10, 20, 60)


def compute_forward_returns(
    rows: pd.DataFrame,
    *,
    project_root: str | Path | None = None,
    horizons: tuple[int, ...] = FORWARD_HORIZONS,
    ohlcv_db_path: str | Path | None = None,
) -> pd.DataFrame:
    """Attach forward-return columns to (run_date, symbol_id, exchange) rows.

    Parameters
    ----------
    rows
        DataFrame with at least ``run_date``, ``symbol_id``, ``exchange``.
        ``run_date`` may be ``date`` / ``datetime`` / ISO string.
    horizons
        Forward windows in trading-day count (default 5/10/20/60).
    ohlcv_db_path
        Optional explicit path to the OHLCV DuckDB whose ``_catalog`` is read.
        Defaults to the operational-domain OHLCV DB (current behaviour). The
        historical backfill passes ``data/research/research_ohlcv.duckdb`` so
        forward returns can be computed for pre-2025 dates that aren't in the
        operational store.

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

    if ohlcv_db_path is not None:
        resolved_db = Path(ohlcv_db_path)
    else:
        paths = get_domain_paths(project_root=project_root, data_domain="operational")
        resolved_db = paths.ohlcv_db_path
    con = duckdb.connect(str(resolved_db), read_only=True)
    try:
        columns = {r[1] for r in con.execute("PRAGMA table_info('_catalog')").fetchall()}
        price = "adjusted_close" if "adjusted_close" in columns else "NULL::DOUBLE"
        exchanges = work["exchange"].dropna().astype(str).unique().tolist()
        symbols = work["symbol_id"].dropna().astype(str).unique().tolist()
        sessions = con.execute(
            "SELECT exchange, CAST(timestamp AS DATE) AS d FROM _catalog "
            "WHERE exchange IN (SELECT UNNEST(?)) AND timestamp IS NOT NULL "
            "GROUP BY exchange, d ORDER BY exchange, d", [exchanges]).fetchdf()
        prices = con.execute(
            f"SELECT symbol_id, exchange, CAST(timestamp AS DATE) AS d, "
            f"CASE WHEN COUNT(*) = 1 THEN MAX({price}) ELSE NULL END AS price "
            "FROM _catalog WHERE symbol_id IN (SELECT UNNEST(?)) AND timestamp IS NOT NULL "
            "GROUP BY symbol_id, exchange, d", [symbols]).fetchdf()
    finally:
        con.close()
    sessions["d"] = pd.to_datetime(sessions["d"]).dt.date
    prices["d"] = pd.to_datetime(prices["d"]).dt.date
    sessions["session_index"] = sessions.groupby("exchange").cumcount()
    work = work.drop(columns=[f"fwd_{n}d_{suffix}" for n in horizons for suffix in ("return", "matured_at")], errors="ignore")
    work = work.merge(sessions.rename(columns={"d": "run_date"}), on=["exchange", "run_date"], how="left")
    work = work.merge(prices.rename(columns={"d": "run_date", "price": "base_price"}),
                      on=["symbol_id", "exchange", "run_date"], how="left")
    reasons = pd.Series("", index=work.index, dtype=object)
    base = pd.to_numeric(work["base_price"], errors="coerce")
    invalid_base = ~np.isfinite(base) | base.le(0) | work["session_index"].isna()
    reasons.loc[invalid_base] = "missing_adjusted_entry_close"
    for n in horizons:
        if int(n) != n or n < 1:
            raise ValueError("Forward horizons must be positive integers")
        target = sessions.copy()
        target["session_index"] -= n
        target = target.rename(columns={"d": "target_date"})
        work = work.merge(target, on=["exchange", "session_index"], how="left")
        work = work.merge(prices.rename(columns={"d": "target_date", "price": "target_price"}),
                          on=["symbol_id", "exchange", "target_date"], how="left")
        future = pd.to_numeric(work["target_price"], errors="coerce")
        missing = work["target_date"].notna() & (~np.isfinite(future) | future.le(0))
        reasons.loc[missing] = reasons.loc[missing] + f"|missing_adjusted_exit_close_{n}d"
        valid = ~invalid_base & ~missing & work["target_date"].notna()
        work[f"fwd_{n}d_return"] = ((future / base - 1) * 100).where(valid)
        work[f"fwd_{n}d_matured_at"] = pd.to_datetime(work["target_date"]).where(valid)
        work = work.drop(columns=["target_date", "target_price"])
    work["return_policy_version"] = "adjusted_exchange_sessions_v1"
    work["data_quality_reason"] = reasons.str.strip("|").replace("", pd.NA)
    work["data_quality_status"] = reasons.map(lambda value: "quarantined" if value else "trusted")
    work = work.drop(columns=["session_index", "base_price"])

    # Large adjusted returns still need review. Retain the values for audit
    # and expose flags so trusted analytics can exclude anomalous outcomes.
    if "fwd_5d_return" in work.columns:
        r5 = pd.to_numeric(work["fwd_5d_return"], errors="coerce")
        work.loc[:, "fwd_5d_anomaly"] = (r5.abs() > FORWARD_RETURN_ANOMALY_5D_PCT).fillna(False)
    else:
        work.loc[:, "fwd_5d_anomaly"] = False
    anomaly_columns = [
        pd.to_numeric(work[f"fwd_{n}d_return"], errors="coerce").abs()
        > FORWARD_RETURN_ANOMALY_THRESHOLDS.get(n, FORWARD_RETURN_ANOMALY_5D_PCT)
        for n in horizons
        if f"fwd_{n}d_return" in work.columns
    ]
    work.loc[:, "fwd_return_anomaly"] = (
        pd.concat(anomaly_columns, axis=1).any(axis=1)
        if anomaly_columns
        else False
    )

    return work
