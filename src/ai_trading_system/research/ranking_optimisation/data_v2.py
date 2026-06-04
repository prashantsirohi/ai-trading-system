"""v2 yearly panels — production factor scores from the live scoring path.

v1 used proxy factors (rs_12_1, prox_52w_high, ...) computed inline. v2 calls
``load_research_ranked_by_date`` from the strategy-optimiser's research loader
to get the SAME factor scores the live ranker produces, then joins forward
returns for the cross-section.

This means the weights v2 discovers can be promoted directly to
``rank_factor_weights.json`` — no proxy-to-production translation step.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.research.backtesting.research_loader import (
    DEFAULT_BENCHMARK_SYMBOL,
    load_research_ranked_by_date,
)
from ai_trading_system.platform.db.paths import get_domain_paths


# Score columns produced by the live ranker. Order matters: it's the order
# weights are reported in.
PRODUCTION_FACTOR_COLUMNS: tuple[str, ...] = (
    "rel_strength_score",
    "vol_intensity_score",
    "trend_score_score",
    "momentum_acceleration_score",
    "prox_high_score",
    "delivery_pct_score",
    "sector_strength_score",
    "above_200dma_score",
)

# Mirrors DEFAULT_FACTOR_WEIGHTS keys in
# ai_trading_system/domains/ranking/contracts.py so the v2 candidate config can
# be merged into production weights with no key-translation step.
SCORE_TO_WEIGHT_KEY: dict[str, str] = {
    "rel_strength_score":          "relative_strength",
    "vol_intensity_score":         "volume_intensity",
    "trend_score_score":           "trend_persistence",
    "momentum_acceleration_score": "momentum_acceleration",
    "prox_high_score":             "proximity_highs",
    "delivery_pct_score":          "delivery_pct",
    "sector_strength_score":       "sector_strength",
    "above_200dma_score":          "above_200dma",
}
WEIGHT_KEY_TO_SCORE: dict[str, str] = {v: k for k, v in SCORE_TO_WEIGHT_KEY.items()}
WEIGHT_KEYS: tuple[str, ...] = tuple(SCORE_TO_WEIGHT_KEY[col] for col in PRODUCTION_FACTOR_COLUMNS)


@dataclass(frozen=True)
class LiveFactorPanel:
    """One cross-section's worth of production factor scores + forward labels."""

    as_of: pd.Timestamp
    horizon_days: int
    df: pd.DataFrame  # cols: symbol_id, anchor_close, forward_return, <PRODUCTION_FACTOR_COLUMNS>
    degenerate_factors: tuple[str, ...]

    @property
    def n(self) -> int:
        return len(self.df)

    @property
    def active_factors(self) -> tuple[str, ...]:
        """Score columns whose variance was above the degeneracy floor on this panel."""
        return tuple(c for c in PRODUCTION_FACTOR_COLUMNS if c not in self.degenerate_factors)


def quarterly_anchors(years: list[int]) -> list[pd.Timestamp]:
    """Q-end anchors (last calendar day of each quarter)."""
    out: list[pd.Timestamp] = []
    for y in years:
        for month in (3, 6, 9, 12):
            anchor = pd.Timestamp(year=y, month=month, day=1) + pd.offsets.MonthEnd(0)
            out.append(anchor)
    return out


def _next_trading_date(
    con: duckdb.DuckDBPyConnection,
    as_of: pd.Timestamp,
    exchange: str,
) -> pd.Timestamp | None:
    """First trading day in _catalog on or after as_of (handles weekend/holiday anchors)."""
    row = con.execute(
        """
        SELECT MIN(CAST(timestamp AS DATE))
        FROM _catalog
        WHERE exchange = ? AND CAST(timestamp AS DATE) >= ?::DATE
          AND close IS NOT NULL AND close > 0
        """,
        [exchange, str(as_of.date())],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return pd.Timestamp(row[0])


def _forward_close_after_h_trading_days(
    con: duckdb.DuckDBPyConnection,
    anchor: pd.Timestamp,
    horizon_days: int,
    exchange: str,
    symbols: list[str],
) -> pd.DataFrame:
    """Per-symbol close at the H-th trading bar strictly after anchor.

    Returns DataFrame with columns ``symbol_id, forward_close``. Symbols without
    enough forward data are absent.
    """
    placeholders = ",".join(["?"] * len(symbols))
    return con.execute(
        f"""
        WITH ranked AS (
          SELECT symbol_id,
                 CAST(timestamp AS DATE) AS d,
                 close,
                 ROW_NUMBER() OVER (
                   PARTITION BY symbol_id ORDER BY timestamp
                 ) AS rn
          FROM _catalog
          WHERE exchange = ?
            AND symbol_id IN ({placeholders})
            AND CAST(timestamp AS DATE) > ?::DATE
            AND close IS NOT NULL AND close > 0
        )
        SELECT symbol_id, close AS forward_close
        FROM ranked
        WHERE rn = ?
        """,
        [exchange, *symbols, str(anchor.date()), int(horizon_days)],
    ).fetchdf()


# Horizons stored in rank_cohort_performance. Other horizons fall through to
# the slow live-recompute path.
_TABLE_AVAILABLE_HORIZONS = (5, 10, 20, 60)
# factor_<short> column in rank_cohort_performance → production score column.
_TABLE_COLUMN_TO_SCORE = {
    "factor_rs":            "rel_strength_score",
    "factor_vol":           "vol_intensity_score",
    "factor_trend":         "trend_score_score",
    "factor_prox":          "prox_high_score",
    "factor_deliv":         "delivery_pct_score",
    "factor_sector":        "sector_strength_score",
    "factor_momentum_accel": "momentum_acceleration_score",
    "factor_above_200dma":  "above_200dma_score",
}
_MIN_FAST_PATH_ROWS = 50  # below this, the fast path is treated as insufficient


def _load_panel_from_table(
    as_of: pd.Timestamp,
    *,
    horizon_days: int,
    project_root: Path,
    degenerate_var_floor: float,
) -> LiveFactorPanel | None:
    """Fast path: read factor scores + forward returns from rank_cohort_performance.

    Returns ``None`` when:
      - research.duckdb is missing,
      - horizon_days is not one of the table-stored horizons,
      - the table has no rows for ``as_of`` (or fewer than the minimum threshold).
    Callers fall back to the live recompute path on ``None``.
    """
    if horizon_days not in _TABLE_AVAILABLE_HORIZONS:
        return None
    research_db = get_domain_paths(project_root=project_root, data_domain="operational").root_dir / "research.duckdb"
    if not research_db.exists():
        return None
    fwd_col = f"fwd_{horizon_days}d_return"
    con = duckdb.connect(str(research_db), read_only=True)
    try:
        # Pick the first stored run_date on/after as_of so weekend/holiday
        # anchors snap forward deterministically (matches the live path).
        anchor_row = con.execute(
            "SELECT MIN(run_date) FROM rank_cohort_performance "
            "WHERE run_date >= ?::DATE "
            "AND COALESCE(data_quality_status, 'trusted') = 'trusted' "
            "AND NOT COALESCE(fwd_return_anomaly, FALSE)",
            [str(as_of.date())],
        ).fetchone()
        if anchor_row is None or anchor_row[0] is None:
            return None
        anchor_date = anchor_row[0]
        rows = con.execute(
            f"""
            SELECT symbol_id,
                   factor_rs, factor_vol, factor_trend, factor_prox,
                   factor_deliv, factor_sector, factor_momentum_accel,
                   factor_above_200dma,
                   {fwd_col}
            FROM rank_cohort_performance
            WHERE run_date = ?
              AND COALESCE(data_quality_status, 'trusted') = 'trusted'
              AND NOT COALESCE(fwd_return_anomaly, FALSE)
            """,
            [anchor_date],
        ).fetchdf()
    finally:
        con.close()
    if rows.empty:
        return None

    # Rename factor_* columns to PRODUCTION_FACTOR_COLUMNS, drop rows without
    # matured forward returns.
    rows = rows.rename(columns={**_TABLE_COLUMN_TO_SCORE, fwd_col: "forward_return"}).copy()
    rows.loc[:, "forward_return"] = pd.to_numeric(rows["forward_return"], errors="coerce") / 100.0
    rows = rows.loc[rows["forward_return"].notna()].reset_index(drop=True)
    if len(rows) < _MIN_FAST_PATH_ROWS:
        return None

    # The table doesn't store the anchor-day close. v2's optimiser doesn't use
    # it for ranking (only forward_return is consumed by fitness_v2). Leave it
    # at 0.0 — the live winners diagnostic path can still call _load_panel_live
    # explicitly for human-readable reports if needed.
    rows["anchor_close"] = 0.0

    panel_df = rows[
        ["symbol_id", "anchor_close", "forward_return", *PRODUCTION_FACTOR_COLUMNS]
    ].copy()

    degenerate: list[str] = []
    for col in PRODUCTION_FACTOR_COLUMNS:
        var = float(np.nanvar(panel_df[col].to_numpy(dtype=float)))
        if var < degenerate_var_floor:
            degenerate.append(col)

    return LiveFactorPanel(
        as_of=pd.Timestamp(anchor_date),
        horizon_days=horizon_days,
        df=panel_df,
        degenerate_factors=tuple(degenerate),
    )


def load_live_factor_panel(
    as_of: str | pd.Timestamp,
    *,
    horizon_days: int = 20,
    project_root: Path | str = Path.cwd(),
    exchange: str = "NSE",
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    degenerate_var_floor: float = 1.0,
    prefer_table: bool = True,
) -> LiveFactorPanel:
    """Build one cross-section panel using production factor scores.

    When ``prefer_table`` is True (default) and the backfilled
    ``rank_cohort_performance`` table has rows for the anchor date with the
    requested horizon, the panel is loaded from the table (fast). Otherwise
    a live recompute via ``load_research_ranked_by_date`` is performed.
    """
    as_of_ts = pd.Timestamp(as_of)
    project_root = Path(project_root)

    if prefer_table:
        fast = _load_panel_from_table(
            as_of_ts,
            horizon_days=horizon_days,
            project_root=project_root,
            degenerate_var_floor=degenerate_var_floor,
        )
        if fast is not None:
            return fast

    ranked_by_date = load_research_ranked_by_date(
        project_root,
        from_date=as_of_ts.date(),
        to_date=(as_of_ts + pd.Timedelta(days=14)).date(),
        exchange=exchange,
        benchmark_symbol=benchmark_symbol,
    )
    if not ranked_by_date:
        return LiveFactorPanel(as_of_ts, horizon_days, pd.DataFrame(), ())

    # Pick the first trading-day frame on/after as_of so weekend/holiday
    # anchors snap forward deterministically.
    anchor_date = min(d for d in ranked_by_date if d >= as_of_ts.date())
    frame = ranked_by_date[anchor_date].copy()
    if frame.empty:
        return LiveFactorPanel(as_of_ts, horizon_days, pd.DataFrame(), ())

    # Ensure every production column exists; fill missing with 0 and flag as degenerate.
    missing_cols: list[str] = []
    for col in PRODUCTION_FACTOR_COLUMNS:
        if col not in frame.columns:
            frame.loc[:, col] = 0.0
            missing_cols.append(col)

    anchor_ts = pd.Timestamp(anchor_date)

    # Forward close lookup from _catalog directly (fast — single query).
    research_db = get_domain_paths(project_root=project_root, data_domain="research").ohlcv_db_path
    symbols = frame["symbol_id"].astype(str).tolist()
    con = duckdb.connect(str(research_db), read_only=True)
    try:
        fwd = _forward_close_after_h_trading_days(con, anchor_ts, horizon_days, exchange, symbols)
    finally:
        con.close()
    if fwd.empty:
        return LiveFactorPanel(as_of_ts, horizon_days, pd.DataFrame(), tuple(missing_cols))

    merged = frame.merge(fwd, on="symbol_id", how="inner")
    if merged.empty:
        return LiveFactorPanel(as_of_ts, horizon_days, pd.DataFrame(), tuple(missing_cols))

    anchor_close = pd.to_numeric(merged["close"], errors="coerce")
    forward_close = pd.to_numeric(merged["forward_close"], errors="coerce")
    merged.loc[:, "anchor_close"] = anchor_close
    merged.loc[:, "forward_return"] = forward_close / anchor_close - 1.0
    panel_df = merged.loc[
        merged["forward_return"].notna() & (anchor_close > 0),
        ["symbol_id", "anchor_close", "forward_return", *PRODUCTION_FACTOR_COLUMNS],
    ].reset_index(drop=True)

    # Flag degenerate factor columns (low variance) so the runner can drop them
    # from the search space for this panel's fold.
    degenerate = list(missing_cols)
    for col in PRODUCTION_FACTOR_COLUMNS:
        if col in degenerate:
            continue
        var = float(np.nanvar(panel_df[col].to_numpy(dtype=float)))
        if var < degenerate_var_floor:
            degenerate.append(col)

    return LiveFactorPanel(
        as_of=anchor_ts,
        horizon_days=horizon_days,
        df=panel_df,
        degenerate_factors=tuple(degenerate),
    )
