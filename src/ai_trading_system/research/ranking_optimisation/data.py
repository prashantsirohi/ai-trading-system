"""Yearly cross-section + forward-return panels for ranking weight search.

A ``FactorPanel`` is one as-of date worth of data: per-symbol factor values
computed from OHLCV history up to that date, plus the realised forward return
through ``as_of + horizon_days``. The optimiser walks year-by-year over a list
of these panels — each panel is a "fold" worth of evidence.

Factor list (price + volume only — no sector/ADX/delivery dependencies) is
deliberately self-contained so the panel loader does not need the full
ranking-domain machinery. These are validation proxies for the live factors;
the optimiser tells you which ones carry signal, then the live ranker can be
tuned to match.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths

DEFAULT_DB_PATH = get_domain_paths(data_domain="research").ohlcv_db_path

# Factor names searched by the optimiser. Each must appear as a column on the
# panel's DataFrame and be such that "higher → better" (no inverse signals).
FACTOR_NAMES: tuple[str, ...] = (
    "rs_12_1",
    "rs_6m",
    "rs_3m",
    "prox_52w_high",
    "above_200dma_pct",
    "trend_persistence",
    "volume_ratio",
    "low_vol",  # negative-of realised_vol so higher = more stable
)


@dataclass(frozen=True)
class FactorPanel:
    """One year's cross-section: factors as-of date + forward returns."""

    as_of: pd.Timestamp
    horizon_days: int
    df: pd.DataFrame  # columns: symbol_id, anchor_close, forward_return, <FACTOR_NAMES>...

    @property
    def n(self) -> int:
        return len(self.df)


def _load_eligible_symbols(
    con: duckdb.DuckDBPyConnection,
    as_of: pd.Timestamp,
    horizon_days: int,
) -> list[str]:
    """NSE symbols with ≥252 days history before as_of and ≥90% of horizon after."""
    horizon_min = max(int(horizon_days * 0.9), 200)
    end_date = (as_of + pd.Timedelta(days=int(horizon_days * 1.5))).date()
    rows = con.execute(
        """
        WITH cov AS (
          SELECT symbol_id,
                 COUNT(*) FILTER (WHERE CAST(timestamp AS DATE) < ?::DATE) AS days_before,
                 COUNT(*) FILTER (WHERE CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE) AS days_forward
          FROM _catalog
          WHERE exchange = 'NSE' AND close IS NOT NULL AND close > 0
          GROUP BY symbol_id
        )
        SELECT symbol_id FROM cov WHERE days_before >= 252 AND days_forward >= ?
        """,
        [str(as_of.date()), str(as_of.date()), str(end_date), horizon_min],
    ).fetchall()
    return [r[0] for r in rows]


def _load_bars(
    con: duckdb.DuckDBPyConnection,
    syms: list[str],
    as_of: pd.Timestamp,
    horizon_days: int,
) -> pd.DataFrame:
    placeholders = ",".join(["?"] * len(syms))
    lookback_start = (as_of - pd.Timedelta(days=400)).date()
    end_date = (as_of + pd.Timedelta(days=int(horizon_days * 1.5))).date()
    return con.execute(
        f"""
        SELECT symbol_id, CAST(timestamp AS DATE) AS d, close, volume
        FROM _catalog
        WHERE exchange = 'NSE'
          AND symbol_id IN ({placeholders})
          AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
          AND close IS NOT NULL AND close > 0
        ORDER BY symbol_id, d
        """,
        syms + [str(lookback_start), str(end_date)],
    ).fetchdf()


def _compute_per_symbol(
    g: pd.DataFrame, as_of: pd.Timestamp, horizon_days: int
) -> dict | None:
    g = g.sort_values("d").reset_index(drop=True)
    pre = g.loc[g["d"] <= as_of]
    post = g.loc[g["d"] > as_of]
    if len(pre) < 252 or len(post) < int(horizon_days * 0.9):
        return None

    anchor_close = float(pre.iloc[-1]["close"])
    if anchor_close <= 0:
        return None
    target_idx = min(horizon_days, len(post)) - 1
    forward_return = float(post.iloc[target_idx]["close"]) / anchor_close - 1.0

    closes = pre["close"].to_numpy(dtype=float)
    vols = pre["volume"].to_numpy(dtype=float)

    sma50 = pd.Series(closes).rolling(50).mean().to_numpy()
    sma200 = closes[-200:].mean() if len(closes) >= 200 else np.nan

    rets = np.diff(closes[-51:]) / closes[-51:-1] if len(closes) >= 51 else np.array([])
    realised_vol = float(rets.std()) if rets.size else np.nan

    return {
        "symbol_id": g["symbol_id"].iloc[0],
        "anchor_close": anchor_close,
        "forward_return": forward_return,
        "rs_12_1": closes[-21] / closes[-252] - 1.0 if len(closes) >= 252 else np.nan,
        "rs_6m": closes[-1] / closes[-126] - 1.0 if len(closes) >= 126 else np.nan,
        "rs_3m": closes[-1] / closes[-63] - 1.0 if len(closes) >= 63 else np.nan,
        "prox_52w_high": closes[-1] / closes[-252:].max() if len(closes) >= 252 else np.nan,
        "above_200dma_pct": (closes[-1] - sma200) / sma200 if not np.isnan(sma200) and sma200 > 0 else np.nan,
        "trend_persistence": float((closes[-50:] > sma50[-50:]).mean()) if len(closes) >= 100 else np.nan,
        "volume_ratio": (
            float(np.median(vols[-20:]) / np.median(vols[-100:]))
            if len(vols) >= 100 and np.median(vols[-100:]) > 0
            else np.nan
        ),
        "low_vol": -realised_vol if not np.isnan(realised_vol) else np.nan,
        "turnover_med": float(np.median(closes[-20:] * vols[-20:])) if len(vols) >= 20 else np.nan,
    }


def load_factor_panel(
    as_of: str | pd.Timestamp,
    *,
    horizon_days: int = 252,
    db_path: str | Path = DEFAULT_DB_PATH,
    min_turnover_crores: float = 1.0,
) -> FactorPanel:
    """Build the cross-section panel for one as-of date."""
    as_of_ts = pd.Timestamp(as_of)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        syms = _load_eligible_symbols(con, as_of_ts, horizon_days)
        if not syms:
            return FactorPanel(as_of_ts, horizon_days, pd.DataFrame())
        bars = _load_bars(con, syms, as_of_ts, horizon_days)
    finally:
        con.close()
    if bars.empty:
        return FactorPanel(as_of_ts, horizon_days, pd.DataFrame())
    bars = bars.assign(d=pd.to_datetime(bars["d"]))
    rows = [
        row
        for _, g in bars.groupby("symbol_id", sort=False)
        if (row := _compute_per_symbol(g, as_of_ts, horizon_days)) is not None
    ]
    df = pd.DataFrame(rows)
    if df.empty:
        return FactorPanel(as_of_ts, horizon_days, df)
    if min_turnover_crores > 0 and "turnover_med" in df.columns:
        floor = min_turnover_crores * 1e7
        df = df.loc[df["turnover_med"].fillna(0) >= floor].reset_index(drop=True)
    return FactorPanel(as_of_ts, horizon_days, df)
