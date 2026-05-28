"""Valuation cycle signals derived from universe valuation daily tables."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ai_trading_system.domains.fundamentals.analytical_store import (
    connect_fundamentals_duckdb,
    ensure_fundamentals_analytical_schema,
)


@dataclass(frozen=True)
class FundamentalValuationCycleResult:
    rows: int
    start_date: str | None
    end_date: str | None


def refresh_fundamental_valuation_cycle_features(
    *,
    fundamentals_db_path: str | Path | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> FundamentalValuationCycleResult:
    conn = connect_fundamentals_duckdb(fundamentals_db_path)
    try:
        ensure_fundamentals_analytical_schema(conn)
        universe = conn.execute(
            """
            SELECT *
            FROM universe_valuation_daily
            WHERE date <= COALESCE(CAST(? AS DATE), DATE '9999-12-31')
            ORDER BY universe_id, date
            """,
            [str(to_date)[:10] if to_date else None],
        ).df()
        features_all = compute_valuation_cycle_features(universe)
        features = _filter_dates(features_all, from_date, to_date)
        if not features.empty:
            start, end = str(features["date"].min())[:10], str(features["date"].max())[:10]
            conn.execute(
                "DELETE FROM valuation_cycle_features WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
            conn.register("_fundamental_valuation_cycle_frame", features)
            try:
                conn.execute("INSERT INTO valuation_cycle_features SELECT * FROM _fundamental_valuation_cycle_frame")
            finally:
                conn.unregister("_fundamental_valuation_cycle_frame")
        elif from_date or to_date:
            start = str(from_date or to_date)[:10]
            end = str(to_date or from_date)[:10]
            conn.execute(
                "DELETE FROM valuation_cycle_features WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
        else:
            start = end = None
    finally:
        conn.close()
    return FundamentalValuationCycleResult(rows=int(len(features)), start_date=start, end_date=end)


def compute_valuation_cycle_features(universe: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "entity_type",
        "entity_id",
        "date",
        "pe_ttm",
        "pe_200dma",
        "pe_distance_from_200dma",
        "pe_5y_median",
        "pe_percentile_5y",
        "pe_zscore_5y",
        "valuation_zone",
        "cycle_signal",
        "created_at",
    ]
    if universe.empty:
        return pd.DataFrame(columns=columns)
    frame = universe.copy()
    frame.loc[:, "date"] = pd.to_datetime(frame["date"]).dt.date
    frame.loc[:, "pe_distance_from_200dma"] = (
        pd.to_numeric(frame["pe_ttm"], errors="coerce")
        / pd.to_numeric(frame["pe_200dma"], errors="coerce").where(pd.to_numeric(frame["pe_200dma"], errors="coerce").ne(0))
        - 1.0
    ) * 100.0
    frame.loc[:, "cycle_signal"] = frame.apply(_cycle_signal, axis=1)
    frame.loc[:, "entity_type"] = "universe"
    frame.loc[:, "entity_id"] = frame["universe_id"]
    frame.loc[:, "created_at"] = pd.Timestamp.now(tz='UTC').tz_localize(None)
    return frame[columns].reset_index(drop=True)


def _cycle_signal(row: pd.Series) -> str:
    zone = str(row.get("valuation_zone") or "unknown")
    distance = _num(row.get("pe_distance_from_200dma"))
    percentile = _num(row.get("pe_percentile_5y"))
    if distance > 0 and percentile > 80:
        return "expensive_bull_phase"
    if distance < 0 and percentile < 25:
        return "valuation_reset_bear_phase"
    if distance > 0 and percentile < 40:
        return "early_bull_recovery"
    if distance < 0 and zone in {"late_bull", "bubble_top_risk"}:
        return "top_risk_warning"
    return "neutral"


def _filter_dates(frame: pd.DataFrame, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    dates = pd.to_datetime(out["date"]).dt.date
    if from_date:
        out = out.loc[dates >= pd.Timestamp(from_date).date()]
        dates = pd.to_datetime(out["date"]).dt.date
    if to_date:
        out = out.loc[dates <= pd.Timestamp(to_date).date()]
    return out.reset_index(drop=True)


def _num(value) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if pd.isna(parsed) else parsed


__all__ = [
    "FundamentalValuationCycleResult",
    "compute_valuation_cycle_features",
    "refresh_fundamental_valuation_cycle_features",
]
