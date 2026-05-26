"""Historical valuation bands and cycle labels."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.features.valuation_schema import ensure_valuation_schema


@dataclass(frozen=True)
class ValuationCycleResult:
    rows: int
    universe_rows: int
    sector_rows: int


def refresh_valuation_cycle_features(
    *,
    ohlcv_db_path: str | Path,
    from_date: str | None = None,
    to_date: str | None = None,
    min_history_days: int = 756,
) -> ValuationCycleResult:
    conn = duckdb.connect(str(ohlcv_db_path))
    try:
        ensure_valuation_schema(conn)
        universe = _load_universe_entities(conn)
        sector = _load_sector_entities(conn)
        combined = pd.concat([universe, sector], ignore_index=True)
        if combined.empty:
            return ValuationCycleResult(0, 0, 0)
        features = _compute_cycle_features(combined, min_history_days=min_history_days)
        if from_date:
            features = features.loc[pd.to_datetime(features["date"]).dt.date >= pd.Timestamp(from_date).date()]
        if to_date:
            features = features.loc[pd.to_datetime(features["date"]).dt.date <= pd.Timestamp(to_date).date()]
        if not features.empty:
            start, end = str(features["date"].min())[:10], str(features["date"].max())[:10]
            conn.execute(
                """
                DELETE FROM valuation_cycle_features
                WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                """,
                [start, end],
            )
            conn.register("_valuation_cycle_frame", features)
            try:
                conn.execute("INSERT INTO valuation_cycle_features SELECT * FROM _valuation_cycle_frame")
            finally:
                conn.unregister("_valuation_cycle_frame")
    finally:
        conn.close()
    return ValuationCycleResult(
        rows=len(features),
        universe_rows=int(features["entity_type"].eq("universe").sum()) if not features.empty else 0,
        sector_rows=int(features["entity_type"].eq("sector").sum()) if not features.empty else 0,
    )


def _load_universe_entities(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            'universe' AS entity_type,
            universe_id AS entity_id,
            date,
            pe_ttm,
            earnings_yield
        FROM universe_index_daily
        WHERE index_type = 'market_cap_weight'
          AND pe_ttm IS NOT NULL
        """
    ).df()


def _load_sector_entities(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            'sector' AS entity_type,
            universe_id || ':' || sector_name AS entity_id,
            date,
            pe_ttm,
            earnings_yield
        FROM sector_valuation_daily
        WHERE pe_ttm IS NOT NULL
        """
    ).df()


def _compute_cycle_features(frame: pd.DataFrame, *, min_history_days: int) -> pd.DataFrame:
    output = frame.copy()
    output.loc[:, "date"] = pd.to_datetime(output["date"]).dt.date
    output = output.sort_values(["entity_type", "entity_id", "date"], kind="stable")
    pieces = []
    for _, group in output.groupby(["entity_type", "entity_id"], sort=True):
        group = group.copy()
        pe = pd.to_numeric(group["pe_ttm"], errors="coerce")
        for label, window in {"3y": 756, "5y": 1260, "10y": 2520}.items():
            min_periods = _effective_min_periods(
                observations=int(pe.notna().sum()),
                requested_min=min_history_days,
                window=window,
            )
            group.loc[:, f"pe_pctile_{label}"] = pe.rolling(window, min_periods=min_periods).apply(_last_percentile, raw=False)
            mean = pe.rolling(window, min_periods=min_periods).mean()
            std = pe.rolling(window, min_periods=min_periods).std()
            group.loc[:, f"pe_zscore_{label}"] = (pe - mean) / std.where(std.ne(0))
        group.loc[:, "valuation_zone"] = group.apply(_valuation_zone, axis=1)
        group.loc[:, "cycle_signal"] = group.apply(_cycle_signal, axis=1)
        pieces.append(group)
    result = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    columns = [
        "entity_type",
        "entity_id",
        "date",
        "pe_ttm",
        "earnings_yield",
        "pe_pctile_3y",
        "pe_pctile_5y",
        "pe_pctile_10y",
        "pe_zscore_3y",
        "pe_zscore_5y",
        "pe_zscore_10y",
        "valuation_zone",
        "cycle_signal",
    ]
    return result[columns] if not result.empty else pd.DataFrame(columns=columns)


def _effective_min_periods(*, observations: int, requested_min: int, window: int) -> int:
    configured = min(requested_min, window)
    if observations >= configured:
        return configured
    if observations >= 252:
        return 252
    return configured


def _last_percentile(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return float("nan")
    return float(clean.rank(pct=True).iloc[-1] * 100.0)


def _valuation_zone(row: pd.Series) -> str:
    pctile = _first_available(row, ["pe_pctile_10y", "pe_pctile_5y", "pe_pctile_3y"])
    if pctile is None:
        return "unknown"
    if pctile < 10:
        return "depressed"
    if pctile < 20:
        return "cheap"
    if pctile <= 60:
        return "fair"
    if pctile > 90:
        return "bubble"
    if pctile > 80:
        return "expensive"
    return "fair"


def _cycle_signal(row: pd.Series) -> str:
    zone = str(row.get("valuation_zone") or "unknown")
    zscore = _first_available(row, ["pe_zscore_10y", "pe_zscore_5y", "pe_zscore_3y"])
    if zone in {"depressed", "cheap"} and zscore is not None and zscore < -1:
        return "bottom_zone"
    if zone == "bubble" and zscore is not None and zscore > 1.5:
        return "top_zone"
    return "neutral"


def _first_available(row: pd.Series, columns: list[str]) -> float | None:
    for column in columns:
        value = row.get(column)
        if value is not None and not pd.isna(value):
            return float(value)
    return None


__all__ = ["ValuationCycleResult", "refresh_valuation_cycle_features"]
