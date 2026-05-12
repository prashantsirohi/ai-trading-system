"""Dynamic backtest loader for the research OHLCV DuckDB store."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.platform.db.paths import ensure_domain_layout


def load_research_ranked_by_date(
    project_root: Path | str,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    exchange: str = "NSE",
    symbols: list[str] | None = None,
    warmup_days: int = 420,
) -> dict[date, pd.DataFrame]:
    """Load research OHLCV and compute engine-ready ranked frames per date.

    This intentionally returns the same shape as ``pipeline_loader``:
    ``dict[date, ranked_df]``. The runner does not care whether those frames came
    from saved pipeline CSVs or from dynamic research calculations.
    """
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    if not paths.ohlcv_db_path.exists():
        return {}

    end = to_date or date.today()
    start = from_date or (end - timedelta(days=365))
    load_start = start - timedelta(days=warmup_days)
    symbol_filter = {s.strip().upper() for s in symbols or [] if s.strip()}

    df = _load_ohlcv(
        paths.ohlcv_db_path,
        exchange=exchange,
        from_date=load_start,
        to_date=end,
        symbols=sorted(symbol_filter) or None,
    )
    if df.empty:
        return {}

    sectors = _load_sector_map(paths.master_db_path)
    ranked = _compute_ranked_frame(df, sectors=sectors)
    ranked = ranked[(ranked["date"] >= pd.Timestamp(start)) & (ranked["date"] <= pd.Timestamp(end))]
    if ranked.empty:
        return {}

    out: dict[date, pd.DataFrame] = {}
    for ts, group in ranked.groupby("date", sort=True):
        day = ts.date() if hasattr(ts, "date") else pd.to_datetime(ts).date()
        frame = group.drop(columns=["date"]).sort_values("eligible_rank", kind="stable").reset_index(drop=True)
        if not frame.empty:
            out[day] = frame
    return out


def _load_ohlcv(
    db_path: Path,
    *,
    exchange: str,
    from_date: date,
    to_date: date,
    symbols: list[str] | None,
) -> pd.DataFrame:
    clauses = ["exchange = ?", "CAST(timestamp AS DATE) >= ?", "CAST(timestamp AS DATE) <= ?"]
    params: list[object] = [exchange, from_date, to_date]
    if symbols:
        clauses.append("symbol_id IN (SELECT UNNEST(?))")
        params.append(symbols)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return conn.execute(
            f"""
            SELECT
                symbol_id,
                exchange,
                CAST(timestamp AS DATE) AS date,
                open,
                high,
                low,
                close,
                volume
            FROM _catalog
            WHERE {' AND '.join(clauses)}
              AND close IS NOT NULL
              AND high IS NOT NULL
              AND low IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol_id, exchange, CAST(timestamp AS DATE)
                ORDER BY timestamp DESC
            ) = 1
            ORDER BY symbol_id, date
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()


def _load_sector_map(master_db_path: Path) -> dict[str, str]:
    if not master_db_path.exists():
        return {}
    conn = sqlite3.connect(str(master_db_path))
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "stock_details" not in tables:
            return {}
        rows = conn.execute("SELECT Symbol, Sector FROM stock_details").fetchall()
        return {str(symbol).strip().upper(): str(sector or "UNKNOWN") for symbol, sector in rows}
    finally:
        conn.close()


def _compute_ranked_frame(df: pd.DataFrame, *, sectors: dict[str, str]) -> pd.DataFrame:
    data = df.reset_index(drop=True).copy(deep=True)
    data.loc[:, "date"] = pd.to_datetime(data["date"])
    data = data.sort_values(["symbol_id", "date"], kind="stable")
    grouped = data.groupby("symbol_id", group_keys=False)

    data["sma_11"] = grouped["close"].transform(lambda s: s.rolling(11, min_periods=11).mean())
    data["sma_20"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    data["sma_50"] = grouped["close"].transform(lambda s: s.rolling(50, min_periods=50).mean())
    data["sma_200"] = grouped["close"].transform(lambda s: s.rolling(200, min_periods=200).mean())
    volume_avg_20 = grouped["volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    data["volume_ratio_20"] = data["volume"] / volume_avg_20.replace(0, pd.NA)
    data["swing_low_20"] = grouped["low"].transform(lambda s: s.rolling(20, min_periods=20).min())

    prev_close = grouped["close"].shift(1)
    tr_components = pd.concat(
        [
            data["high"] - data["low"],
            (data["high"] - prev_close).abs(),
            (data["low"] - prev_close).abs(),
        ],
        axis=1,
    )
    data["true_range"] = tr_components.max(axis=1)
    data["atr_14"] = data.groupby("symbol_id")["true_range"].transform(
        lambda s: s.rolling(14, min_periods=14).mean()
    )
    data["return_60d"] = grouped["close"].pct_change(60)
    data["return_20d"] = grouped["close"].pct_change(20)
    data["is_stage2_uptrend"] = (
        (data["close"] > data["sma_50"])
        & (data["sma_50"] > data["sma_200"])
        & (data["sma_200"] > 0)
    )
    data["sector_name"] = data["symbol_id"].astype(str).str.upper().map(sectors).fillna("UNKNOWN")

    daily = data.groupby("date", group_keys=False)
    data["rs_pct"] = daily["return_60d"].rank(pct=True).fillna(0.0)
    data["trend_pct"] = daily["return_20d"].rank(pct=True).fillna(0.0)
    data["volume_pct"] = daily["volume_ratio_20"].rank(pct=True).fillna(0.0)
    data["composite_score"] = (
        55.0 * data["rs_pct"]
        + 30.0 * data["trend_pct"]
        + 15.0 * data["volume_pct"]
    ).clip(lower=0.0, upper=100.0)
    data["sector_strength_score"] = daily["composite_score"].rank(pct=True).fillna(0.0)
    data["eligible_rank"] = daily["composite_score"].rank(method="first", ascending=False).astype(int)

    return data[
        [
            "date",
            "symbol_id",
            "exchange",
            "close",
            "composite_score",
            "eligible_rank",
            "is_stage2_uptrend",
            "sector_name",
            "sector_strength_score",
            "sma_11",
            "sma_20",
            "sma_50",
            "sma_200",
            "atr_14",
            "volume_ratio_20",
            "swing_low_20",
            "volume",
        ]
    ].dropna(subset=["close"]).reset_index(drop=True)
