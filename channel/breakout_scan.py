"""Dedicated breakout scanner for operational ranking outputs."""

from __future__ import annotations

import os
import sqlite3
from typing import Optional

import duckdb
import pandas as pd

from utils.logger import logger


def _load_sector_map(master_db_path: str) -> dict[str, str]:
    if not os.path.exists(master_db_path):
        return {}
    conn = sqlite3.connect(master_db_path)
    try:
        rows = conn.execute(
            "SELECT Symbol, Sector FROM stock_details WHERE Symbol IS NOT NULL"
        ).fetchall()
    finally:
        conn.close()
    return {symbol: sector for symbol, sector in rows if sector}


def _load_supertrend_flags(
    feature_store_dir: str,
    symbols: list[str],
    date: str,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """Load latest supertrend direction per symbol up to the ranking date."""
    feature_dir = os.path.join(feature_store_dir, "supertrend", exchange)
    if not os.path.isdir(feature_dir) or not symbols:
        return pd.DataFrame(columns=["symbol_id", "supertrend_dir_10_3"])

    rows: list[pd.DataFrame] = []
    cutoff = pd.to_datetime(date)
    for symbol in symbols:
        path = os.path.join(feature_dir, f"{symbol}.parquet")
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_parquet(path, columns=["symbol_id", "timestamp", "supertrend_dir_10_3"])
        except Exception:
            continue
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[df["timestamp"] <= cutoff]
        if df.empty:
            continue
        rows.append(df.sort_values("timestamp").tail(1)[["symbol_id", "supertrend_dir_10_3"]])

    if not rows:
        return pd.DataFrame(columns=["symbol_id", "supertrend_dir_10_3"])
    return pd.concat(rows, ignore_index=True).drop_duplicates("symbol_id", keep="last")


def scan_breakouts(
    ohlcv_db_path: str,
    feature_store_dir: str,
    master_db_path: str,
    date: Optional[str] = None,
    exchange: str = "NSE",
    top_n: int = 25,
    range_window: int = 20,
    min_breakout_pct: float = 0.0,
    min_volume_ratio: float = 1.2,
    min_adx: float = 20.0,
) -> pd.DataFrame:
    """Build an explicit breakout scan from current OHLCV and feature-store inputs.

    The scanner is intentionally conservative:
    - close above prior `range_window` day high
    - volume expansion vs 20-day average
    - supertrend in bullish state
    - optional ADX confirmation
    """
    conn = duckdb.connect(ohlcv_db_path, read_only=True)
    try:
        if date is None:
            latest = conn.execute(
                f"SELECT MAX(timestamp) FROM _catalog WHERE exchange = '{exchange}'"
            ).fetchone()[0]
            if latest is None:
                return pd.DataFrame()
            date = str(pd.Timestamp(latest).date())

        query = f"""
            WITH base AS (
                SELECT
                    symbol_id,
                    exchange,
                    CAST(timestamp AS DATE) AS trade_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN {range_window} PRECEDING AND 1 PRECEDING
                    ) AS prior_range_high,
                    MIN(low) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN {range_window} PRECEDING AND 1 PRECEDING
                    ) AS prior_range_low,
                    AVG(volume) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS vol_20_avg,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                    ) AS high_52w,
                    AVG(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20,
                    AVG(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS sma_50
                FROM _catalog
                WHERE exchange = '{exchange}'
                  AND timestamp <= '{date}'
            )
            SELECT *
            FROM base
            WHERE trade_date = '{date}'
        """
        latest = conn.execute(query).fetchdf()

        adx_path = os.path.join(feature_store_dir, "adx", exchange)
        if os.path.isdir(adx_path):
            adx_df = conn.execute(
                f"""
                SELECT symbol_id, adx_14
                FROM read_parquet('{adx_path}/*.parquet')
                WHERE timestamp <= '{date}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) = 1
                """
            ).fetchdf()
        else:
            adx_df = pd.DataFrame(columns=["symbol_id", "adx_14"])
    finally:
        conn.close()

    if latest.empty:
        return pd.DataFrame()

    latest = latest.merge(adx_df, on="symbol_id", how="left")
    symbols = latest["symbol_id"].astype(str).tolist()
    supertrend_df = _load_supertrend_flags(feature_store_dir, symbols, date, exchange=exchange)
    latest = latest.merge(supertrend_df, on="symbol_id", how="left")

    sector_map = _load_sector_map(master_db_path)
    latest["sector"] = latest["symbol_id"].map(sector_map).fillna("Other")

    latest["vol_20_avg"] = latest["vol_20_avg"].replace(0, pd.NA)
    latest["breakout_pct"] = (
        (latest["close"] - latest["prior_range_high"]) / latest["prior_range_high"].replace(0, pd.NA) * 100
    )
    latest["range_width_pct"] = (
        (latest["prior_range_high"] - latest["prior_range_low"]) / latest["prior_range_low"].replace(0, pd.NA) * 100
    )
    latest["volume_ratio"] = latest["volume"] / latest["vol_20_avg"]
    latest["near_52w_high_pct"] = (
        (1 - latest["close"] / latest["high_52w"].replace(0, pd.NA)) * 100
    )
    latest["supertrend_bullish"] = latest["supertrend_dir_10_3"].fillna(-1).eq(1)
    latest["adx_14"] = latest["adx_14"].fillna(0.0)
    latest["above_sma_20"] = latest["close"] > latest["sma_20"].fillna(latest["close"])
    latest["above_sma_50"] = latest["close"] > latest["sma_50"].fillna(latest["close"])
    latest["is_range_breakout"] = latest["close"] > latest["prior_range_high"].fillna(float("inf"))

    candidates = latest[
        latest["is_range_breakout"]
        & (latest["breakout_pct"].fillna(-999) >= min_breakout_pct)
        & (latest["volume_ratio"].fillna(0) >= min_volume_ratio)
        & latest["supertrend_bullish"]
        & (latest["adx_14"] >= min_adx)
        & latest["above_sma_20"]
    ].copy()

    if candidates.empty:
        logger.info("Breakout scan found no candidates for %s", date)
        return pd.DataFrame(
            columns=[
                "symbol_id",
                "sector",
                "breakout_pct",
                "volume_ratio",
                "adx_14",
                "near_52w_high_pct",
                "range_width_pct",
                "setup_quality",
                "breakout_tag",
            ]
        )

    candidates["setup_quality"] = (
        candidates["breakout_pct"].fillna(0) * 0.35
        + candidates["volume_ratio"].fillna(0) * 25
        + candidates["adx_14"].fillna(0) * 0.25
        + (100 - candidates["near_52w_high_pct"].clip(lower=0, upper=100).fillna(100)) * 0.15
        - candidates["range_width_pct"].clip(lower=0).fillna(0) * 0.05
    )
    candidates["breakout_tag"] = "range_breakout_volume_supertrend"

    cols = [
        "symbol_id",
        "sector",
        "close",
        "prior_range_high",
        "breakout_pct",
        "volume_ratio",
        "adx_14",
        "near_52w_high_pct",
        "range_width_pct",
        "supertrend_dir_10_3",
        "setup_quality",
        "breakout_tag",
    ]
    return (
        candidates[cols]
        .sort_values(["setup_quality", "breakout_pct"], ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
