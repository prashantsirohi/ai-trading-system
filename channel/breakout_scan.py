"""Dedicated breakout scanner for operational ranking outputs."""

from __future__ import annotations

import os
import sqlite3
from typing import Optional

import duckdb
import pandas as pd

from analytics.regime_detector import RegimeDetector
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
    """Load latest and previous supertrend direction per symbol up to the ranking date."""
    feature_dir = os.path.join(feature_store_dir, "supertrend", exchange)
    if not os.path.isdir(feature_dir) or not symbols:
        return pd.DataFrame(
            columns=[
                "symbol_id",
                "supertrend_dir_10_3",
                "prev_supertrend_dir_10_3",
                "supertrend_10_3",
            ]
        )

    rows: list[pd.DataFrame] = []
    cutoff = pd.to_datetime(date)
    for symbol in symbols:
        path = os.path.join(feature_dir, f"{symbol}.parquet")
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_parquet(
                path,
                columns=["symbol_id", "timestamp", "supertrend_dir_10_3", "supertrend_10_3"],
            )
        except Exception:
            continue
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[df["timestamp"] <= cutoff]
        if df.empty:
            continue
        tail = df.sort_values("timestamp").tail(2).copy()
        latest = tail.iloc[-1]
        prev_dir = (
            int(tail.iloc[-2]["supertrend_dir_10_3"])
            if len(tail) > 1 and pd.notna(tail.iloc[-2]["supertrend_dir_10_3"])
            else pd.NA
        )
        rows.append(
            pd.DataFrame(
                [
                    {
                        "symbol_id": latest["symbol_id"],
                        "supertrend_dir_10_3": latest.get("supertrend_dir_10_3"),
                        "prev_supertrend_dir_10_3": prev_dir,
                        "supertrend_10_3": latest.get("supertrend_10_3"),
                    }
                ]
            )
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "symbol_id",
                "supertrend_dir_10_3",
                "prev_supertrend_dir_10_3",
                "supertrend_10_3",
            ]
        )
    return pd.concat(rows, ignore_index=True).drop_duplicates("symbol_id", keep="last")


def scan_breakouts(
    ohlcv_db_path: str,
    feature_store_dir: str,
    master_db_path: str,
    date: Optional[str] = None,
    exchange: str = "NSE",
    top_n: int = 25,
    min_volume_ratio: float = 1.2,
    min_adx: float = 18.0,
) -> pd.DataFrame:
    """Build a breakout monitor with setup families and market-regime context."""
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
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS prior_range_high,
                    MIN(low) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS prior_range_low,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                    ) AS prior_base_high_30,
                    MIN(low) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                    ) AS prior_base_low_30,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
                    ) AS prior_base_high_60,
                    MIN(low) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
                    ) AS prior_base_low_60,
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
        atr_path = os.path.join(feature_store_dir, "atr", exchange)
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

        if os.path.isdir(atr_path):
            atr_df = conn.execute(
                f"""
                SELECT symbol_id, atr_14
                FROM read_parquet('{atr_path}/*.parquet')
                WHERE timestamp <= '{date}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) = 1
                """
            ).fetchdf()
        else:
            atr_df = pd.DataFrame(columns=["symbol_id", "atr_14"])
    finally:
        conn.close()

    if latest.empty:
        return pd.DataFrame()

    latest = latest.merge(adx_df, on="symbol_id", how="left")
    latest = latest.merge(atr_df, on="symbol_id", how="left")
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
        (latest["prior_range_high"] - latest["prior_range_low"]) / latest["prior_range_high"].replace(0, pd.NA) * 100
    )
    latest["base_width_pct_30"] = (
        (latest["prior_base_high_30"] - latest["prior_base_low_30"]) / latest["prior_base_high_30"].replace(0, pd.NA) * 100
    )
    latest["base_width_pct_60"] = (
        (latest["prior_base_high_60"] - latest["prior_base_low_60"]) / latest["prior_base_high_60"].replace(0, pd.NA) * 100
    )
    latest["volume_ratio"] = latest["volume"] / latest["vol_20_avg"]
    latest["near_52w_high_pct"] = (
        (1 - latest["close"] / latest["high_52w"].replace(0, pd.NA)) * 100
    )
    latest["atr_pct"] = latest["atr_14"] / latest["close"].replace(0, pd.NA) * 100
    latest["contraction_ratio"] = latest["range_width_pct"] / latest["base_width_pct_60"].replace(0, pd.NA)
    latest["supertrend_bullish"] = latest["supertrend_dir_10_3"].fillna(-1).eq(1)
    latest["supertrend_flip_up"] = (
        latest["supertrend_dir_10_3"].fillna(-1).eq(1)
        & latest["prev_supertrend_dir_10_3"].fillna(-1).eq(-1)
    )
    latest["adx_14"] = latest["adx_14"].fillna(0.0)
    latest["above_sma_20"] = latest["close"] > latest["sma_20"].fillna(latest["close"])
    latest["above_sma_50"] = latest["close"] > latest["sma_50"].fillna(latest["close"])
    latest["is_range_breakout"] = latest["close"] > latest["prior_range_high"].fillna(float("inf"))
    latest["is_base_breakout_30"] = latest["close"] > latest["prior_base_high_30"].fillna(float("inf"))
    latest["is_base_breakout_60"] = latest["close"] > latest["prior_base_high_60"].fillna(float("inf"))

    common_filter = (
        latest["vol_20_avg"].notna()
        & latest["high_52w"].notna()
        & (latest["volume_ratio"].fillna(0) >= min_volume_ratio)
        & (latest["adx_14"] >= min_adx)
        & latest["above_sma_20"]
        & latest["above_sma_50"]
        & latest["supertrend_bullish"]
    )

    base_breakouts = latest[
        common_filter
        & latest["is_base_breakout_30"]
        & latest["prior_base_high_30"].notna()
        & latest["base_width_pct_30"].between(4, 18, inclusive="both")
        & latest["base_width_pct_60"].between(6, 28, inclusive="both")
        & (latest["breakout_pct"].fillna(999) <= 4.0)
        & (latest["near_52w_high_pct"].fillna(999) <= 12.0)
        & (latest["contraction_ratio"].fillna(999) <= 0.9)
    ].copy()
    if not base_breakouts.empty:
        base_breakouts["setup_family"] = "base_breakout"
        base_breakouts["setup_quality"] = (
            base_breakouts["volume_ratio"].clip(0, 4) * 14
            + base_breakouts["adx_14"].clip(0, 60) * 0.6
            + (12 - base_breakouts["near_52w_high_pct"].clip(0, 12)) * 2.2
            + (18 - base_breakouts["base_width_pct_30"].clip(4, 18)) * 1.2
            - base_breakouts["breakout_pct"].clip(0, 4) * 1.5
        )

    contraction_breakouts = latest[
        common_filter
        & latest["is_range_breakout"]
        & latest["prior_range_high"].notna()
        & latest["range_width_pct"].between(2, 12, inclusive="both")
        & latest["base_width_pct_60"].between(8, 30, inclusive="both")
        & (latest["contraction_ratio"].fillna(999) <= 0.7)
        & (latest["breakout_pct"].fillna(999) <= 3.5)
        & (latest["near_52w_high_pct"].fillna(999) <= 10.0)
        & (latest["atr_pct"].fillna(999) <= 5.0)
    ].copy()
    if not contraction_breakouts.empty:
        contraction_breakouts["setup_family"] = "contraction_breakout"
        contraction_breakouts["setup_quality"] = (
            contraction_breakouts["volume_ratio"].clip(0, 4) * 16
            + contraction_breakouts["adx_14"].clip(0, 60) * 0.5
            + (10 - contraction_breakouts["near_52w_high_pct"].clip(0, 10)) * 2.0
            + (0.8 - contraction_breakouts["contraction_ratio"].clip(0, 0.8)) * 30
            - contraction_breakouts["range_width_pct"].clip(2, 12) * 0.8
        )

    supertrend_breakouts = latest[
        common_filter
        & latest["supertrend_flip_up"]
        & latest["is_range_breakout"]
        & latest["prior_range_high"].notna()
        & (latest["breakout_pct"].fillna(999) <= 3.0)
        & latest["range_width_pct"].between(3, 20, inclusive="both")
        & (latest["near_52w_high_pct"].fillna(999) <= 14.0)
    ].copy()
    if not supertrend_breakouts.empty:
        supertrend_breakouts["setup_family"] = "supertrend_flip_breakout"
        supertrend_breakouts["setup_quality"] = (
            supertrend_breakouts["volume_ratio"].clip(0, 4) * 12
            + supertrend_breakouts["adx_14"].clip(0, 60) * 0.55
            + (14 - supertrend_breakouts["near_52w_high_pct"].clip(0, 14)) * 1.8
            + supertrend_breakouts["breakout_pct"].clip(0, 3) * 6
        )

    candidates = pd.concat(
        [base_breakouts, contraction_breakouts, supertrend_breakouts],
        ignore_index=True,
    )

    if candidates.empty:
        logger.info("Breakout scan found no candidates for %s", date)
        return pd.DataFrame(
            columns=[
                "symbol_id",
                "sector",
                "setup_family",
                "execution_label",
                "market_regime",
                "market_bias",
                "setup_quality",
                "breakout_tag",
            ]
        )

    candidates = candidates.sort_values("setup_quality", ascending=False)
    candidates = candidates.drop_duplicates("symbol_id", keep="first")

    regime = RegimeDetector(
        ohlcv_db_path=ohlcv_db_path,
        feature_store_dir=feature_store_dir,
    ).get_market_regime(exchange=exchange, date=date)
    market_regime = regime.get("market_regime", "UNKNOWN")
    market_bias = regime.get("market_bias", "UNKNOWN")

    def _execution_label(row: pd.Series) -> str:
        if market_bias == "BEARISH":
            if row["setup_family"] == "supertrend_flip_breakout":
                return "COUNTER_TREND_BREAKOUT"
            return "RELATIVE_STRENGTH_BREAKOUT"
        if market_bias == "NEUTRAL":
            return "EARLY_BREAKOUT"
        return "ACTIONABLE_BREAKOUT"

    candidates["market_regime"] = market_regime
    candidates["market_bias"] = market_bias
    candidates["execution_label"] = candidates.apply(_execution_label, axis=1)
    candidates["breakout_tag"] = candidates["setup_family"]

    cols = [
        "symbol_id",
        "sector",
        "setup_family",
        "execution_label",
        "market_regime",
        "market_bias",
        "close",
        "prior_range_high",
        "breakout_pct",
        "base_width_pct_30",
        "base_width_pct_60",
        "contraction_ratio",
        "volume_ratio",
        "adx_14",
        "near_52w_high_pct",
        "range_width_pct",
        "supertrend_dir_10_3",
        "prev_supertrend_dir_10_3",
        "setup_quality",
        "breakout_tag",
    ]
    return (
        candidates[cols]
        .sort_values(["setup_quality", "breakout_pct"], ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
