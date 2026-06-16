"""Daily gainer intake from trusted OHLCV catalog and rank artifacts."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.investigator.utils import as_symbol, symbol_column


def latest_trading_date(ohlcv_db_path: Path) -> str | None:
    with duckdb.connect(str(ohlcv_db_path), read_only=True) as conn:
        row = conn.execute(
            """
            SELECT MAX(CAST(timestamp AS DATE))
            FROM _catalog
            WHERE exchange = 'NSE'
              AND COALESCE(is_benchmark, FALSE) = FALSE
              AND lower(COALESCE(instrument_type, 'equity')) IN ('equity', 'eq')
            """
        ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def load_daily_gainers(
    *,
    ohlcv_db_path: Path,
    ranked_signals: pd.DataFrame,
    as_of: str | None = None,
    min_return_pct: float = 5.0,
    min_volume_ratio: float = 2.0,
    min_market_cap_cr: float = 500.0,
) -> pd.DataFrame:
    """Return latest NSE gainer triggers, enriched with rank fields when present."""

    return load_investigator_intake(
        ohlcv_db_path=ohlcv_db_path,
        ranked_signals=ranked_signals,
        as_of=as_of,
        min_return_pct=min_return_pct,
        min_volume_ratio=min_volume_ratio,
        min_market_cap_cr=min_market_cap_cr,
    )


def load_investigator_intake(
    *,
    ohlcv_db_path: Path,
    ranked_signals: pd.DataFrame,
    as_of: str | None = None,
    min_return_pct: float = 5.0,
    min_volume_ratio: float = 2.0,
    min_market_cap_cr: float = 500.0,
    weekly_return_pct: float = 8.0,
    stealth_5d_pct: float = 3.0,
    stealth_20d_pct: float = 8.0,
    min_green_days_5d: int = 3,
    include_weekly: bool = True,
    include_stealth: bool = True,
) -> pd.DataFrame:
    """Return latest NSE investigator triggers, enriched with rank fields when present."""

    resolved_as_of = as_of or latest_trading_date(ohlcv_db_path)
    if not resolved_as_of:
        return _empty()
    with duckdb.connect(str(ohlcv_db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            WITH base AS (
              SELECT
                symbol_id,
                CAST(timestamp AS DATE) AS trade_date,
                open,
                high,
                low,
                close,
                volume,
                LAG(close) OVER (PARTITION BY symbol_id ORDER BY timestamp) AS prev_close,
                LAG(close, 5) OVER (PARTITION BY symbol_id ORDER BY timestamp) AS close_5d_ago,
                LAG(close, 10) OVER (PARTITION BY symbol_id ORDER BY timestamp) AS close_10d_ago,
                LAG(close, 20) OVER (PARTITION BY symbol_id ORDER BY timestamp) AS close_20d_ago,
                (close / NULLIF(LAG(close) OVER (PARTITION BY symbol_id ORDER BY timestamp), 0) - 1.0) * 100.0 AS row_daily_return_pct
              FROM _catalog
              WHERE exchange = 'NSE'
                AND COALESCE(is_benchmark, FALSE) = FALSE
                AND lower(COALESCE(instrument_type, 'equity')) IN ('equity', 'eq')
                AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
            ),
            last_rows AS (
              SELECT
                symbol_id,
                trade_date,
                open,
                high,
                low,
                close,
                volume,
                prev_close,
                close_5d_ago,
                close_10d_ago,
                close_20d_ago,
                row_daily_return_pct,
                ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY trade_date DESC) AS rn,
                MAX(row_daily_return_pct) OVER (
                    PARTITION BY symbol_id
                    ORDER BY trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS max_daily_gain_5d,
                SUM(
                    CASE
                        WHEN row_daily_return_pct > 0
                        THEN 1 ELSE 0
                    END
                ) OVER (
                    PARTITION BY symbol_id
                    ORDER BY trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS green_days_5d,
                AVG(volume) OVER (
                    PARTITION BY symbol_id
                    ORDER BY trade_date
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                ) AS avg_volume_5,
                AVG(volume) OVER (
                    PARTITION BY symbol_id
                    ORDER BY trade_date
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS avg_volume_20
              FROM base
            ),
            latest AS (
              SELECT * FROM last_rows WHERE rn = 1
            ),
            delivery AS (
              SELECT symbol_id, delivery_pct
              FROM (
                SELECT
                  symbol_id,
                  delivery_pct,
                  ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) AS rn
                FROM _delivery
                WHERE exchange = 'NSE'
                  AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
              )
              WHERE rn = 1
            )
            SELECT
              latest.symbol_id,
              latest.trade_date,
              latest.open,
              latest.high,
              latest.low,
              latest.close,
              latest.prev_close,
              latest.volume,
              latest.avg_volume_5,
              latest.avg_volume_20,
              latest.volume / NULLIF(latest.avg_volume_5, 0) AS volume_ratio_5d,
              latest.volume / NULLIF(latest.avg_volume_20, 0) AS volume_ratio_20,
              (latest.close / NULLIF(latest.prev_close, 0) - 1.0) * 100.0 AS daily_return_pct,
              (latest.close / NULLIF(latest.close_5d_ago, 0) - 1.0) * 100.0 AS return_5d,
              (latest.close / NULLIF(latest.close_10d_ago, 0) - 1.0) * 100.0 AS return_10d,
              (latest.close / NULLIF(latest.close_20d_ago, 0) - 1.0) * 100.0 AS return_20d,
              latest.max_daily_gain_5d,
              latest.green_days_5d,
              delivery.delivery_pct
            FROM latest
            LEFT JOIN delivery ON delivery.symbol_id = latest.symbol_id
            WHERE latest.prev_close > 0
            """
            ,
            [resolved_as_of, resolved_as_of],
        ).fetchdf()
    if rows.empty:
        return _empty()
    rows.loc[:, "symbol_id"] = rows["symbol_id"].map(as_symbol)
    rows = _attach_rank(rows, ranked_signals)
    if "market_cap_cr" in rows.columns:
        market_cap_ok = rows["market_cap_cr"].isna() | (pd.to_numeric(rows["market_cap_cr"], errors="coerce") >= min_market_cap_cr)
    else:
        market_cap_ok = pd.Series(True, index=rows.index)
    daily_return = pd.to_numeric(rows["daily_return_pct"], errors="coerce")
    return_5d = pd.to_numeric(rows["return_5d"], errors="coerce")
    return_20d = pd.to_numeric(rows["return_20d"], errors="coerce")
    max_daily_gain_5d = pd.to_numeric(rows["max_daily_gain_5d"], errors="coerce")
    green_days_5d = pd.to_numeric(rows["green_days_5d"], errors="coerce")
    volume_ratio_20 = pd.to_numeric(rows["volume_ratio_20"], errors="coerce")
    daily_spike = (daily_return >= float(min_return_pct)) & (volume_ratio_20 >= float(min_volume_ratio))
    weekly_gainer = (
        bool(include_weekly)
        & (return_5d >= float(weekly_return_pct))
        & (daily_return < float(min_return_pct))
        & (max_daily_gain_5d < float(min_return_pct))
    )
    stealth_accumulation = (
        bool(include_stealth)
        & (daily_return < float(min_return_pct))
        & (return_5d >= float(stealth_5d_pct))
        & (return_20d >= float(stealth_20d_pct))
        & (green_days_5d >= int(min_green_days_5d))
    )
    mask = (daily_spike | weekly_gainer | stealth_accumulation) & market_cap_ok
    out = rows.loc[mask].copy()
    out.loc[:, "trigger_reason"] = "STEALTH_ACCUMULATION"
    out.loc[daily_spike.loc[out.index], "trigger_reason"] = "DAILY_GAINER"
    out.loc[weekly_gainer.loc[out.index] & ~daily_spike.loc[out.index], "trigger_reason"] = "WEEKLY_GAINER"
    priority = {"DAILY_GAINER": 0, "WEEKLY_GAINER": 1, "STEALTH_ACCUMULATION": 2}
    out.loc[:, "_trigger_priority"] = out["trigger_reason"].map(priority).fillna(99)
    return (
        out.sort_values(
            ["_trigger_priority", "daily_return_pct", "return_5d", "symbol_id"],
            ascending=[True, False, False, True],
            kind="stable",
        )
        .drop(columns=["_trigger_priority"])
        .reset_index(drop=True)
    )


def _attach_rank(gainers: pd.DataFrame, ranked: pd.DataFrame) -> pd.DataFrame:
    if ranked is None or ranked.empty:
        return gainers
    sym_col = symbol_column(ranked)
    if sym_col is None:
        return gainers
    rank = ranked.copy()
    rank.loc[:, "symbol_id"] = rank[sym_col].map(as_symbol)
    if "rank_position" not in rank.columns:
        if "rank" in rank.columns:
            rank.loc[:, "rank_position"] = pd.to_numeric(rank["rank"], errors="coerce")
        else:
            rank.loc[:, "rank_position"] = range(1, len(rank) + 1)
    desired = [
        "symbol_id",
        "composite_score",
        "rank_position",
        "relative_strength",
        "rel_strength",
        "trend_persistence",
        "volume_intensity",
        "proximity_to_highs",
        "delivery_pct",
        "sector_strength",
        "sector",
        "market_cap_cr",
    ]
    cols = [col for col in desired if col in rank.columns]
    merged = gainers.merge(rank[cols], on="symbol_id", how="left", suffixes=("", "_rank"))
    if "delivery_pct_rank" in merged.columns:
        merged.loc[:, "delivery_pct"] = merged["delivery_pct"].combine_first(merged["delivery_pct_rank"])
        merged = merged.drop(columns=["delivery_pct_rank"])
    return merged


def _empty() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol_id",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "prev_close",
            "volume",
            "avg_volume_5",
            "avg_volume_20",
            "volume_ratio_5d",
            "volume_ratio_20",
            "daily_return_pct",
            "return_5d",
            "return_10d",
            "return_20d",
            "max_daily_gain_5d",
            "green_days_5d",
            "delivery_pct",
            "trigger_reason",
        ]
    )
