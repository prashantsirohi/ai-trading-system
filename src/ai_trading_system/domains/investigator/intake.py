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

    resolved_as_of = as_of or latest_trading_date(ohlcv_db_path)
    if not resolved_as_of:
        return _empty()
    with duckdb.connect(str(ohlcv_db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            WITH last_rows AS (
              SELECT
                symbol_id,
                CAST(timestamp AS DATE) AS trade_date,
                open,
                high,
                low,
                close,
                volume,
                ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) AS rn,
                AVG(volume) OVER (
                    PARTITION BY symbol_id
                    ORDER BY timestamp
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS avg_volume_20
              FROM _catalog
              WHERE exchange = 'NSE'
                AND COALESCE(is_benchmark, FALSE) = FALSE
                AND lower(COALESCE(instrument_type, 'equity')) IN ('equity', 'eq')
                AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
            ),
            latest AS (
              SELECT * FROM last_rows WHERE rn = 1
            ),
            prev AS (
              SELECT symbol_id, close AS prev_close
              FROM last_rows
              WHERE rn = 2
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
              prev.prev_close,
              latest.volume,
              latest.avg_volume_20,
              latest.volume / NULLIF(latest.avg_volume_20, 0) AS volume_ratio_20,
              (latest.close / NULLIF(prev.prev_close, 0) - 1.0) * 100.0 AS daily_return_pct,
              delivery.delivery_pct
            FROM latest
            JOIN prev ON prev.symbol_id = latest.symbol_id
            LEFT JOIN delivery ON delivery.symbol_id = latest.symbol_id
            WHERE prev.prev_close > 0
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
    mask = (
        (pd.to_numeric(rows["daily_return_pct"], errors="coerce") >= float(min_return_pct))
        & (pd.to_numeric(rows["volume_ratio_20"], errors="coerce") >= float(min_volume_ratio))
        & market_cap_ok
    )
    out = rows.loc[mask].copy()
    out.loc[:, "trigger_reason"] = "DAILY_GAINER"
    return out.sort_values(["daily_return_pct", "symbol_id"], ascending=[False, True], kind="stable").reset_index(drop=True)


def _attach_rank(gainers: pd.DataFrame, ranked: pd.DataFrame) -> pd.DataFrame:
    if ranked is None or ranked.empty:
        return gainers
    sym_col = symbol_column(ranked)
    if sym_col is None:
        return gainers
    rank = ranked.copy()
    rank.loc[:, "symbol_id"] = rank[sym_col].map(as_symbol)
    if "rank_position" not in rank.columns:
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
            "avg_volume_20",
            "volume_ratio_20",
            "daily_return_pct",
            "delivery_pct",
            "trigger_reason",
        ]
    )
