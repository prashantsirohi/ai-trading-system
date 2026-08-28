"""Daily gainer intake from trusted OHLCV catalog and rank artifacts."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.investigator.utils import as_symbol, symbol_column


DEFAULT_WEEKLY_RETURN_PCT = 5.0
WEEKLY_GAINER_THRESHOLD_COMPARISON = "strictly_greater"
WEEKLY_GAINER_COMPARISON_EPSILON = 1e-9
WEEKLY_GAINER_RECENT_DAILY_SPIKE_POLICY = "track"


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
    weekly_return_pct: float = DEFAULT_WEEKLY_RETURN_PCT,
    stealth_5d_pct: float = 3.0,
    stealth_20d_pct: float = 8.0,
    min_green_days_5d: int = 3,
    include_weekly: bool = True,
    include_stealth: bool = True,
    symbols: list[str] | None = None,
    require_trigger: bool = True,
    include_receipts: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    """Return latest NSE investigator triggers, enriched with rank fields when present."""

    resolved_as_of = as_of or latest_trading_date(ohlcv_db_path)
    if not resolved_as_of:
        empty = _empty()
        return (empty, _empty_receipts()) if include_receipts else empty
    with duckdb.connect(str(ohlcv_db_path), read_only=True) as conn:
        has_delivery = bool(
            conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_delivery'"
            ).fetchone()[0]
        )
        delivery_cte = (
            """
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
            """
            if has_delivery
            else """
            delivery AS (
              SELECT
                CAST(NULL AS VARCHAR) AS symbol_id,
                CAST(NULL AS DOUBLE) AS delivery_pct
              WHERE FALSE
            )
            """
        )
        params = [resolved_as_of, resolved_as_of] if has_delivery else [resolved_as_of]
        rows = conn.execute(
            f"""
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
            {delivery_cte}
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
            params,
        ).fetchdf()
    if rows.empty:
        empty = _empty()
        return (empty, _empty_receipts()) if include_receipts else empty
    rows.loc[:, "symbol_id"] = rows["symbol_id"].map(as_symbol)
    rows = _attach_rank(rows, ranked_signals)
    if symbols is not None:
        selected_symbols = {as_symbol(symbol) for symbol in symbols if as_symbol(symbol)}
        rows = rows.loc[rows["symbol_id"].isin(selected_symbols)].copy()
    if "market_cap_cr" in rows.columns:
        market_cap_ok = rows["market_cap_cr"].isna() | (pd.to_numeric(rows["market_cap_cr"], errors="coerce") >= min_market_cap_cr)
    else:
        market_cap_ok = pd.Series(True, index=rows.index)
    daily_return = pd.to_numeric(rows["daily_return_pct"], errors="coerce")
    return_5d = pd.to_numeric(rows["return_5d"], errors="coerce")
    return_20d = pd.to_numeric(rows["return_20d"], errors="coerce")
    green_days_5d = pd.to_numeric(rows["green_days_5d"], errors="coerce")
    volume_ratio_20 = pd.to_numeric(rows["volume_ratio_20"], errors="coerce")
    daily_spike = (daily_return >= float(min_return_pct)) & (volume_ratio_20 >= float(min_volume_ratio))
    weekly_gainer = (
        bool(include_weekly)
        & (
            return_5d
            > float(weekly_return_pct) + WEEKLY_GAINER_COMPARISON_EPSILON
        )
    )
    stealth_accumulation = (
        bool(include_stealth)
        & (daily_return < float(min_return_pct))
        & (return_5d >= float(stealth_5d_pct))
        & (return_20d >= float(stealth_20d_pct))
        & (green_days_5d >= int(min_green_days_5d))
    )
    trigger_mask = daily_spike | weekly_gainer | stealth_accumulation
    mask = (trigger_mask if require_trigger else pd.Series(True, index=rows.index)) & market_cap_ok
    selected_trigger = pd.Series("", index=rows.index, dtype=object)
    selected_trigger.loc[stealth_accumulation] = "STEALTH_ACCUMULATION"
    selected_trigger.loc[weekly_gainer] = "WEEKLY_GAINER"
    selected_trigger.loc[daily_spike] = "DAILY_GAINER"
    receipts = _build_intake_receipts(
        rows=rows,
        daily_spike=daily_spike,
        weekly_gainer=weekly_gainer,
        stealth_accumulation=stealth_accumulation,
        market_cap_ok=market_cap_ok,
        selected_trigger=selected_trigger,
        min_return_pct=float(min_return_pct),
        min_volume_ratio=float(min_volume_ratio),
        weekly_return_pct=float(weekly_return_pct),
        stealth_5d_pct=float(stealth_5d_pct),
        stealth_20d_pct=float(stealth_20d_pct),
        min_green_days_5d=int(min_green_days_5d),
    )
    out = rows.loc[mask].copy()
    if out.empty:
        empty = _empty()
        return (empty, receipts) if include_receipts else empty
    out.loc[:, "trigger_reason"] = (
        selected_trigger.loc[out.index] if require_trigger else pd.NA
    )
    priority = {"DAILY_GAINER": 0, "WEEKLY_GAINER": 1, "STEALTH_ACCUMULATION": 2}
    out.loc[:, "_trigger_priority"] = out["trigger_reason"].map(priority).fillna(99)
    result = (
        out.sort_values(
            ["_trigger_priority", "daily_return_pct", "return_5d", "symbol_id"],
            ascending=[True, False, False, True],
            kind="stable",
        )
        .drop(columns=["_trigger_priority"])
        .reset_index(drop=True)
    )
    return (result, receipts) if include_receipts else result


def load_investigator_snapshot(
    *,
    ohlcv_db_path: Path,
    ranked_signals: pd.DataFrame,
    symbols: list[str],
    as_of: str | None = None,
) -> pd.DataFrame:
    """Load fresh trusted market rows for already-admitted Investigator symbols."""

    if not symbols:
        return _empty()
    return load_investigator_intake(
        ohlcv_db_path=ohlcv_db_path,
        ranked_signals=ranked_signals,
        as_of=as_of,
        min_market_cap_cr=0.0,
        symbols=symbols,
        require_trigger=False,
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
        "sector_name",
        "market_cap_cr",
    ]
    cols = [col for col in desired if col in rank.columns]
    merged = gainers.merge(rank[cols], on="symbol_id", how="left", suffixes=("", "_rank"))
    if "delivery_pct_rank" in merged.columns:
        merged.loc[:, "delivery_pct"] = merged["delivery_pct"].combine_first(merged["delivery_pct_rank"])
        merged = merged.drop(columns=["delivery_pct_rank"])
    if "sector_name_rank" in merged.columns:
        if "sector_name" in merged.columns:
            merged.loc[:, "sector_name"] = _first_present(merged["sector_name"], merged["sector_name_rank"])
        else:
            merged.loc[:, "sector_name"] = merged["sector_name_rank"]
        merged = merged.drop(columns=["sector_name_rank"])
    if "sector" in merged.columns and "sector_name" in merged.columns:
        merged.loc[:, "sector"] = _first_present(merged["sector"], merged["sector_name"])
    elif "sector_name" in merged.columns:
        merged.loc[:, "sector"] = merged["sector_name"]
    return merged


def _first_present(primary: pd.Series, fallback: pd.Series) -> pd.Series:
    primary_clean = primary.mask(primary.astype(str).str.strip().eq(""))
    fallback_clean = fallback.mask(fallback.astype(str).str.strip().eq(""))
    return primary_clean.combine_first(fallback_clean)


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


def _empty_receipts() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol_id",
            "trade_date",
            "daily_return_pct",
            "return_5d",
            "return_20d",
            "max_daily_gain_5d",
            "green_days_5d",
            "volume_ratio_20",
            "market_cap_cr",
            "daily_gainer_eligible",
            "weekly_gainer_eligible",
            "stealth_accumulation_eligible",
            "tracked",
            "selected_trigger_reason",
            "decision_state",
            "reason_codes",
        ]
    )


def _build_intake_receipts(
    *,
    rows: pd.DataFrame,
    daily_spike: pd.Series,
    weekly_gainer: pd.Series,
    stealth_accumulation: pd.Series,
    market_cap_ok: pd.Series,
    selected_trigger: pd.Series,
    min_return_pct: float,
    min_volume_ratio: float,
    weekly_return_pct: float,
    stealth_5d_pct: float,
    stealth_20d_pct: float,
    min_green_days_5d: int,
) -> pd.DataFrame:
    """Return one deterministic inclusion/exclusion receipt per evaluated symbol."""

    tracked = (daily_spike | weekly_gainer | stealth_accumulation) & market_cap_ok
    receipts = pd.DataFrame(index=rows.index)
    for column in (
        "symbol_id",
        "trade_date",
        "daily_return_pct",
        "return_5d",
        "return_20d",
        "max_daily_gain_5d",
        "green_days_5d",
        "volume_ratio_20",
        "market_cap_cr",
    ):
        receipts.loc[:, column] = (
            rows[column] if column in rows.columns else pd.NA
        )
    receipts.loc[:, "daily_gainer_eligible"] = daily_spike.astype(bool)
    receipts.loc[:, "weekly_gainer_eligible"] = weekly_gainer.astype(bool)
    receipts.loc[:, "stealth_accumulation_eligible"] = (
        stealth_accumulation.astype(bool)
    )
    receipts.loc[:, "tracked"] = tracked.astype(bool)
    receipts.loc[:, "selected_trigger_reason"] = selected_trigger.where(tracked, "")
    receipts.loc[:, "decision_state"] = tracked.map(
        {True: "TRACKED", False: "EXCLUDED"}
    )
    receipts.loc[:, "reason_codes"] = [
        _intake_reason_codes(
            row=rows.loc[index],
            tracked=bool(tracked.loc[index]),
            market_cap_ok=bool(market_cap_ok.loc[index]),
            selected_trigger=str(selected_trigger.loc[index]),
            min_return_pct=min_return_pct,
            min_volume_ratio=min_volume_ratio,
            weekly_return_pct=weekly_return_pct,
            stealth_5d_pct=stealth_5d_pct,
            stealth_20d_pct=stealth_20d_pct,
            min_green_days_5d=min_green_days_5d,
        )
        for index in rows.index
    ]
    return receipts.sort_values("symbol_id", kind="stable").reset_index(drop=True)


def _intake_reason_codes(
    *,
    row: pd.Series,
    tracked: bool,
    market_cap_ok: bool,
    selected_trigger: str,
    min_return_pct: float,
    min_volume_ratio: float,
    weekly_return_pct: float,
    stealth_5d_pct: float,
    stealth_20d_pct: float,
    min_green_days_5d: int,
) -> str:
    if tracked:
        return f"TRACKED_{selected_trigger}"
    reasons: list[str] = []
    if not market_cap_ok:
        reasons.append("MARKET_CAP_BELOW_THRESHOLD")
    daily_return = pd.to_numeric(pd.Series([row.get("daily_return_pct")]), errors="coerce").iloc[0]
    volume_ratio = pd.to_numeric(pd.Series([row.get("volume_ratio_20")]), errors="coerce").iloc[0]
    return_5d = pd.to_numeric(pd.Series([row.get("return_5d")]), errors="coerce").iloc[0]
    return_20d = pd.to_numeric(pd.Series([row.get("return_20d")]), errors="coerce").iloc[0]
    green_days = pd.to_numeric(pd.Series([row.get("green_days_5d")]), errors="coerce").iloc[0]
    if pd.isna(daily_return):
        reasons.append("DAILY_RETURN_MISSING")
    elif daily_return < min_return_pct:
        reasons.append("DAILY_RETURN_BELOW_THRESHOLD")
    if pd.isna(volume_ratio):
        reasons.append("DAILY_VOLUME_RATIO_MISSING")
    elif volume_ratio < min_volume_ratio:
        reasons.append("DAILY_VOLUME_RATIO_BELOW_THRESHOLD")
    if pd.isna(return_5d):
        reasons.append("WEEKLY_RETURN_MISSING")
    elif return_5d <= weekly_return_pct + WEEKLY_GAINER_COMPARISON_EPSILON:
        reasons.append("WEEKLY_RETURN_NOT_ABOVE_THRESHOLD")
    if pd.isna(return_5d) or return_5d < stealth_5d_pct:
        reasons.append("STEALTH_5D_RETURN_BELOW_THRESHOLD")
    if pd.isna(return_20d) or return_20d < stealth_20d_pct:
        reasons.append("STEALTH_20D_RETURN_BELOW_THRESHOLD")
    if pd.isna(green_days) or green_days < min_green_days_5d:
        reasons.append("STEALTH_GREEN_DAYS_BELOW_THRESHOLD")
    return "|".join(dict.fromkeys(reasons)) or "NO_TRIGGER_MATCHED"
