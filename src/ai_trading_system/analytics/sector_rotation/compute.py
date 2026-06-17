"""Sector and stock RRG rotation computation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.analytics.sector_rotation.contracts import (
    ACCUMULATION_LABEL,
    DISTRIBUTION_LABEL,
    IMPROVING,
    LEADING,
    NEUTRAL_LABEL,
    SectorRotationResult,
    bucket_outperformance,
    classify_quadrant,
    score_quadrant,
)
from ai_trading_system.analytics.sector_rotation.custom_indices import (
    attach_metadata,
    build_benchmark_index,
    build_rotation_indices,
    build_sector_custom_indices,
    load_ohlcv_catalog,
    load_symbol_metadata,
)
from ai_trading_system.analytics.sector_rotation.delivery_signals import (
    compute_accumulation_distribution,
)


def compute_sector_rotation(
    *,
    ohlcv_db_path: str | Path,
    master_db_path: str | Path,
    run_date: str,
    ranked_df: pd.DataFrame | None = None,
    exchange: str = "NSE",
) -> SectorRotationResult:
    """Compute sector rotation artifacts from runtime DuckDB/metadata sources."""
    ohlcv = load_ohlcv_catalog(ohlcv_db_path, run_date=run_date, exchange=exchange)
    metadata = load_symbol_metadata(master_db_path, exchange=exchange)
    enriched = attach_metadata(ohlcv, metadata)
    sector_indices, sector_weighting_methods = build_rotation_indices(
        enriched,
        group_col="sector",
        group_type="sector",
    )
    industry_indices, industry_weighting_methods = build_sector_custom_indices(enriched)
    rotation_indices = pd.concat([sector_indices, industry_indices], ignore_index=True, sort=False)
    custom_indices = industry_indices
    benchmark, benchmark_name = build_benchmark_index(ohlcv, rotation_indices, metadata=metadata)
    accumulation = compute_accumulation_distribution(
        ohlcv_db_path,
        ohlcv,
        run_date=run_date,
        exchange=exchange,
    )

    sector_rotation_history = _compute_rotation_rrg_history(sector_indices, benchmark)
    sector_rotation = _compute_rotation_rrg_latest(sector_rotation_history)
    industry_rotation_history = _compute_rotation_rrg_history(industry_indices, benchmark)
    industry_rotation = _compute_rotation_rrg_latest(industry_rotation_history)
    stock_rotation = _compute_stock_rrg(
        enriched,
        industry_indices,
        sector_rotation,
        industry_rotation,
        accumulation,
        ranked_df=ranked_df,
    )
    payload = build_sector_rotation_payload(
        sector_rotation=sector_rotation,
        industry_rotation=industry_rotation,
        stock_rotation=stock_rotation,
        accumulation_distribution=accumulation,
        custom_indices=custom_indices,
        benchmark_name=benchmark_name,
        run_date=run_date,
    )
    metadata_payload: dict[str, Any] = {
        "benchmark_name": benchmark_name,
        "sector_count": int(sector_rotation["rotation_group_name"].nunique()) if "rotation_group_name" in sector_rotation.columns else 0,
        "industry_count": int(industry_rotation["rotation_group_name"].nunique()) if "rotation_group_name" in industry_rotation.columns else 0,
        "stock_count": int(len(stock_rotation)),
        "accumulation_count": int((accumulation.get("delivery_signal") == ACCUMULATION_LABEL).sum()) if not accumulation.empty else 0,
        "distribution_count": int((accumulation.get("delivery_signal") == DISTRIBUTION_LABEL).sum()) if not accumulation.empty else 0,
        "weighting_methods": {
            "sector": sector_weighting_methods,
            "industry": industry_weighting_methods,
        },
    }
    return SectorRotationResult(
        sector_rotation=sector_rotation,
        sector_rotation_history=sector_rotation_history,
        industry_rotation=industry_rotation,
        industry_rotation_history=industry_rotation_history,
        stock_rotation=stock_rotation,
        accumulation_distribution=accumulation,
        rotation_indices=rotation_indices,
        sector_custom_indices=custom_indices,
        payload=payload,
        metadata=metadata_payload,
    )


def build_sector_rotation_payload(
    *,
    sector_rotation: pd.DataFrame,
    industry_rotation: pd.DataFrame | None = None,
    stock_rotation: pd.DataFrame,
    accumulation_distribution: pd.DataFrame,
    custom_indices: pd.DataFrame,
    benchmark_name: str,
    run_date: str,
) -> dict[str, Any]:
    """Build compact frontend/operator payload."""
    sector_rotation = _ensure_frame(sector_rotation)
    industry_rotation = _ensure_frame(industry_rotation)
    stock_rotation = _ensure_frame(stock_rotation)
    accumulation_distribution = _ensure_frame(accumulation_distribution)

    def rotation_records(frame: pd.DataFrame, quadrant: str, limit: int = 10) -> list[dict[str, Any]]:
        if frame.empty:
            return []
        focused = frame.loc[frame["quadrant"] == quadrant].copy()
        sort_columns = [column for column in ("rs_ratio", "rs_momentum", "rotation_group_name") if column in focused.columns]
        ascending = [False, False, True][: len(sort_columns)]
        return _records(focused.sort_values(sort_columns, ascending=ascending, kind="stable") if sort_columns else focused, limit)

    accumulation = accumulation_distribution.loc[
        accumulation_distribution.get("delivery_signal", pd.Series([], dtype=str)) == ACCUMULATION_LABEL
    ].copy()
    distribution = accumulation_distribution.loc[
        accumulation_distribution.get("delivery_signal", pd.Series([], dtype=str)) == DISTRIBUTION_LABEL
    ].copy()
    if not accumulation.empty:
        accumulation = accumulation.sort_values("accumulation_score", ascending=False, kind="stable")
    if not distribution.empty:
        distribution = distribution.sort_values("accumulation_score", ascending=False, kind="stable")
    return {
        "run_date": run_date,
        "benchmark_name": benchmark_name,
        "top_leading": rotation_records(sector_rotation, LEADING),
        "top_improving": rotation_records(sector_rotation, IMPROVING),
        "weakening": rotation_records(sector_rotation, "Weakening"),
        "lagging": rotation_records(sector_rotation, "Lagging"),
        "industry_top_leading": rotation_records(industry_rotation, LEADING),
        "industry_top_improving": rotation_records(industry_rotation, IMPROVING),
        "industry_weakening": rotation_records(industry_rotation, "Weakening"),
        "industry_lagging": rotation_records(industry_rotation, "Lagging"),
        "accumulation": _records(accumulation, 20),
        "distribution": _records(distribution, 20),
        "watchlist_candidates": _records(
            stock_rotation.loc[stock_rotation.get("watchlist_candidate", pd.Series(False, index=stock_rotation.index)).astype(bool)]
            if not stock_rotation.empty
            else stock_rotation,
            25,
        ),
        "top_industry_watchlist_candidates": _records(
            stock_rotation.loc[stock_rotation.get("watchlist_candidate", pd.Series(False, index=stock_rotation.index)).astype(bool)]
            if not stock_rotation.empty
            else stock_rotation,
            25,
        ),
        "custom_indices_tail": _records(custom_indices.sort_values(["date", "rotation_group_name"], kind="stable").tail(120), 120),
    }


def _compute_rotation_rrg_history(rotation_indices: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "date",
        "rotation_group_type",
        "rotation_group_name",
        "parent_sector",
        "rotation_index",
        "benchmark_index",
        "rs_line",
        "rs_ratio",
        "rs_momentum",
        "quadrant",
        "return_5d",
        "return_20d",
        "return_60d",
        "benchmark_return_20d",
        "alpha_20d",
        "alpha_60d",
        "outperformance_bucket",
        "weighting_method",
        "constituent_count",
    ]
    if rotation_indices is None or rotation_indices.empty or benchmark is None or benchmark.empty:
        return pd.DataFrame(columns=columns)
    bench = benchmark.copy()
    bench = bench.assign(date=pd.to_datetime(bench["date"], errors="coerce").dt.normalize().astype("datetime64[ns]"))
    bench = bench.dropna(subset=["date"]).sort_values("date", kind="stable")
    data = rotation_indices.copy()
    if "rotation_group_name" not in data.columns and "industry" in data.columns:
        data.loc[:, "rotation_group_type"] = "industry"
        data.loc[:, "rotation_group_name"] = data["industry"]
        data.loc[:, "parent_sector"] = data.get("sector", data["industry"])
        data.loc[:, "rotation_index"] = data["sector_index"]
    data = data.assign(date=pd.to_datetime(data["date"], errors="coerce").dt.normalize().astype("datetime64[ns]"))
    data = data.dropna(subset=["date"]).sort_values(["rotation_group_type", "rotation_group_name", "date"], kind="stable")
    data = data.assign(date=pd.to_datetime(data["date"], errors="coerce").dt.normalize().astype("datetime64[ns]"))
    if not bench.empty:
        date_index = pd.DatetimeIndex(sorted(set(data["date"].dropna()) | set(bench["date"].dropna())))
        aligned_benchmark = (
            bench.drop_duplicates(subset=["date"], keep="last")
            .set_index("date")
            .reindex(date_index)
            .ffill()
            .bfill()
            .rename_axis("date")
            .reset_index()
        )
        aligned_benchmark = aligned_benchmark.assign(
            date=pd.to_datetime(aligned_benchmark["date"], errors="coerce").dt.normalize().astype("datetime64[ns]")
        )
    else:
        aligned_benchmark = bench
    data = data.merge(aligned_benchmark, on="date", how="left").sort_values(["rotation_group_type", "rotation_group_name", "date"], kind="stable")
    data.loc[:, "rs_line"] = data["rotation_index"] / data["benchmark_index"]
    grouped = data.groupby(["rotation_group_type", "rotation_group_name"], group_keys=False)
    data.loc[:, "rs_ratio"] = 100.0 * data["rs_line"] / grouped["rs_line"].rolling(63, min_periods=20).mean().reset_index(level=[0, 1], drop=True)
    data.loc[:, "rs_momentum"] = 100.0 * data["rs_ratio"] / grouped["rs_ratio"].rolling(20, min_periods=5).mean().reset_index(level=[0, 1], drop=True)
    data.loc[:, "return_5d"] = grouped["rotation_index"].pct_change(5)
    data.loc[:, "return_20d"] = grouped["rotation_index"].pct_change(20)
    data.loc[:, "return_60d"] = grouped["rotation_index"].pct_change(60)
    data.loc[:, "benchmark_return_20d"] = grouped["benchmark_index"].pct_change(20)
    data.loc[:, "alpha_20d"] = data["return_20d"] - data["benchmark_return_20d"]
    data.loc[:, "alpha_60d"] = data["return_60d"] - grouped["benchmark_index"].pct_change(60)
    data.loc[:, "quadrant"] = data.apply(lambda row: classify_quadrant(row["rs_ratio"], row["rs_momentum"]), axis=1)
    data.loc[:, "outperformance_bucket"] = data["alpha_20d"].map(bucket_outperformance)
    data.loc[:, "date"] = pd.to_datetime(data["date"], errors="coerce").dt.date.astype(str)
    if "industry" not in data.columns:
        data.loc[:, "industry"] = data["rotation_group_name"].where(data["rotation_group_type"].eq("industry"), data["rotation_group_name"])
    if "sector" not in data.columns:
        data.loc[:, "sector"] = data["parent_sector"].where(data["rotation_group_type"].eq("industry"), data["rotation_group_name"])
    data.loc[:, "sector_index"] = data["rotation_index"]
    columns_with_compat = columns + ["industry", "sector", "sector_index"]
    return data[columns_with_compat].sort_values(["rotation_group_type", "rotation_group_name", "date"], kind="stable").reset_index(drop=True)


def _compute_rotation_rrg_latest(history: pd.DataFrame) -> pd.DataFrame:
    if history is None or history.empty:
        return pd.DataFrame(columns=list(_compute_rotation_rrg_history(pd.DataFrame(), pd.DataFrame()).columns))
    latest = (
        history.dropna(subset=["rotation_group_name"])
        .sort_values(["rotation_group_type", "rotation_group_name", "date"], kind="stable")
        .drop_duplicates(subset=["rotation_group_type", "rotation_group_name"], keep="last")
        .copy()
    )
    return latest.sort_values(["quadrant", "rs_ratio", "rotation_group_name"], ascending=[True, False, True], kind="stable").reset_index(drop=True)


def _compute_stock_rrg(
    enriched_ohlcv: pd.DataFrame,
    industry_indices: pd.DataFrame,
    sector_rotation: pd.DataFrame,
    industry_rotation: pd.DataFrame,
    accumulation: pd.DataFrame,
    *,
    ranked_df: pd.DataFrame | None,
) -> pd.DataFrame:
    columns = [
        "symbol",
        "company_name",
        "sector",
        "industry",
        "market_cap",
        "close",
        "return_1d",
        "return_1w",
        "return_1m",
        "rs_ratio",
        "rs_momentum",
        "quadrant",
        "sector_quadrant",
        "industry_quadrant",
        "composite_score",
        "sector_rotation_score",
        "industry_rotation_score",
        "stock_rotation_score",
        "accumulation_score",
        "rotation_adjusted_score",
        "near_52w_high_pct",
        "delivery_signal",
        "watchlist_candidate",
    ]
    if enriched_ohlcv is None or enriched_ohlcv.empty or industry_indices is None or industry_indices.empty:
        return pd.DataFrame(columns=columns)
    group_column = "rotation_group_name" if "rotation_group_name" in industry_indices.columns else "industry"
    value_column = "rotation_index" if "rotation_index" in industry_indices.columns else "sector_index"
    industry_pivot = industry_indices.pivot_table(index="date", columns=group_column, values=value_column, aggfunc="last")
    industry_pivot.index = pd.to_datetime(industry_pivot.index, errors="coerce").normalize()
    industry_pivot = industry_pivot.loc[~industry_pivot.index.duplicated(keep="last")]
    data = enriched_ohlcv.copy()
    data.loc[:, "date"] = pd.to_datetime(data["date"], errors="coerce").dt.normalize()
    records = []
    sector_quadrants = (
        sector_rotation.set_index("rotation_group_name")["quadrant"].astype(str).to_dict()
        if sector_rotation is not None and not sector_rotation.empty and "rotation_group_name" in sector_rotation.columns
        else {}
    )
    industry_quadrants = (
        industry_rotation.set_index("rotation_group_name")["quadrant"].astype(str).to_dict()
        if industry_rotation is not None and not industry_rotation.empty and "rotation_group_name" in industry_rotation.columns
        else {}
    )
    for symbol, rows in data.groupby("symbol", dropna=False):
        rows = rows.sort_values("date", kind="stable").copy()
        sector = str(rows["sector"].dropna().iloc[-1] if "sector" in rows.columns and rows["sector"].notna().any() else "Other")
        industry = str(rows["industry"].dropna().iloc[-1] if rows["industry"].notna().any() else "Other")
        if industry not in industry_pivot.columns:
            continue
        industry_series = industry_pivot[industry].reindex(rows["date"]).ffill()
        stock_rs_line = rows["close"].to_numpy(dtype=float) / industry_series.to_numpy(dtype=float)
        stock_frame = rows.assign(stock_rs_line=stock_rs_line)
        stock_frame.loc[:, "rs_ratio"] = 100.0 * stock_frame["stock_rs_line"] / stock_frame["stock_rs_line"].rolling(63, min_periods=20).mean()
        stock_frame.loc[:, "rs_momentum"] = 100.0 * stock_frame["rs_ratio"] / stock_frame["rs_ratio"].rolling(20, min_periods=5).mean()
        stock_frame.loc[:, "return_1d"] = stock_frame["close"].pct_change(1)
        stock_frame.loc[:, "return_1w"] = stock_frame["close"].pct_change(5)
        stock_frame.loc[:, "return_1m"] = stock_frame["close"].pct_change(20)
        stock_frame.loc[:, "high_52w"] = stock_frame["close"].rolling(252, min_periods=20).max()
        latest = stock_frame.iloc[-1]
        high_52w = pd.to_numeric(pd.Series([latest.get("high_52w")]), errors="coerce").iloc[0]
        close = pd.to_numeric(pd.Series([latest.get("close")]), errors="coerce").iloc[0]
        near_high = ((high_52w - close) / high_52w * 100.0) if pd.notna(high_52w) and high_52w else pd.NA
        quadrant = classify_quadrant(latest.get("rs_ratio"), latest.get("rs_momentum"))
        records.append(
            {
                "symbol": str(symbol),
                "company_name": latest.get("company_name") or str(symbol),
                "sector": sector,
                "industry": industry,
                "market_cap": latest.get("market_cap"),
                "close": close,
                "return_1d": latest.get("return_1d"),
                "return_1w": latest.get("return_1w"),
                "return_1m": latest.get("return_1m"),
                "rs_ratio": latest.get("rs_ratio"),
                "rs_momentum": latest.get("rs_momentum"),
                "quadrant": quadrant,
                "sector_quadrant": sector_quadrants.get(sector, "Lagging"),
                "industry_quadrant": industry_quadrants.get(industry, "Lagging"),
                "near_52w_high_pct": near_high,
            }
        )
    output = pd.DataFrame.from_records(records)
    if output.empty:
        return pd.DataFrame(columns=columns)

    output = _attach_rank_overlay(output, ranked_df)
    if accumulation is not None and not accumulation.empty:
        delivery_cols = ["symbol", "delivery_signal", "accumulation_score"]
        output = output.merge(accumulation[delivery_cols], on="symbol", how="left")
    else:
        output.loc[:, "delivery_signal"] = NEUTRAL_LABEL
        output.loc[:, "accumulation_score"] = 50.0
    output.loc[:, "delivery_signal"] = output["delivery_signal"].fillna(NEUTRAL_LABEL)
    output.loc[:, "accumulation_score"] = pd.to_numeric(output["accumulation_score"], errors="coerce").fillna(50.0)
    output.loc[:, "sector_rotation_score"] = output["sector_quadrant"].map(score_quadrant).fillna(20.0)
    output.loc[:, "industry_rotation_score"] = output["industry_quadrant"].map(score_quadrant).fillna(20.0)
    output.loc[:, "stock_rotation_score"] = output["quadrant"].map(score_quadrant).fillna(20.0)
    output.loc[:, "composite_score"] = pd.to_numeric(output["composite_score"], errors="coerce").fillna(0.0)
    output.loc[:, "rotation_adjusted_score"] = (
        output["composite_score"] * 0.65
        + output["sector_rotation_score"] * 0.10
        + output["industry_rotation_score"] * 0.15
        + output["stock_rotation_score"] * 0.05
        + output["accumulation_score"] * 0.05
    ).round(4)
    output.loc[:, "watchlist_candidate"] = (
        output["sector_quadrant"].isin([LEADING, IMPROVING])
        & output["industry_quadrant"].isin([LEADING, IMPROVING])
        & output["quadrant"].isin([LEADING, IMPROVING])
        & (output["composite_score"] >= 70)
        & (pd.to_numeric(output["near_52w_high_pct"], errors="coerce") <= 15)
        & output["delivery_signal"].isin([ACCUMULATION_LABEL, NEUTRAL_LABEL])
    )
    for column in ("return_1d", "return_1w", "return_1m", "rs_ratio", "rs_momentum", "near_52w_high_pct"):
        output.loc[:, column] = pd.to_numeric(output[column], errors="coerce").round(4)
    return output[columns].sort_values(["watchlist_candidate", "rotation_adjusted_score", "symbol"], ascending=[False, False, True], kind="stable").reset_index(drop=True)


def _attach_rank_overlay(stock_rotation: pd.DataFrame, ranked_df: pd.DataFrame | None) -> pd.DataFrame:
    output = stock_rotation.copy()
    if ranked_df is None or ranked_df.empty:
        output.loc[:, "composite_score"] = 0.0
        return output
    ranked = ranked_df.copy()
    symbol_col = "symbol_id" if "symbol_id" in ranked.columns else "symbol" if "symbol" in ranked.columns else None
    if not symbol_col:
        output.loc[:, "composite_score"] = 0.0
        return output
    columns = [symbol_col]
    for optional in ("composite_score", "prox_high", "near_52w_high_pct"):
        if optional in ranked.columns:
            columns.append(optional)
    ranked = ranked[columns].rename(columns={symbol_col: "symbol", "prox_high": "near_52w_high_pct_rank"})
    output = output.merge(ranked.drop_duplicates(subset=["symbol"], keep="first"), on="symbol", how="left")
    if "near_52w_high_pct_rank" in output.columns:
        output.loc[:, "near_52w_high_pct"] = output["near_52w_high_pct"].combine_first(output["near_52w_high_pct_rank"])
        output = output.drop(columns=["near_52w_high_pct_rank"])
    if "composite_score" not in output.columns:
        output.loc[:, "composite_score"] = 0.0
    return output


def _records(frame: pd.DataFrame, limit: int) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    safe = frame.head(limit).copy()
    safe = safe.where(pd.notna(safe), None)
    return safe.to_dict(orient="records")


def _ensure_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    return frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
