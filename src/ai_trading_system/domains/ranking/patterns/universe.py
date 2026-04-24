"""Broad seed-universe helpers for operational pattern scanning."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.domains.ranking.patterns.cache import PatternCacheStore
from ai_trading_system.platform.logging.logger import logger


def apply_pattern_liquidity_gate(
    frame: pd.DataFrame,
    *,
    min_liquidity_score: float = 0.2,
    min_price: float = 20.0,
) -> pd.DataFrame:
    """Filter to feature-ready, liquid names suitable for broad pattern scans."""
    if frame.empty:
        return frame.copy()

    output = frame.copy()
    feature_ready = output.get("feature_ready", pd.Series(False, index=output.index)).fillna(False).astype(bool)
    liquidity_score = pd.to_numeric(output.get("liquidity_score"), errors="coerce").fillna(0.0)
    close = pd.to_numeric(output.get("close"), errors="coerce").fillna(0.0)
    mask = (
        feature_ready
        & (liquidity_score >= float(min_liquidity_score))
        & (close >= float(min_price))
    )
    return output.loc[mask].copy()


def apply_stage2_structural_gate(frame: pd.DataFrame) -> pd.DataFrame:
    """Apply a local structural Stage 2 proxy used only for phase-1 seed priority."""
    if frame.empty:
        return frame.copy()

    output = frame.copy()
    close = pd.to_numeric(output.get("close"), errors="coerce")
    sma_150 = pd.to_numeric(output.get("sma_150"), errors="coerce")
    sma_200 = pd.to_numeric(output.get("sma_200"), errors="coerce")
    sma200_slope = pd.to_numeric(output.get("sma200_slope_20d_pct"), errors="coerce")
    near_high = pd.to_numeric(output.get("near_52w_high_pct"), errors="coerce")

    structural_mask = (
        (close > sma_150)
        & (close > sma_200)
        & (sma_150 > sma_200)
        & (sma200_slope > 0.0)
        & (near_high <= 25.0)
    ).fillna(False)
    output.loc[:, "is_stage2_structural_proxy"] = structural_mask
    return output.loc[output["is_stage2_structural_proxy"]].copy()


def merge_cached_pattern_symbols(
    frame: pd.DataFrame,
    *,
    project_root: str | Path,
    signal_date: str,
    exchange: str = "NSE",
    data_domain: str = "operational",
) -> tuple[list[str], dict[str, Any]]:
    """Return cached pattern symbols ordered ahead of generic seed candidates."""
    metadata: dict[str, Any] = {
        "latest_cached_signal_date": None,
        "cached_symbol_count": 0,
    }
    if frame.empty or str(data_domain).lower() != "operational":
        return [], metadata

    try:
        store = PatternCacheStore(Path(project_root) / "data" / "control_plane.duckdb")
    except Exception as exc:
        logger.warning("Pattern seed cache unavailable: %s", exc)
        return [], metadata

    latest_cache_date = store.latest_cached_signal_date(as_of_date=signal_date, exchange=exchange)
    metadata["latest_cached_signal_date"] = latest_cache_date
    if latest_cache_date is None:
        return [], metadata

    cached = store.load_latest_active_signals_before(as_of_date=signal_date, exchange=exchange)
    if cached.empty or "symbol_id" not in cached.columns:
        return [], metadata

    live_symbols = {str(symbol).strip().upper() for symbol in frame["symbol_id"].astype(str)}
    cached = cached.copy()
    cached.loc[:, "symbol_id"] = cached["symbol_id"].astype(str).str.upper()
    cached = cached.loc[cached["symbol_id"].isin(live_symbols)].copy()
    if cached.empty:
        return [], metadata

    cached.loc[:, "pattern_score"] = pd.to_numeric(cached.get("pattern_score"), errors="coerce")
    cached.loc[:, "signal_date"] = pd.to_datetime(cached.get("signal_date"), errors="coerce")
    cached = (
        cached.sort_values(
            ["pattern_score", "signal_date", "symbol_id"],
            ascending=[False, False, True],
            na_position="last",
        )
        .drop_duplicates(subset=["symbol_id"], keep="first")
        .reset_index(drop=True)
    )
    symbols = cached["symbol_id"].astype(str).tolist()
    metadata["cached_symbol_count"] = len(symbols)
    return symbols, metadata


def build_pattern_seed_universe(
    *,
    project_root: str | Path,
    ohlcv_db_path: str | Path,
    signal_date: str,
    exchange: str = "NSE",
    data_domain: str = "operational",
    max_symbols: int = 400,
    min_liquidity_score: float = 0.2,
    unusual_mover_min_vol20_avg: float = 100_000.0,
) -> tuple[list[str], dict[str, Any]]:
    """Build a broad pattern-scan seed universe from the tradable universe."""
    snapshot = _load_latest_universe_snapshot(
        ohlcv_db_path=ohlcv_db_path,
        signal_date=signal_date,
        exchange=exchange,
    )
    if snapshot.empty:
        return [], {
            "seed_source_counts": {
                "cached": 0,
                "stage2_structural": 0,
                "unusual_movers": 0,
                "liquidity_remaining": 0,
            },
            "broad_universe_count": 0,
            "feature_ready_count": 0,
            "liquidity_pass_count": 0,
            "seed_symbol_count": 0,
            "latest_cached_signal_date": None,
            "pattern_seed_max_symbols": int(max_symbols or 0),
            "seed_symbols_digest": "empty",
        }

    liquid = apply_pattern_liquidity_gate(
        snapshot,
        min_liquidity_score=min_liquidity_score,
    )
    structural = apply_stage2_structural_gate(liquid)
    unusual = _identify_unusual_movers(
        snapshot,
        min_liquidity_score=min_liquidity_score,
        unusual_mover_min_vol20_avg=unusual_mover_min_vol20_avg,
    )
    cached_symbols, cache_metadata = merge_cached_pattern_symbols(
        snapshot,
        project_root=project_root,
        signal_date=signal_date,
        exchange=exchange,
        data_domain=data_domain,
    )

    structural_symbols = _ordered_symbols(
        structural,
        sort_columns=["near_52w_high_pct", "liquidity_score", "close", "symbol_id"],
        ascending=[True, False, False, True],
    )
    unusual_symbols = _ordered_symbols(
        unusual,
        sort_columns=["unusual_mover_score", "liquidity_score", "return_1d_abs_pct", "symbol_id"],
        ascending=[False, False, False, True],
    )
    remaining_liquid = liquid.loc[
        ~liquid["symbol_id"].astype(str).str.upper().isin(set(structural_symbols) | set(unusual_symbols))
    ].copy()
    remaining_symbols = _ordered_symbols(
        remaining_liquid,
        sort_columns=["liquidity_score", "close", "symbol_id"],
        ascending=[False, False, True],
    )

    ordered = _ordered_union(
        cached_symbols,
        structural_symbols,
        unusual_symbols,
        remaining_symbols,
    )
    seed_symbols = ordered[: int(max_symbols)] if max_symbols else ordered
    seed_digest = hashlib.sha256(
        json.dumps(seed_symbols, sort_keys=False).encode("utf-8")
    ).hexdigest() if seed_symbols else "empty"

    metadata = {
        "seed_source_counts": {
            "cached": len(cached_symbols),
            "stage2_structural": len(structural_symbols),
            "unusual_movers": len(unusual_symbols),
            "liquidity_remaining": len(remaining_symbols),
        },
        "seed_source_symbols": {
            "cached": list(cached_symbols),
            "stage2_structural": list(structural_symbols),
            "unusual_movers": list(unusual_symbols),
            "liquidity_remaining": list(remaining_symbols),
        },
        "broad_universe_count": int(len(snapshot)),
        "feature_ready_count": int(snapshot.get("feature_ready", pd.Series(dtype=bool)).fillna(False).sum()),
        "liquidity_pass_count": int(len(liquid)),
        "seed_symbol_count": int(len(seed_symbols)),
        "latest_cached_signal_date": cache_metadata.get("latest_cached_signal_date"),
        "pattern_seed_max_symbols": int(max_symbols or 0),
        "pattern_min_liquidity_score": float(min_liquidity_score),
        "pattern_unusual_mover_min_vol20_avg": float(unusual_mover_min_vol20_avg),
        "seed_symbols_digest": seed_digest,
    }
    return seed_symbols, metadata


def _load_latest_universe_snapshot(
    *,
    ohlcv_db_path: str | Path,
    signal_date: str,
    exchange: str,
    lookback_bars: int = 260,
) -> pd.DataFrame:
    conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        frame = conn.execute(
            """
            WITH scoped AS (
                SELECT
                    symbol_id,
                    exchange,
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp DESC
                    ) AS rn_desc
                FROM _catalog
                WHERE exchange = ?
                  AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
            )
            SELECT
                symbol_id,
                exchange,
                timestamp,
                open,
                high,
                low,
                close,
                volume
            FROM scoped
            WHERE rn_desc <= ?
            ORDER BY symbol_id, timestamp
            """,
            [str(exchange), str(signal_date), int(lookback_bars)],
        ).fetchdf()
    finally:
        conn.close()

    if frame.empty:
        return frame

    frame = frame.copy()
    frame.loc[:, "symbol_id"] = frame["symbol_id"].astype(str).str.upper()
    frame.loc[:, "timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume"]:
        frame.loc[:, column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_values(["symbol_id", "timestamp"]).reset_index(drop=True)

    grouped = frame.groupby("symbol_id", group_keys=False)
    frame.loc[:, "bar_count"] = grouped["timestamp"].transform("size")
    prev_close = grouped["close"].shift(1)
    true_range = pd.concat(
        [
            (frame["high"] - frame["low"]).abs(),
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    frame.loc[:, "true_range_today"] = pd.to_numeric(true_range, errors="coerce")
    frame.loc[:, "daily_range"] = (frame["high"] - frame["low"]).abs()

    frame.loc[:, "atr_20"] = grouped["true_range_today"].transform(
        lambda series: series.rolling(20, min_periods=5).mean()
    )
    frame.loc[:, "prev20_true_range_avg"] = grouped["true_range_today"].transform(
        lambda series: series.shift(1).rolling(20, min_periods=5).mean()
    )
    frame.loc[:, "range_expansion_20"] = (
        frame["true_range_today"] / frame["prev20_true_range_avg"].replace(0, np.nan)
    )

    frame.loc[:, "vol_20_avg"] = grouped["volume"].transform(
        lambda series: series.shift(1).rolling(20, min_periods=5).mean()
    )
    frame.loc[:, "vol_20_std"] = grouped["volume"].transform(
        lambda series: series.shift(1).rolling(20, min_periods=5).std(ddof=0)
    )
    frame.loc[:, "volume_ratio_20"] = frame["volume"] / frame["vol_20_avg"].replace(0, np.nan)
    frame.loc[:, "volume_zscore_20"] = (
        (frame["volume"] - frame["vol_20_avg"]) / frame["vol_20_std"].replace(0, np.nan)
    )
    frame.loc[:, "return_1d_pct"] = grouped["close"].pct_change(fill_method=None) * 100.0

    frame.loc[:, "sma_150"] = grouped["close"].transform(
        lambda series: series.rolling(150, min_periods=100).mean()
    )
    frame.loc[:, "sma_200"] = grouped["close"].transform(
        lambda series: series.rolling(200, min_periods=100).mean()
    )
    sma200_prev20 = frame.groupby("symbol_id")["sma_200"].shift(20)
    frame.loc[:, "sma200_slope_20d_pct"] = ((frame["sma_200"] / sma200_prev20) - 1.0) * 100.0
    frame.loc[:, "high_252"] = grouped["high"].transform(
        lambda series: series.rolling(252, min_periods=20).max()
    )
    frame.loc[:, "near_52w_high_pct"] = (
        (1.0 - (frame["close"] / frame["high_252"].replace(0, np.nan))) * 100.0
    ).clip(lower=0.0, upper=100.0)

    latest = (
        frame.drop_duplicates(subset=["symbol_id"], keep="last")
        .copy()
        .reset_index(drop=True)
    )
    latest.loc[:, "feature_ready"] = latest["bar_count"] >= 50
    latest.loc[:, "turnover"] = latest["close"] * latest["volume"]
    latest.loc[:, "liquidity_score"] = latest["turnover"].rank(pct=True, method="average")
    latest.loc[:, "atr_20_pct"] = (
        latest["atr_20"] / latest["close"].replace(0, np.nan)
    ) * 100.0
    latest.loc[:, "return_1d_abs_pct"] = latest["return_1d_pct"].abs()
    return latest


def _identify_unusual_movers(
    frame: pd.DataFrame,
    *,
    min_liquidity_score: float,
    unusual_mover_min_vol20_avg: float,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    output = frame.copy()
    liquidity_score = pd.to_numeric(output.get("liquidity_score"), errors="coerce").fillna(0.0)
    close = pd.to_numeric(output.get("close"), errors="coerce").fillna(0.0)
    vol_20_avg = pd.to_numeric(output.get("vol_20_avg"), errors="coerce").fillna(0.0)
    quality_mask = (
        (liquidity_score >= float(min_liquidity_score))
        & (close >= 50.0)
        & (vol_20_avg >= float(unusual_mover_min_vol20_avg))
    )

    output.loc[:, "unusual_mover_score"] = 0
    output.loc[:, "is_unusual_mover"] = False
    if not quality_mask.any():
        return output.loc[output["is_unusual_mover"]].copy()

    atr_pct = pd.to_numeric(output.get("atr_20_pct"), errors="coerce")
    return_abs_pct = pd.to_numeric(output.get("return_1d_abs_pct"), errors="coerce").fillna(0.0)
    adaptive_price_threshold = pd.Series(1.5, index=output.index, dtype=float)
    adaptive_price_threshold.loc[atr_pct.notna()] = np.maximum(1.5, 1.2 * atr_pct.loc[atr_pct.notna()])
    output.loc[:, "unusual_mover_score"] += (
        quality_mask & (return_abs_pct >= adaptive_price_threshold)
    ).astype(int)

    volume_zscore = pd.to_numeric(output.get("volume_zscore_20"), errors="coerce")
    volume_ratio = pd.to_numeric(output.get("volume_ratio_20"), errors="coerce").fillna(0.0)
    strong_volume = quality_mask & (volume_zscore >= 2.0)
    fallback_volume = quality_mask & ~strong_volume & (volume_ratio >= 1.5)
    output.loc[strong_volume, "unusual_mover_score"] += 2
    output.loc[fallback_volume, "unusual_mover_score"] += 1

    true_range_today = pd.to_numeric(output.get("true_range_today"), errors="coerce")
    atr_20 = pd.to_numeric(output.get("atr_20"), errors="coerce")
    range_expansion = pd.to_numeric(output.get("range_expansion_20"), errors="coerce").fillna(0.0)
    has_atr = atr_20.notna() & (atr_20 > 0.0)
    atr_range = quality_mask & has_atr & (true_range_today >= (1.5 * atr_20))
    fallback_range = quality_mask & ~has_atr & (range_expansion >= 1.5)
    output.loc[atr_range | fallback_range, "unusual_mover_score"] += 1

    output.loc[:, "is_unusual_mover"] = output["unusual_mover_score"] >= 2
    return output.loc[output["is_unusual_mover"]].copy()


def _ordered_union(*groups: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for symbol in group:
            normalized = str(symbol).strip().upper()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def _ordered_symbols(
    frame: pd.DataFrame,
    *,
    sort_columns: list[str],
    ascending: list[bool],
) -> list[str]:
    if frame.empty or "symbol_id" not in frame.columns:
        return []
    sortable = frame.copy()
    present_pairs = [
        (column, asc)
        for column, asc in zip(sort_columns, ascending)
        if column in sortable.columns
    ]
    present_columns = [column for column, _ in present_pairs]
    if present_columns:
        present_ascending = [asc for _, asc in present_pairs]
        sortable = sortable.sort_values(
            present_columns,
            ascending=present_ascending,
            na_position="last",
        )
    return sortable["symbol_id"].astype(str).str.upper().tolist()
