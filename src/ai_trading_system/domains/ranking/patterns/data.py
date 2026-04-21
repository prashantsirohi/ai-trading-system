"""Data loading helpers for research and operational pattern scanning."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd

from analytics.alpha.dataset_builder import AlphaDatasetBuilder
from analytics.ml_engine import AlphaEngine
from ai_trading_system.platform.db.paths import ensure_domain_layout


def _normalize_symbols(symbols: Iterable[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    normalized = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    return normalized or None


def load_pattern_frame(
    project_root: str | Path,
    *,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
    symbols: Iterable[str] | None = None,
    data_domain: str = "research",
) -> pd.DataFrame:
    """Load OHLCV plus derived fields for pattern scanning in one data domain."""

    symbol_list = _normalize_symbols(symbols)
    root = Path(project_root)
    frame: pd.DataFrame
    if str(data_domain or "research").lower() == "operational":
        frame = _load_operational_pattern_frame(
            project_root=root,
            from_date=from_date,
            to_date=to_date,
            exchange=exchange,
            symbols=symbol_list,
            data_domain=data_domain,
        )
    else:
        engine = AlphaEngine(data_domain=data_domain)
        builder = AlphaDatasetBuilder(project_root=root, data_domain=data_domain)
        raw = engine.prepare_training_data(
            symbols=symbol_list,
            from_date=from_date,
            to_date=to_date,
            exchange=exchange,
            horizons=[5, 10, 20, 40],
        )
        frame = builder._add_price_structure_features(raw.copy())
    frame = frame.sort_values(["symbol_id", "timestamp"]).reset_index(drop=True)
    by_symbol = frame.groupby("symbol_id", group_keys=False)

    prev_close = by_symbol["close"].shift(1)
    true_range = pd.concat(
        [
            (frame["high"] - frame["low"]).abs(),
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1).rename("true_range")
    frame["true_range"] = true_range
    fallback_atr = frame.groupby("symbol_id")["true_range"].transform(
        lambda series: series.rolling(14, min_periods=1).mean()
    )
    if "atr_value" not in frame.columns or frame["atr_value"].fillna(0).eq(0).all():
        frame["atr_value"] = fallback_atr
    else:
        frame["atr_value"] = frame["atr_value"].fillna(fallback_atr)

    if "volume_ratio_20" not in frame.columns or frame["volume_ratio_20"].isna().all():
        volume_avg_20 = by_symbol["volume"].transform(
            lambda series: series.shift(1).rolling(20, min_periods=5).mean()
        )
        frame["volume_ratio_20"] = frame["volume"] / volume_avg_20.replace(0, np.nan)
    frame["volume_ratio_20"] = frame["volume_ratio_20"].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    frame["sma50_slope_20d_pct"] = by_symbol["sma_50"].transform(
        lambda series: series.pct_change(20, fill_method=None) * 100.0
    )
    frame["above_sma200"] = (frame["close"] > frame["sma_200"]).fillna(False)
    frame = frame.copy()
    frame.loc[:, "timestamp"] = pd.to_datetime(frame["timestamp"])
    frame.loc[:, "exchange"] = frame.get("exchange", exchange)

    keep_cols = [
        "symbol_id",
        "exchange",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "atr_value",
        "volume_ratio_20",
        "sma_20",
        "sma_50",
        "sma_200",
        "sma50_slope_20d_pct",
        "above_sma200",
        "return_5d",
        "return_10d",
        "return_20d",
        "return_40d",
    ]
    present = [column for column in keep_cols if column in frame.columns]
    return frame[present].copy()


def _load_operational_pattern_frame(
    *,
    project_root: Path,
    from_date: str,
    to_date: str,
    exchange: str,
    symbols: list[str] | None,
    data_domain: str,
) -> pd.DataFrame:
    """Load a lightweight operational frame without ML target preparation."""

    paths = ensure_domain_layout(project_root=project_root, data_domain=data_domain)
    conn = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    try:
        symbol_filter = ""
        params: list[object] = [str(exchange or "NSE"), str(from_date), str(to_date)]
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            symbol_filter = f" AND symbol_id IN ({placeholders})"
            params.extend(symbols)
        frame = conn.execute(
            f"""
            SELECT symbol_id, exchange, timestamp, open, high, low, close, volume
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              {symbol_filter}
            ORDER BY symbol_id, timestamp
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()

    if frame.empty:
        return frame

    frame = frame.copy()
    frame.loc[:, "timestamp"] = pd.to_datetime(frame["timestamp"])
    frame = frame.sort_values(["symbol_id", "timestamp"]).reset_index(drop=True)
    by_symbol = frame.groupby("symbol_id", group_keys=False)

    frame.loc[:, "sma_20"] = by_symbol["close"].transform(lambda series: series.rolling(20, min_periods=1).mean())
    frame.loc[:, "sma_50"] = by_symbol["close"].transform(lambda series: series.rolling(50, min_periods=1).mean())
    frame.loc[:, "sma_200"] = by_symbol["close"].transform(lambda series: series.rolling(200, min_periods=1).mean())
    for horizon in (5, 10, 20, 40):
        frame.loc[:, f"return_{horizon}d"] = by_symbol["close"].transform(
            lambda series, h=horizon: series.shift(-h) / series - 1.0
        )
    return frame


def load_pattern_research_frame(
    project_root: str | Path,
    *,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
    symbols: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Backward-compatible research loader."""

    return load_pattern_frame(
        project_root,
        from_date=from_date,
        to_date=to_date,
        exchange=exchange,
        symbols=symbols,
        data_domain="research",
    )
