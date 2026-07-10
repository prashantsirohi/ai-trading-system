"""Dataset assembly for single-symbol diagnostic reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.platform.db.paths import DataDomainPaths

from .loaders import (
    load_artifact_timeline,
    load_feature_history,
    load_ohlcv,
    load_weekly_stage_history,
    normalize_symbol,
)


@dataclass(frozen=True)
class SymbolReportData:
    """All source frames needed to render a single-symbol report."""

    symbol: str
    exchange: str
    from_date: date
    to_date: date
    price_features: pd.DataFrame
    stages: pd.DataFrame
    artifacts: pd.DataFrame
    diagnostics: pd.DataFrame


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _has_rejection(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    text = str(value).strip()
    return text not in {"", "[]", "nan", "None"}


def _attach_stage_asof(frame: pd.DataFrame, stages: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or stages.empty:
        return frame
    stage_cols = [
        column
        for column in [
            "week_end_date",
            "stage_label",
            "stage_confidence",
            "stage_transition",
            "bars_in_stage",
            "stage_entry_date",
        ]
        if column in stages.columns
    ]
    if not stage_cols:
        return frame
    left = frame.sort_values("timestamp").copy()
    right = stages[stage_cols].sort_values("week_end_date").copy()
    left = left.assign(timestamp=pd.to_datetime(left["timestamp"], errors="coerce").astype("datetime64[ns]"))
    right = right.assign(week_end_date=pd.to_datetime(right["week_end_date"], errors="coerce").astype("datetime64[ns]"))
    return pd.merge_asof(
        left,
        right,
        left_on="timestamp",
        right_on="week_end_date",
        direction="backward",
        suffixes=("", "_stage_snapshot"),
    )


def _diagnostic_label(row: pd.Series) -> str:
    if _coerce_bool(row.get("ranked_emitted")):
        if not _coerce_bool(row.get("eligible_rank")) or _has_rejection(row.get("rejection_reasons")):
            return "rejected"
        return "captured"
    stage = str(row.get("stage_label") or row.get("weekly_stage_label") or "").upper()
    if stage == "S2" or _coerce_bool(row.get("pattern_emitted")) or _coerce_bool(row.get("stock_scan_emitted")):
        return "not_emitted"
    return "observed"


def build_diagnostics(artifacts: pd.DataFrame, stages: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """Build artifact-date diagnostic events with price and stage context."""
    if artifacts.empty:
        return pd.DataFrame(columns=["timestamp", "diagnostic_status"])
    diagnostics = _attach_stage_asof(artifacts, stages)
    if not prices.empty:
        price_cols = [column for column in ["timestamp", "close", "high", "low"] if column in prices.columns]
        diagnostics = diagnostics.merge(prices[price_cols], on="timestamp", how="left")
    diagnostics.loc[:, "diagnostic_status"] = diagnostics.apply(_diagnostic_label, axis=1)
    return diagnostics


def build_symbol_report(
    paths: DataDomainPaths,
    *,
    symbol: str,
    exchange: str = "NSE",
    from_date: str | date,
    to_date: str | date,
) -> SymbolReportData:
    """Load and join all frames for a single-symbol report."""
    symbol_id = normalize_symbol(symbol)
    start = pd.Timestamp(from_date).date()
    end = pd.Timestamp(to_date).date()

    prices = load_ohlcv(
        paths.ohlcv_db_path,
        symbol=symbol_id,
        exchange=exchange,
        from_date=start,
        to_date=end,
    )
    if prices.empty:
        raise ValueError(f"No OHLCV rows found for {symbol_id}/{exchange} between {start} and {end}")

    features = load_feature_history(
        paths.feature_store_dir,
        symbol=symbol_id,
        exchange=exchange,
        from_date=start,
        to_date=end,
    )
    price_features = prices.merge(features, on=["symbol_id", "exchange", "timestamp"], how="left") if not features.empty else prices
    price_features = price_features.sort_values("timestamp").reset_index(drop=True)

    stages = load_weekly_stage_history(
        paths.ohlcv_db_path,
        symbol=symbol_id,
        from_date=start,
        to_date=end,
    )
    artifacts = load_artifact_timeline(
        paths.pipeline_runs_dir,
        symbol=symbol_id,
        from_date=start,
        to_date=end,
    )
    diagnostics = build_diagnostics(artifacts, stages, price_features)

    return SymbolReportData(
        symbol=symbol_id,
        exchange=exchange,
        from_date=start,
        to_date=end,
        price_features=price_features,
        stages=stages,
        artifacts=artifacts,
        diagnostics=diagnostics,
    )
