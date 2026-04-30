"""Research utilities for pattern setup-quality calibration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


DEFAULT_QUALITY_BINS = [0, 50, 65, 80, 90, 100]
DEFAULT_QUALITY_LABELS = ["0-50", "50-65", "65-80", "80-90", "90-100"]


def _close_pivot(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame()
    if {"date", "symbol_id", "close"}.issubset(prices.columns):
        frame = prices.copy(deep=True)
        frame.loc[:, "date"] = pd.to_datetime(frame["date"], errors="coerce")
        return frame.pivot_table(index="date", columns="symbol_id", values="close").sort_index()
    if {"timestamp", "symbol_id", "close"}.issubset(prices.columns):
        frame = prices.rename(columns={"timestamp": "date"}).copy(deep=True)
        frame.loc[:, "date"] = pd.to_datetime(frame["date"], errors="coerce")
        return frame.pivot_table(index="date", columns="symbol_id", values="close").sort_index()
    pivot = prices.copy()
    pivot.index = pd.to_datetime(pivot.index, errors="coerce")
    return pivot.sort_index()


def _event_date_column(signals: pd.DataFrame) -> str:
    for column in ["signal_date", "breakout_date", "date", "timestamp"]:
        if column in signals.columns:
            return column
    raise ValueError("signals must include one of signal_date, breakout_date, date, or timestamp")


def compute_pattern_setup_quality_calibration(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    horizons: Iterable[int] = (20, 60),
    quality_bins: list[float] | None = None,
    quality_labels: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute forward-return hit rates by pattern family and setup-quality bucket.

    This is intentionally a research hook: it reports calibration evidence but
    does not update production pattern scores.
    """
    if signals.empty:
        return pd.DataFrame(), pd.DataFrame()
    if "symbol_id" not in signals.columns or "setup_quality" not in signals.columns:
        raise ValueError("signals must include symbol_id and setup_quality")

    pivot = _close_pivot(prices)
    if pivot.empty:
        return pd.DataFrame(), pd.DataFrame()

    family_col = "pattern_family" if "pattern_family" in signals.columns else "pattern_type"
    if family_col not in signals.columns:
        raise ValueError("signals must include pattern_family or pattern_type")

    bins = quality_bins or DEFAULT_QUALITY_BINS
    labels = quality_labels or DEFAULT_QUALITY_LABELS
    date_col = _event_date_column(signals)
    events = signals.copy(deep=True)
    events.loc[:, "event_date"] = pd.to_datetime(events[date_col], errors="coerce")
    events.loc[:, "setup_quality"] = pd.to_numeric(events["setup_quality"], errors="coerce")
    events.loc[:, "setup_quality_bucket"] = pd.cut(
        events["setup_quality"].clip(lower=min(bins), upper=max(bins)),
        bins=bins,
        labels=labels,
        include_lowest=True,
        right=False,
    ).astype("object")
    events.loc[events["setup_quality"] >= max(bins), "setup_quality_bucket"] = labels[-1]

    detail_rows = []
    dates = pivot.index
    for _, row in events.dropna(subset=["event_date", "setup_quality_bucket"]).iterrows():
        symbol = str(row["symbol_id"])
        if symbol not in pivot.columns:
            continue
        entry_loc = dates.searchsorted(pd.Timestamp(row["event_date"]))
        if entry_loc >= len(dates):
            continue
        entry_price = pivot.iloc[entry_loc].get(symbol)
        if pd.isna(entry_price) or entry_price <= 0:
            continue
        detail = {
            "symbol_id": symbol,
            "pattern_family": str(row[family_col]),
            "setup_quality": float(row["setup_quality"]),
            "setup_quality_bucket": str(row["setup_quality_bucket"]),
            "event_date": dates[entry_loc].date().isoformat(),
        }
        for horizon in horizons:
            exit_loc = entry_loc + int(horizon)
            if exit_loc >= len(dates):
                detail[f"return_{int(horizon)}d"] = np.nan
                detail[f"hit_{int(horizon)}d"] = np.nan
                continue
            exit_price = pivot.iloc[exit_loc].get(symbol)
            ret = (
                float(exit_price / entry_price - 1.0)
                if pd.notna(exit_price) and exit_price > 0
                else np.nan
            )
            detail[f"return_{int(horizon)}d"] = ret
            detail[f"hit_{int(horizon)}d"] = bool(ret > 0) if pd.notna(ret) else np.nan
        detail_rows.append(detail)

    detail_df = pd.DataFrame(detail_rows)
    if detail_df.empty:
        return detail_df, pd.DataFrame()

    summary_rows = []
    group_cols = ["pattern_family", "setup_quality_bucket"]
    for keys, group in detail_df.groupby(group_cols, dropna=False, sort=True):
        family, bucket = keys
        row = {
            "pattern_family": family,
            "setup_quality_bucket": bucket,
            "signals": int(len(group)),
            "avg_setup_quality": round(float(group["setup_quality"].mean()), 2),
        }
        for horizon in horizons:
            ret_col = f"return_{int(horizon)}d"
            returns = pd.to_numeric(group[ret_col], errors="coerce").dropna()
            row[f"observations_{int(horizon)}d"] = int(len(returns))
            row[f"hit_rate_{int(horizon)}d"] = (
                round(float((returns > 0).mean()), 4) if not returns.empty else np.nan
            )
            row[f"avg_return_{int(horizon)}d"] = (
                round(float(returns.mean()), 6) if not returns.empty else np.nan
            )
            row[f"median_return_{int(horizon)}d"] = (
                round(float(returns.median()), 6) if not returns.empty else np.nan
            )
        summary_rows.append(row)
    return detail_df, pd.DataFrame(summary_rows)


def write_pattern_setup_quality_calibration_report(
    detail: pd.DataFrame,
    summary: pd.DataFrame,
    output_dir: str | Path,
    *,
    stem: str = "pattern_setup_quality_calibration",
) -> dict[str, str]:
    """Write CSV and JSON calibration reports."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    detail_path = out / f"{stem}_detail.csv"
    summary_csv_path = out / f"{stem}_summary.csv"
    summary_json_path = out / f"{stem}_summary.json"
    detail.to_csv(detail_path, index=False)
    summary.to_csv(summary_csv_path, index=False)
    summary_json_path.write_text(
        json.dumps(summary.to_dict(orient="records"), indent=2, default=str),
        encoding="utf-8",
    )
    return {
        "detail_csv": str(detail_path),
        "summary_csv": str(summary_csv_path),
        "summary_json": str(summary_json_path),
    }
