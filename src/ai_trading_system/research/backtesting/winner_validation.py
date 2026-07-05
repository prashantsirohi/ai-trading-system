"""Winner-validation research report.

This module is intentionally read-only. It validates whether current ranking
factor choices would have surfaced historical yearly winners near their rally
lows, without changing operational ranking, execution, or publish behavior.
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from ai_trading_system.domains.ranking.contracts import DEFAULT_FACTOR_WEIGHTS
from ai_trading_system.analytics.patterns.contracts import PatternScanConfig
from ai_trading_system.analytics.patterns.evaluation import _scan_pattern_signals
from ai_trading_system.platform.db.paths import get_domain_paths


warnings.filterwarnings("ignore", category=FutureWarning, message="ChainedAssignmentError.*")

DEFAULT_YEARS = (2021, 2022, 2023, 2024, 2025, 2026)
BENCHMARK_SYMBOLS = {"NIFTY50", "NIFTY", "NIFTY 50", "NIFTYBANK", "BANKNIFTY"}
STATIC_REGIME_BY_YEAR = {
    2021: "bear_recovery",
    2022: "bear_recovery",
    2023: "bull",
    2024: "bull",
    2025: "bear_consolidation",
    2026: "bear_consolidation",
}
POSITIVE_PATTERN_FAMILIES = {
    "darvas_box",
    "flag",
    "pocket_pivot",
    "flat_base",
    "inside_week_breakout",
    "vcp",
    "high_tight_flag",
    "three_weeks_tight",
    "stage2_reclaim",
    "round_bottom",
    "double_bottom",
    "ipo_base",
}
FACTOR_COLUMNS = (
    "relative_strength",
    "volume_intensity_normalized",
    "trend_score",
    "momentum_acceleration",
    "proximity_highs",
    "delivery_pct",
    "above_200dma_pct",
)
FACTOR_TO_WEIGHT_KEY = {
    "relative_strength": "relative_strength",
    "volume_intensity_normalized": "volume_intensity",
    "trend_score": "trend_persistence",
    "momentum_acceleration": "momentum_acceleration",
    "proximity_highs": "proximity_highs",
    "delivery_pct": "delivery_pct",
    "above_200dma_pct": "above_200dma",
}
CAPTURE_THRESHOLDS = (20, 30, 50, 70, 80)


@dataclass(frozen=True)
class StudyWindow:
    year: int
    start: date
    end: date
    label: str


@dataclass(frozen=True)
class WinnerValidationConfig:
    years: tuple[int, ...] = DEFAULT_YEARS
    top_n: int = 25
    data_domain: str = "research"
    exchange: str = "NSE"
    output_dir: Path | None = None
    project_root: Path | str | None = None
    pattern_lookback_days: int = 430
    pattern_max_age_days: int = 90
    full_year_min_days: int = 180
    partial_year_min_days: int = 60
    show_progress: bool = False


def _year_window(con: duckdb.DuckDBPyConnection, year: int, exchange: str) -> StudyWindow:
    max_date = con.execute(
        "SELECT MAX(CAST(timestamp AS DATE)) FROM _catalog WHERE exchange = ?",
        [exchange],
    ).fetchone()[0]
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), max_date)
    label = f"{year} YTD through {end.isoformat()}" if year == max_date.year else str(year)
    return StudyWindow(year=year, start=start, end=end, label=label)


def _load_winners(
    con: duckdb.DuckDBPyConnection,
    window: StudyWindow,
    *,
    exchange: str,
    top_n: int,
    min_days: int,
) -> pd.DataFrame:
    winners = con.execute(
        """
        WITH daily AS (
            SELECT
                UPPER(symbol_id) AS symbol_id,
                exchange,
                CAST(timestamp AS DATE) AS trade_date,
                open,
                high,
                low,
                close,
                volume,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(symbol_id), exchange, CAST(timestamp AS DATE)
                    ORDER BY ingestion_ts DESC NULLS LAST, timestamp DESC
                ) AS rn
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
              AND close > 0
        ),
        clean AS (
            SELECT * EXCLUDE rn FROM daily WHERE rn = 1
        ),
        discontinuities AS (
            SELECT
                symbol_id,
                SUM(
                    CASE
                        WHEN prev_close IS NULL OR prev_close <= 0 THEN 0
                        WHEN DATE_DIFF('day', prev_trade_date, trade_date) > 7 THEN 0
                        WHEN close / prev_close >= 4.0 THEN 1
                        WHEN close / prev_close <= 0.25 THEN 1
                        ELSE 0
                    END
                ) AS discontinuity_count
            FROM (
                SELECT
                    symbol_id,
                    trade_date,
                    close,
                    LAG(trade_date) OVER (PARTITION BY symbol_id ORDER BY trade_date) AS prev_trade_date,
                    LAG(close) OVER (PARTITION BY symbol_id ORDER BY trade_date) AS prev_close
                FROM clean
            )
            GROUP BY 1
        ),
        endpoints AS (
            SELECT
                symbol_id,
                MIN(trade_date) AS start_date,
                MAX(trade_date) AS end_date,
                COUNT(*) AS trading_days
            FROM clean
            GROUP BY 1
            HAVING COUNT(*) >= ?
        ),
        endpoint_prices AS (
            SELECT
                e.symbol_id,
                e.start_date,
                e.end_date,
                e.trading_days,
                s.close AS start_close,
                n.close AS end_close,
                (n.close / s.close - 1.0) * 100.0 AS window_return_pct
            FROM endpoints e
            JOIN clean s ON s.symbol_id = e.symbol_id AND s.trade_date = e.start_date
            JOIN clean n ON n.symbol_id = e.symbol_id AND n.trade_date = e.end_date
        ),
        extremes AS (
            SELECT
                p.*,
                (SELECT trade_date FROM clean c WHERE c.symbol_id = p.symbol_id ORDER BY close ASC, trade_date ASC LIMIT 1) AS low_date,
                (SELECT close FROM clean c WHERE c.symbol_id = p.symbol_id ORDER BY close ASC, trade_date ASC LIMIT 1) AS low_price,
                (SELECT trade_date FROM clean c WHERE c.symbol_id = p.symbol_id ORDER BY close DESC, trade_date ASC LIMIT 1) AS high_date,
                (SELECT close FROM clean c WHERE c.symbol_id = p.symbol_id ORDER BY close DESC, trade_date ASC LIMIT 1) AS high_price
            FROM endpoint_prices p
        )
        SELECT
            e.*,
            (e.high_price / NULLIF(e.low_price, 0) - 1.0) * 100.0 AS low_to_high_rally_pct,
            DATE_DIFF('day', e.low_date, e.high_date) AS low_to_high_calendar_days
        FROM extremes e
        LEFT JOIN discontinuities d ON d.symbol_id = e.symbol_id
        WHERE COALESCE(d.discontinuity_count, 0) = 0
          AND e.symbol_id NOT IN ('NIFTY50', 'NIFTY', 'NIFTY 50', 'NIFTYBANK', 'BANKNIFTY')
        ORDER BY window_return_pct DESC, e.symbol_id ASC
        LIMIT ?
        """,
        [exchange, window.start, window.end, int(min_days), int(top_n)],
    ).fetchdf()
    return winners


def _load_panel(
    con: duckdb.DuckDBPyConnection,
    signal_date: date,
    *,
    exchange: str,
    lookback_days: int = 430,
) -> pd.DataFrame:
    start = signal_date - timedelta(days=lookback_days)
    bars = con.execute(
        """
        SELECT
            UPPER(symbol_id) AS symbol_id,
            exchange,
            CAST(timestamp AS DATE) AS trade_date,
            open,
            high,
            low,
            close,
            volume
        FROM _catalog
        WHERE exchange = ?
          AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
          AND close > 0
          AND symbol_id NOT IN ('NIFTY50', 'NIFTY', 'NIFTY 50', 'NIFTYBANK', 'BANKNIFTY')
        ORDER BY symbol_id, trade_date
        """,
        [exchange, start, signal_date],
    ).fetchdf()
    if bars.empty:
        return bars
    bars = bars.copy()
    bars.loc[:, "trade_date"] = pd.to_datetime(bars["trade_date"])
    return bars


def _adx_14(group: pd.DataFrame) -> float:
    if len(group) < 20:
        return np.nan
    high = group["high"].astype(float)
    low = group["low"].astype(float)
    close = group["close"].astype(float)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = pd.concat(
        [(high - low).abs(), (high - close.shift()).abs(), (low - close.shift()).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(14, min_periods=14).mean()
    plus_di = 100.0 * pd.Series(plus_dm, index=group.index).rolling(14, min_periods=14).mean() / atr
    minus_di = 100.0 * pd.Series(minus_dm, index=group.index).rolling(14, min_periods=14).mean() / atr
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100.0
    return float(dx.rolling(14, min_periods=5).mean().iloc[-1])


def _safe_return(close: pd.Series, n: int) -> float:
    if len(close) <= n:
        return np.nan
    prior = float(close.iloc[-n - 1])
    if prior <= 0:
        return np.nan
    return (float(close.iloc[-1]) / prior - 1.0) * 100.0


def _latest_factor_panel(
    con: duckdb.DuckDBPyConnection,
    signal_date: date,
    *,
    exchange: str,
    lookback_days: int,
) -> pd.DataFrame:
    bars = _load_panel(con, signal_date, exchange=exchange, lookback_days=lookback_days)
    if bars.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for symbol_id, group in bars.groupby("symbol_id", sort=False):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < 30:
            continue
        close = group["close"].astype(float)
        high = group["high"].astype(float)
        volume = group["volume"].astype(float)
        last_close = float(close.iloc[-1])
        vol20 = volume.shift(1).rolling(20, min_periods=5).mean().iloc[-1]
        vol_mean20 = volume.rolling(20, min_periods=10).mean().iloc[-1]
        vol_std20 = volume.rolling(20, min_periods=10).std().iloc[-1]
        vol_z20 = (float(volume.iloc[-1]) - vol_mean20) / vol_std20 if vol_std20 and vol_std20 > 0 else 0.0
        volume_ratio = float(volume.iloc[-1] / vol20) if vol20 and vol20 > 0 else 1.0
        z_component = min(max(1.0 + min(max(vol_z20, -2.0), 5.0) / 2.0, 0.0), 3.5)
        volume_intensity = min(max(volume_ratio, 0.0), 5.0) * 0.6 + z_component * 0.4
        sma20 = close.rolling(20, min_periods=10).mean().iloc[-1]
        sma50 = close.rolling(50, min_periods=30).mean().iloc[-1]
        sma200 = close.rolling(200, min_periods=150).mean().iloc[-1]
        adx14 = _adx_14(group)
        adx_score = min(max(0.0 if pd.isna(adx14) else adx14, 0.0), 50.0) / 50.0 * 100.0
        sma_alignment = (40.0 if last_close > sma20 else 0.0) + (60.0 if last_close > sma50 else 0.0)
        high_52w = float(high.tail(252).max()) if len(high) else np.nan
        rows.append(
            {
                "symbol_id": symbol_id,
                "close": last_close,
                "return_5": _safe_return(close, 5),
                "return_10": _safe_return(close, 10),
                "return_20": _safe_return(close, 20),
                "return_60": _safe_return(close, 60),
                "return_120": _safe_return(close, 120),
                "volume_ratio_20": volume_ratio,
                "volume_zscore_20": vol_z20,
                "volume_intensity_normalized": volume_intensity,
                "adx_14": adx14,
                "sma_20": sma20,
                "sma_50": sma50,
                "sma_200": sma200,
                "trend_score": adx_score * 0.7 + sma_alignment * 0.3,
                "momentum_acceleration": (
                    0.6 * (_safe_return(close, 5) - _safe_return(close, 20))
                    + 0.4 * (_safe_return(close, 10) - _safe_return(close, 20))
                ),
                "proximity_highs": (last_close / high_52w) * 100.0 if high_52w and high_52w > 0 else np.nan,
                "above_200dma_pct": ((last_close - sma200) / sma200) * 100.0 if sma200 and sma200 > 0 else 0.0,
                "high_52w": high_52w,
                "history_bars": len(group),
            }
        )
    panel = pd.DataFrame(rows)
    if panel.empty:
        return panel

    for col in ("return_20", "return_60", "return_120"):
        panel.loc[:, col] = pd.to_numeric(panel[col], errors="coerce").fillna(0.0)
    panel.loc[:, "relative_strength"] = (
        0.2 * panel["return_20"].rank(pct=True) * 100.0
        + 0.5 * panel["return_60"].rank(pct=True) * 100.0
        + 0.3 * panel["return_120"].rank(pct=True) * 100.0
    )

    try:
        delivery = con.execute(
            """
            SELECT UPPER(symbol_id) AS symbol_id, delivery_pct
            FROM _delivery
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) <= ?::DATE
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY UPPER(symbol_id)
                ORDER BY CAST(timestamp AS DATE) DESC
            ) = 1
            """,
            [exchange, signal_date],
        ).fetchdf()
    except duckdb.Error:
        delivery = pd.DataFrame()
    if not delivery.empty:
        panel = panel.merge(delivery, on="symbol_id", how="left")
    if "delivery_pct" not in panel.columns:
        panel.loc[:, "delivery_pct"] = np.nan
    panel.loc[:, "delivery_pct"] = pd.to_numeric(panel["delivery_pct"], errors="coerce")
    median_delivery = panel["delivery_pct"].dropna().median()
    panel.loc[:, "delivery_pct"] = panel["delivery_pct"].fillna(float(median_delivery if pd.notna(median_delivery) else 20.0))

    for factor in FACTOR_COLUMNS:
        panel.loc[:, f"{factor}_pctile"] = pd.to_numeric(panel[factor], errors="coerce").rank(pct=True) * 100.0
    panel.loc[:, "active_technical_proxy_score"] = (
        panel["relative_strength_pctile"] * DEFAULT_FACTOR_WEIGHTS["relative_strength"]
        + panel["trend_score_pctile"] * DEFAULT_FACTOR_WEIGHTS["trend_persistence"]
        + panel["proximity_highs_pctile"] * DEFAULT_FACTOR_WEIGHTS["proximity_highs"]
    )
    panel.loc[:, "dormant_equal_score"] = panel[[f"{f}_pctile" for f in FACTOR_COLUMNS]].mean(axis=1)
    return panel


def _load_symbol_pattern_frame(
    con: duckdb.DuckDBPyConnection,
    symbol_id: str,
    signal_date: date,
    *,
    exchange: str,
    lookback_days: int,
) -> pd.DataFrame:
    from_date = signal_date - timedelta(days=lookback_days)
    frame = con.execute(
        """
        SELECT
            UPPER(symbol_id) AS symbol_id,
            exchange,
            timestamp,
            open,
            high,
            low,
            close,
            volume
        FROM _catalog
        WHERE exchange = ?
          AND UPPER(symbol_id) = ?
          AND CAST(timestamp AS DATE) BETWEEN ?::DATE AND ?::DATE
          AND close > 0
        ORDER BY timestamp
        """,
        [exchange, symbol_id.upper(), from_date, signal_date],
    ).fetchdf()
    if frame.empty:
        return frame
    frame = frame.copy()
    frame.loc[:, "timestamp"] = pd.to_datetime(frame["timestamp"])
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    frame.loc[:, "sma_20"] = frame["close"].rolling(20, min_periods=1).mean()
    frame.loc[:, "sma_50"] = frame["close"].rolling(50, min_periods=1).mean()
    frame.loc[:, "sma_150"] = frame["close"].rolling(150, min_periods=50).mean()
    frame.loc[:, "sma_200"] = frame["close"].rolling(200, min_periods=50).mean()
    prev_close = frame["close"].shift(1)
    true_range = pd.concat(
        [
            (frame["high"] - frame["low"]).abs(),
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    frame.loc[:, "atr_value"] = true_range.rolling(14, min_periods=1).mean()
    vol_avg_20 = frame["volume"].shift(1).rolling(20, min_periods=5).mean()
    frame.loc[:, "volume_ratio_20"] = (frame["volume"] / vol_avg_20.replace(0, np.nan)).replace(
        [np.inf, -np.inf],
        np.nan,
    ).fillna(0.0)
    for window in (20, 50):
        mean = frame["volume"].rolling(window, min_periods=max(5, window // 2)).mean()
        std = frame["volume"].rolling(window, min_periods=max(5, window // 2)).std()
        frame.loc[:, f"volume_zscore_{window}"] = ((frame["volume"] - mean) / std.replace(0, np.nan)).fillna(0.0)
    for horizon in (5, 10, 20, 40):
        frame.loc[:, f"return_{horizon}d"] = frame["close"].shift(-horizon) / frame["close"] - 1.0
    frame.loc[:, "sma150_slope_20d_pct"] = frame["sma_150"].pct_change(20, fill_method=None) * 100.0
    frame.loc[:, "sma50_slope_20d_pct"] = frame["sma_50"].pct_change(20, fill_method=None) * 100.0
    frame.loc[:, "above_sma200"] = (frame["close"] > frame["sma_200"]).fillna(False)
    return frame


def _pattern_labels(
    con: duckdb.DuckDBPyConnection,
    symbol_id: str,
    signal_date: date,
    *,
    exchange: str,
    lookback_days: int,
    max_age_days: int,
) -> str:
    try:
        frame = _load_symbol_pattern_frame(
            con,
            symbol_id,
            signal_date,
            exchange=exchange,
            lookback_days=lookback_days,
        )
        if frame.empty:
            return ""
        config = PatternScanConfig(
            exchange=exchange,
            data_domain="research",
            symbols=(symbol_id,),
            recent_signal_max_age_bars=40,
            min_history_bars=80,
        )
        signals, _stats, _processed = _scan_pattern_signals(frame, config=config)
        if signals.empty:
            return ""
        signals = signals.copy()
        signals.loc[:, "signal_date"] = pd.to_datetime(signals["signal_date"], errors="coerce")
        cutoff = pd.Timestamp(signal_date) - pd.Timedelta(days=max_age_days)
        recent = signals.loc[signals["signal_date"].between(cutoff, pd.Timestamp(signal_date))]
        if recent.empty:
            return ""
        recent = recent.sort_values(["signal_date", "pattern_score"], ascending=[False, False]).head(5)
        return ";".join(
            f"{row.pattern_family}:{row.pattern_state}:{pd.Timestamp(row.signal_date).date().isoformat()}"
            for row in recent.itertuples(index=False)
        )
    except Exception as exc:
        return f"pattern_error:{type(exc).__name__}"


def _static_regime(year: int) -> str:
    return STATIC_REGIME_BY_YEAR.get(int(year), "unknown")


def _pattern_events_for_row(row: pd.Series | dict[str, Any]) -> list[dict[str, Any]]:
    signal_date = pd.Timestamp(row["signal_date"]).date()
    raw = str(row.get("patterns_near_low", "") or "")
    events: list[dict[str, Any]] = []
    for token in raw.split(";"):
        if not token or token.startswith("pattern_error"):
            continue
        parts = token.split(":")
        if len(parts) != 3:
            continue
        family, state, event_date_raw = parts
        try:
            event_date = pd.Timestamp(event_date_raw).date()
        except (TypeError, ValueError):
            continue
        age_days = (signal_date - event_date).days
        if age_days < 0:
            continue
        events.append(
            {
                "pattern_family": family,
                "pattern_state": state,
                "pattern_date": event_date.isoformat(),
                "pattern_age_days": age_days,
                "is_positive_pattern": family in POSITIVE_PATTERN_FAMILIES,
            }
        )
    return events


def _pattern_age_bucket(age_days: int) -> str:
    if age_days <= 5:
        return "0-5"
    if age_days <= 20:
        return "6-20"
    if age_days <= 40:
        return "21-40"
    if age_days <= 60:
        return "41-60"
    return ">60"


def _spearman(x: pd.Series, y: pd.Series) -> float:
    valid = pd.concat([x, y], axis=1).dropna()
    if len(valid) < 4:
        return np.nan
    if valid.iloc[:, 0].nunique(dropna=True) < 2 or valid.iloc[:, 1].nunique(dropna=True) < 2:
        return np.nan
    result = spearmanr(valid.iloc[:, 0], valid.iloc[:, 1])
    return float(result.correlation) if not pd.isna(result.correlation) else np.nan


def _summarize_factor(frame: pd.DataFrame, factor: str) -> dict[str, float]:
    pctile = pd.to_numeric(frame[f"{factor}_pctile"], errors="coerce")
    return {
        "winner_median": float(pd.to_numeric(frame[factor], errors="coerce").median()),
        "winner_pctile_median": float(pctile.median()),
        "top_quartile_hit_rate": float((pctile >= 75).mean() * 100.0),
        "ic_vs_rally": _spearman(pd.to_numeric(frame[factor], errors="coerce"), frame["low_to_high_rally_pct"]),
    }


def _factor_summary_by_group(frame: pd.DataFrame, group_col: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group_value, group in frame.groupby(group_col, dropna=False):
        for factor in FACTOR_COLUMNS:
            stats = _summarize_factor(group, factor)
            rows.append(
                {
                    group_col: group_value,
                    "factor": factor,
                    "active_proxy_weight": DEFAULT_FACTOR_WEIGHTS.get(FACTOR_TO_WEIGHT_KEY[factor], 0.0),
                    "live_ranker_weight": DEFAULT_FACTOR_WEIGHTS.get(FACTOR_TO_WEIGHT_KEY[factor], 0.0),
                    **stats,
                    "n": int(len(group)),
                }
            )
    return rows


def _active_proxy_distribution(frame: pd.DataFrame) -> dict[str, Any]:
    pctile = pd.to_numeric(frame["active_technical_proxy_pctile"], errors="coerce")
    return {
        "median": float(pctile.median()),
        "above_50_count": int((pctile >= 50).sum()),
        "above_50_rate": float((pctile >= 50).mean()),
        "above_70_count": int((pctile >= 70).sum()),
        "above_70_rate": float((pctile >= 70).mean()),
        "above_80_count": int((pctile >= 80).sum()),
        "above_80_rate": float((pctile >= 80).mean()),
    }


def _capture_rates(frame: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    pctile = pd.to_numeric(frame["active_technical_proxy_pctile"], errors="coerce")
    return {
        f"pctile_ge_{threshold}": {
            "threshold": threshold,
            "captured_count": int((pctile >= threshold).sum()),
            "capture_rate": float((pctile >= threshold).mean()),
        }
        for threshold in CAPTURE_THRESHOLDS
    }


def _rally_quartile_summary(frame: pd.DataFrame) -> dict[str, Any]:
    ranked = frame.copy()
    if len(ranked) < 4:
        bottom = ranked.nsmallest(1, "low_to_high_rally_pct")
        top = ranked.nlargest(1, "low_to_high_rally_pct")
        return {
            "bottom": {
                "count": int(len(bottom)),
                "median_rally_pct": float(bottom["low_to_high_rally_pct"].median()),
                "median_active_technical_proxy_pctile": float(bottom["active_technical_proxy_pctile"].median()),
            },
            "top": {
                "count": int(len(top)),
                "median_rally_pct": float(top["low_to_high_rally_pct"].median()),
                "median_active_technical_proxy_pctile": float(top["active_technical_proxy_pctile"].median()),
            },
        }
    ranked.loc[:, "rally_quartile"] = pd.qcut(
        ranked["low_to_high_rally_pct"].rank(method="first"),
        4,
        labels=("bottom", "q2", "q3", "top"),
    )
    out: dict[str, Any] = {}
    for label in ("bottom", "top"):
        sub = ranked[ranked["rally_quartile"].astype(str) == label]
        out[label] = {
            "count": int(len(sub)),
            "median_rally_pct": float(sub["low_to_high_rally_pct"].median()),
            "median_active_technical_proxy_pctile": float(sub["active_technical_proxy_pctile"].median()),
        }
    return out


def _worst_missed_examples(frame: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    missed = frame.loc[pd.to_numeric(frame["active_technical_proxy_pctile"], errors="coerce") < 20].copy()
    missed = missed.sort_values(["low_to_high_rally_pct", "active_technical_proxy_pctile"], ascending=[False, True])
    columns = [
        "year",
        "symbol_id",
        "signal_date",
        "low_to_high_rally_pct",
        "active_technical_proxy_pctile",
        "patterns_near_low",
    ]
    return missed[columns].head(limit).to_dict(orient="records")


def _pattern_summaries(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    event_rows: list[dict[str, Any]] = []
    positive_by_symbol = 0
    for row in frame.to_dict(orient="records"):
        events = _pattern_events_for_row(row)
        positive_events = [event for event in events if event["is_positive_pattern"]]
        if positive_events:
            positive_by_symbol += 1
        for event in events:
            event_rows.append(
                {
                    "year": row["year"],
                    "symbol_id": row["symbol_id"],
                    "signal_date": row["signal_date"],
                    **event,
                    "age_bucket": _pattern_age_bucket(int(event["pattern_age_days"])),
                }
            )
    events_df = pd.DataFrame(event_rows)
    if events_df.empty:
        mix = pd.DataFrame(columns=["pattern_family", "count"])
        timing = pd.DataFrame(columns=["age_bucket", "count", "share"])
    else:
        mix = (
            events_df.groupby("pattern_family")
            .size()
            .reset_index(name="count")
            .sort_values(["count", "pattern_family"], ascending=[False, True])
        )
        positive_events = events_df[events_df["is_positive_pattern"]].copy()
        if positive_events.empty:
            timing = pd.DataFrame(columns=["age_bucket", "count", "share"])
        else:
            timing = positive_events.groupby("age_bucket").size().reindex(["0-5", "6-20", "21-40", "41-60", ">60"], fill_value=0).reset_index(name="count")
            total = max(int(timing["count"].sum()), 1)
            timing.loc[:, "share"] = timing["count"] / total
    presence = {
        "positive_pattern_symbols": int(positive_by_symbol),
        "winner_count": int(len(frame)),
        "positive_pattern_presence_rate": float(positive_by_symbol / len(frame)) if len(frame) else 0.0,
        "head_shoulders_diagnostic_only": True,
    }
    return mix, timing, presence


def _build_summary_payload(
    frame: pd.DataFrame,
    *,
    windows: list[StudyWindow],
    factor_summary: pd.DataFrame,
    pattern_mix: pd.DataFrame,
    pattern_timing: pd.DataFrame,
    pattern_presence: dict[str, Any],
    config: WinnerValidationConfig,
    db_path: Path,
) -> dict[str, Any]:
    year_rows = []
    for window in windows:
        sub = frame[frame["year"] == window.year]
        if sub.empty:
            continue
        year_rows.append(
            {
                "year": window.year,
                "window_start": window.start.isoformat(),
                "window_end": window.end.isoformat(),
                "regime_bucket": _static_regime(window.year),
                "winners_analyzed": int(len(sub)),
                "median_window_return_pct": float(sub["window_return_pct"].median()),
                "median_low_to_high_rally_pct": float(sub["low_to_high_rally_pct"].median()),
                "median_active_technical_proxy_pctile": float(sub["active_technical_proxy_pctile"].median()),
            }
        )
    return {
        "status": "ok",
        "analysis": "winner_validation_report",
        "generated_at": datetime.now(UTC).isoformat(),
        "config": {
            **asdict(config),
            "output_dir": str(config.output_dir) if config.output_dir else None,
            "project_root": str(config.project_root) if config.project_root else None,
        },
        "data_source": str(db_path),
        "winner_count": int(len(frame)),
        "years": year_rows,
        "active_proxy_distribution": _active_proxy_distribution(frame),
        "capture_rates": _capture_rates(frame),
        "rally_quartiles": _rally_quartile_summary(frame),
        "worst_missed_examples": _worst_missed_examples(frame),
        "pattern_presence": pattern_presence,
        "top_pattern_families": pattern_mix.head(10).to_dict(orient="records"),
        "pattern_timing": pattern_timing.to_dict(orient="records"),
        "factor_summary": factor_summary.to_dict(orient="records"),
    }


def _write_markdown_report(summary: dict[str, Any], out_dir: Path) -> None:
    lines: list[str] = []
    lines.append("# Winner Validation Report")
    lines.append("")
    lines.append(
        "Signal date is each winner's yearly low, so this is an oracle-style diagnostic for early discovery, not a production trading signal."
    )
    lines.append("")
    dist = summary["active_proxy_distribution"]
    lines.append("## Core Diagnosis")
    lines.append("")
    lines.append(
        "The active technical proxy behaves like a late-stage breakout / strength confirmation score, not an early multi-bagger discovery score."
    )
    lines.append("")
    lines.append("| Metric | Result |")
    lines.append("|---|---:|")
    lines.append(f"| Winners analyzed | {summary['winner_count']} |")
    lines.append(f"| Median active proxy percentile | {dist['median']:.1f} |")
    lines.append(f"| Winners above 50th percentile | {dist['above_50_count']} ({dist['above_50_rate']:.1%}) |")
    lines.append(f"| Winners above 70th percentile | {dist['above_70_count']} ({dist['above_70_rate']:.1%}) |")
    lines.append(f"| Winners above 80th percentile | {dist['above_80_count']} ({dist['above_80_rate']:.1%}) |")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    lines.append("| Year | Regime | Window | Winners | Median return | Median low-to-high rally | Median active proxy pctile |")
    lines.append("|---|---|---|---:|---:|---:|---:|")
    for row in summary["years"]:
        lines.append(
            f"| {row['year']} | {row['regime_bucket']} | {row['window_start']} to {row['window_end']} | "
            f"{row['winners_analyzed']} | {row['median_window_return_pct']:.1f}% | "
            f"{row['median_low_to_high_rally_pct']:.1f}% | {row['median_active_technical_proxy_pctile']:.1f} |"
        )
    lines.append("")
    lines.append("## Factor Gaps")
    lines.append("")
    overall = [row for row in summary["factor_summary"] if row.get("scope") == "overall"]
    lines.append("| Factor | Active proxy weight | Live ranker weight | Winner pctile median | Top-quartile hit rate | IC vs rally |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for row in overall:
        lines.append(
            f"| {row['factor']} | {row['active_proxy_weight']:.2f} | {row.get('live_ranker_weight', 0.0):.2f} | "
            f"{row['winner_pctile_median']:.1f} | "
            f"{row['top_quartile_hit_rate']:.1f}% | {row['ic_vs_rally']:.2f} |"
        )
    lines.append("")
    lines.append("## Capture Rates")
    lines.append("")
    lines.append("| Active proxy percentile threshold | Captured winners | Capture rate |")
    lines.append("|---:|---:|---:|")
    for item in summary["capture_rates"].values():
        lines.append(f"| >= {item['threshold']} | {item['captured_count']} | {item['capture_rate']:.1%} |")
    lines.append("")
    lines.append("## Rally Quartiles")
    lines.append("")
    lines.append("| Rally quartile | Count | Median rally | Median active proxy pctile |")
    lines.append("|---|---:|---:|---:|")
    for label in ("bottom", "top"):
        row = summary["rally_quartiles"][label]
        lines.append(
            f"| {label} | {row['count']} | {row['median_rally_pct']:.1f}% | "
            f"{row['median_active_technical_proxy_pctile']:.1f} |"
        )
    lines.append("")
    lines.append("## Worst Missed Examples")
    lines.append("")
    lines.append("| Year | Symbol | Signal date | Low-to-high rally | Active proxy pctile | Pattern clue |")
    lines.append("|---|---|---|---:|---:|---|")
    for row in summary["worst_missed_examples"][:8]:
        clue = row.get("patterns_near_low") or ""
        lines.append(
            f"| {row['year']} | {row['symbol_id']} | {row['signal_date']} | "
            f"{row['low_to_high_rally_pct']:.1f}% | {row['active_technical_proxy_pctile']:.1f} | {clue} |"
        )
    lines.append("")
    lines.append("## Pattern Evidence")
    lines.append("")
    presence = summary["pattern_presence"]
    lines.append(
        f"Positive pattern presence, excluding `head_shoulders`: "
        f"{presence['positive_pattern_symbols']} / {presence['winner_count']} "
        f"({presence['positive_pattern_presence_rate']:.1%})."
    )
    lines.append("")
    lines.append("| Pattern family | Count near winner low |")
    lines.append("|---|---:|")
    for row in summary["top_pattern_families"]:
        lines.append(f"| {row['pattern_family']} | {row['count']} |")
    lines.append("")
    lines.append("| Pattern age bucket | Count | Share |")
    lines.append("|---|---:|---:|")
    for row in summary["pattern_timing"]:
        lines.append(f"| {row['age_bucket']} | {row['count']} | {row['share']:.1%} |")
    lines.append("")
    lines.append("## Recommendations")
    lines.append("")
    lines.append("1. Keep the current ranker as a breakout / execution confirmation engine.")
    lines.append("2. Do not change production rank weights from this oracle report alone; forward backtest first.")
    lines.append("3. Use the evidence to design a separate early accumulation score in a later phase.")
    lines.append("4. Treat `head_shoulders` as diagnostic-only until bearish/inverse/failed-reclaim variants are separated.")
    lines.append("")
    lines.append(f"Generated from `{summary['data_source']}`.")
    (out_dir / "winner_study_summary.md").write_text("\n".join(lines))


def _write_outputs(
    frame: pd.DataFrame,
    *,
    windows: list[StudyWindow],
    out_dir: Path,
    config: WinnerValidationConfig,
    db_path: Path,
) -> dict[str, Any]:
    factor_rows = [{"scope": "overall", **row} for row in _factor_summary_by_group(frame.assign(scope="overall"), "scope")]
    factor_rows.extend({"scope": "year", **row} for row in _factor_summary_by_group(frame, "year"))
    factor_rows.extend({"scope": "regime_bucket", **row} for row in _factor_summary_by_group(frame, "regime_bucket"))
    factor_summary = pd.DataFrame(factor_rows)
    pattern_mix, pattern_timing, pattern_presence = _pattern_summaries(frame)
    summary = _build_summary_payload(
        frame,
        windows=windows,
        factor_summary=factor_summary,
        pattern_mix=pattern_mix,
        pattern_timing=pattern_timing,
        pattern_presence=pattern_presence,
        config=config,
        db_path=db_path,
    )

    frame.to_csv(out_dir / "per_winner_multi_year.csv", index=False)
    factor_summary.to_csv(out_dir / "factor_gap_summary.csv", index=False)
    pattern_mix.to_csv(out_dir / "pattern_mix.csv", index=False)
    pattern_timing.to_csv(out_dir / "pattern_timing_summary.csv", index=False)
    (out_dir / "winner_validation_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    _write_markdown_report(summary, out_dir)
    return summary


def _progress_bar(items, *, enabled: bool, desc: str, unit: str):
    if not enabled:
        return items
    try:
        from tqdm import tqdm

        return tqdm(items, desc=desc, unit=unit, leave=True)
    except Exception:
        return items


def _progress_note(enabled: bool, message: str) -> None:
    if enabled:
        print(message, file=sys.stderr, flush=True)


def run_winner_validation_report(config: WinnerValidationConfig | None = None) -> dict[str, Any]:
    active_config = config or WinnerValidationConfig()
    paths = get_domain_paths(project_root=active_config.project_root, data_domain=active_config.data_domain)
    out_dir = active_config.output_dir or (paths.reports_dir / "winner_analysis" / "multi_year")
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    try:
        windows = [_year_window(con, year, active_config.exchange) for year in active_config.years]
        panel_cache: dict[str, pd.DataFrame] = {}
        rows: list[dict[str, Any]] = []
        _progress_note(
            active_config.show_progress,
            f"Winner validation: {len(windows)} year window(s), top_n={active_config.top_n}, output={out_dir}",
        )
        for window in _progress_bar(windows, enabled=active_config.show_progress, desc="Years", unit="year"):
            min_days = active_config.partial_year_min_days if window.end < date(window.year, 12, 31) else active_config.full_year_min_days
            winners = _load_winners(
                con,
                window,
                exchange=active_config.exchange,
                top_n=active_config.top_n,
                min_days=min_days,
            )
            _progress_note(
                active_config.show_progress,
                f"Year {window.year}: loaded {len(winners)} winner candidate(s) from {window.start} to {window.end}",
            )
            winner_iter = _progress_bar(
                list(winners.itertuples(index=False)),
                enabled=active_config.show_progress,
                desc=f"{window.year} winners",
                unit="winner",
            )
            for winner in winner_iter:
                signal_date = pd.Timestamp(winner.low_date).date()
                key = f"{active_config.exchange}:{signal_date.isoformat()}"
                if key not in panel_cache:
                    panel_cache[key] = _latest_factor_panel(
                        con,
                        signal_date,
                        exchange=active_config.exchange,
                        lookback_days=active_config.pattern_lookback_days,
                    )
                panel = panel_cache[key]
                if panel.empty:
                    continue
                match = panel[panel["symbol_id"] == winner.symbol_id]
                if match.empty:
                    continue
                record = match.iloc[0].to_dict()
                active_score = float(record.get("active_technical_proxy_score", np.nan))
                dormant_score = float(record.get("dormant_equal_score", np.nan))
                record.update(
                    {
                        "year": window.year,
                        "regime_bucket": _static_regime(window.year),
                        "study_window": window.label,
                        "symbol_id": winner.symbol_id,
                        "start_date": winner.start_date,
                        "end_date": winner.end_date,
                        "signal_date": signal_date,
                        "high_date": winner.high_date,
                        "window_return_pct": float(winner.window_return_pct),
                        "low_to_high_rally_pct": float(winner.low_to_high_rally_pct),
                        "low_to_high_calendar_days": int(winner.low_to_high_calendar_days),
                        "active_technical_proxy_pctile": float(
                            (panel["active_technical_proxy_score"] <= active_score).mean() * 100.0
                        ),
                        "dormant_equal_pctile": float(
                            (panel["dormant_equal_score"] <= dormant_score).mean() * 100.0
                        ),
                        "patterns_near_low": _pattern_labels(
                            con,
                            str(winner.symbol_id),
                            signal_date,
                            exchange=active_config.exchange,
                            lookback_days=active_config.pattern_lookback_days,
                            max_age_days=active_config.pattern_max_age_days,
                        ),
                    }
                )
                rows.append(record)
    finally:
        con.close()

    all_rows = pd.DataFrame(rows)
    if all_rows.empty:
        raise RuntimeError("winner validation report produced no rows")
    ordered = [
        "year",
        "regime_bucket",
        "study_window",
        "symbol_id",
        "signal_date",
        "high_date",
        "window_return_pct",
        "low_to_high_rally_pct",
        "low_to_high_calendar_days",
        "active_technical_proxy_score",
        "active_technical_proxy_pctile",
        "dormant_equal_score",
        "dormant_equal_pctile",
        "patterns_near_low",
    ]
    factor_cols = [col for factor in FACTOR_COLUMNS for col in (factor, f"{factor}_pctile")]
    other_cols = [col for col in all_rows.columns if col not in set(ordered + factor_cols)]
    all_rows = all_rows[ordered + factor_cols + other_cols]
    summary = _write_outputs(
        all_rows,
        windows=windows,
        out_dir=out_dir,
        config=active_config,
        db_path=paths.ohlcv_db_path,
    )
    summary["artifact_dir"] = str(out_dir)
    _progress_note(
        active_config.show_progress,
        f"Winner validation complete: {len(all_rows)} analyzed winner row(s), artifacts written to {out_dir}",
    )
    return summary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", nargs="+", type=int, default=list(DEFAULT_YEARS))
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--data-domain", default="research")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--quiet", action="store_true", help="Disable progress output")
    args = parser.parse_args(argv)
    summary = run_winner_validation_report(
        WinnerValidationConfig(
            years=tuple(args.years),
            top_n=args.top_n,
            data_domain=args.data_domain,
            exchange=args.exchange,
            output_dir=args.output_dir,
            project_root=args.project_root,
            show_progress=not args.quiet,
        )
    )
    print(f"Wrote winner validation artifacts to {summary['artifact_dir']}")


if __name__ == "__main__":
    main()
