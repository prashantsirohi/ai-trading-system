"""Point-in-time, read-only R0 replay for the four pattern scan lanes."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

import numpy as np
import pandas as pd

from ai_trading_system.analytics.patterns.contracts import PatternScanConfig
from ai_trading_system.analytics.patterns.detectors import (
    _build_head_shoulders_signal,
    _score_signal_rows,
    detect_3wt_signals,
    detect_ascending_base_signals,
    detect_ascending_triangle_signals,
    detect_cup_handle_signals,
    detect_darvas_box_signals,
    detect_double_bottom_signals,
    detect_flag_signals,
    detect_flat_base_signals,
    detect_head_shoulders_filter,
    detect_inside_day_signals,
    detect_inside_week_breakout_signals,
    detect_ipo_base_signals,
    detect_pocket_pivot_signals,
    detect_round_bottom_signals,
    detect_stage2_reclaim_signals,
    detect_symmetrical_triangle_signals,
    detect_vcp_signals,
)
from ai_trading_system.analytics.patterns.signal import find_local_extrema, kernel_smooth
from ai_trading_system.domains.features.indicators import add_stage2_features

from .policy import PATTERN_FAMILIES, R0Policy, default_r0_policy


CONTEXT_COLUMNS: tuple[str, ...] = (
    "symbol_id", "exchange", "as_of_date", "bar_count", "close",
    "liquidity_gate_passed", "liquidity_policy_version", "sma_50",
    "sma_150", "sma_200", "sma_200_slope", "distance_from_52w_high",
    "stage2_score", "stage2_label", "stage2_input_valid", "weekly_stage",
    "weekly_stage_as_of", "weekly_stage_age_trading_days",
    "weekly_stage_is_fresh", "structure_observation_id",
)


@dataclass(frozen=True)
class CalibrationResult:
    context: pd.DataFrame
    detector_invocations: pd.DataFrame
    signals: pd.DataFrame
    outcomes: pd.DataFrame
    controls: pd.DataFrame
    metrics: pd.DataFrame
    winner_recall: pd.DataFrame
    runtime_diagnostics: dict[str, Any]
    summary: dict[str, Any]
    policy: R0Policy
    source_hashes: dict[str, str]


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _frame_hash(frame: pd.DataFrame) -> str:
    if frame is None or frame.empty:
        return _sha256_bytes(b"")
    normalized = frame.copy()
    normalized = normalized.reindex(sorted(normalized.columns), axis=1)
    for column in normalized.columns:
        if pd.api.types.is_datetime64_any_dtype(normalized[column]):
            normalized.loc[:, column] = pd.to_datetime(normalized[column]).dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    normalized = normalized.sort_values(list(normalized.columns), kind="stable", na_position="last")
    return _sha256_bytes(normalized.to_csv(index=False, lineterminator="\n").encode("utf-8"))


def _normalize_market_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"symbol_id", "exchange", "timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"market frame is missing required columns: {sorted(missing)}")
    output = frame.copy()
    output.loc[:, "symbol_id"] = output["symbol_id"].astype(str).str.strip().str.upper()
    output.loc[:, "exchange"] = output["exchange"].astype(str).str.strip().str.upper()
    output.loc[:, "timestamp"] = pd.to_datetime(output["timestamp"], errors="raise")
    for column in ("open", "high", "low", "close", "volume"):
        output.loc[:, column] = pd.to_numeric(output[column], errors="coerce")
    if output.duplicated(["symbol_id", "exchange", "timestamp"]).any():
        raise ValueError("market frame contains duplicate symbol/exchange/timestamp rows")
    return output.sort_values(["symbol_id", "exchange", "timestamp"], kind="stable").reset_index(drop=True)


def _relative_strength_scores(groups: dict[tuple[str, str], pd.DataFrame]) -> dict[tuple[str, str], float]:
    rows: list[dict[str, Any]] = []
    for key, group in groups.items():
        closes = pd.to_numeric(group["close"], errors="coerce")
        record: dict[str, Any] = {"key": key}
        for horizon in (20, 60, 120):
            record[f"return_{horizon}"] = (
                float(closes.iloc[-1] / closes.iloc[-horizon - 1] - 1.0)
                if len(closes) > horizon and closes.iloc[-horizon - 1] > 0
                else 0.0
            )
        rows.append(record)
    cross = pd.DataFrame(rows)
    if cross.empty:
        return {}
    score = (
        cross["return_20"].rank(pct=True, method="average") * 20.0
        + cross["return_60"].rank(pct=True, method="average") * 50.0
        + cross["return_120"].rank(pct=True, method="average") * 30.0
    )
    return dict(zip(cross["key"], score.astype(float)))


def _enrich_symbol_frame(frame: pd.DataFrame, *, rel_strength_score: float) -> pd.DataFrame:
    output = frame.sort_values("timestamp", kind="stable").reset_index(drop=True).copy()
    close = pd.to_numeric(output["close"], errors="coerce")
    high = pd.to_numeric(output["high"], errors="coerce")
    volume = pd.to_numeric(output["volume"], errors="coerce")
    output.loc[:, "near_52w_high_pct"] = (
        (1.0 - close / high.rolling(252, min_periods=20).max().replace(0, np.nan)) * 100.0
    ).clip(0.0, 100.0)
    output.loc[:, "volume_ratio_20"] = volume / volume.shift(1).rolling(20, min_periods=10).mean().replace(0, np.nan)
    output.loc[:, "rel_strength_score"] = float(rel_strength_score)
    return add_stage2_features(output)


def _trading_day_age(exchange_dates: pd.DatetimeIndex, observed: Any, as_of: pd.Timestamp) -> int | None:
    if observed is None or pd.isna(observed):
        return None
    observed_ts = pd.Timestamp(observed).normalize()
    return int(((exchange_dates > observed_ts) & (exchange_dates <= as_of.normalize())).sum())


def _latest_weekly_rows(weekly_stage_frame: pd.DataFrame | None, *, as_of: pd.Timestamp) -> dict[str, dict[str, Any]]:
    if weekly_stage_frame is None or weekly_stage_frame.empty:
        return {}
    frame = weekly_stage_frame.copy()
    symbol_col = "symbol" if "symbol" in frame.columns else "symbol_id"
    date_col = "week_end_date" if "week_end_date" in frame.columns else "as_of_date"
    if symbol_col not in frame.columns or date_col not in frame.columns:
        raise ValueError("weekly-stage frame requires symbol and week_end_date columns")
    frame.loc[:, symbol_col] = frame[symbol_col].astype(str).str.upper()
    frame.loc[:, date_col] = pd.to_datetime(frame[date_col], errors="coerce")
    frame = frame.loc[frame[date_col].notna() & (frame[date_col] <= as_of)].copy()
    if frame.empty:
        return {}
    latest = frame.sort_values([symbol_col, date_col], kind="stable").drop_duplicates(symbol_col, keep="last")
    return {str(row[symbol_col]): row.to_dict() for _, row in latest.iterrows()}


def _active_excluded_symbols(exclusion_frame: pd.DataFrame | None, *, as_of: pd.Timestamp) -> set[str]:
    if exclusion_frame is None or exclusion_frame.empty:
        return set()
    required = {"symbol_id", "effective_from"}
    if not required.issubset(exclusion_frame.columns):
        raise ValueError("exclusion frame requires symbol_id and effective_from for point-in-time replay")
    frame = exclusion_frame.copy()
    frame.loc[:, "symbol_id"] = frame["symbol_id"].astype(str).str.strip().str.upper()
    frame.loc[:, "effective_from"] = pd.to_datetime(frame["effective_from"], errors="raise").dt.normalize()
    effective_to = (
        pd.to_datetime(frame["effective_to"], errors="coerce").dt.normalize()
        if "effective_to" in frame.columns
        else pd.Series(pd.NaT, index=frame.index)
    )
    active = (frame["effective_from"] <= as_of) & (effective_to.isna() | (effective_to >= as_of))
    return set(frame.loc[active, "symbol_id"])


def _stage1_metrics(frame: pd.DataFrame, policy: R0Policy) -> dict[str, Any]:
    cfg = policy.stage1
    close = pd.to_numeric(frame["close"], errors="coerce")
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    latest = frame.iloc[-1]
    lookback = frame.tail(cfg.base_lookback_bars)
    base_high = float(pd.to_numeric(lookback["high"], errors="coerce").max())
    base_low = float(pd.to_numeric(lookback["low"], errors="coerce").min())
    base_depth = (base_high - base_low) / base_high if base_high > 0 else np.nan
    current_range = (high - low).tail(cfg.contraction_window_bars).median()
    previous_range = (high - low).iloc[-2 * cfg.contraction_window_bars : -cfg.contraction_window_bars].median()
    current_volume = volume.tail(cfg.contraction_window_bars).median()
    previous_volume = volume.iloc[-2 * cfg.contraction_window_bars : -cfg.contraction_window_bars].median()
    rs_short = close.iloc[-1] / close.iloc[-cfg.rs_short_bars - 1] - 1.0 if len(close) > cfg.rs_short_bars else np.nan
    rs_long = close.iloc[-1] / close.iloc[-cfg.rs_long_bars - 1] - 1.0 if len(close) > cfg.rs_long_bars else np.nan
    sma150 = float(latest.get("sma_150", np.nan))
    sma200 = float(latest.get("sma_200", np.nan))
    sma150_slope = float(latest.get("sma150_slope_20d_pct", np.nan))
    sma200_slope = float(latest.get("sma200_slope_20d_pct", np.nan))
    latest_close = float(close.iloc[-1])
    checks = {
        "sma150_band": bool(np.isfinite(sma150) and abs(latest_close / sma150 - 1.0) <= cfg.sma150_band_pct),
        "sma150_flat": bool(np.isfinite(sma150_slope) and abs(sma150_slope) <= cfg.sma150_slope_abs_max_pct),
        "base_depth": bool(np.isfinite(base_depth) and base_depth <= cfg.max_base_depth_pct),
        "range_contraction": bool(np.isfinite(previous_range) and previous_range > 0 and current_range / previous_range <= cfg.max_range_contraction_ratio),
        "pivot_proximity": bool(base_high > 0 and max(0.0, base_high - latest_close) / base_high <= cfg.max_pivot_distance_pct),
        "rs_trend": bool(np.isfinite(rs_short) and np.isfinite(rs_long) and (rs_short - rs_long) * 100.0 >= cfg.min_rs_trend_delta_pct),
        "volume_dry_up": bool(np.isfinite(previous_volume) and previous_volume > 0 and current_volume / previous_volume <= cfg.max_volume_dry_up_ratio),
        "long_term_not_deteriorating": bool(
            np.isfinite(sma200) and np.isfinite(sma200_slope)
            and latest_close / sma200 >= cfg.min_close_to_sma200_ratio
            and sma200_slope >= cfg.min_sma200_slope_pct
        ),
    }
    return {
        "stage1_structure_checks_passed": all(checks.values()),
        "stage1_structure_checks": checks,
        "base_depth_pct": float(base_depth * 100.0) if np.isfinite(base_depth) else np.nan,
        "range_contraction_ratio": float(current_range / previous_range) if np.isfinite(previous_range) and previous_range > 0 else np.nan,
        "volume_dry_up_ratio": float(current_volume / previous_volume) if np.isfinite(previous_volume) and previous_volume > 0 else np.nan,
        "pivot_distance_pct": float(max(0.0, base_high - latest_close) / base_high * 100.0) if base_high > 0 else np.nan,
        "rs_trend_delta_pct": float((rs_short - rs_long) * 100.0) if np.isfinite(rs_short) and np.isfinite(rs_long) else np.nan,
    }


def build_point_in_time_context(
    market_frame: pd.DataFrame,
    *,
    as_of_date: str,
    weekly_stage_frame: pd.DataFrame | None = None,
    policy: R0Policy | None = None,
    exclusion_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Reconstruct structural inputs using no market or stage row after ``as_of``."""

    active = policy or default_r0_policy()
    active.validate()
    as_of = pd.Timestamp(as_of_date).normalize()
    market = _normalize_market_frame(market_frame)
    scoped = market.loc[market["timestamp"].dt.normalize() <= as_of].copy()
    if scoped.empty:
        return pd.DataFrame(columns=CONTEXT_COLUMNS)
    if scoped["timestamp"].max().normalize() > as_of:
        raise RuntimeError("point-in-time reconstruction admitted a future market row")
    groups = {
        (str(symbol), str(exchange)): group.copy()
        for (symbol, exchange), group in scoped.groupby(["symbol_id", "exchange"], sort=True)
        if str(symbol) != active.outcomes.benchmark_symbol
    }
    rs_scores = _relative_strength_scores(groups)
    exchange_dates = {
        exchange: pd.DatetimeIndex(sorted(frame["timestamp"].dt.normalize().unique()))
        for exchange, frame in scoped.groupby("exchange", sort=False)
    }
    weekly = _latest_weekly_rows(weekly_stage_frame, as_of=as_of)
    bad_symbols = _active_excluded_symbols(exclusion_frame, as_of=as_of)
    rows: list[dict[str, Any]] = []
    latest_turnovers: list[tuple[tuple[str, str], float]] = []
    enriched_groups: dict[tuple[str, str], pd.DataFrame] = {}
    for key, group in groups.items():
        enriched = _enrich_symbol_frame(group, rel_strength_score=rs_scores.get(key, 0.0))
        enriched_groups[key] = enriched
        latest_turnovers.append((key, float(enriched.iloc[-1]["close"] * enriched.iloc[-1]["volume"])))
    turnover_series = pd.Series({key: value for key, value in latest_turnovers}, dtype=float)
    liquidity_percentiles = turnover_series.rank(pct=True, method="average").to_dict()

    for (symbol, exchange), frame in enriched_groups.items():
        latest = frame.iloc[-1]
        bar_count = int(len(frame))
        sessions = exchange_dates.get(exchange, pd.DatetimeIndex([]))
        first_date = frame["timestamp"].iloc[0].normalize()
        expected = int(((sessions >= first_date) & (sessions <= as_of)).sum())
        observed_dates = int(frame["timestamp"].dt.normalize().nunique())
        continuity = observed_dates / expected if expected else 0.0
        ohlcv_valid = (
            frame[["open", "high", "low", "close", "volume"]].notna().all(axis=1)
            & (frame[["open", "high", "low", "close"]] > 0).all(axis=1)
            & (frame["volume"] >= 0)
            & (frame["high"] >= frame[["open", "close", "low"]].max(axis=1))
            & (frame["low"] <= frame[["open", "close", "high"]].min(axis=1))
        )
        valid_ratio = float(ohlcv_valid.mean()) if len(frame) else 0.0
        estimation = frame.tail(active.early_ipo_liquidity.min_estimation_sessions)
        median_turnover = float((estimation["close"] * estimation["volume"]).median())
        median_volume = float(estimation["volume"].median())
        latest_session = sessions.max() if len(sessions) else None
        observed_latest = latest_session is not None and latest["timestamp"].normalize() == latest_session
        early_checks = {
            "bar_band": active.early_ipo_liquidity.min_bars <= bar_count <= active.early_ipo_liquidity.max_bars,
            "missing_sessions": (1.0 - continuity) <= active.early_ipo_liquidity.max_missing_session_ratio,
            "median_turnover": median_turnover >= active.early_ipo_liquidity.min_median_turnover,
            "median_volume": median_volume >= active.early_ipo_liquidity.min_median_volume,
            "minimum_close": float(latest["close"]) >= active.early_ipo_liquidity.min_close,
            "estimation_sessions": len(estimation) >= active.early_ipo_liquidity.min_estimation_sessions,
            "exchange": exchange in active.early_ipo_liquidity.allowed_exchanges,
            "continuity": continuity >= active.early_ipo_liquidity.min_continuity_ratio,
            "ohlcv_quality": valid_ratio >= active.early_ipo_liquidity.min_valid_ohlcv_ratio,
            "latest_session": observed_latest or not active.early_ipo_liquidity.require_latest_session_observation,
            "no_exclusion": symbol not in bad_symbols,
        }
        early_pass = all(early_checks.values())
        standard_pass = (
            bar_count >= active.standard_liquidity.min_bars
            and float(latest["close"]) >= active.standard_liquidity.min_close
            and float(liquidity_percentiles.get((symbol, exchange), 0.0)) >= active.standard_liquidity.min_liquidity_percentile
            and symbol not in bad_symbols
        )
        weekly_row = weekly.get(symbol, {})
        weekly_date = weekly_row.get("week_end_date", weekly_row.get("as_of_date"))
        weekly_age = _trading_day_age(sessions, weekly_date, as_of)
        weekly_fresh = weekly_age is not None and weekly_age <= active.weekly_freshness.max_age_trading_days
        weekly_label = str(weekly_row.get("stage_label", weekly_row.get("weekly_stage", "")) or "").upper()
        stage2_valid = bool(
            bar_count >= active.stage2.min_complete_long_history_bars
            and bool(latest.get("is_stage2_structural", False))
            and pd.notna(latest.get("sma_150"))
            and pd.notna(latest.get("sma_200"))
            and pd.notna(latest.get("sma200_slope_20d_pct"))
        )
        stage1 = _stage1_metrics(frame, active)
        base_record: dict[str, Any] = {
            "symbol_id": symbol,
            "exchange": exchange,
            "as_of_date": as_of.date().isoformat(),
            "bar_count": bar_count,
            "close": float(latest["close"]),
            "liquidity_gate_passed": bool(early_pass if bar_count < 50 else standard_pass),
            "standard_liquidity_gate_passed": bool(standard_pass),
            "early_ipo_liquidity_gate_passed": bool(early_pass),
            "liquidity_policy_version": active.early_ipo_liquidity.version if bar_count < 50 else active.standard_liquidity.version,
            "liquidity_percentile": float(liquidity_percentiles.get((symbol, exchange), 0.0)),
            "median_turnover": median_turnover,
            "median_volume": median_volume,
            "continuity_ratio": continuity,
            "ohlcv_valid_ratio": valid_ratio,
            "early_ipo_gate_checks": _json(early_checks),
            "sma_50": float(latest.get("sma_50", np.nan)),
            "sma_150": float(latest.get("sma_150", np.nan)),
            "sma_200": float(latest.get("sma_200", np.nan)),
            "sma_200_slope": float(latest.get("sma200_slope_20d_pct", np.nan)),
            "distance_from_52w_high": float(latest.get("near_52w_high_pct", np.nan)),
            "stage2_score": float(latest.get("stage2_score", 0.0)),
            "stage2_label": str(latest.get("stage2_label", "non_stage2")),
            "stage2_input_valid": stage2_valid,
            "weekly_stage": weekly_label or None,
            "weekly_stage_as_of": pd.Timestamp(weekly_date).date().isoformat() if weekly_date is not None and not pd.isna(weekly_date) else None,
            "weekly_stage_age_trading_days": weekly_age,
            "weekly_stage_is_fresh": bool(weekly_fresh),
            **stage1,
            "market_regime": str(latest.get("market_regime", "unknown") or "unknown"),
        }
        identity_fields = {key: base_record.get(key) for key in CONTEXT_COLUMNS if key != "structure_observation_id"}
        base_record["structure_observation_id"] = _sha256_bytes(_json(identity_fields).encode("utf-8"))
        rows.append(base_record)
    return pd.DataFrame(rows).sort_values(["exchange", "symbol_id"], kind="stable").reset_index(drop=True)


def _history_band(bar_count: int) -> str:
    if 35 <= bar_count < 50:
        return "35_49"
    if 50 <= bar_count < 120:
        return "50_119"
    if 120 <= bar_count < 180:
        return "120_179"
    if bar_count >= 180:
        return "180_plus"
    return "under_35"


def _matrix_key(lane: str, bar_count: int) -> str | None:
    band = _history_band(bar_count)
    key = f"{lane}:{band}"
    return key if lane != "no_lane" else None


def classify_lanes(context: pd.DataFrame, *, policy: R0Policy | None = None) -> pd.DataFrame:
    """Assign exactly one lane per context row using frozen precedence."""

    active = policy or default_r0_policy()
    rows: list[dict[str, Any]] = []
    for _, source in context.iterrows():
        row = source.to_dict()
        bars = int(row.get("bar_count", 0) or 0)
        reasons: list[str] = []
        lane = "no_lane"
        if 35 <= bars < 50:
            if bool(row.get("early_ipo_liquidity_gate_passed", False)):
                lane = "ipo_early_base"
                reasons.append("AGE_35_49_EARLY_IPO_LIQUIDITY_PASS")
            else:
                reasons.append("EARLY_IPO_LIQUIDITY_FAIL")
        elif 50 <= bars < 180:
            if bool(row.get("standard_liquidity_gate_passed", False)):
                lane = "young_listing_base"
                reasons.append("AGE_50_179_STANDARD_LIQUIDITY_PASS")
            else:
                reasons.append("STANDARD_LIQUIDITY_FAIL")
        elif bars >= 180 and bool(row.get("standard_liquidity_gate_passed", False)):
            if (
                bool(row.get("stage2_input_valid", False))
                and float(row.get("stage2_score", 0.0) or 0.0) >= active.stage2.score_threshold
            ):
                lane = "stage2_continuation"
                reasons.extend(("MATURE_STAGE2_INPUTS_VALID", "STAGE2_SCORE_THRESHOLD_PASS"))
            elif (
                bool(row.get("weekly_stage_is_fresh", False))
                and str(row.get("weekly_stage") or "").upper() in active.weekly_freshness.allowed_stage1_labels
                and bool(row.get("stage1_structure_checks_passed", False))
            ):
                lane = "stage1_base"
                reasons.extend(("FRESH_WEEKLY_STAGE1", "STAGE1_STRUCTURE_PASS"))
            else:
                reasons.append("MATURE_STRUCTURE_NOT_ELIGIBLE")
        elif bars >= 180:
            reasons.append("STANDARD_LIQUIDITY_FAIL")
        else:
            reasons.append("INSUFFICIENT_HISTORY")
        key = _matrix_key(lane, bars)
        family_row = active.families.matrix.get(key, {}) if key else {}
        row.update(
            {
                "scan_lane_as_of": lane,
                "history_band": _history_band(bars),
                "lane_assignment_reason_codes": _json(reasons),
                "lane_policy_version": active.version,
                "pattern_family_policy_version": active.families.version,
                "enabled_pattern_families": _json([family for family in PATTERN_FAMILIES if family_row.get(family) == "allowed"]),
                "suppression_pattern_families": _json([family for family in PATTERN_FAMILIES if family_row.get(family) == "suppression_only"]),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


Detector = Callable[..., tuple[list[Any], Any]]
ProgressCallback = Callable[[dict[str, Any]], None]


def _detectors() -> dict[str, Detector]:
    return {
        "cup_handle": detect_cup_handle_signals,
        "round_bottom": detect_round_bottom_signals,
        "double_bottom": detect_double_bottom_signals,
        "flag": lambda *args, **kwargs: detect_flag_signals(*args, **kwargs, high_tight_only=False),
        "high_tight_flag": lambda *args, **kwargs: detect_flag_signals(*args, **kwargs, high_tight_only=True),
        "ascending_triangle": detect_ascending_triangle_signals,
        "symmetrical_triangle": detect_symmetrical_triangle_signals,
        "ascending_base": detect_ascending_base_signals,
        "vcp": detect_vcp_signals,
        "flat_base": detect_flat_base_signals,
        "stage2_reclaim": detect_stage2_reclaim_signals,
        "darvas_box": detect_darvas_box_signals,
        "pocket_pivot": detect_pocket_pivot_signals,
        "ipo_base": detect_ipo_base_signals,
        "inside_week_breakout": detect_inside_week_breakout_signals,
        "three_weeks_tight": detect_3wt_signals,
        "inside_day": detect_inside_day_signals,
    }


def _scan_one_lane_symbol(
    frame: pd.DataFrame,
    context: dict[str, Any],
    *,
    as_of_date: str,
    policy: R0Policy,
    config: PatternScanConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Process-pool-safe exact-family scan for one symbol/date."""

    signal_rows: list[dict[str, Any]] = []
    invocation_rows: list[dict[str, Any]] = []
    lane = str(context.get("scan_lane_as_of", "no_lane"))
    if lane == "no_lane" or frame.empty:
        return signal_rows, invocation_rows
    symbol = str(context["symbol_id"])
    exchange = str(context["exchange"])
    ordered = _enrich_symbol_frame(frame.tail(policy.reconstruction.history_lookback_bars), rel_strength_score=0.0)
    ordered.loc[:, "stage2_score"] = float(context.get("stage2_score", 0.0) or 0.0)
    ordered.loc[:, "stage2_label"] = str(context.get("stage2_label", "non_stage2"))
    smoothed = kernel_smooth(ordered["close"], bandwidth=config.bandwidth, method=config.smoothing_method)
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)
    key = _matrix_key(lane, int(context["bar_count"]))
    matrix_row = policy.families.matrix[key] if key else {}
    detector_map = _detectors()

    if matrix_row.get("head_shoulders") == "suppression_only":
        is_hs, neckline = detect_head_shoulders_filter(
            ordered, smoothed=smoothed, extrema=extrema, config=config,
        )
        invocation_rows.append({
            "as_of_date": as_of_date, "symbol_id": symbol, "exchange": exchange,
            "scan_lane": lane, "history_band": context["history_band"],
            "pattern_family": "head_shoulders", "disposition": "suppression_only",
            "candidate_count": int(bool(is_hs)), "confirmed_count": int(bool(is_hs)),
            "watchlist_count": 0,
        })
        if is_hs:
            signal = _build_head_shoulders_signal(ordered, neckline=neckline, config=config)
            if signal is not None:
                record = signal.to_record()
                record["pattern_score"] = 0.0
                signal_rows.append(record)
            return signal_rows, invocation_rows

    for family in PATTERN_FAMILIES:
        disposition = matrix_row.get(family, "excluded")
        if disposition != "allowed":
            continue
        detector = detector_map.get(family)
        if detector is None:
            raise RuntimeError(f"no detector registered for allowed family {family}")
        signals, stats = detector(ordered, smoothed=smoothed, extrema=extrema, config=config)
        invocation_rows.append({
            "as_of_date": as_of_date, "symbol_id": symbol, "exchange": exchange,
            "scan_lane": lane, "history_band": context["history_band"],
            "pattern_family": family, "disposition": disposition,
            "candidate_count": int(stats.candidate_count),
            "confirmed_count": int(stats.confirmed_count),
            "watchlist_count": int(stats.watchlist_count),
        })
        for signal in signals:
            if signal.pattern_family != family:
                raise RuntimeError(
                    f"detector dispatch mismatch: requested {family}, emitted {signal.pattern_family}"
                )
            record = signal.to_record()
            record.update({
                "exchange": exchange,
                "as_of_date": as_of_date,
                "scan_lane_at_detection": lane,
                "scan_lane_as_of": lane,
                "evidence_origin": "fresh" if str(signal.signal_date) == as_of_date else "carry_forward",
                "lane_assignment_reason_codes": context["lane_assignment_reason_codes"],
                "lane_policy_version": policy.version,
                "liquidity_policy_version": context["liquidity_policy_version"],
                "pattern_family_policy_version": policy.families.version,
                "stage2_score_at_detection": float(context.get("stage2_score", 0.0) or 0.0),
                "stage2_score_as_of": float(context.get("stage2_score", 0.0) or 0.0),
                "structure_observation_id": context["structure_observation_id"],
                "history_band": context["history_band"],
                "liquidity_percentile": float(context.get("liquidity_percentile", 0.0) or 0.0),
                "liquidity_cohort": int(min(10, max(1, np.ceil(float(context.get("liquidity_percentile", 0.0) or 0.0) * 10.0)))),
                "market_regime": str(context.get("market_regime", "unknown") or "unknown"),
            })
            signal_rows.append(record)
    return signal_rows, invocation_rows


def scan_lane_patterns(
    market_frame: pd.DataFrame,
    classified_context: pd.DataFrame,
    *,
    as_of_date: str,
    policy: R0Policy | None = None,
    scan_config: PatternScanConfig | None = None,
    workers: int = 1,
    progress_callback: ProgressCallback | None = None,
    progress_every: int = 25,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Invoke only policy-enabled detector families for each assigned lane."""

    active = policy or default_r0_policy()
    config = scan_config or PatternScanConfig(data_domain="research", recent_signal_max_age_bars=5)
    market = _normalize_market_frame(market_frame)
    as_of = pd.Timestamp(as_of_date).normalize()
    scoped = market.loc[market["timestamp"].dt.normalize() <= as_of].copy()
    signal_rows: list[dict[str, Any]] = []
    invocation_rows: list[dict[str, Any]] = []
    eligible = classified_context.loc[
        classified_context["scan_lane_as_of"].astype(str) != "no_lane"
    ].sort_values(["exchange", "symbol_id"], kind="stable")
    market_groups = {
        (str(symbol), str(exchange)): frame.tail(active.reconstruction.history_lookback_bars).copy()
        for (symbol, exchange), frame in scoped.groupby(["symbol_id", "exchange"], sort=False)
    }
    tasks = [
        (market_groups.get((str(row["symbol_id"]), str(row["exchange"])), pd.DataFrame()), row.to_dict())
        for _, row in eligible.iterrows()
    ]
    total = len(tasks)
    started = perf_counter()

    def collect(result: tuple[list[dict[str, Any]], list[dict[str, Any]]], completed: int) -> None:
        rows, calls = result
        signal_rows.extend(rows)
        invocation_rows.extend(calls)
        if progress_callback and (completed == 1 or completed == total or completed % max(1, progress_every) == 0):
            elapsed = perf_counter() - started
            rate = completed / elapsed if elapsed > 0 else 0.0
            progress_callback({
                "event": "scan_progress", "as_of_date": as_of.date().isoformat(),
                "completed_symbols": completed, "total_symbols": total,
                "elapsed_seconds": elapsed, "symbols_per_second": rate,
                "eta_seconds": (total - completed) / rate if rate > 0 else None,
                "signal_rows": len(signal_rows), "detector_invocations": len(invocation_rows),
            })

    worker_count = min(max(1, int(workers)), max(1, total))
    if worker_count == 1:
        for completed, (frame, context) in enumerate(tasks, start=1):
            collect(
                _scan_one_lane_symbol(
                    frame, context, as_of_date=as_of.date().isoformat(), policy=active, config=config,
                ),
                completed,
            )
    elif tasks:
        completed_count = 0
        executor: ProcessPoolExecutor | None = None
        futures = []
        try:
            executor = ProcessPoolExecutor(max_workers=worker_count)
            futures = [
                executor.submit(
                    _scan_one_lane_symbol,
                    frame,
                    context,
                    as_of_date=as_of.date().isoformat(),
                    policy=active,
                    config=config,
                )
                for frame, context in tasks
            ]
            for completed_count, future in enumerate(as_completed(futures), start=1):
                collect(future.result(), completed_count)
            executor.shutdown(wait=True)
        except KeyboardInterrupt:
            for future in futures:
                future.cancel()
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
            raise
        except (PermissionError, NotImplementedError, BlockingIOError, OSError) as exc:
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
            if completed_count:
                raise
            if progress_callback:
                progress_callback({
                    "event": "parallel_fallback", "as_of_date": as_of.date().isoformat(),
                    "workers": worker_count, "reason": f"{type(exc).__name__}: {exc}",
                })
            for completed, (frame, context) in enumerate(tasks, start=1):
                collect(
                    _scan_one_lane_symbol(
                        frame, context, as_of_date=as_of.date().isoformat(),
                        policy=active, config=config,
                    ),
                    completed,
                )
        except Exception:
            for future in futures:
                future.cancel()
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
            raise
    signals = pd.DataFrame(signal_rows)
    if not signals.empty:
        bullish = signals[signals["pattern_family"] != "head_shoulders"].copy()
        suppression = signals[signals["pattern_family"] == "head_shoulders"].copy()
        if not bullish.empty:
            bullish = _score_signal_rows(bullish)
        signals = pd.concat([bullish, suppression], ignore_index=True, sort=False)
        signals = signals.sort_values(["as_of_date", "symbol_id", "pattern_family", "signal_date"], kind="stable").reset_index(drop=True)
    invocations = pd.DataFrame(invocation_rows)
    if not invocations.empty:
        invocations = invocations.sort_values(["as_of_date", "symbol_id", "pattern_family"], kind="stable").reset_index(drop=True)
    return signals, invocations


def _forward_outcomes(signals: pd.DataFrame, market_frame: pd.DataFrame, policy: R0Policy) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame()
    market = _normalize_market_frame(market_frame)
    rows: list[dict[str, Any]] = []
    grouped = {(s, e): g.reset_index(drop=True) for (s, e), g in market.groupby(["symbol_id", "exchange"], sort=False)}
    for _, signal in signals.iterrows():
        group = grouped.get((str(signal["symbol_id"]), str(signal["exchange"])))
        if group is None:
            continue
        decision_date = pd.Timestamp(signal["as_of_date"]).normalize()
        future = group.loc[group["timestamp"].dt.normalize() > decision_date].reset_index(drop=True)
        benchmark = grouped.get((policy.outcomes.benchmark_symbol, str(signal["exchange"])))
        benchmark_as_of = (
            benchmark.loc[benchmark["timestamp"].dt.normalize() <= decision_date].reset_index(drop=True)
            if benchmark is not None else pd.DataFrame()
        )
        benchmark_future = (
            benchmark.loc[benchmark["timestamp"].dt.normalize() > decision_date].reset_index(drop=True)
            if benchmark is not None else pd.DataFrame()
        )
        entry = float(signal.get("breakout_level", np.nan))
        invalidation = float(signal.get("invalidation_price", np.nan))
        base = signal.to_dict()
        for horizon in policy.outcomes.horizons:
            complete = len(future) >= horizon
            window = future.head(horizon)
            as_of_close_rows = group.loc[group["timestamp"].dt.normalize() <= decision_date, "close"]
            start_close = float(as_of_close_rows.iloc[-1]) if not as_of_close_rows.empty else np.nan
            end_close = float(window["close"].iloc[-1]) if complete else np.nan
            ret = end_close / start_close - 1.0 if complete and start_close > 0 else np.nan
            benchmark_complete = len(benchmark_future) >= horizon and not benchmark_as_of.empty
            benchmark_return = (
                float(benchmark_future["close"].iloc[horizon - 1] / benchmark_as_of["close"].iloc[-1] - 1.0)
                if benchmark_complete and float(benchmark_as_of["close"].iloc[-1]) > 0 else np.nan
            )
            mfe = float(window["high"].max() / start_close - 1.0) if complete and start_close > 0 else np.nan
            mae = float(window["low"].min() / start_close - 1.0) if complete and start_close > 0 else np.nan
            confirmed = bool(not window.empty and np.isfinite(entry) and (window["high"] > entry * (1.0 + policy.outcomes.breakout_buffer_pct)).any())
            breakout_hits = window.index[window["high"] > entry * (1.0 + policy.outcomes.breakout_buffer_pct)].tolist() if not window.empty and np.isfinite(entry) else []
            failed = bool(complete and np.isfinite(invalidation) and end_close <= invalidation)
            rows.append({
                **base,
                "horizon_sessions": int(horizon),
                "outcome_window_complete": bool(complete),
                "forward_return": ret,
                "benchmark_return": benchmark_return,
                "benchmark_relative_return": ret - benchmark_return if np.isfinite(ret) and np.isfinite(benchmark_return) else np.nan,
                "sector_relative_return": np.nan,
                "maximum_favourable_excursion": mfe,
                "maximum_adverse_excursion": mae,
                "confirmed_breakout": confirmed,
                "sessions_to_breakout": int(breakout_hits[0] + 1) if breakout_hits else np.nan,
                "failed_breakout": failed,
                "invalidated_setup": failed,
                "outcome_policy_version": policy.outcomes.version,
            })
    return pd.DataFrame(rows)


def _wilson_interval(successes: int, total: int, z: float = 1.959963984540054) -> tuple[float, float]:
    if total <= 0:
        return np.nan, np.nan
    p = successes / total
    denominator = 1.0 + z * z / total
    centre = (p + z * z / (2.0 * total)) / denominator
    margin = z * np.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total) / denominator
    return max(0.0, centre - margin), min(1.0, centre + margin)


def _calibration_metrics(outcomes: pd.DataFrame, signals: pd.DataFrame, policy: R0Policy) -> pd.DataFrame:
    if outcomes.empty:
        return pd.DataFrame()
    complete = outcomes.loc[outcomes["outcome_window_complete"].fillna(False)].copy()
    if complete.empty:
        return pd.DataFrame()
    dimensions = [
        "scan_lane_as_of", "pattern_family", "history_band", "pattern_state",
        "evidence_origin", "market_regime", "liquidity_cohort", "horizon_sessions",
    ]
    rows: list[dict[str, Any]] = []
    for keys, group in complete.groupby(dimensions, dropna=False, sort=True):
        values = dict(zip(dimensions, keys if isinstance(keys, tuple) else (keys,)))
        confirmed = int(group["confirmed_breakout"].fillna(False).sum())
        failed = int(group["failed_breakout"].fillna(False).sum())
        total = int(len(group))
        confirm_low, confirm_high = _wilson_interval(confirmed, total)
        failure_low, failure_high = _wilson_interval(failed, total)
        rows.append({
            **values,
            "sample_size": total,
            "unique_signal_count": int(group["signal_id"].astype(str).nunique()),
            "mean_forward_return": float(pd.to_numeric(group["forward_return"], errors="coerce").mean()),
            "median_forward_return": float(pd.to_numeric(group["forward_return"], errors="coerce").median()),
            "mean_benchmark_relative_return": float(pd.to_numeric(group["benchmark_relative_return"], errors="coerce").mean()),
            "median_mfe": float(pd.to_numeric(group["maximum_favourable_excursion"], errors="coerce").median()),
            "median_mae": float(pd.to_numeric(group["maximum_adverse_excursion"], errors="coerce").median()),
            "breakout_confirmation_rate": confirmed / total,
            "breakout_confirmation_ci_low": confirm_low,
            "breakout_confirmation_ci_high": confirm_high,
            "breakout_failure_rate": failed / total,
            "breakout_failure_ci_low": failure_low,
            "breakout_failure_ci_high": failure_high,
            "median_sessions_to_breakout": float(pd.to_numeric(group["sessions_to_breakout"], errors="coerce").median()),
            "minimum_sample_passed": total >= policy.outcomes.minimum_observations_per_lane_family,
            "confidence_level": policy.outcomes.confidence_level,
            "outcome_policy_version": policy.outcomes.version,
        })
    metrics = pd.DataFrame(rows)
    persistence = (
        signals.groupby(["symbol_id", "exchange", "pattern_family", "signal_date"], dropna=False)
        .agg(signal_persistence_snapshots=("as_of_date", "nunique"), duplicate_signal_rows=("signal_id", "size"))
        .reset_index()
    ) if not signals.empty else pd.DataFrame()
    if not persistence.empty:
        persistence.loc[:, "duplicate_signal_rows"] = (persistence["duplicate_signal_rows"] - 1).clip(lower=0)
        aggregate = persistence.groupby("pattern_family").agg(
            median_signal_persistence=("signal_persistence_snapshots", "median"),
            duplicate_signal_frequency=("duplicate_signal_rows", lambda values: float((values > 0).mean())),
        ).reset_index()
        metrics = metrics.merge(aggregate, on="pattern_family", how="left")
    return metrics


def _winner_recall(signals: pd.DataFrame, winner_windows: pd.DataFrame | None) -> pd.DataFrame:
    if winner_windows is None or winner_windows.empty:
        return pd.DataFrame()
    required = {"symbol_id", "first_guard_pass"}
    if not required.issubset(winner_windows.columns):
        raise ValueError("winner windows require symbol_id and first_guard_pass columns")
    rows: list[dict[str, Any]] = []
    for index, source in winner_windows.reset_index(drop=True).iterrows():
        symbol = str(source["symbol_id"]).strip().upper()
        guard = pd.Timestamp(source["first_guard_pass"]).normalize()
        matches = signals.loc[
            (signals["symbol_id"].astype(str).str.upper() == symbol)
            & (pd.to_datetime(signals["signal_date"], errors="coerce") <= guard)
        ].copy() if not signals.empty else pd.DataFrame()
        start_value = source.get("window_start", source.get("rally_start"))
        if start_value is not None and not pd.isna(start_value) and not matches.empty:
            matches = matches.loc[pd.to_datetime(matches["signal_date"]) >= pd.Timestamp(start_value).normalize()]
        first = matches.sort_values(["signal_date", "pattern_family"], kind="stable").iloc[0] if not matches.empty else None
        first_date = pd.Timestamp(first["signal_date"]).normalize() if first is not None else None
        rows.append({
            "winner_window_id": str(source.get("winner_window_id", source.get("window_id", index))),
            "symbol_id": symbol,
            "first_guard_pass": guard.date().isoformat(),
            "signal_before_first_guard_pass": first is not None,
            "first_signal_date": first_date.date().isoformat() if first_date is not None else None,
            "sessions_earlier": int(len(pd.bdate_range(first_date, guard)) - 1) if first_date is not None else None,
            "first_scan_lane": first.get("scan_lane_as_of") if first is not None else None,
            "first_pattern_family": first.get("pattern_family") if first is not None else None,
            "population_role": "recall_only_not_precision",
        })
    return pd.DataFrame(rows)


def _matched_controls(signals: pd.DataFrame, classified: pd.DataFrame, policy: R0Policy) -> pd.DataFrame:
    if signals.empty or classified.empty:
        return pd.DataFrame()
    signal_symbols = set(zip(signals["as_of_date"].astype(str), signals["symbol_id"].astype(str)))
    rows: list[dict[str, Any]] = []
    for _, signal in signals.iterrows():
        candidates = classified.loc[
            (classified["as_of_date"].astype(str) == str(signal["as_of_date"]))
            & (classified["scan_lane_as_of"] == signal["scan_lane_as_of"])
            & (classified["history_band"] == signal["history_band"])
        ].copy()
        candidates = candidates.loc[
            ~candidates.apply(lambda row: (str(row["as_of_date"]), str(row["symbol_id"])) in signal_symbols, axis=1)
        ]
        if candidates.empty:
            continue
        target = float(classified.loc[
            (classified["as_of_date"].astype(str) == str(signal["as_of_date"]))
            & (classified["symbol_id"] == signal["symbol_id"]), "liquidity_percentile"
        ].iloc[0])
        candidates.loc[:, "distance"] = (pd.to_numeric(candidates["liquidity_percentile"], errors="coerce") - target).abs()
        control = candidates.sort_values(["distance", "symbol_id"], kind="stable").iloc[0]
        rows.append({
            "signal_id": signal.get("signal_id"),
            "signal_symbol_id": signal["symbol_id"],
            "control_symbol_id": control["symbol_id"],
            "exchange": control["exchange"],
            "as_of_date": signal["as_of_date"],
            "scan_lane": signal["scan_lane_as_of"],
            "history_band": signal["history_band"],
            "pattern_family": signal["pattern_family"],
            "signal_liquidity_percentile": target,
            "control_liquidity_percentile": float(control["liquidity_percentile"]),
            "matching_method": policy.outcomes.matched_control_method,
        })
    return pd.DataFrame(rows)


def _checkpoint_signature(policy: R0Policy, source_hashes: dict[str, str]) -> str:
    return _sha256_bytes(_json({"policy_hash": policy.content_hash, "source_hashes": source_hashes}).encode("utf-8"))


def _load_date_checkpoint(
    checkpoint_root: Path,
    *,
    as_of_date: str,
    signature: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    date_root = checkpoint_root / as_of_date
    meta_path = date_root / "complete.json"
    if not meta_path.is_file():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("signature") != signature:
            return None
        return (
            pd.read_parquet(date_root / "context.parquet"),
            pd.read_parquet(date_root / "signals.parquet"),
            pd.read_parquet(date_root / "invocations.parquet"),
        )
    except (OSError, ValueError, KeyError):
        return None


def _write_date_checkpoint(
    checkpoint_root: Path,
    *,
    as_of_date: str,
    signature: str,
    context: pd.DataFrame,
    signals: pd.DataFrame,
    invocations: pd.DataFrame,
) -> None:
    date_root = checkpoint_root / as_of_date
    date_root.mkdir(parents=True, exist_ok=True)
    context.to_parquet(date_root / "context.parquet", index=False)
    _with_empty_schema("r0_pattern_lane_signals.csv", signals).to_parquet(
        date_root / "signals.parquet", index=False,
    )
    _with_empty_schema("r0_pattern_detector_invocations.csv", invocations).to_parquet(
        date_root / "invocations.parquet", index=False,
    )
    meta = {
        "as_of_date": as_of_date,
        "signature": signature,
        "context_rows": int(len(context)),
        "signal_rows": int(len(signals)),
        "invocation_rows": int(len(invocations)),
    }
    pending = date_root / "complete.json.pending"
    pending.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pending.replace(date_root / "complete.json")


def run_calibration(
    market_frame: pd.DataFrame,
    *,
    as_of_dates: list[str] | tuple[str, ...],
    weekly_stage_frame: pd.DataFrame | None = None,
    policy: R0Policy | None = None,
    exclusion_frame: pd.DataFrame | None = None,
    scan_config: PatternScanConfig | None = None,
    winner_windows: pd.DataFrame | None = None,
    workers: int = 1,
    progress_callback: ProgressCallback | None = None,
    progress_every: int = 25,
    checkpoint_dir: str | Path | None = None,
    resume: bool = True,
) -> CalibrationResult:
    """Run deterministic multi-date R0 replay without any operational writes."""

    active = policy or default_r0_policy()
    dates = sorted({pd.Timestamp(value).date().isoformat() for value in as_of_dates})
    if not dates:
        raise ValueError("at least one as-of date is required")
    market = _normalize_market_frame(market_frame)
    if progress_callback:
        progress_callback({"event": "source_hash_start", "market_rows": int(len(market))})
    source_hashes = {
        "market_frame": _frame_hash(market),
        "weekly_stage_frame": _frame_hash(weekly_stage_frame if weekly_stage_frame is not None else pd.DataFrame()),
        "winner_windows": _frame_hash(winner_windows if winner_windows is not None else pd.DataFrame()),
        "exclusions": _frame_hash(exclusion_frame if exclusion_frame is not None else pd.DataFrame()),
    }
    signature = _checkpoint_signature(active, source_hashes)
    checkpoint_root = Path(checkpoint_dir).resolve() if checkpoint_dir else None
    if progress_callback:
        progress_callback({"event": "source_hash_complete", "signature": signature})
    contexts: list[pd.DataFrame] = []
    signals: list[pd.DataFrame] = []
    invocations: list[pd.DataFrame] = []
    runtime_by_date: list[dict[str, Any]] = []
    for date_index, as_of in enumerate(dates, start=1):
        started = perf_counter()
        if progress_callback:
            progress_callback({
                "event": "date_start", "as_of_date": as_of,
                "date_index": date_index, "date_count": len(dates),
            })
        cached = (
            _load_date_checkpoint(checkpoint_root, as_of_date=as_of, signature=signature)
            if checkpoint_root is not None and resume else None
        )
        if cached is not None:
            classified, signal_frame, invocation_frame = cached
            if progress_callback:
                progress_callback({
                    "event": "checkpoint_loaded", "as_of_date": as_of,
                    "context_rows": len(classified), "signal_rows": len(signal_frame),
                    "invocation_rows": len(invocation_frame),
                })
        else:
            context = build_point_in_time_context(
                market, as_of_date=as_of, weekly_stage_frame=weekly_stage_frame,
                policy=active, exclusion_frame=exclusion_frame,
            )
            classified = classify_lanes(context, policy=active)
            if progress_callback:
                progress_callback({
                    "event": "context_complete", "as_of_date": as_of,
                    "universe_symbols": len(classified),
                    "eligible_symbols": int((classified["scan_lane_as_of"] != "no_lane").sum()) if not classified.empty else 0,
                    "lane_counts": classified.get("scan_lane_as_of", pd.Series(dtype=str)).value_counts().to_dict(),
                })
            signal_frame, invocation_frame = scan_lane_patterns(
                market, classified, as_of_date=as_of, policy=active,
                scan_config=scan_config, workers=workers,
                progress_callback=progress_callback, progress_every=progress_every,
            )
            if checkpoint_root is not None:
                _write_date_checkpoint(
                    checkpoint_root, as_of_date=as_of, signature=signature,
                    context=classified, signals=signal_frame, invocations=invocation_frame,
                )
                if progress_callback:
                    progress_callback({"event": "checkpoint_written", "as_of_date": as_of})
        contexts.append(classified)
        signals.append(signal_frame)
        invocations.append(invocation_frame)
        scanned_symbols = int((classified["scan_lane_as_of"] != "no_lane").sum()) if not classified.empty else 0
        elapsed = perf_counter() - started
        runtime_by_date.append({
            "as_of_date": as_of,
            "elapsed_seconds": elapsed,
            "scanned_symbols": scanned_symbols,
            "seconds_per_scanned_symbol": elapsed / scanned_symbols if scanned_symbols else None,
            "detector_invocations": int(len(invocation_frame)),
        })
        if progress_callback:
            progress_callback({
                "event": "date_complete", "as_of_date": as_of,
                "date_index": date_index, "date_count": len(dates),
                "elapsed_seconds": elapsed, "signal_rows": int(len(signal_frame)),
                "detector_invocations": int(len(invocation_frame)),
            })
    if progress_callback:
        progress_callback({"event": "aggregation_start", "date_count": len(dates)})
    context_all = pd.concat(contexts, ignore_index=True, sort=False) if contexts else pd.DataFrame()
    signals_all = pd.concat([frame for frame in signals if not frame.empty], ignore_index=True, sort=False) if any(not frame.empty for frame in signals) else pd.DataFrame()
    invocations_all = pd.concat([frame for frame in invocations if not frame.empty], ignore_index=True, sort=False) if any(not frame.empty for frame in invocations) else pd.DataFrame()
    outcomes = _forward_outcomes(signals_all, market, active)
    controls = _matched_controls(signals_all, context_all, active)
    metrics = _calibration_metrics(outcomes, signals_all, active)
    winner_recall = _winner_recall(signals_all, winner_windows)
    summary = {
        "policy_version": active.version,
        "policy_hash": active.content_hash,
        "as_of_dates": dates,
        "universe_rows": int(len(context_all)),
        "lane_counts": context_all.get("scan_lane_as_of", pd.Series(dtype=str)).value_counts().sort_index().to_dict(),
        "detector_invocations": int(len(invocations_all)),
        "emitted_rows": int(len(signals_all)),
        "outcome_rows": int(len(outcomes)),
        "matched_control_rows": int(len(controls)),
        "metric_rows": int(len(metrics)),
        "winner_recall_rows": int(len(winner_recall)),
        "operationally_admitted_rows": 0,
        "production_artifacts_written": 0,
    }
    return CalibrationResult(
        context=context_all,
        detector_invocations=invocations_all,
        signals=signals_all,
        outcomes=outcomes,
        controls=controls,
        metrics=metrics,
        winner_recall=winner_recall,
        runtime_diagnostics={
            "note": "Wall-clock diagnostics are observational and excluded from reproducibility hashes.",
            "dates": runtime_by_date,
        },
        summary=summary,
        policy=active,
        source_hashes=source_hashes,
    )


def _artifact_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False, lineterminator="\n").encode("utf-8")


def _with_empty_schema(filename: str, frame: pd.DataFrame) -> pd.DataFrame:
    if not frame.empty or len(frame.columns):
        return frame
    schemas = {
        "r0_pattern_structure_context.csv": list(CONTEXT_COLUMNS) + [
            "scan_lane_as_of", "history_band", "lane_assignment_reason_codes",
            "lane_policy_version", "pattern_family_policy_version",
        ],
        "r0_pattern_detector_invocations.csv": [
            "as_of_date", "symbol_id", "exchange", "scan_lane", "history_band",
            "pattern_family", "disposition", "candidate_count", "confirmed_count",
            "watchlist_count",
        ],
        "r0_pattern_lane_signals.csv": [
            "signal_id", "symbol_id", "exchange", "pattern_family", "pattern_state",
            "signal_date", "as_of_date", "scan_lane_at_detection", "scan_lane_as_of",
            "evidence_origin", "lane_policy_version", "liquidity_policy_version",
            "pattern_family_policy_version", "structure_observation_id",
        ],
        "r0_pattern_outcomes.csv": [
            "signal_id", "symbol_id", "exchange", "pattern_family", "as_of_date",
            "horizon_sessions", "outcome_window_complete", "forward_return",
            "benchmark_relative_return", "maximum_favourable_excursion",
            "maximum_adverse_excursion", "confirmed_breakout", "failed_breakout",
            "invalidated_setup", "sessions_to_breakout", "outcome_policy_version",
        ],
        "r0_pattern_matched_controls.csv": [
            "signal_id", "signal_symbol_id", "control_symbol_id", "exchange",
            "as_of_date", "scan_lane", "history_band", "pattern_family",
            "signal_liquidity_percentile", "control_liquidity_percentile",
            "matching_method",
        ],
        "r0_pattern_metrics.csv": [
            "scan_lane_as_of", "pattern_family", "history_band", "pattern_state",
            "evidence_origin", "market_regime", "liquidity_cohort",
            "horizon_sessions", "sample_size", "minimum_sample_passed",
            "confidence_level", "outcome_policy_version",
        ],
        "r0_pattern_winner_recall.csv": [
            "winner_window_id", "symbol_id", "first_guard_pass",
            "signal_before_first_guard_pass", "first_signal_date", "sessions_earlier",
            "first_scan_lane", "first_pattern_family", "population_role",
        ],
    }
    return pd.DataFrame(columns=schemas[filename])


def write_calibration_result(result: CalibrationResult, output_dir: str | Path) -> tuple[Path, ...]:
    """Write a new immutable research bundle; never overwrite an existing bundle."""

    root = Path(output_dir).resolve()
    forbidden_parts = {"pipeline_runs", "stage_store"}
    if forbidden_parts.intersection(root.parts):
        raise ValueError("R0 output must not be written under production artifact or stage-store trees")
    if root.exists() and any(root.iterdir()):
        raise FileExistsError(f"immutable calibration output already exists: {root}")
    root.mkdir(parents=True, exist_ok=True)
    frames = {
        "r0_pattern_structure_context.csv": result.context,
        "r0_pattern_detector_invocations.csv": result.detector_invocations,
        "r0_pattern_lane_signals.csv": result.signals,
        "r0_pattern_outcomes.csv": result.outcomes,
        "r0_pattern_matched_controls.csv": result.controls,
        "r0_pattern_metrics.csv": result.metrics,
        "r0_pattern_winner_recall.csv": result.winner_recall,
    }
    paths: list[Path] = []
    dataset_hashes: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    for filename, frame in frames.items():
        frame = _with_empty_schema(filename, frame)
        payload = _artifact_bytes(frame)
        path = root / filename
        path.write_bytes(payload)
        paths.append(path)
        dataset_hashes[filename] = _sha256_bytes(payload)
        row_counts[filename] = int(len(frame))
    policy_path = root / "r0_pattern_policies.json"
    policy_payload = (json.dumps(result.policy.to_metadata(), indent=2, sort_keys=True) + "\n").encode("utf-8")
    policy_path.write_bytes(policy_payload)
    paths.append(policy_path)
    dataset_hashes[policy_path.name] = _sha256_bytes(policy_payload)
    summary_path = root / "r0_pattern_summary.json"
    summary_payload = (json.dumps(result.summary, indent=2, sort_keys=True) + "\n").encode("utf-8")
    summary_path.write_bytes(summary_payload)
    paths.append(summary_path)
    dataset_hashes[summary_path.name] = _sha256_bytes(summary_payload)
    runtime_path = root / "r0_pattern_runtime.json"
    runtime_path.write_text(json.dumps(result.runtime_diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths.append(runtime_path)
    manifest = {
        "schema_version": "pattern-r0-manifest-v1",
        "builder_version": "pattern-r0-builder-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "policy_version": result.policy.version,
        "policy_hash": result.policy.content_hash,
        "policy_versions": {
            "standard_liquidity": result.policy.standard_liquidity.version,
            "early_ipo_liquidity": result.policy.early_ipo_liquidity.version,
            "stage2": result.policy.stage2.version,
            "weekly_freshness": result.policy.weekly_freshness.version,
            "stage1_structure": result.policy.stage1.version,
            "families": result.policy.families.version,
            "outcomes": result.policy.outcomes.version,
            "reconstruction": result.policy.reconstruction.version,
        },
        "source_hashes": result.source_hashes,
        "dataset_hashes": dataset_hashes,
        "row_counts": row_counts,
        "reproducibility_status": "REPRODUCIBLE",
        "non_reproducibility_bound_diagnostics": [runtime_path.name],
        "operational_side_effects": False,
    }
    manifest_path = root / "r0_pattern_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths.append(manifest_path)
    return tuple(paths)
