"""Dedicated breakout scanner for operational ranking outputs."""

from __future__ import annotations

import os
import sqlite3
from typing import Iterable, Optional

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.analytics.regime_detector import RegimeDetector
from ai_trading_system.platform.logging.logger import logger

VOLUME_Z20_CONFIRM_THRESHOLD = 2.0
VOLUME_Z50_CONFIRM_THRESHOLD = 2.0
VOLUME_Z20_STRONG_THRESHOLD = 3.0


def _load_sector_map(master_db_path: str) -> dict[str, str]:
    if not os.path.exists(master_db_path):
        return {}
    conn = sqlite3.connect(master_db_path)
    try:
        rows = conn.execute("""
            SELECT s.symbol_id, COALESCE(sm.system_sector, 'Other')
            FROM symbols s
            LEFT JOIN sector_mapping sm ON s.sector = sm.industry
            WHERE s.exchange = 'NSE'
        """).fetchall()
    finally:
        conn.close()
    return {symbol: sector for symbol, sector in rows}


def _load_supertrend_flags(
    feature_store_dir: str,
    symbols: list[str],
    date: str,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """Load latest and previous supertrend direction per symbol up to the ranking date."""
    feature_dir = os.path.join(feature_store_dir, "supertrend", exchange)
    if not os.path.isdir(feature_dir) or not symbols:
        return pd.DataFrame(
            columns=[
                "symbol_id",
                "supertrend_dir_10_3",
                "prev_supertrend_dir_10_3",
                "supertrend_10_3",
            ]
        )

    rows: list[pd.DataFrame] = []
    cutoff = pd.to_datetime(date)
    for symbol in symbols:
        path = os.path.join(feature_dir, f"{symbol}.parquet")
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_parquet(
                path,
                columns=["symbol_id", "timestamp", "supertrend_dir_10_3", "supertrend_10_3"],
            )
        except Exception:
            continue
        df.loc[:, "timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[df["timestamp"] <= cutoff]
        if df.empty:
            continue
        tail = df.sort_values("timestamp").tail(2).copy()
        latest = tail.iloc[-1]
        prev_dir = (
            int(tail.iloc[-2]["supertrend_dir_10_3"])
            if len(tail) > 1 and pd.notna(tail.iloc[-2]["supertrend_dir_10_3"])
            else pd.NA
        )
        rows.append(
            pd.DataFrame(
                [
                    {
                        "symbol_id": latest["symbol_id"],
                        "supertrend_dir_10_3": latest.get("supertrend_dir_10_3"),
                        "prev_supertrend_dir_10_3": prev_dir,
                        "supertrend_10_3": latest.get("supertrend_10_3"),
                    }
                ]
            )
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "symbol_id",
                "supertrend_dir_10_3",
                "prev_supertrend_dir_10_3",
                "supertrend_10_3",
            ]
        )
    return pd.concat(rows, ignore_index=True).drop_duplicates("symbol_id", keep="last")


def _as_bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame.get(column, pd.Series(False, index=frame.index)).fillna(False).astype(bool)


def _to_float_series(frame: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _canonical_family_from_legacy(setup_family: str) -> str:
    mapping = {
        "base_breakout": "resistance_breakout_50d",
        "contraction_breakout": "consolidation_breakout",
        "supertrend_flip_breakout": "volatility_expansion_breakout",
    }
    return mapping.get(str(setup_family or "").strip(), "resistance_breakout_50d")


def _normalize_market_bias_allowlist(values: Iterable[str] | str | None) -> set[str]:
    if values is None:
        return {"BULLISH", "NEUTRAL"}
    if isinstance(values, str):
        values = [item.strip() for item in values.split(",")]
    out = {str(item).strip().upper() for item in values if str(item).strip()}
    return out or {"BULLISH", "NEUTRAL"}


def _prepare_rank_context(ranked_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if ranked_df is None or ranked_df.empty or "symbol_id" not in ranked_df.columns:
        return pd.DataFrame(
            columns=[
                "symbol_id",
                "rel_strength_score",
                "sector_rs_value",
                "sector_rs_percentile",
                "stage2_score",
                "is_stage2_structural",
                "is_stage2_candidate",
                "is_stage2_uptrend",
                "stage2_label",
                "stage2_hard_fail_reason",
                "stage2_fail_reason",
            ]
        )

    cols = [
        col
        for col in [
            "symbol_id",
            "rel_strength_score",
            "sector_rs_value",
            "stage2_score",
            "is_stage2_structural",
            "is_stage2_candidate",
            "is_stage2_uptrend",
            "stage2_label",
            "stage2_hard_fail_reason",
            "stage2_fail_reason",
        ]
        if col in ranked_df.columns
    ]
    ctx = ranked_df[cols].copy()
    ctx.loc[:, "symbol_id"] = ctx["symbol_id"].astype(str)
    if "rel_strength_score" not in ctx.columns:
        ctx.loc[:, "rel_strength_score"] = np.nan
    if "sector_rs_value" not in ctx.columns:
        ctx.loc[:, "sector_rs_value"] = np.nan
    ctx.loc[:, "sector_rs_percentile"] = (
        pd.to_numeric(ctx["sector_rs_value"], errors="coerce").rank(pct=True, method="average") * 100.0
    )
    return ctx.drop_duplicates("symbol_id", keep="first")


def _volume_confirmation_columns(
    frame: pd.DataFrame,
    *,
    ratio_threshold: float = 1.5,
) -> dict[str, pd.Series]:
    ratio_series = (
        _as_bool_series(frame, "is_volume_ratio_confirmed")
        if "is_volume_ratio_confirmed" in frame.columns
        else (
            _as_bool_series(frame, "is_volume_confirmed_breakout")
            if "is_volume_confirmed_breakout" in frame.columns
            else _to_float_series(frame, "volume_ratio", default=np.nan).ge(float(ratio_threshold)).fillna(False)
        )
    )
    z20 = _to_float_series(frame, "volume_zscore_20", default=np.nan)
    z50 = _to_float_series(frame, "volume_zscore_50", default=np.nan)
    is_z20_confirmed = z20.ge(VOLUME_Z20_CONFIRM_THRESHOLD).fillna(False)
    is_z50_confirmed = z50.ge(VOLUME_Z50_CONFIRM_THRESHOLD).fillna(False)
    is_any_volume_confirmed = ratio_series | is_z20_confirmed | is_z50_confirmed
    is_strong_volume_confirmation = (ratio_series & is_z20_confirmed) | z20.ge(VOLUME_Z20_STRONG_THRESHOLD).fillna(False)
    return {
        "is_volume_ratio_confirmed": ratio_series.fillna(False).astype(bool),
        "is_z20_confirmed": is_z20_confirmed.astype(bool),
        "is_z50_confirmed": is_z50_confirmed.astype(bool),
        "is_any_volume_confirmed": is_any_volume_confirmed.astype(bool),
        "is_strong_volume_confirmation": is_strong_volume_confirmation.astype(bool),
    }


def compute_breakout_v2_scores(
    candidates: pd.DataFrame,
    *,
    market_bias: str,
    breadth_score: float,
    market_bias_allowlist: Iterable[str] | str | None = None,
    min_breadth_score: float = 45.0,
    sector_rs_min: float | None = None,
    sector_rs_percentile_min: float | None = 60.0,
    breakout_qualified_min_score: int = 3,
    breakout_symbol_trend_gate_enabled: bool = True,
    breakout_symbol_near_high_max_pct: float = 15.0,
    market_stage: str = "S2",
) -> pd.DataFrame:
    """
    Apply breakout-v2 score contract and regime gates.

    Expects candidates to already include breakout booleans and rank context columns.
    """
    if candidates is None or candidates.empty:
        return pd.DataFrame()

    df = candidates.copy()
    market_bias = str(market_bias or "UNKNOWN").upper()
    allowed_biases = _normalize_market_bias_allowlist(market_bias_allowlist)
    breadth_score = float(breadth_score or 0.0)

    # v2 score contract (kept separate from the main composite rank).
    volume_confirmation = _volume_confirmation_columns(df)
    price_breakout_detected = (
        _as_bool_series(df, "is_resistance_breakout_50d")
        | _as_bool_series(df, "is_high_52w_breakout")
        | _as_bool_series(df, "is_consolidation_breakout")
        | _as_bool_series(df, "is_volatility_expansion_breakout")
    )
    effective_any_volume_confirmation = price_breakout_detected & volume_confirmation["is_any_volume_confirmed"]
    effective_strong_volume_confirmation = price_breakout_detected & volume_confirmation["is_strong_volume_confirmation"]
    effective_combined_volume_confirmation = (
        price_breakout_detected
        & volume_confirmation["is_volume_ratio_confirmed"]
        & (volume_confirmation["is_z20_confirmed"] | volume_confirmation["is_z50_confirmed"])
    )
    ratio_only_confirmation = (
        price_breakout_detected
        & volume_confirmation["is_volume_ratio_confirmed"]
        & ~effective_combined_volume_confirmation
    )
    z20_only_confirmation = (
        price_breakout_detected
        & volume_confirmation["is_z20_confirmed"]
        & ~volume_confirmation["is_volume_ratio_confirmed"]
    )
    z50_only_confirmation = (
        price_breakout_detected
        & volume_confirmation["is_z50_confirmed"]
        & ~volume_confirmation["is_volume_ratio_confirmed"]
        & ~volume_confirmation["is_z20_confirmed"]
    )
    df.loc[:, "breakout_score"] = (
        _as_bool_series(df, "is_resistance_breakout_50d").astype(int) * 1
        + _as_bool_series(df, "is_high_52w_breakout").astype(int) * 2
        + _as_bool_series(df, "is_consolidation_breakout").astype(int) * 2
        + volume_confirmation["is_volume_ratio_confirmed"].astype(int) * 1
        + (_to_float_series(df, "rel_strength_score", default=0.0).fillna(0.0) >= 80.0).astype(int) * 2
    )
    volume_confirmation_bonus = np.select(
        [
            effective_strong_volume_confirmation,
            effective_combined_volume_confirmation,
            z20_only_confirmation | z50_only_confirmation,
        ],
        [2, 2, 1],
        default=0,
    )
    df.loc[:, "breakout_score"] = df["breakout_score"] + volume_confirmation_bonus

    # ── Market-regime score deflation ────────────────────────────────────────
    # In S3 (topping) markets, raise the effective qualification bar by
    # deflating raw scores; in S4 (bear) it's a safety net — the service layer
    # sets breakout_active=False before we even get here.
    if market_stage == "S3":
        df.loc[:, "breakout_score"] = (df["breakout_score"] * 0.65).round().astype(int)
        if "setup_quality" in df.columns:
            df.loc[:, "setup_quality"] = df["setup_quality"] * 0.65
    elif market_stage == "S4":
        df.loc[:, "breakout_score"] = 0
        if "setup_quality" in df.columns:
            df.loc[:, "setup_quality"] = 0.0

    if "setup_quality" not in df.columns:
        df.loc[:, "setup_quality"] = np.nan
    df.loc[:, "setup_quality"] = pd.to_numeric(df["setup_quality"], errors="coerce")
    df.loc[:, "setup_quality"] = df["setup_quality"].fillna(
        df["breakout_score"] * 20.0
        + _to_float_series(df, "volume_ratio", default=0.0).fillna(0.0).clip(0, 4) * 8.0
        + _to_float_series(df, "adx_14", default=0.0).fillna(0.0).clip(0, 60) * 0.4
        + (12.0 - _to_float_series(df, "near_52w_high_pct", default=12.0).fillna(12.0).clip(0, 12)) * 1.5
    )

    market_bias_ok = market_bias in allowed_biases
    breadth_ok = breadth_score >= float(min_breadth_score)

    sector_rs_value = _to_float_series(df, "sector_rs_value")
    sector_rs_percentile = _to_float_series(df, "sector_rs_percentile")

    if sector_rs_min is None:
        sector_abs_ok = pd.Series(True, index=df.index)
    else:
        sector_abs_ok = sector_rs_value >= float(sector_rs_min)
    if sector_rs_percentile_min is None:
        sector_pct_ok = pd.Series(True, index=df.index)
    else:
        sector_pct_ok = sector_rs_percentile >= float(sector_rs_percentile_min)

    # Regime layer (market + breadth + sector RS gates).
    sector_gate_ok = sector_abs_ok & sector_pct_ok
    regime_gate_ok = pd.Series(market_bias_ok and breadth_ok, index=df.index)
    all_regime_gates_ok = regime_gate_ok & sector_gate_ok

    above_sma200 = _as_bool_series(df, "above_sma200")
    if "above_sma200" not in df.columns:
        close_series = _to_float_series(df, "close")
        sma200_series = _to_float_series(df, "sma_200")
        derived = close_series > sma200_series
        above_sma200 = derived.where(close_series.notna() & sma200_series.notna(), True).fillna(True).astype(bool)

    sma50_slope_20d_pct = _to_float_series(df, "sma50_slope_20d_pct")
    sma50_slope_positive = (sma50_slope_20d_pct > 0).where(sma50_slope_20d_pct.notna(), True).fillna(True).astype(bool)

    near_52w_high_pct = _to_float_series(df, "near_52w_high_pct")
    near_52w_high_ok = (
        near_52w_high_pct <= float(breakout_symbol_near_high_max_pct)
    ).where(near_52w_high_pct.notna(), True).fillna(True).astype(bool)

    # ── Tier computation — structural Stage 2 is authoritative when present ─
    s2_score = _to_float_series(df, "stage2_score")
    has_structural_stage2 = "is_stage2_structural" in df.columns
    has_candidate_stage2 = "is_stage2_candidate" in df.columns
    structural_stage2 = (
        _as_bool_series(df, "is_stage2_structural")
        if has_structural_stage2
        else pd.Series(False, index=df.index)
    )
    candidate_stage2 = (
        _as_bool_series(df, "is_stage2_candidate")
        if has_candidate_stage2
        else (s2_score >= 50.0).fillna(False)
    )
    stage2_available = has_structural_stage2 or (s2_score.notna().sum() > len(df) * 0.5)

    if has_structural_stage2:
        candidate_tier = np.select(
            [
                structural_stage2 & (s2_score >= 85.0),
                structural_stage2 & (s2_score >= 70.0),
                candidate_stage2,
            ],
            ["A", "B", "C"],
            default="D",
        )
        pass_count = np.select(
            [candidate_tier == "A", candidate_tier == "B", candidate_tier == "C"],
            [3, 2, 1],
            default=0,
        )
        pass_count = pd.Series(pass_count, index=df.index).astype(int)
        fail_count = 3 - pass_count
    elif stage2_available:
        candidate_tier = np.select(
            [s2_score >= 85.0, s2_score >= 70.0, s2_score >= 50.0],
            ["A", "B", "C"],
            default="D",  # non_stage2 — filtered by symbol trend
        )
        pass_count = (s2_score.fillna(0.0) / 25.0).clip(0, 3).round().astype(int)
        fail_count = 3 - pass_count
    else:
        # Legacy fallback: 3-condition pass_count (unchanged)
        pass_count = (
            above_sma200.astype(int)
            + sma50_slope_positive.astype(int)
            + near_52w_high_ok.astype(int)
        )
        fail_count = 3 - pass_count
        candidate_tier = np.select(
            [fail_count == 0, fail_count == 1],
            ["A", "B"],
            default="C",
        )

    if not breakout_symbol_trend_gate_enabled:
        pass_count = pd.Series(3, index=df.index)
        fail_count = pd.Series(0, index=df.index)
        candidate_tier = np.array(["A"] * len(df), dtype=object)

    trend_reasons: list[str] = []
    trend_negative_reasons: list[str] = []
    for i in range(len(df)):
        row_reasons = [
            "ABOVE_SMA200" if bool(above_sma200.iloc[i]) else "BELOW_SMA200",
            "SMA50_SLOPE_POSITIVE" if bool(sma50_slope_positive.iloc[i]) else "SMA50_SLOPE_NEGATIVE",
            "NEAR_52W_HIGH" if bool(near_52w_high_ok.iloc[i]) else "FAR_FROM_52W_HIGH",
        ]
        trend_reasons.append(",".join(row_reasons))
        trend_negative_reasons.append(",".join([reason for reason in row_reasons if reason.startswith(("BELOW_", "FAR_", "SMA50_SLOPE_NEGATIVE"))]))

    def _regime_reasons(i: int) -> str:
        reasons: list[str] = []
        if not market_bias_ok:
            reasons.append("market_bias_not_allowed")
        if not breadth_ok:
            reasons.append("breadth_below_threshold")
        if sector_rs_min is not None and not bool(sector_abs_ok.iloc[i]):
            reasons.append("sector_rs_below_threshold")
        if sector_rs_percentile_min is not None and not bool(sector_pct_ok.iloc[i]):
            reasons.append("sector_rs_below_percentile")
        if pd.isna(sector_rs_value.iloc[i]):
            reasons.append("sector_rs_missing")
        return ",".join(reasons)

    filtered_by_regime = ~all_regime_gates_ok
    tier_series = pd.Series(candidate_tier, index=df.index)
    if breakout_symbol_trend_gate_enabled:
        if stage2_available:
            filtered_by_symbol_trend = (~filtered_by_regime) & (tier_series == "D")
        else:
            filtered_by_symbol_trend = (~filtered_by_regime) & (tier_series == "C")
    else:
        filtered_by_symbol_trend = pd.Series(False, index=df.index)

    # Pre-fetch Stage 2 explainability fields for rejection/watchlist reasons.
    stage2_hard_fail_reason_col = df.get("stage2_hard_fail_reason", pd.Series("", index=df.index)).fillna("")
    stage2_fail_reason_col = df.get("stage2_fail_reason", pd.Series("", index=df.index)).fillna("")

    states: list[str] = []
    reasons: list[str] = []
    for i in range(len(df)):
        tier_i = candidate_tier[i]
        if bool(filtered_by_regime.iloc[i]):
            states.append("filtered_by_regime")
            reasons.append(_regime_reasons(i))
        elif bool(filtered_by_symbol_trend.iloc[i]):
            states.append("filtered_by_symbol_trend")
            if stage2_available and tier_i == "D":
                reasons.append(
                    str(stage2_hard_fail_reason_col.iloc[i])
                    or str(stage2_fail_reason_col.iloc[i])
                    or "non_structural_stage2"
                )
            else:
                reasons.append(trend_negative_reasons[i])
        elif tier_i == "A":
            states.append("qualified")
            reasons.append("")
        else:
            states.append("watchlist")
            if stage2_available:
                reasons.append(
                    str(stage2_fail_reason_col.iloc[i])
                    or str(stage2_hard_fail_reason_col.iloc[i])
                    or "score_below_qualified_threshold"
                )
            else:
                reasons.append(trend_negative_reasons[i] or "score_below_qualified_threshold")

    df.loc[:, "breakout_detected"] = True
    df.loc[:, "filtered_by_regime"] = filtered_by_regime.fillna(False).astype(bool)
    df.loc[:, "filtered_by_symbol_trend"] = filtered_by_symbol_trend.fillna(False).astype(bool)
    df.loc[:, "above_sma200"] = above_sma200.fillna(False).astype(bool)
    df.loc[:, "sma50_slope_20d_pct"] = sma50_slope_20d_pct
    df.loc[:, "symbol_trend_fail_count"] = fail_count.astype(int)
    df.loc[:, "symbol_trend_score"] = (pass_count / 3.0 * 100.0).round(2)
    df.loc[:, "symbol_trend_reasons"] = trend_reasons
    df.loc[:, "candidate_tier"] = candidate_tier
    df.loc[:, "is_volume_ratio_confirmed"] = volume_confirmation["is_volume_ratio_confirmed"]
    df.loc[:, "is_z20_confirmed"] = volume_confirmation["is_z20_confirmed"]
    df.loc[:, "is_z50_confirmed"] = volume_confirmation["is_z50_confirmed"]
    df.loc[:, "is_any_volume_confirmed"] = volume_confirmation["is_any_volume_confirmed"]
    df.loc[:, "is_strong_volume_confirmation"] = volume_confirmation["is_strong_volume_confirmation"]
    df.loc[:, "is_any_volume_confirmed_breakout"] = effective_any_volume_confirmation.astype(bool)
    # Stage 2 enrichment columns (passthrough when present, else derived)
    df.loc[:, "stage2_score"] = s2_score
    if has_structural_stage2:
        df.loc[:, "is_stage2_structural"] = structural_stage2.fillna(False).astype(bool)
        df.loc[:, "is_stage2_candidate"] = candidate_stage2.fillna(False).astype(bool)
        df.loc[:, "is_stage2_uptrend"] = structural_stage2.fillna(False).astype(bool)
        df.loc[:, "stage2_gate_passed"] = structural_stage2.fillna(False).astype(bool)
    else:
        df.loc[:, "is_stage2_uptrend"] = s2_score >= 70.0
        df.loc[:, "stage2_gate_passed"] = s2_score >= 70.0
    if "stage2_label" not in df.columns:
        if has_structural_stage2:
            df.loc[:, "stage2_label"] = np.select(
                [
                    structural_stage2 & (s2_score >= 85.0),
                    structural_stage2 & (s2_score >= 70.0),
                    (~structural_stage2) & candidate_stage2,
                ],
                ["strong_stage2", "stage2", "stage1_to_stage2"],
                default="non_stage2",
            )
        else:
            df.loc[:, "stage2_label"] = np.select(
                [s2_score >= 85.0, s2_score >= 70.0, s2_score >= 50.0],
                ["strong_stage2", "stage2", "stage1_to_stage2"],
                default="non_stage2",
            )

    df.loc[:, "breakout_state"] = states
    df.loc[:, "filter_reason"] = reasons
    df.loc[:, "market_bias_allowed"] = bool(market_bias_ok)
    df.loc[:, "breadth_gate_passed"] = bool(breadth_ok)
    df.loc[:, "sector_gate_passed"] = sector_gate_ok.fillna(False).astype(bool)
    df.loc[:, "regime_gate_passed"] = regime_gate_ok.fillna(False).astype(bool)

    state_priority = {
        "qualified": 0,
        "watchlist": 1,
        "filtered_by_symbol_trend": 2,
        "filtered_by_regime": 3,
    }
    df.loc[:, "state_priority"] = df["breakout_state"].map(state_priority).fillna(9).astype(int)
    df.loc[:, "breakout_rank"] = (
        df.sort_values(
            ["state_priority", "breakout_score", "setup_quality", "breakout_pct"],
            ascending=[True, False, False, True],
            na_position="last",
        )
        .reset_index(drop=True)
        .index
        + 1
    )
    df = df.sort_values("breakout_rank", ascending=True).reset_index(drop=True)
    return df.drop(columns=["state_priority"], errors="ignore")


def _empty_breakout_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol_id",
            "sector",
            "setup_family",
            "legacy_setup_family",
            "taxonomy_family",
            "execution_label",
            "market_regime",
            "market_bias",
            "breakout_detected",
            "filtered_by_regime",
            "filtered_by_symbol_trend",
            "breakout_state",
            "filter_reason",
            "breakout_score",
            "breakout_rank",
            "candidate_tier",
            "symbol_trend_score",
            "symbol_trend_reasons",
            "symbol_trend_fail_count",
            "sma50_slope_20d_pct",
            "above_sma200",
            "volume_ratio",
            "volume_zscore_20",
            "volume_zscore_50",
            "is_volume_ratio_confirmed",
            "is_z20_confirmed",
            "is_z50_confirmed",
            "is_any_volume_confirmed",
            "is_any_volume_confirmed_breakout",
            "is_strong_volume_confirmation",
            "setup_quality",
            "breakout_tag",
        ]
    )


def scan_breakouts(
    ohlcv_db_path: str,
    feature_store_dir: str,
    master_db_path: str,
    date: Optional[str] = None,
    exchange: str = "NSE",
    top_n: int = 25,
    min_volume_ratio: float = 1.2,
    min_adx: float = 18.0,
    ranked_df: Optional[pd.DataFrame] = None,
    breakout_engine: str = "v2",
    include_legacy_families: bool = True,
    market_bias_allowlist: Iterable[str] | str | None = None,
    min_breadth_score: float = 45.0,
    sector_rs_min: float | None = None,
    sector_rs_percentile_min: float | None = 60.0,
    breakout_qualified_min_score: int = 3,
    breakout_symbol_trend_gate_enabled: bool = True,
    breakout_symbol_near_high_max_pct: float = 15.0,
    market_stage: str = "S2",
) -> pd.DataFrame:
    """Build a breakout monitor with setup families and market-regime context."""
    conn = duckdb.connect(ohlcv_db_path, read_only=True)
    try:
        if date is None:
            latest_row = conn.execute(
                f"SELECT MAX(CAST(timestamp AS DATE)) FROM _catalog WHERE exchange = '{exchange}'"
            ).fetchone()
            latest_date = latest_row[0] if latest_row else None
            if latest_date is None:
                return _empty_breakout_frame()
            date = str(latest_date)
        else:
            aligned_row = conn.execute(
                f"""
                SELECT MAX(CAST(timestamp AS DATE))
                FROM _catalog
                WHERE exchange = '{exchange}'
                  AND CAST(timestamp AS DATE) <= DATE '{date}'
                """
            ).fetchone()
            aligned = aligned_row[0] if aligned_row else None
            if aligned is not None:
                date = str(aligned)

        query = f"""
            WITH base AS (
                SELECT
                    symbol_id,
                    exchange,
                    CAST(timestamp AS DATE) AS trade_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS prior_range_high,
                    MIN(low) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS prior_range_low,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                    ) AS prior_base_high_30,
                    MIN(low) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                    ) AS prior_base_low_30,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
                    ) AS prior_base_high_60,
                    MIN(low) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
                    ) AS prior_base_low_60,
                    AVG(volume) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS vol_20_avg,
                    STDDEV_POP(volume) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS vol_20_std,
                    AVG(volume) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
                    ) AS vol_50_avg,
                    STDDEV_POP(volume) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
                    ) AS vol_50_std,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                    ) AS high_52w,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
                    ) AS prior_high_50,
                    MAX(high) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING
                    ) AS prior_high_252,
                    AVG(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20,
                    AVG(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS sma_50
                    ,
                    AVG(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY timestamp
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS sma_200
                FROM _catalog
                WHERE exchange = '{exchange}'
                  AND timestamp <= '{date}'
            ),
            enriched AS (
                SELECT
                    *,
                    LAG(sma_50, 20) OVER (PARTITION BY symbol_id ORDER BY trade_date) AS sma_50_lag_20
                FROM base
            )
            SELECT *
            FROM enriched
            WHERE trade_date = '{date}'
        """
        latest = conn.execute(query).fetchdf()

        adx_path = os.path.join(feature_store_dir, "adx", exchange)
        atr_path = os.path.join(feature_store_dir, "atr", exchange)
        if os.path.isdir(adx_path):
            adx_df = conn.execute(
                f"""
                SELECT symbol_id, adx_14
                FROM read_parquet('{adx_path}/*.parquet')
                WHERE timestamp <= '{date}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) = 1
                """
            ).fetchdf()
        else:
            adx_df = pd.DataFrame(columns=["symbol_id", "adx_14"])

        if os.path.isdir(atr_path):
            atr_df = conn.execute(
                f"""
                SELECT symbol_id, atr_14
                FROM read_parquet('{atr_path}/*.parquet')
                WHERE timestamp <= '{date}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) = 1
                """
            ).fetchdf()
        else:
            atr_df = pd.DataFrame(columns=["symbol_id", "atr_14"])
    finally:
        conn.close()

    if latest.empty:
        logger.info("Breakout scan found no market snapshot rows for %s", date)
        return _empty_breakout_frame()

    latest = latest.merge(adx_df, on="symbol_id", how="left")
    latest = latest.merge(atr_df, on="symbol_id", how="left")
    symbols = latest["symbol_id"].astype(str).tolist()
    supertrend_df = _load_supertrend_flags(feature_store_dir, symbols, date, exchange=exchange)
    latest = latest.merge(supertrend_df, on="symbol_id", how="left")

    sector_map = _load_sector_map(master_db_path)
    latest.loc[:, "sector"] = latest["symbol_id"].map(sector_map).fillna("Other")

    latest.loc[:, "vol_20_avg"] = latest["vol_20_avg"].replace(0, pd.NA)
    latest.loc[:, "vol_50_avg"] = latest["vol_50_avg"].replace(0, pd.NA)
    latest.loc[:, "breakout_pct"] = (
        (latest["close"] - latest["prior_range_high"]) / latest["prior_range_high"].replace(0, pd.NA) * 100
    )
    latest.loc[:, "range_width_pct"] = (
        (latest["prior_range_high"] - latest["prior_range_low"]) / latest["prior_range_high"].replace(0, pd.NA) * 100
    )
    latest.loc[:, "base_width_pct_30"] = (
        (latest["prior_base_high_30"] - latest["prior_base_low_30"]) / latest["prior_base_high_30"].replace(0, pd.NA) * 100
    )
    latest.loc[:, "base_width_pct_60"] = (
        (latest["prior_base_high_60"] - latest["prior_base_low_60"]) / latest["prior_base_high_60"].replace(0, pd.NA) * 100
    )
    latest.loc[:, "volume_ratio"] = latest["volume"] / latest["vol_20_avg"]
    latest.loc[:, "volume_zscore_20"] = (
        (latest["volume"] - latest["vol_20_avg"]) / latest["vol_20_std"].replace(0, pd.NA)
    )
    latest.loc[:, "volume_zscore_50"] = (
        (latest["volume"] - latest["vol_50_avg"]) / latest["vol_50_std"].replace(0, pd.NA)
    )
    latest.loc[:, "near_52w_high_pct"] = (
        (1 - latest["close"] / latest["high_52w"].replace(0, pd.NA)) * 100
    )
    latest.loc[:, "atr_pct"] = latest["atr_14"] / latest["close"].replace(0, pd.NA) * 100
    latest.loc[:, "day_range_pct"] = (
        (latest["high"] - latest["low"]) / latest["close"].replace(0, pd.NA) * 100
    )
    latest.loc[:, "contraction_ratio"] = latest["range_width_pct"] / latest["base_width_pct_60"].replace(0, pd.NA)
    latest.loc[:, "supertrend_bullish"] = latest["supertrend_dir_10_3"].fillna(-1).eq(1)
    latest.loc[:, "supertrend_flip_up"] = (
        latest["supertrend_dir_10_3"].fillna(-1).eq(1)
        & latest["prev_supertrend_dir_10_3"].fillna(-1).eq(-1)
    )
    latest.loc[:, "adx_14"] = latest["adx_14"].fillna(0.0)
    latest.loc[:, "above_sma_20"] = latest["close"] > latest["sma_20"].fillna(latest["close"])
    latest.loc[:, "above_sma_50"] = latest["close"] > latest["sma_50"].fillna(latest["close"])
    latest.loc[:, "above_sma200"] = latest["close"] > latest["sma_200"].fillna(latest["close"])
    latest.loc[:, "sma50_slope_20d_pct"] = (
        (latest["sma_50"] / latest["sma_50_lag_20"]) - 1.0
    ) * 100.0
    latest.loc[:, "is_range_breakout"] = latest["close"] > latest["prior_range_high"].fillna(float("inf"))
    latest.loc[:, "is_base_breakout_30"] = latest["close"] > latest["prior_base_high_30"].fillna(float("inf"))
    latest.loc[:, "is_base_breakout_60"] = latest["close"] > latest["prior_base_high_60"].fillna(float("inf"))
    latest.loc[:, "is_resistance_breakout_50d"] = latest["close"] > latest["prior_high_50"].fillna(float("inf"))
    latest.loc[:, "is_high_52w_breakout"] = latest["close"] > latest["prior_high_252"].fillna(float("inf"))
    latest.loc[:, "is_range_contraction"] = latest["contraction_ratio"].fillna(999).le(0.7)
    latest.loc[:, "is_consolidation_breakout"] = latest["is_range_breakout"] & latest["is_range_contraction"]
    latest.loc[:, "is_volatility_expansion_breakout"] = (
        latest["is_range_breakout"]
        & latest["day_range_pct"].fillna(0).ge(latest["atr_pct"].fillna(0) * 1.2)
        & latest["atr_pct"].fillna(999).le(5.5)
    )
    volume_confirmation = _volume_confirmation_columns(latest, ratio_threshold=1.5)
    latest.loc[:, "is_volume_ratio_confirmed"] = volume_confirmation["is_volume_ratio_confirmed"]
    latest.loc[:, "is_z20_confirmed"] = volume_confirmation["is_z20_confirmed"]
    latest.loc[:, "is_z50_confirmed"] = volume_confirmation["is_z50_confirmed"]
    latest.loc[:, "is_any_volume_confirmed"] = volume_confirmation["is_any_volume_confirmed"]
    latest.loc[:, "is_strong_volume_confirmation"] = volume_confirmation["is_strong_volume_confirmation"]
    latest.loc[:, "is_volume_confirmed_breakout"] = latest["is_volume_ratio_confirmed"]

    # Legacy families (kept for backward compatibility and migration continuity).
    common_filter = (
        latest["vol_20_avg"].notna()
        & latest["high_52w"].notna()
        & (latest["volume_ratio"].fillna(0) >= min_volume_ratio)
        & (latest["adx_14"] >= min_adx)
        & latest["above_sma_20"]
        & latest["above_sma_50"]
        & latest["supertrend_bullish"]
    )

    base_breakouts = latest[
        common_filter
        & latest["is_base_breakout_30"]
        & latest["prior_base_high_30"].notna()
        & latest["base_width_pct_30"].between(4, 18, inclusive="both")
        & latest["base_width_pct_60"].between(6, 28, inclusive="both")
        & (latest["breakout_pct"].fillna(999) <= 4.0)
        & (latest["near_52w_high_pct"].fillna(999) <= 12.0)
        & (latest["contraction_ratio"].fillna(999) <= 0.9)
    ].copy()
    if not base_breakouts.empty:
        base_breakouts.loc[:, "legacy_setup_family"] = "base_breakout"
        base_breakouts.loc[:, "setup_family"] = "resistance_breakout_50d"
        base_breakouts.loc[:, "taxonomy_family"] = "resistance_breakout_50d"
        base_breakouts.loc[:, "setup_quality"] = (
            base_breakouts["volume_ratio"].clip(0, 4) * 14
            + base_breakouts["adx_14"].clip(0, 60) * 0.6
            + (12 - base_breakouts["near_52w_high_pct"].clip(0, 12)) * 2.2
            + (18 - base_breakouts["base_width_pct_30"].clip(4, 18)) * 1.2
            - base_breakouts["breakout_pct"].clip(0, 4) * 1.5
        )

    contraction_breakouts = latest[
        common_filter
        & latest["is_range_breakout"]
        & latest["prior_range_high"].notna()
        & latest["range_width_pct"].between(2, 12, inclusive="both")
        & latest["base_width_pct_60"].between(8, 30, inclusive="both")
        & (latest["contraction_ratio"].fillna(999) <= 0.7)
        & (latest["breakout_pct"].fillna(999) <= 3.5)
        & (latest["near_52w_high_pct"].fillna(999) <= 10.0)
        & (latest["atr_pct"].fillna(999) <= 5.0)
    ].copy()
    if not contraction_breakouts.empty:
        contraction_breakouts.loc[:, "legacy_setup_family"] = "contraction_breakout"
        contraction_breakouts.loc[:, "setup_family"] = "consolidation_breakout"
        contraction_breakouts.loc[:, "taxonomy_family"] = "consolidation_breakout"
        contraction_breakouts.loc[:, "setup_quality"] = (
            contraction_breakouts["volume_ratio"].clip(0, 4) * 16
            + contraction_breakouts["adx_14"].clip(0, 60) * 0.5
            + (10 - contraction_breakouts["near_52w_high_pct"].clip(0, 10)) * 2.0
            + (0.8 - contraction_breakouts["contraction_ratio"].clip(0, 0.8)) * 30
            - contraction_breakouts["range_width_pct"].clip(2, 12) * 0.8
        )

    supertrend_breakouts = latest[
        common_filter
        & latest["supertrend_flip_up"]
        & latest["is_range_breakout"]
        & latest["prior_range_high"].notna()
        & (latest["breakout_pct"].fillna(999) <= 3.0)
        & latest["range_width_pct"].between(3, 20, inclusive="both")
        & (latest["near_52w_high_pct"].fillna(999) <= 14.0)
    ].copy()
    if not supertrend_breakouts.empty:
        supertrend_breakouts.loc[:, "legacy_setup_family"] = "supertrend_flip_breakout"
        supertrend_breakouts.loc[:, "setup_family"] = "volatility_expansion_breakout"
        supertrend_breakouts.loc[:, "taxonomy_family"] = "volatility_expansion_breakout"
        supertrend_breakouts.loc[:, "setup_quality"] = (
            supertrend_breakouts["volume_ratio"].clip(0, 4) * 12
            + supertrend_breakouts["adx_14"].clip(0, 60) * 0.55
            + (14 - supertrend_breakouts["near_52w_high_pct"].clip(0, 14)) * 1.8
            + supertrend_breakouts["breakout_pct"].clip(0, 3) * 6
        )

    legacy_candidates = pd.concat(
        [base_breakouts, contraction_breakouts, supertrend_breakouts],
        ignore_index=True,
    )

    # Canonical taxonomy candidates.
    taxonomy_filter = (
        latest["high_52w"].notna()
        & latest["prior_high_50"].notna()
        & latest["prior_range_high"].notna()
        & latest["above_sma_20"]
        & latest["above_sma_50"]
    )
    taxonomy_trigger = (
        latest["is_resistance_breakout_50d"]
        | latest["is_high_52w_breakout"]
        | latest["is_consolidation_breakout"]
        | latest["is_volatility_expansion_breakout"]
    )
    canonical_candidates = latest[taxonomy_filter & taxonomy_trigger].copy()
    if not canonical_candidates.empty:
        canonical_candidates.loc[:, "legacy_setup_family"] = pd.NA
        canonical_candidates.loc[:, "taxonomy_family"] = np.select(
            [
                canonical_candidates["is_high_52w_breakout"],
                canonical_candidates["is_consolidation_breakout"],
                canonical_candidates["is_volatility_expansion_breakout"],
                canonical_candidates["is_resistance_breakout_50d"],
            ],
            [
                "high_52w_breakout",
                "consolidation_breakout",
                "volatility_expansion_breakout",
                "resistance_breakout_50d",
            ],
            default="resistance_breakout_50d",
        )
        canonical_candidates.loc[:, "setup_family"] = canonical_candidates["taxonomy_family"]
        canonical_candidates.loc[:, "setup_quality"] = (
            canonical_candidates["volume_ratio"].fillna(0).clip(0, 4) * 10
            + canonical_candidates["adx_14"].fillna(0).clip(0, 60) * 0.45
            + (12 - canonical_candidates["near_52w_high_pct"].fillna(12).clip(0, 12)) * 1.5
            + canonical_candidates["breakout_pct"].fillna(0).clip(0, 5) * 4
        )

    breakout_engine = str(breakout_engine or "v2").strip().lower()
    include_legacy = bool(include_legacy_families)
    if breakout_engine == "legacy":
        candidates = legacy_candidates.copy()
    else:
        if include_legacy and not legacy_candidates.empty:
            candidates = pd.concat([canonical_candidates, legacy_candidates], ignore_index=True)
        else:
            candidates = canonical_candidates.copy()

    if candidates.empty:
        logger.info("Breakout scan found no candidates for %s", date)
        return _empty_breakout_frame()

    candidates = candidates.sort_values("setup_quality", ascending=False)
    candidates = candidates.drop_duplicates("symbol_id", keep="first").reset_index(drop=True)

    rank_context = _prepare_rank_context(ranked_df)
    if not rank_context.empty:
        candidates = candidates.merge(rank_context, on="symbol_id", how="left")
    else:
        candidates.loc[:, "rel_strength_score"] = np.nan
        candidates.loc[:, "sector_rs_value"] = np.nan
        candidates.loc[:, "sector_rs_percentile"] = np.nan

    regime = RegimeDetector(
        ohlcv_db_path=ohlcv_db_path,
        feature_store_dir=feature_store_dir,
    ).get_market_regime(exchange=exchange, date=date)
    market_regime = regime.get("market_regime", "UNKNOWN")
    market_bias = regime.get("market_bias", "UNKNOWN")
    breadth_score = float(regime.get("breadth_score", 0.0) or 0.0)

    if breakout_engine == "legacy":
        candidates.loc[:, "breakout_score"] = np.nan
        candidates.loc[:, "breakout_state"] = "watchlist"
        candidates.loc[:, "filter_reason"] = ""
        candidates.loc[:, "breakout_rank"] = (
            candidates["setup_quality"]
            .rank(method="first", ascending=False)
            .astype(int)
        )
        candidates.loc[:, "market_bias_allowed"] = True
        candidates.loc[:, "breadth_gate_passed"] = True
        candidates.loc[:, "sector_gate_passed"] = True
        candidates.loc[:, "regime_gate_passed"] = True
        candidates.loc[:, "breakout_detected"] = True
        candidates.loc[:, "filtered_by_regime"] = False
        candidates.loc[:, "filtered_by_symbol_trend"] = False
        candidates.loc[:, "candidate_tier"] = "A"
        candidates.loc[:, "symbol_trend_score"] = 100.0
        candidates.loc[:, "symbol_trend_reasons"] = "ABOVE_SMA200,SMA50_SLOPE_POSITIVE,NEAR_52W_HIGH"
        candidates.loc[:, "symbol_trend_fail_count"] = 0
        if "sma50_slope_20d_pct" not in candidates.columns:
            candidates.loc[:, "sma50_slope_20d_pct"] = np.nan
        if "above_sma200" not in candidates.columns:
            candidates.loc[:, "above_sma200"] = True
    else:
        candidates = compute_breakout_v2_scores(
            candidates,
            market_bias=market_bias,
            breadth_score=breadth_score,
            market_bias_allowlist=market_bias_allowlist,
            min_breadth_score=min_breadth_score,
            sector_rs_min=sector_rs_min,
            sector_rs_percentile_min=sector_rs_percentile_min,
            breakout_qualified_min_score=breakout_qualified_min_score,
            breakout_symbol_trend_gate_enabled=breakout_symbol_trend_gate_enabled,
            breakout_symbol_near_high_max_pct=breakout_symbol_near_high_max_pct,
            market_stage=market_stage,
        )

    def _execution_label(row: pd.Series) -> str:
        state = str(row.get("breakout_state", "watchlist"))
        if state in {"filtered_by_regime", "filtered_by_symbol_trend"}:
            return "FILTERED_BREAKOUT"
        if state == "watchlist":
            return "WATCHLIST_BREAKOUT"
        if market_bias == "BEARISH":
            return "RELATIVE_STRENGTH_BREAKOUT"
        if market_bias == "NEUTRAL":
            return "EARLY_BREAKOUT"
        return "ACTIONABLE_BREAKOUT"

    candidates.loc[:, "market_regime"] = market_regime
    candidates.loc[:, "market_bias"] = market_bias
    candidates.loc[:, "execution_label"] = candidates.apply(_execution_label, axis=1)
    candidates.loc[:, "breakout_tag"] = candidates["taxonomy_family"].fillna(candidates["setup_family"])

    cols = [
        "symbol_id",
        "sector",
        "setup_family",
        "legacy_setup_family",
        "taxonomy_family",
        "execution_label",
        "market_regime",
        "market_bias",
        "breakout_detected",
        "filtered_by_regime",
        "filtered_by_symbol_trend",
        "breakout_state",
        "filter_reason",
        "breakout_score",
        "breakout_rank",
        "candidate_tier",
        "symbol_trend_score",
        "symbol_trend_reasons",
        "symbol_trend_fail_count",
        "close",
        "prior_range_high",
        "breakout_pct",
        "base_width_pct_30",
        "base_width_pct_60",
        "contraction_ratio",
        "volume_ratio",
        "volume_zscore_20",
        "volume_zscore_50",
        "is_volume_ratio_confirmed",
        "is_z20_confirmed",
        "is_z50_confirmed",
        "is_any_volume_confirmed",
        "is_any_volume_confirmed_breakout",
        "is_strong_volume_confirmation",
        "adx_14",
        "near_52w_high_pct",
        "sma50_slope_20d_pct",
        "above_sma200",
        "range_width_pct",
        "supertrend_dir_10_3",
        "prev_supertrend_dir_10_3",
        "setup_quality",
        "breakout_tag",
        "rel_strength_score",
        "sector_rs_value",
        "sector_rs_percentile",
        "is_resistance_breakout_50d",
        "is_high_52w_breakout",
        "is_consolidation_breakout",
        "is_volatility_expansion_breakout",
        "is_volume_confirmed_breakout",
        "market_bias_allowed",
        "breadth_gate_passed",
        "sector_gate_passed",
        "regime_gate_passed",
    ]
    available_cols = [col for col in cols if col in candidates.columns]
    return (
        candidates[available_cols]
        .sort_values(["breakout_rank", "setup_quality"], ascending=[True, False], na_position="last")
        .head(top_n)
        .reset_index(drop=True)
    )
