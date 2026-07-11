"""Early accumulation sidecar scoring for emerging winner discovery."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd


POSITIVE_PATTERN_BASE_SCORES: dict[str, float] = {
    "pocket_pivot": 95.0,
    "vcp": 92.0,
    "high_tight_flag": 90.0,
    "flat_base": 88.0,
    "stage2_reclaim": 88.0,
    "darvas_box": 85.0,
    "cup_handle": 85.0,
    "inside_week_breakout": 82.0,
    "flag": 78.0,
    "round_bottom": 75.0,
    "double_bottom": 75.0,
    "ipo_base": 70.0,
    "three_weeks_tight": 78.0,
}
DIAGNOSTIC_PATTERN_FAMILIES = {"head_shoulders"}
SCORE_WEIGHTS = {
    "base_pattern_freshness_score": 0.24,
    "above_200dma_reclaim_score": 0.18,
    "delivery_accumulation_score": 0.15,
    "momentum_recovery_score": 0.14,
    "trend_repair_score": 0.12,
    "volume_confirmation_score": 0.10,
    "relative_strength_score_early": 0.07,
}
OUTPUT_COLUMNS = [
    "symbol_id",
    "exchange",
    "date",
    "close",
    "sector_name",
    "early_accumulation_rank",
    "early_accumulation_score",
    "base_pattern_freshness_score",
    "above_200dma_reclaim_score",
    "delivery_accumulation_score",
    "momentum_recovery_score",
    "trend_repair_score",
    "volume_confirmation_score",
    "relative_strength_score_early",
    "top_pattern_family",
    "top_pattern_state",
    "top_pattern_signal_date",
    "top_pattern_age_days",
    "pattern_count_60d",
    "head_shoulders_diagnostic",
    "sma_200",
    "close_vs_sma200_pct",
    "sma200_slope_20d_pct",
    "days_since_200dma_reclaim",
    "trend_score",
    "adx_14",
    "sma50_slope_20d_pct",
    "sma_20",
    "sma_50",
    "sma_150",
    "sma150_slope_20d_pct",
    "sma50_sma200_gap_pct",
    "sma50_sma200_gap_delta_5d",
    "sma50_sma200_gap_delta_20d",
    "sma50_sma200_gap_delta_60d",
    "sma20_sma50_gap_pct",
    "sma20_sma50_gap_delta_10d",
    "golden_cross_days_since",
    "golden_cross_failed",
    "sector_strength_score",
    "liquidity_score",
    "weekly_stage_label",
    "distribution_flag",
    "delivery_pct",
    "delivery_pct_score",
    "delivery_pct_imputed",
    "volume_ratio_20",
    "volume_zscore_20",
    "momentum_acceleration",
    "return_20",
    "return_60",
    "rel_strength_score",
    "active_rank",
    "active_rank_pctile",
    "early_purity_bucket",
    "composite_score",
    "composite_score_adjusted",
    "breakout_state",
    "breakout_qualified",
    "breakout_type",
    "breakout_score",
    "graduation_status",
    "watchlist_reason",
]
BUCKET_PRIORITY = {
    "true_early": 0,
    "emerging": 1,
    "near_graduation": 2,
    "already_graduated": 3,
    "unknown_rank_context": 4,
}


@dataclass(frozen=True)
class EarlyAccumulationConfig:
    enabled: bool = True
    top_n: int = 150
    min_score: float = 45.0
    pattern_max_age_days: int = 60
    pattern_lookback_days: int = 120
    require_liquidity: bool = True
    min_price: float = 20.0
    min_avg_value_traded: float | None = None
    exclude_illiquid: bool = True
    preview_top_n: int = 10
    true_early_top_n: int = 50
    emerging_top_n: int = 50
    near_graduation_top_n: int = 30
    already_graduated_top_n: int = 20
    min_true_early_rows: int = 25
    min_emerging_rows: int = 25

    @classmethod
    def from_params(cls, params: dict[str, Any] | None) -> "EarlyAccumulationConfig":
        params = params or {}

        def _bool(name: str, default: bool) -> bool:
            raw = params.get(name, default)
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

        min_avg_raw = params.get("early_accumulation_min_avg_value_traded")
        min_avg = None if min_avg_raw in (None, "") else float(min_avg_raw)
        return cls(
            enabled=_bool("early_accumulation_enabled", True),
            top_n=int(params.get("early_accumulation_top_n", 150) or 150),
            min_score=float(params.get("early_accumulation_min_score", 45.0) or 45.0),
            pattern_max_age_days=int(params.get("early_accumulation_pattern_max_age_days", 60) or 60),
            pattern_lookback_days=int(params.get("early_accumulation_pattern_lookback_days", 120) or 120),
            require_liquidity=_bool("early_accumulation_require_liquidity", True),
            min_price=float(params.get("early_accumulation_min_price", 20.0) or 20.0),
            min_avg_value_traded=min_avg,
            exclude_illiquid=_bool("early_accumulation_exclude_illiquid", True),
            preview_top_n=int(params.get("early_accumulation_preview_top_n", 10) or 10),
            true_early_top_n=int(params.get("early_accumulation_true_early_top_n", 50) or 50),
            emerging_top_n=int(params.get("early_accumulation_emerging_top_n", 50) or 50),
            near_graduation_top_n=int(params.get("early_accumulation_near_graduation_top_n", 30) or 30),
            already_graduated_top_n=int(params.get("early_accumulation_already_graduated_top_n", 20) or 20),
            min_true_early_rows=int(params.get("early_accumulation_min_true_early_rows", 25) or 25),
            min_emerging_rows=int(params.get("early_accumulation_min_emerging_rows", 25) or 25),
        )


def empty_early_accumulation_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def pattern_age_multiplier(age_days: float | int | None) -> float:
    if age_days is None or pd.isna(age_days) or float(age_days) < 0:
        return 0.0
    age = float(age_days)
    if age <= 5:
        return 1.0
    if age <= 20:
        return 0.80
    if age <= 40:
        return 0.55
    if age <= 60:
        return 0.30
    return 0.0


def _num(frame: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _text(frame: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="object")
    return frame[column].fillna(default).astype(str)


def _clip(series: pd.Series | np.ndarray | float, lower: float = 0.0, upper: float = 100.0) -> pd.Series:
    return pd.Series(series).astype(float).clip(lower=lower, upper=upper)


def _normalize_symbol_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol_id"])
    output = frame.copy()
    if "symbol_id" not in output.columns:
        for candidate in ("Symbol", "symbol", "index"):
            if candidate in output.columns:
                output.loc[:, "symbol_id"] = output[candidate]
                break
    if "symbol_id" not in output.columns:
        return pd.DataFrame(columns=["symbol_id"])
    output.loc[:, "symbol_id"] = output["symbol_id"].astype(str).str.upper()
    if "exchange" in output.columns:
        output.loc[:, "exchange"] = output["exchange"].fillna("NSE").astype(str)
    else:
        output.loc[:, "exchange"] = "NSE"
    return output


def _pattern_summary(
    pattern_df: pd.DataFrame | None,
    *,
    as_of_date: str,
    max_age_days: int,
) -> pd.DataFrame:
    frame = _normalize_symbol_frame(pattern_df)
    columns = [
        "symbol_id",
        "exchange",
        "base_pattern_freshness_score",
        "top_pattern_family",
        "top_pattern_state",
        "top_pattern_signal_date",
        "top_pattern_age_days",
        "pattern_count_60d",
        "head_shoulders_diagnostic",
    ]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    family = _text(frame, "pattern_family").str.lower().str.strip()
    signal_date = pd.to_datetime(
        frame.get("fresh_signal_date", frame.get("signal_date", frame.get("as_of_date"))),
        errors="coerce",
    )
    age_days = (pd.Timestamp(as_of_date) - signal_date).dt.days
    frame = frame.assign(_family=family, _signal_date=signal_date, _age_days=age_days)
    frame = frame.loc[frame["_age_days"].notna() & (frame["_age_days"] >= 0) & (frame["_age_days"] <= max_age_days)].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)

    frame.loc[:, "_is_positive"] = frame["_family"].isin(POSITIVE_PATTERN_BASE_SCORES)
    frame.loc[:, "_is_head_shoulders"] = frame["_family"].isin(DIAGNOSTIC_PATTERN_FAMILIES)
    frame.loc[:, "_base_score"] = frame["_family"].map(POSITIVE_PATTERN_BASE_SCORES).fillna(0.0)
    frame.loc[:, "_decay"] = frame["_age_days"].map(pattern_age_multiplier)
    frame.loc[:, "_decayed_score"] = frame["_base_score"] * frame["_decay"]

    rows: list[dict[str, Any]] = []
    for (symbol_id, exchange), group in frame.groupby(["symbol_id", "exchange"], dropna=False):
        positives = group.loc[group["_is_positive"]].copy()
        head_shoulders = bool(group["_is_head_shoulders"].any())
        if positives.empty:
            rows.append(
                {
                    "symbol_id": symbol_id,
                    "exchange": exchange,
                    "base_pattern_freshness_score": 0.0,
                    "top_pattern_family": None,
                    "top_pattern_state": None,
                    "top_pattern_signal_date": None,
                    "top_pattern_age_days": np.nan,
                    "pattern_count_60d": 0,
                    "head_shoulders_diagnostic": head_shoulders,
                }
            )
            continue
        positives = positives.sort_values(
            ["_decayed_score", "_base_score", "_signal_date", "symbol_id"],
            ascending=[False, False, False, True],
            kind="stable",
        )
        best = positives.iloc[0]
        pattern_count = int(len(positives))
        score = min(100.0, float(best["_decayed_score"]) + min(10.0, 3.0 * max(0, pattern_count - 1)))
        rows.append(
            {
                "symbol_id": symbol_id,
                "exchange": exchange,
                "base_pattern_freshness_score": score,
                "top_pattern_family": best.get("pattern_family"),
                "top_pattern_state": best.get("pattern_state", best.get("pattern_lifecycle_state")),
                "top_pattern_signal_date": pd.Timestamp(best["_signal_date"]).date().isoformat(),
                "top_pattern_age_days": int(best["_age_days"]),
                "pattern_count_60d": pattern_count,
                "head_shoulders_diagnostic": head_shoulders,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _breakout_summary(breakout_df: pd.DataFrame | None) -> pd.DataFrame:
    frame = _normalize_symbol_frame(breakout_df)
    columns = ["symbol_id", "exchange", "breakout_state", "breakout_qualified", "breakout_type", "breakout_score"]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    if "breakout_state" not in frame.columns:
        frame.loc[:, "breakout_state"] = ""
    if "breakout_type" not in frame.columns:
        frame.loc[:, "breakout_type"] = frame.get("candidate_tier", "")
    if "breakout_score" not in frame.columns:
        frame.loc[:, "breakout_score"] = np.nan
    if "breakout_qualified" not in frame.columns:
        qualified = frame.get("qualified", pd.Series(False, index=frame.index))
        if pd.api.types.is_bool_dtype(qualified):
            qualified = qualified.fillna(False).astype(bool)
        else:
            qualified = qualified.astype(str).str.lower().isin({"1", "true", "yes", "y", "qualified"})
        frame.loc[:, "breakout_qualified"] = (
            frame["breakout_state"].fillna("").astype(str).str.lower().eq("qualified")
            | qualified
        )
    score = _num(frame, "breakout_score", 0.0)
    frame = frame.assign(_score=score).sort_values(
        ["symbol_id", "_score"], ascending=[True, False], kind="stable"
    )
    return frame.drop_duplicates(["symbol_id", "exchange"], keep="first")[columns]


def _zone_score(close_vs_sma200_pct: pd.Series) -> pd.Series:
    pct = pd.to_numeric(close_vs_sma200_pct, errors="coerce")
    score = pd.Series(50.0, index=pct.index, dtype="float64")
    score = score.mask((pct >= -5.0) & (pct <= 25.0), 100.0)
    score = score.mask((pct < -5.0) & (pct >= -15.0), 70.0 + (pct + 15.0) * 3.0)
    score = score.mask((pct < -15.0), 25.0 + (pct + 30.0).clip(lower=0.0, upper=15.0) * 3.0)
    score = score.mask((pct > 25.0) & (pct <= 50.0), 100.0 - (pct - 25.0) * 2.0)
    score = score.mask((pct > 50.0), 35.0)
    return score.clip(0.0, 100.0)


def _reclaim_freshness_score(frame: pd.DataFrame, close_vs_sma200_pct: pd.Series) -> pd.Series:
    days = _num(frame, "days_since_200dma_reclaim")
    fresh = pd.Series(55.0, index=frame.index, dtype="float64")
    fresh = fresh.mask(days.between(0, 5, inclusive="both"), 100.0)
    fresh = fresh.mask(days.between(6, 20, inclusive="both"), 85.0)
    fresh = fresh.mask(days.between(21, 60, inclusive="both"), 65.0)
    fresh = fresh.mask(days > 60, 45.0)
    fresh = fresh.mask(days.isna() & (close_vs_sma200_pct >= 0), 60.0)
    fresh = fresh.mask(days.isna() & (close_vs_sma200_pct < 0), 40.0)
    return fresh.clip(0.0, 100.0)


def _missing_column(frame: pd.DataFrame, column: str) -> bool:
    return column not in frame.columns or pd.to_numeric(frame[column], errors="coerce").notna().sum() == 0


def _warn_missing(
    frame: pd.DataFrame,
    column: str,
    missing_input_columns: list[str],
    warnings: list[str],
    message: str,
) -> None:
    if _missing_column(frame, column):
        missing_input_columns.append(column)
        warnings.append(message)


def _delivery_score(frame: pd.DataFrame, warnings: list[str]) -> pd.Series:
    if "delivery_pct" not in frame.columns and "delivery_pct_latest" in frame.columns:
        delivery = _num(frame, "delivery_pct_latest")
    else:
        delivery = _num(frame, "delivery_pct")
    if delivery.notna().sum() == 0:
        warnings.append("delivery_pct unavailable for early_accumulation_scan; neutral score applied")
        return pd.Series(50.0, index=frame.index, dtype="float64")
    pctile = delivery.rank(pct=True) * 100.0
    score = pctile.fillna(50.0)
    imputed = _imputed_mask(frame)
    confirmation = (_num(frame, "volume_zscore_20", 0.0) >= 1.5) | (_num(frame, "base_pattern_freshness_score", 0.0) >= 70.0)
    return score.mask(imputed & ~confirmation, score.clip(upper=55.0)).clip(0.0, 100.0)


def _imputed_mask(frame: pd.DataFrame) -> pd.Series:
    for column in ("delivery_pct_imputed", "delivery_pct_filled"):
        if column in frame.columns:
            raw = frame[column]
            if pd.api.types.is_bool_dtype(raw):
                return raw.fillna(False).astype(bool)
            if pd.api.types.is_numeric_dtype(raw):
                return pd.to_numeric(raw, errors="coerce").fillna(0).astype(float) > 0
            return raw.astype(str).str.lower().isin({"1", "true", "yes", "y"})
    return _num(frame, "delivery_pct").isna()


def _momentum_score(frame: pd.DataFrame) -> pd.Series:
    ret20 = _num(frame, "return_20", 0.0).fillna(0.0)
    ret60 = _num(frame, "return_60", 0.0).fillna(0.0)
    accel = _num(frame, "momentum_acceleration", 0.0).fillna(0.0)
    recovery = 50.0 + ret20.clip(-20, 35) * 1.2 + accel.clip(-20, 25) * 1.1
    recovery += np.where((ret20 > 0) & (ret60 < ret20), 10.0, 0.0)
    recovery -= np.where(ret20 > 60.0, 25.0, 0.0)
    return pd.Series(recovery, index=frame.index).clip(0.0, 100.0)


def _trend_repair_score(frame: pd.DataFrame) -> pd.Series:
    trend = _num(frame, "trend_score", 50.0).fillna(50.0)
    adx = _num(frame, "adx_14", 20.0).fillna(20.0).clip(0.0, 50.0) / 50.0 * 100.0
    sma50_slope = _num(frame, "sma50_slope_20d_pct", 0.0).fillna(0.0)
    above50 = (_num(frame, "close") > _num(frame, "sma_50")).fillna(False)
    above200 = (_num(frame, "close") > _num(frame, "sma_200")).fillna(False)
    slope_score = (50.0 + sma50_slope.clip(-10, 10) * 4.0).clip(0.0, 100.0)
    alignment = above50.astype(float) * 55.0 + above200.astype(float) * 45.0
    return (trend * 0.35 + adx * 0.20 + slope_score * 0.25 + alignment * 0.20).clip(0.0, 100.0)


def _volume_score(frame: pd.DataFrame) -> pd.Series:
    ratio = frame.get("volume_ratio_20", frame.get("vol_intensity", frame.get("volume_intensity_normalized")))
    ratio = pd.to_numeric(ratio, errors="coerce").fillna(1.0) if ratio is not None else pd.Series(1.0, index=frame.index)
    zscore = _num(frame, "volume_zscore_20", 0.0).fillna(0.0)
    score = 45.0 + ratio.clip(0.0, 4.0) * 12.0 + zscore.clip(-2.0, 5.0) * 6.0
    score += np.where(_text(frame, "top_pattern_family").str.lower().eq("pocket_pivot"), 12.0, 0.0)
    return pd.Series(score, index=frame.index).clip(0.0, 100.0)


def _relative_strength_score(frame: pd.DataFrame) -> pd.Series:
    if "rel_strength_score" in frame.columns:
        return _num(frame, "rel_strength_score", 50.0).fillna(50.0).clip(0.0, 100.0)
    if "relative_strength" in frame.columns:
        return _num(frame, "relative_strength", 50.0).rank(pct=True).fillna(0.5) * 100.0
    return pd.Series(50.0, index=frame.index, dtype="float64")


def _watchlist_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    family = row.get("top_pattern_family")
    if family:
        reasons.append(f"{family} pattern")
    if float(row.get("above_200dma_reclaim_score") or 0.0) >= 75.0:
        reasons.append("200DMA reclaim")
    if float(row.get("delivery_accumulation_score") or 0.0) >= 75.0:
        reasons.append("delivery accumulation")
    if float(row.get("volume_confirmation_score") or 0.0) >= 75.0:
        reasons.append("volume confirmation")
    return "; ".join(reasons[:3]) or "early accumulation score"


def _graduation_status(row: pd.Series) -> str:
    if bool(row.get("breakout_qualified")):
        return "breakout_qualified"
    if float(row.get("active_rank_pctile") or 0.0) >= 70.0:
        return "active_rank_graduated"
    if float(row.get("base_pattern_freshness_score") or 0.0) >= 70.0:
        return "pattern_confirmed"
    if float(row.get("above_200dma_reclaim_score") or 0.0) >= 70.0:
        return "reclaim_confirmed"
    return "early_watchlist"


def _early_purity_bucket(row: pd.Series) -> str:
    if bool(row.get("breakout_qualified")):
        return "already_graduated"
    pctile = row.get("active_rank_pctile")
    if pctile is None or pd.isna(pctile):
        return "unknown_rank_context"
    pctile = float(pctile)
    if pctile < 50.0:
        return "true_early"
    if pctile < 70.0:
        return "emerging"
    if pctile < 85.0:
        return "near_graduation"
    return "already_graduated"


def _distribution(series: pd.Series) -> dict[str, float | None]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {"min": None, "median": None, "mean": None, "max": None}
    return {
        "min": float(values.min()),
        "median": float(values.median()),
        "mean": float(values.mean()),
        "max": float(values.max()),
    }


def _select_bucketed(frame: pd.DataFrame, config: EarlyAccumulationConfig, warnings: list[str]) -> pd.DataFrame:
    scored = frame.sort_values(["early_accumulation_score", "symbol_id"], ascending=[False, True], kind="stable")
    filtered = scored.loc[scored["early_accumulation_score"] >= config.min_score].copy()
    if filtered.empty and not scored.empty:
        warnings.append("no candidates passed early_accumulation_min_score; outputting top candidates by score")
        filtered = scored.head(config.top_n).copy()
    caps = {
        "true_early": int(config.true_early_top_n),
        "emerging": int(config.emerging_top_n),
        "near_graduation": int(config.near_graduation_top_n),
        "already_graduated": int(config.already_graduated_top_n),
        "unknown_rank_context": int(config.true_early_top_n),
    }
    pieces = []
    for bucket in BUCKET_PRIORITY:
        bucket_frame = filtered.loc[filtered["early_purity_bucket"].eq(bucket)].copy()
        if bucket_frame.empty:
            continue
        pieces.append(bucket_frame.head(caps.get(bucket, config.top_n)))
    selected = pd.concat(pieces, ignore_index=True) if pieces else filtered.head(config.top_n).copy()
    selected.loc[:, "_bucket_priority"] = selected["early_purity_bucket"].map(BUCKET_PRIORITY).fillna(99)
    selected = selected.sort_values(
        ["_bucket_priority", "early_accumulation_score", "symbol_id"],
        ascending=[True, False, True],
        kind="stable",
    ).head(config.top_n)
    return selected.drop(columns=["_bucket_priority"], errors="ignore").reset_index(drop=True)


def _apply_liquidity_filters(frame: pd.DataFrame, config: EarlyAccumulationConfig, warnings: list[str]) -> pd.DataFrame:
    filtered = frame.copy()
    filtered = filtered.loc[_num(filtered, "close", 0.0).fillna(0.0) >= config.min_price].copy()
    if not config.require_liquidity and not config.exclude_illiquid:
        return filtered
    if config.min_avg_value_traded is not None:
        if "avg_value_traded_20" in filtered.columns:
            value = _num(filtered, "avg_value_traded_20", 0.0).fillna(0.0)
        elif "vol_20_avg" in filtered.columns:
            value = _num(filtered, "vol_20_avg", 0.0).fillna(0.0) * _num(filtered, "close", 0.0).fillna(0.0)
        else:
            warnings.append("avg traded value unavailable for early_accumulation liquidity filter")
            value = pd.Series(config.min_avg_value_traded, index=filtered.index)
        filtered = filtered.loc[value >= float(config.min_avg_value_traded)].copy()
    if config.exclude_illiquid and "liquidity_score" in filtered.columns:
        filtered = filtered.loc[_num(filtered, "liquidity_score", 1.0).fillna(0.0) > 0.0].copy()
    return filtered


def build_early_accumulation_scan(
    *,
    ranked_universe: pd.DataFrame,
    pattern_df: pd.DataFrame | None = None,
    breakout_df: pd.DataFrame | None = None,
    as_of_date: str,
    config: EarlyAccumulationConfig | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    config = config or EarlyAccumulationConfig()
    warnings: list[str] = []
    missing_input_columns: list[str] = []
    if not config.enabled:
        return empty_early_accumulation_frame(), {
            "enabled": False,
            "rows": 0,
            "warnings": ["early_accumulation disabled by config"],
            "missing_input_columns": [],
            "config": asdict(config),
        }

    ranked = _normalize_symbol_frame(ranked_universe)
    if ranked.empty:
        return empty_early_accumulation_frame(), {
            "enabled": True,
            "rows": 0,
            "warnings": ["ranked_universe is empty"],
            "missing_input_columns": ["ranked_universe"],
            "config": asdict(config),
        }
    ranked = ranked.copy()
    ranked.loc[:, "date"] = as_of_date
    if "sector_name" not in ranked.columns and "sector" in ranked.columns:
        ranked.loc[:, "sector_name"] = ranked["sector"]

    ranked = _apply_liquidity_filters(ranked, config, warnings)
    if ranked.empty:
        return empty_early_accumulation_frame(), {
            "enabled": True,
            "rows": 0,
            "warnings": warnings,
            "missing_input_columns": missing_input_columns,
            "config": asdict(config),
        }
    for column, message in (
        ("sma_200", "sma_200 missing; above_200dma_reclaim_score used neutral fallback"),
        ("sma50_slope_20d_pct", "sma50_slope_20d_pct missing; trend repair used neutral slope"),
        ("days_since_200dma_reclaim", "days_since_200dma_reclaim unavailable; reclaim freshness set neutral"),
        ("adx_14", "adx_14 missing; trend repair used neutral ADX"),
    ):
        _warn_missing(ranked, column, missing_input_columns, warnings, message)

    pattern = _pattern_summary(pattern_df, as_of_date=as_of_date, max_age_days=config.pattern_max_age_days)
    breakout = _breakout_summary(breakout_df)
    if breakout.empty:
        warnings.append("breakout_scan artifact missing; breakout_qualified set false")
        missing_input_columns.extend(["breakout_state", "breakout_qualified", "breakout_type", "breakout_score"])
    frame = ranked.merge(pattern, on=["symbol_id", "exchange"], how="left")
    frame = frame.merge(breakout, on=["symbol_id", "exchange"], how="left")
    for column, default in (
        ("base_pattern_freshness_score", 0.0),
        ("pattern_count_60d", 0),
        ("head_shoulders_diagnostic", False),
    ):
        if column not in frame.columns:
            frame.loc[:, column] = default
        frame.loc[:, column] = frame[column].mask(frame[column].isna(), default)
    for column, default in (
        ("breakout_state", ""),
        ("breakout_qualified", False),
        ("breakout_type", pd.NA),
        ("breakout_score", pd.NA),
    ):
        if column not in frame.columns:
            frame.loc[:, column] = default
        frame.loc[:, column] = frame[column].mask(frame[column].isna(), default)
    frame.loc[:, "breakout_qualified"] = frame["breakout_qualified"].astype(bool)

    close = _num(frame, "close")
    sma200 = _num(frame, "sma_200")
    close_vs_sma200 = ((close / sma200.replace(0, np.nan)) - 1.0) * 100.0
    if "above_200dma_pct" in frame.columns:
        close_vs_sma200 = close_vs_sma200.fillna(_num(frame, "above_200dma_pct"))
    frame.loc[:, "close_vs_sma200_pct"] = close_vs_sma200
    slope_score = (50.0 + _num(frame, "sma200_slope_20d_pct", 0.0).fillna(0.0).clip(-10, 10) * 5.0).clip(0.0, 100.0)
    frame.loc[:, "above_200dma_reclaim_score"] = (
        _zone_score(frame["close_vs_sma200_pct"].fillna(0.0)) * 0.40
        + slope_score * 0.30
        + _reclaim_freshness_score(frame, frame["close_vs_sma200_pct"].fillna(0.0)) * 0.30
    ).clip(0.0, 100.0)
    frame.loc[:, "delivery_pct_imputed"] = _imputed_mask(frame)
    frame.loc[:, "delivery_accumulation_score"] = _delivery_score(frame, warnings)
    frame.loc[:, "momentum_recovery_score"] = _momentum_score(frame)
    frame.loc[:, "trend_repair_score"] = _trend_repair_score(frame)
    frame.loc[:, "volume_confirmation_score"] = _volume_score(frame)
    frame.loc[:, "relative_strength_score_early"] = _relative_strength_score(frame)

    if "volume_ratio_20" not in frame.columns:
        frame.loc[:, "volume_ratio_20"] = frame.get("vol_intensity", frame.get("volume_intensity_normalized", 1.0))
    if "delivery_pct_score" not in frame.columns:
        frame.loc[:, "delivery_pct_score"] = frame["delivery_accumulation_score"]
    if "active_rank" not in frame.columns:
        rank_source = _num(frame, "rank")
        frame.loc[:, "active_rank"] = rank_source
    if _missing_column(frame, "active_rank"):
        missing_input_columns.append("active_rank")
        warnings.append("active_rank unavailable; active rank context left null")
    if "active_rank_pctile" in frame.columns and _num(frame, "active_rank_pctile").notna().sum() > 0:
        frame.loc[:, "active_rank_pctile"] = _num(frame, "active_rank_pctile")
    else:
        score_source = _num(frame, "composite_score_adjusted").fillna(_num(frame, "composite_score"))
        if score_source.notna().sum() > 0:
            frame.loc[:, "active_rank_pctile"] = score_source.rank(pct=True) * 100.0
            missing_input_columns.append("active_rank_pctile")
            warnings.append("active rank percentile missing; computed from composite_score")
        else:
            frame.loc[:, "active_rank_pctile"] = np.nan
            missing_input_columns.extend(["active_rank_pctile", "composite_score"])
            warnings.append("active rank percentile and composite_score missing; early_purity_bucket set unknown_rank_context")

    score = pd.Series(0.0, index=frame.index, dtype="float64")
    for column, weight in SCORE_WEIGHTS.items():
        score = score + _num(frame, column, 0.0).fillna(0.0).clip(0.0, 100.0) * weight
    frame.loc[:, "early_accumulation_score"] = score.clip(0.0, 100.0)
    frame.loc[:, "early_purity_bucket"] = frame.apply(_early_purity_bucket, axis=1)
    frame.loc[:, "graduation_status"] = frame.apply(_graduation_status, axis=1)
    frame.loc[:, "watchlist_reason"] = frame.apply(_watchlist_reason, axis=1)

    filtered = frame.loc[frame["early_accumulation_score"] >= config.min_score].copy()
    selected = _select_bucketed(frame, config, warnings)
    selected.loc[:, "early_accumulation_rank"] = np.arange(1, len(selected) + 1)
    bucket_counts = selected["early_purity_bucket"].astype(str).value_counts().to_dict() if not selected.empty else {}
    if int(bucket_counts.get("true_early", 0)) < int(config.min_true_early_rows):
        warnings.append("true_early candidates below configured minimum; review min_score or input coverage")
    if int(bucket_counts.get("emerging", 0)) < int(config.min_emerging_rows):
        warnings.append("emerging candidates below configured minimum; review min_score or input coverage")
    if selected.empty:
        warnings.append("early_accumulation_scan produced no candidates")
    elif int(bucket_counts.get("already_graduated", 0)) / max(1, len(selected)) >= 0.8:
        warnings.append("output dominated by already_graduated candidates; lower min_score or adjust bucket caps")

    for column in OUTPUT_COLUMNS:
        if column not in selected.columns:
            selected.loc[:, column] = pd.NA
    output = selected[OUTPUT_COLUMNS].reset_index(drop=True)
    status_counts = output["graduation_status"].astype(str).value_counts().to_dict() if not output.empty else {}
    deduped_missing = sorted(set(str(column) for column in missing_input_columns))
    after_score_count = int(len(filtered))
    summary = {
        "enabled": True,
        "rows": int(len(output)),
        "candidate_rows_before_score_filter": int(len(frame)),
        "candidate_rows_after_score_filter": after_score_count,
        "min_score": float(config.min_score),
        "top_n": int(config.top_n),
        "bucket_counts": {str(k): int(v) for k, v in bucket_counts.items()},
        "graduation_status_counts": {str(k): int(v) for k, v in status_counts.items()},
        "score_distribution": _distribution(output.get("early_accumulation_score", pd.Series(dtype=float))),
        "active_rank_pctile_distribution": _distribution(output.get("active_rank_pctile", pd.Series(dtype=float))),
        "missing_input_columns": deduped_missing,
        "warnings": warnings,
        "artifact_path": None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": asdict(config),
    }
    return output, summary


def early_accumulation_preview(frame: pd.DataFrame, *, limit: int = 10) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    columns = [
        "symbol_id",
        "exchange",
        "close",
        "sector_name",
        "early_accumulation_rank",
        "early_accumulation_score",
        "early_purity_bucket",
        "top_pattern_family",
        "top_pattern_age_days",
        "graduation_status",
        "watchlist_reason",
    ]
    available = [column for column in columns if column in frame.columns]
    return frame[available].head(int(limit)).to_dict(orient="records")
