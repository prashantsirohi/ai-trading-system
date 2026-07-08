"""Rank-stage context enrichment for Investigator candidates."""

from __future__ import annotations

from typing import Any

import pandas as pd


STAGE_CONTEXT_COLUMNS = [
    "stage_label",
    "stage_score",
    "stage_reason",
    "price_above_sma50",
    "price_above_sma200",
    "sma50_slope_positive",
    "sma200_slope_positive",
    "near_52w_high_pct",
    "base_age_days",
    "volume_dryup_flag",
    "accumulation_flag",
    "distribution_flag",
]
PATTERN_CONTEXT_COLUMNS = [
    "pattern_family",
    "pattern_state",
    "pattern_score",
    "pattern_rank",
    "setup_quality",
    "setup_quality_bucket",
]
BREAKOUT_CONTEXT_COLUMNS = [
    "breakout_type",
    "breakout_score",
    "breakout_rank",
    "candidate_tier",
    "qualified_breakout",
    "breakout_state",
]
COMPOSITE_CONTEXT_COLUMNS = [
    "final_score_bucket",
    "composite_score",
    "relative_strength",
    "volume_intensity",
    "trend_persistence",
    "proximity_to_highs",
    "delivery_pct",
    "sector_strength",
]
CONTEXT_COLUMNS = STAGE_CONTEXT_COLUMNS + PATTERN_CONTEXT_COLUMNS + BREAKOUT_CONTEXT_COLUMNS + COMPOSITE_CONTEXT_COLUMNS

PATTERN_DEFAULTS: dict[str, Any] = {
    "pattern_family": "NONE",
    "pattern_state": "NONE",
    "pattern_score": 0.0,
    "pattern_rank": pd.NA,
    "setup_quality": "NONE",
    "setup_quality_bucket": "NONE",
}
BREAKOUT_DEFAULTS: dict[str, Any] = {
    "breakout_type": "NONE",
    "breakout_score": 0.0,
    "breakout_rank": pd.NA,
    "candidate_tier": "NONE",
    "qualified_breakout": False,
    "breakout_state": "NONE",
}


def enrich_investigator_context(
    candidates: pd.DataFrame,
    *,
    ranked: pd.DataFrame | None = None,
    stock_scan: pd.DataFrame | None = None,
    breakout_scan: pd.DataFrame | None = None,
    pattern_scan: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Attach rank-stage stage/pattern/breakout context to Investigator candidates."""

    out = _normalise_symbols(candidates)
    if out.empty:
        for column in CONTEXT_COLUMNS:
            out.loc[:, column] = pd.Series(dtype=object)
        return out, _diagnostics(out, ranked, stock_scan, breakout_scan, pattern_scan)

    rank_context = _rank_context(ranked, stock_scan)
    if not rank_context.empty:
        out = _merge_prefer_right(out, rank_context, on="symbol_id")

    pattern_context = normalise_pattern_context(pattern_scan)
    if not pattern_context.empty:
        out = _merge_prefer_right(out, pattern_context, on="symbol_id")

    breakout_context = normalise_breakout_context(breakout_scan)
    if not breakout_context.empty:
        out = _merge_prefer_right(out, breakout_context, on="symbol_id")

    classified = out.apply(classify_stage_row, axis=1, result_type="expand")
    for column in ("stage_label", "stage_score", "stage_reason"):
        out.loc[:, column] = classified[column]

    _ensure_bool_context(out)
    _ensure_pattern_defaults(out)
    _ensure_breakout_defaults(out)
    _ensure_composite_defaults(out)
    out.loc[:, "final_score_bucket"] = _score_bucket(out.get("composite_score", pd.Series(pd.NA, index=out.index)))
    return out, _diagnostics(out, ranked, stock_scan, breakout_scan, pattern_scan)


def normalise_pattern_context(pattern_scan: pd.DataFrame | None) -> pd.DataFrame:
    """Return one normalized rank-pattern row per symbol."""

    if pattern_scan is None or pattern_scan.empty or "symbol_id" not in pattern_scan.columns:
        return pd.DataFrame(columns=["symbol_id", *PATTERN_CONTEXT_COLUMNS])
    frame = _normalise_symbols(pattern_scan)
    out = pd.DataFrame(index=frame.index)
    out.loc[:, "symbol_id"] = frame["symbol_id"]
    out.loc[:, "pattern_family"] = _first_available(frame, ["pattern_family", "family", "pattern_type", "setup_family"]).map(_pattern_family)
    out.loc[:, "pattern_state"] = _first_available(frame, ["pattern_state", "pattern_lifecycle_state", "state"]).map(_pattern_state)
    out.loc[:, "pattern_score"] = pd.to_numeric(_first_available(frame, ["pattern_score", "score"]), errors="coerce").fillna(0.0)
    out.loc[:, "pattern_rank"] = pd.to_numeric(_first_available(frame, ["pattern_rank", "rank"]), errors="coerce")
    setup_raw = _first_available(frame, ["setup_quality", "quality_score", "setup_score"])
    out.loc[:, "setup_quality"] = setup_raw
    out.loc[:, "setup_quality_bucket"] = setup_raw.map(_setup_quality_bucket)
    out = out.sort_values(["pattern_score", "setup_quality_bucket", "symbol_id"], ascending=[False, True, True], kind="stable")
    return out.drop_duplicates("symbol_id", keep="first").reset_index(drop=True)


def normalise_breakout_context(breakout_scan: pd.DataFrame | None) -> pd.DataFrame:
    """Return one normalized breakout row per symbol."""

    if breakout_scan is None or breakout_scan.empty or "symbol_id" not in breakout_scan.columns:
        return pd.DataFrame(columns=["symbol_id", *BREAKOUT_CONTEXT_COLUMNS])
    frame = _normalise_symbols(breakout_scan)
    out = pd.DataFrame(index=frame.index)
    out.loc[:, "symbol_id"] = frame["symbol_id"]
    out.loc[:, "breakout_type"] = _first_available(
        frame,
        ["breakout_type", "setup_family", "taxonomy_family", "breakout_tag", "execution_label"],
        "NONE",
    ).map(_clean_upper)
    out.loc[:, "breakout_score"] = pd.to_numeric(_first_available(frame, ["breakout_score", "conviction_score"]), errors="coerce").fillna(0.0)
    out.loc[:, "breakout_rank"] = pd.to_numeric(_first_available(frame, ["breakout_rank", "rank"]), errors="coerce")
    out.loc[:, "candidate_tier"] = _first_available(frame, ["candidate_tier"], "NONE").map(_candidate_tier)
    state = _first_available(frame, ["breakout_state", "state"], "NONE").fillna("NONE").astype(str)
    out.loc[:, "breakout_state"] = state.str.strip().replace("", "NONE")
    qualified = _first_available(frame, ["qualified_breakout", "breakout_qualified", "qualified"], False)
    out.loc[:, "qualified_breakout"] = qualified.map(_truthy) | out["breakout_state"].astype(str).str.lower().eq("qualified")
    out = out.sort_values(["qualified_breakout", "candidate_tier", "breakout_score"], ascending=[False, True, False], kind="stable")
    return out.drop_duplicates("symbol_id", keep="first").reset_index(drop=True)


def classify_stage_row(row: pd.Series) -> dict[str, Any]:
    """Classify a symbol into the Investigator market-stage vocabulary."""

    close = _num(row.get("close"))
    sma50 = _num(_first_value(row, ["sma_50", "sma50", "ema_50"]))
    sma200 = _num(_first_value(row, ["sma_200", "sma200", "ema_200"]))
    sma50_slope = _num(row.get("sma50_slope_20d_pct"))
    sma200_slope = _num(row.get("sma200_slope_20d_pct"))
    near_high = _num(row.get("near_52w_high_pct"))
    rel_strength = _num(_first_value(row, ["relative_strength", "rel_strength_score", "relative_strength_score"]))
    distribution = _truthy(row.get("distribution_flag")) or str(row.get("breakout_state") or "").lower() in {"failed", "filtered_by_symbol_trend"}
    dryup_or_base = _truthy(row.get("volume_dryup_flag")) or _truthy(row.get("accumulation_flag")) or pd.notna(_num(row.get("base_age_days")))

    required = [close, sma200, sma50_slope, near_high]
    if any(pd.isna(value) for value in required):
        return {"stage_label": "UNKNOWN", "stage_score": 0.0, "stage_reason": "required stage fields missing"}

    above50 = pd.notna(sma50) and close > sma50
    above200 = close > sma200
    sma50_pos = sma50_slope > 0
    sma200_nonneg = pd.isna(sma200_slope) or sma200_slope >= 0
    weak_rs = pd.notna(rel_strength) and rel_strength < 40
    near_sma200 = abs(close / sma200 - 1.0) <= 0.05 if sma200 else False

    if above50 and above200 and sma50_pos and sma200_nonneg and near_high <= 15:
        label = "STAGE_2_CONFIRMED"
        score = 90.0
    elif above200 and sma50_pos and near_high <= 25:
        label = "STAGE_2_EARLY"
        score = 72.0
    elif close < sma200 and sma50_slope <= 0 and weak_rs:
        label = "STAGE_4_DECLINE"
        score = 20.0
    elif (close >= sma200 * 0.95) and (sma50_slope <= 0 or distribution):
        label = "STAGE_3_DISTRIBUTION"
        score = 40.0
    elif (near_sma200 or dryup_or_base or near_high > 15) and not (close < sma200 and sma50_slope <= 0):
        label = "STAGE_1_BASE"
        score = 55.0
    else:
        label = "UNKNOWN"
        score = 0.0

    reasons = [
        "close>sma50" if above50 else "close<=sma50",
        "close>sma200" if above200 else "close<=sma200",
        "sma50_slope>0" if sma50_pos else "sma50_slope<=0",
        f"near_high={near_high:.1f}%",
    ]
    return {"stage_label": label, "stage_score": score, "stage_reason": "; ".join(reasons)}


def rank_pattern_symbols(pattern_scan: pd.DataFrame | None) -> set[str]:
    if pattern_scan is None or pattern_scan.empty or "symbol_id" not in pattern_scan.columns:
        return set()
    return set(pattern_scan["symbol_id"].fillna("").astype(str).str.strip().str.upper().loc[lambda s: s.ne("")])


def _rank_context(ranked: pd.DataFrame | None, stock_scan: pd.DataFrame | None) -> pd.DataFrame:
    frames = [_normalise_symbols(frame) for frame in (ranked, stock_scan) if frame is not None and not frame.empty and "symbol_id" in frame.columns]
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged = merged.drop_duplicates("symbol_id", keep="last").reset_index(drop=True)
    out = pd.DataFrame(index=merged.index)
    out.loc[:, "symbol_id"] = merged["symbol_id"]
    passthrough = [
        "close",
        "sma_50",
        "sma_200",
        "sma50_slope_20d_pct",
        "sma200_slope_20d_pct",
        "near_52w_high_pct",
        "base_age_days",
        "volume_dryup_flag",
        "accumulation_flag",
        "distribution_flag",
        "delivery_pct",
    ]
    for column in passthrough:
        if column in merged.columns:
            out.loc[:, column] = merged[column]
    out.loc[:, "price_above_sma50"] = _bool_or_compare(merged, "price_above_sma50", "close", "sma_50")
    out.loc[:, "price_above_sma200"] = _bool_or_compare(merged, "price_above_sma200", "close", "sma_200")
    out.loc[:, "sma50_slope_positive"] = _numeric(merged, "sma50_slope_20d_pct").gt(0)
    out.loc[:, "sma200_slope_positive"] = _numeric(merged, "sma200_slope_20d_pct").ge(0)
    out.loc[:, "composite_score"] = pd.to_numeric(_first_available(merged, ["composite_score"]), errors="coerce")
    out.loc[:, "relative_strength"] = pd.to_numeric(_first_available(merged, ["relative_strength", "rel_strength_score", "relative_strength_score"]), errors="coerce")
    out.loc[:, "volume_intensity"] = pd.to_numeric(_first_available(merged, ["volume_intensity", "vol_intensity", "vol_intensity_score"]), errors="coerce")
    out.loc[:, "trend_persistence"] = pd.to_numeric(_first_available(merged, ["trend_persistence", "trend_score", "trend_score_score"]), errors="coerce")
    out.loc[:, "proximity_to_highs"] = pd.to_numeric(_first_available(merged, ["proximity_to_highs", "prox_high", "prox_high_score"]), errors="coerce")
    out.loc[:, "sector_strength"] = pd.to_numeric(_first_available(merged, ["sector_strength", "sector_strength_score", "sector_rs_value"]), errors="coerce")
    return out.drop_duplicates("symbol_id", keep="last").reset_index(drop=True)


def _merge_prefer_right(left: pd.DataFrame, right: pd.DataFrame, *, on: str) -> pd.DataFrame:
    if right.empty:
        return left.copy()
    overlap = [col for col in right.columns if col != on and col in left.columns]
    merged = left.merge(right, on=on, how="left", suffixes=("", "_ctx"))
    for column in overlap:
        ctx = f"{column}_ctx"
        if ctx in merged.columns:
            merged.loc[:, column] = merged[ctx].combine_first(merged[column])
            merged = merged.drop(columns=[ctx])
    return merged


def _diagnostics(
    out: pd.DataFrame,
    ranked: pd.DataFrame | None,
    stock_scan: pd.DataFrame | None,
    breakout_scan: pd.DataFrame | None,
    pattern_scan: pd.DataFrame | None,
) -> dict[str, Any]:
    return {
        "stage_pattern_enabled": True,
        "ranked_signals_joined": bool(ranked is not None and not ranked.empty),
        "pattern_scan_joined": bool(pattern_scan is not None and not pattern_scan.empty),
        "breakout_scan_joined": bool(breakout_scan is not None and not breakout_scan.empty),
        "stock_scan_joined": bool(stock_scan is not None and not stock_scan.empty),
        "candidate_rows": int(len(out)),
        "pattern_matched_rows": int(out.get("pattern_family", pd.Series(dtype=object)).fillna("NONE").ne("NONE").sum()) if not out.empty else 0,
        "breakout_matched_rows": int(out.get("candidate_tier", pd.Series(dtype=object)).fillna("NONE").ne("NONE").sum()) if not out.empty else 0,
        "stage_known_rows": int(out.get("stage_label", pd.Series(dtype=object)).fillna("UNKNOWN").ne("UNKNOWN").sum()) if not out.empty else 0,
        "stage_unknown_rows": int(out.get("stage_label", pd.Series(dtype=object)).fillna("UNKNOWN").eq("UNKNOWN").sum()) if not out.empty else 0,
        "warnings": _warnings(out, ranked, stock_scan, breakout_scan, pattern_scan),
    }


def _warnings(out: pd.DataFrame, ranked: pd.DataFrame | None, stock_scan: pd.DataFrame | None, breakout_scan: pd.DataFrame | None, pattern_scan: pd.DataFrame | None) -> list[str]:
    warnings: list[str] = []
    if pattern_scan is None or pattern_scan.empty:
        warnings.append("pattern_scan missing")
    if breakout_scan is None or breakout_scan.empty:
        warnings.append("breakout_scan missing")
    rank_source = stock_scan if stock_scan is not None and not stock_scan.empty else ranked
    if rank_source is None or rank_source.empty or not {"close", "sma_200", "sma50_slope_20d_pct", "near_52w_high_pct"}.issubset(rank_source.columns):
        warnings.append("required SMA columns missing")
    if not out.empty:
        unknown_pct = out.get("stage_label", pd.Series("UNKNOWN", index=out.index)).fillna("UNKNOWN").eq("UNKNOWN").mean()
        if unknown_pct >= 0.5:
            warnings.append("high UNKNOWN stage percentage")
    return warnings


def _ensure_bool_context(out: pd.DataFrame) -> None:
    for column in ("price_above_sma50", "price_above_sma200", "sma50_slope_positive", "sma200_slope_positive", "volume_dryup_flag", "accumulation_flag", "distribution_flag"):
        if column not in out.columns:
            out.loc[:, column] = pd.NA


def _ensure_pattern_defaults(out: pd.DataFrame) -> None:
    for column, default in PATTERN_DEFAULTS.items():
        if column not in out.columns:
            out.loc[:, column] = default
        else:
            values = out[column].astype(object).where(out[column].notna(), default)
            out.drop(columns=[column], inplace=True)
            out.loc[:, column] = values


def _ensure_breakout_defaults(out: pd.DataFrame) -> None:
    for column, default in BREAKOUT_DEFAULTS.items():
        if column not in out.columns:
            out.loc[:, column] = default
        else:
            values = out[column].astype(object).where(out[column].notna(), default)
            out.drop(columns=[column], inplace=True)
            out.loc[:, column] = values
    out.loc[:, "qualified_breakout"] = out["qualified_breakout"].map(_truthy).fillna(False)


def _ensure_composite_defaults(out: pd.DataFrame) -> None:
    for column in COMPOSITE_CONTEXT_COLUMNS:
        if column not in out.columns:
            out.loc[:, column] = pd.NA


def _normalise_symbols(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    out = frame.copy()
    if "symbol_id" not in out.columns and "symbol" in out.columns:
        out.loc[:, "symbol_id"] = out["symbol"]
    if "symbol_id" in out.columns:
        out.loc[:, "symbol_id"] = out["symbol_id"].fillna("").astype(str).str.strip().str.upper()
        out = out.loc[out["symbol_id"].ne("")].copy()
    return out


def _first_available(frame: pd.DataFrame, columns: list[str], default: Any = pd.NA) -> pd.Series:
    out = pd.Series(default, index=frame.index, dtype=object)
    for column in columns:
        if column in frame.columns:
            out = frame[column].astype(object).combine_first(out)
    return out


def _first_value(row: pd.Series, columns: list[str]) -> Any:
    for column in columns:
        value = row.get(column)
        if pd.notna(value):
            return value
    return pd.NA


def _bool_or_compare(frame: pd.DataFrame, bool_column: str, left: str, right: str) -> pd.Series:
    if bool_column in frame.columns:
        return frame[bool_column].map(_truthy)
    left_values = pd.to_numeric(frame[left] if left in frame.columns else pd.Series(pd.NA, index=frame.index), errors="coerce")
    right_values = pd.to_numeric(frame[right] if right in frame.columns else pd.Series(pd.NA, index=frame.index), errors="coerce")
    return (left_values > right_values).where(left_values.notna() & right_values.notna(), pd.NA)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    source = frame[column] if column in frame.columns else pd.Series(pd.NA, index=frame.index)
    return pd.to_numeric(source, errors="coerce")


def _pattern_family(value: Any) -> str:
    text = _clean_upper(value)
    if not text or text in {"NAN", "NONE"}:
        return "NONE"
    if "CUP" in text:
        return "CUP_HANDLE"
    if "CONSOL" in text or "BASE" in text:
        return "CONSOLIDATION"
    if "ROUND" in text:
        return "ROUNDING_BOTTOM"
    if "FLAG" in text:
        return "FLAG"
    if "DARVAS" in text:
        return "DARVAS"
    if "VOLUME" in text or "SHOCK" in text:
        return "VOLUME_SHOCK"
    return "OTHER"


def _pattern_state(value: Any) -> str:
    text = _clean_upper(value)
    if not text or text in {"NAN", "NONE"}:
        return "NONE"
    if "FAIL" in text or "INVALID" in text:
        return "FAILED"
    if "CONFIRM" in text or text == "CONFIRMED":
        return "CONFIRMED"
    if "BREAK" in text or "S2" in text:
        return "BREAKING_OUT"
    if "FORM" in text or "BASE" in text or "ACCUM" in text:
        return "FORMING"
    return "FORMING"


def _setup_quality_bucket(value: Any) -> str:
    if isinstance(value, str):
        text = _clean_upper(value)
        if text in {"HIGH", "MEDIUM", "LOW", "NONE"}:
            return text
    numeric = _num(value)
    if pd.isna(numeric):
        return "NONE"
    if numeric >= 70:
        return "HIGH"
    if numeric >= 45:
        return "MEDIUM"
    return "LOW"


def _candidate_tier(value: Any) -> str:
    text = _clean_upper(value)
    return text if text in {"A", "B", "C", "D"} else "NONE"


def _score_bucket(series: Any) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    out = pd.Series("UNKNOWN", index=values.index, dtype=object)
    out = out.mask(values.ge(55) & values.lt(65), "55-64")
    out = out.mask(values.ge(65) & values.lt(75), "65-74")
    out = out.mask(values.ge(75) & values.lt(85), "75-84")
    out = out.mask(values.ge(85), "85+")
    return out


def _clean_upper(value: Any) -> str:
    return str(value or "").strip().upper().replace(" ", "_").replace("-", "_")


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y", "qualified"}


def _num(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return float("nan") if pd.isna(out) else out
