"""Enrich technical rank artifacts with latest fundamental scores."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.fundamentals.contracts import WATCHLIST_BUCKET_PRIORITY, WATCHLIST_OUTPUT_COLUMNS
from ai_trading_system.domains.fundamentals.industry_schema import normalize_industry_key
from ai_trading_system.platform.db.paths import get_domain_paths


DEFAULT_SCORES_PATH = Path("fundamentals/fundamental_scores_latest.csv")
DEFAULT_TRENDS_PATH = Path("fundamentals/fundamental_trends_latest.csv")
DEFAULT_CATALYSTS_PATH = Path("fundamentals/catalyst_scores_latest.csv")
DEFAULT_INDUSTRY_SCORES_PATH = Path("fundamentals/industry_fundamental_scores_latest.csv")
DEFAULT_INDUSTRY_TRENDS_PATH = Path("fundamentals/industry_fundamental_trends_latest.csv")
DEFAULT_OUTPUT_PATH = Path("fundamentals/watchlist_candidates_latest.csv")


def _resolve_fundamentals_default(path: str | Path | None) -> Path | None:
    if path is None:
        return None
    configured = Path(path)
    if configured.is_absolute():
        return configured
    if configured.parts[:1] == ("fundamentals",) or configured.parts[:2] == ("data", "fundamentals"):
        return get_domain_paths().fundamentals_dir / configured.name
    return configured


_NEUTRAL_INDUSTRY_SCORES = {
    "industry_fundamental_score": 50.0,
    "industry_growth_score": 50.0,
    "industry_quality_score": 50.0,
    "industry_valuation_score": 50.0,
    "industry_momentum_score": 50.0,
}


@dataclass(frozen=True)
class EnrichmentMetrics:
    rank_rows: int
    filtered_rank_rows: int
    matched_rank_rows: int
    missing_fundamental_rows: int
    output_rows: int
    watchlist_bucket_counts: dict[str, int]
    matched_industry_rows: int = 0
    missing_industry_rows: int = 0
    industry_label_counts: dict[str, int] = field(default_factory=dict)
    industry_trend_label_counts: dict[str, int] = field(default_factory=dict)


def normalize_symbol(value: Any) -> str:
    if pd.isna(value):
        return ""
    symbol = str(value).strip().upper()
    if symbol.startswith("NSE:"):
        symbol = symbol[4:]
    if symbol.endswith(".NS"):
        symbol = symbol[:-3]
    return symbol.strip()


def _read_csv_optional(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _read_industry_scores(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    resolved = Path(path)
    if not resolved.exists() or resolved.stat().st_size == 0:
        return pd.DataFrame()
    try:
        frame = pd.read_csv(resolved)
    except EmptyDataError:
        return pd.DataFrame()
    if frame.empty:
        return frame
    if "industry_key" not in frame.columns:
        if "industry" not in frame.columns:
            return pd.DataFrame()
        frame.loc[:, "industry_key"] = frame["industry"].map(normalize_industry_key)
    keep = [
        "industry_key",
        "industry_fundamental_score",
        "industry_growth_score",
        "industry_quality_score",
        "industry_valuation_score",
        "industry_momentum_score",
        "industry_fundamental_label",
        "industry_warning",
    ]
    available = [column for column in keep if column in frame.columns]
    deduped = frame[available].drop_duplicates(subset=["industry_key"], keep="first")
    return deduped.reset_index(drop=True)


def _read_industry_trends(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    resolved = Path(path)
    if not resolved.exists() or resolved.stat().st_size == 0:
        return pd.DataFrame()
    try:
        frame = pd.read_csv(resolved)
    except EmptyDataError:
        return pd.DataFrame()
    if frame.empty:
        return frame
    if "industry_key" not in frame.columns:
        if "industry" not in frame.columns:
            return pd.DataFrame()
        frame.loc[:, "industry_key"] = frame["industry"].map(normalize_industry_key)
    keep = [
        "industry_key",
        "industry_fundamental_score_delta",
        "industry_trend_label",
        "industry_trend_reason",
    ]
    available = [column for column in keep if column in frame.columns]
    if "industry_fundamental_score_delta" not in available:
        return pd.DataFrame()
    deduped = frame[available].drop_duplicates(subset=["industry_key"], keep="first")
    return deduped.rename(
        columns={"industry_fundamental_score_delta": "industry_score_delta"}
    ).reset_index(drop=True)


def _read_symbol_latest(path: str | Path | None, *, date_column: str | None = None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame(columns=["symbol"])
    resolved = Path(path)
    frame = _symbol_frame(_read_csv_optional(resolved))
    if frame.empty:
        return frame
    if date_column and date_column in frame.columns:
        frame.loc[:, "_sort_date"] = pd.to_datetime(frame[date_column], errors="coerce")
        frame = frame.sort_values(["_sort_date", "symbol"], ascending=[False, True], na_position="last", kind="stable")
        frame = frame.drop(columns=["_sort_date"], errors="ignore")
    return frame.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)


def _read_ranked(rank_dir: Path) -> pd.DataFrame:
    path = rank_dir / "ranked_signals.csv"
    if not path.exists():
        raise FileNotFoundError(f"ranked_signals.csv not found under {rank_dir}")
    return pd.read_csv(path)


def _symbol_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol"])
    output = frame.copy()
    if "symbol" not in output.columns:
        for candidate in ("symbol_id", "Symbol", "NSE Code"):
            if candidate in output.columns:
                output.loc[:, "symbol"] = output[candidate]
                break
    if "symbol" not in output.columns:
        return pd.DataFrame(columns=["symbol"])
    output.loc[:, "symbol"] = output["symbol"].map(normalize_symbol)
    return output.loc[output["symbol"].ne("")].copy()


def _best_by_symbol(frame: pd.DataFrame, score_columns: list[str]) -> pd.DataFrame:
    output = _symbol_frame(frame)
    if output.empty:
        return output
    for column in score_columns:
        if column not in output.columns:
            output.loc[:, column] = pd.NA
        output.loc[:, column] = pd.to_numeric(output[column], errors="coerce")
    output = output.sort_values([*score_columns, "symbol"], ascending=[False] * len(score_columns) + [True], na_position="last", kind="stable")
    return output.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)


def _num(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default).astype(float)


def _float(value: Any) -> float:
    try:
        if pd.isna(value):
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def _truthy_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    values = frame[column]
    text = values.astype("string").str.strip().str.lower()
    numeric = pd.to_numeric(values, errors="coerce")
    return text.isin({"1", "true", "t", "yes", "y", "qualified"}) | numeric.gt(0).fillna(False)


def _first_available(frame: pd.DataFrame, columns: list[str], default: Any = pd.NA) -> pd.Series:
    text_default = isinstance(default, str)
    result = pd.Series(default, index=frame.index, dtype="object" if text_default else None)
    for column in columns:
        if column in frame.columns:
            replacement = frame[column].astype("object") if text_default else frame[column]
            result = result.where(result.notna(), replacement)
    return result.astype("object").fillna(default) if text_default else result


def _assign_first_available(frame: pd.DataFrame, target: str, columns: list[str], default: Any = pd.NA) -> pd.DataFrame:
    values = _first_available(frame, columns, default)
    output = frame.drop(columns=[target], errors="ignore")
    output.loc[:, target] = values
    return output


def _breakout_pattern_score(frame: pd.DataFrame) -> pd.Series:
    breakout = _num(frame, "breakout_score", default=float("nan"))
    pattern = _num(frame, "pattern_score", default=float("nan"))
    both = breakout.notna() & pattern.notna()
    score = pd.Series(50.0, index=frame.index, dtype=float)
    score.loc[breakout.notna() & pattern.isna()] = breakout.loc[breakout.notna() & pattern.isna()]
    score.loc[pattern.notna() & breakout.isna()] = pattern.loc[pattern.notna() & breakout.isna()]
    score.loc[both] = (breakout.loc[both] * 0.6) + (pattern.loc[both] * 0.4)
    bonus = frame.get("candidate_tier", pd.Series("", index=frame.index)).astype("string").str.upper().eq("A") & _truthy_series(frame, "qualified")
    score.loc[bonus] = score.loc[bonus] + 5.0
    return score.clip(0, 100)


def _bucket(frame: pd.DataFrame) -> pd.Series:
    composite = _num(frame, "composite_score")
    fundamental_tier = frame.get("fundamental_tier", pd.Series("", index=frame.index)).astype("string").str.upper()
    hard = _truthy_series(frame, "hard_red_flag")
    qualified = _truthy_series(frame, "qualified")
    pattern_score = _num(frame, "pattern_score")
    strong_pattern = pattern_score.ge(75)
    has_setup = qualified | strong_pattern

    bucket = pd.Series("IGNORE_FOR_NOW", index=frame.index)
    bucket.loc[hard | fundamental_tier.eq("REJECT")] = "AVOID_RED_FLAG"
    bucket.loc[composite.ge(70) & fundamental_tier.eq("C") & ~hard] = "TECHNICAL_ONLY_RISK"
    bucket.loc[composite.ge(65) & fundamental_tier.isin(["B", "C"]) & ~hard] = "STUDY_ONLY"
    bucket.loc[composite.ge(70) & fundamental_tier.isin(["A", "B"]) & ~hard & has_setup] = "ADD_TO_WATCHLIST"
    bucket.loc[hard | fundamental_tier.eq("REJECT")] = "AVOID_RED_FLAG"
    return bucket


def _near_high_series(frame: pd.DataFrame) -> pd.Series:
    near_pct = _num(frame, "near_52w_high_pct", default=float("nan"))
    proximity = _num(frame, "proximity_to_highs", default=float("nan"))
    prox_high = _num(frame, "prox_high", default=float("nan"))
    prox_high_score = _num(frame, "prox_high_score", default=float("nan"))
    return (
        near_pct.le(10).fillna(False)
        | proximity.ge(70).fillna(False)
        | proximity.le(10).fillna(False)
        | prox_high.le(10).fillna(False)
        | prox_high_score.ge(70).fillna(False)
    )


def _fundamental_tracking_bucket(frame: pd.DataFrame) -> pd.Series:
    q_score = _num(frame, "quarterly_result_score", 50.0)
    v_score = _num(frame, "valuation_history_score", 50.0)
    composite = _num(frame, "composite_score", 0.0)
    sector = _num(frame, "sector_strength", 50.0)
    pattern_score = _num(frame, "pattern_score", 0.0)
    valuation_bucket = frame.get("valuation_history_bucket", pd.Series("", index=frame.index)).fillna("").astype(str)
    result_bucket = frame.get("quarterly_result_bucket", pd.Series("", index=frame.index)).fillna("").astype(str)
    fundamental_tier = frame.get("fundamental_tier", pd.Series("", index=frame.index)).astype("string").str.upper()
    hard = _truthy_series(frame, "hard_red_flag")
    qualified = _truthy_series(frame, "qualified")
    candidate_tier = frame.get("candidate_tier", pd.Series("", index=frame.index)).astype("string").str.upper()
    setup = qualified | candidate_tier.eq("A") | pattern_score.ge(75)
    near_high = _near_high_series(frame)
    allowed_value = valuation_bucket.isin(["DEEPLY_BELOW_HISTORY", "BELOW_OWN_MEDIAN", "FAIR_VALUE"])
    expensive = valuation_bucket.eq("EXPENSIVE_VS_HISTORY")
    expensive_override = ~expensive | (q_score.ge(90) & composite.ge(80))

    bucket = pd.Series("IGNORE_FOR_NOW", index=frame.index)
    bucket.loc[q_score.ge(70)] = "F1_FUNDAMENTAL_WATCH"
    bucket.loc[q_score.ge(70) & valuation_bucket.isin(["DEEPLY_BELOW_HISTORY", "BELOW_OWN_MEDIAN"]) & composite.ge(55)] = (
        "F2_RESULT_VALUE_ACCUMULATION"
    )
    bucket.loc[
        q_score.ge(75)
        & valuation_bucket.isin(["DEEPLY_BELOW_HISTORY", "BELOW_OWN_MEDIAN", "FAIR_VALUE"])
        & composite.ge(65)
        & sector.ge(60)
    ] = "F3_FUND_VALUE_TECH_READY"
    bucket.loc[
        q_score.ge(80)
        & allowed_value
        & composite.ge(70)
        & sector.ge(65)
        & near_high
        & setup
        & expensive_override
    ] = "F4_ACTION_CANDIDATE"
    bucket.loc[result_bucket.eq("DETERIORATING")] = "D1_RESULT_DOWNTURN"
    bucket.loc[hard | fundamental_tier.eq("REJECT")] = "D2_AVOID_RED_FLAG"
    return bucket


def _reason(row: pd.Series) -> str:
    bucket = str(row.get("watchlist_bucket", ""))
    tier = str(row.get("fundamental_tier", "") or "missing")
    candidate_tier = str(row.get("candidate_tier", "") or "").upper()
    red_flags = str(row.get("red_flags", "") or "").strip()
    if bucket == "ADD_TO_WATCHLIST":
        setup = f"Tier {candidate_tier} breakout" if candidate_tier else "strong setup"
        reason = f"Strong technical rank + {setup} + Fundamental {tier} + no red flags"
    if bucket == "AVOID_RED_FLAG":
        reason = f"Strong RS but fundamental red flag: {red_flags or 'Reject tier'}"
    elif bucket == "STUDY_ONLY":
        reason = f"Good fundamentals but no qualified breakout yet" if not bool(row.get("qualified")) else f"Study setup + Fundamental {tier}"
    elif bucket == "TECHNICAL_ONLY_RISK":
        reason = "Technical rank is strong but fundamentals are mixed"
    elif bucket != "ADD_TO_WATCHLIST":
        reason = "No strong technical/fundamental alignment yet"
    trend_label = str(row.get("fundamental_trend_label") or "").upper()
    trend_reason = str(row.get("trend_reason") or "").strip()
    if trend_label == "IMPROVING":
        reason = f"{reason}; improving fundamentals"
    elif trend_label == "DETERIORATING":
        reason = f"{reason}; deteriorating fundamentals"
    elif trend_label == "VALUE_TRAP_RISK":
        reason = f"{reason}; value-trap risk{': ' + trend_reason if trend_reason else ''}"
    return reason


def _fundamental_tracking_reason(row: pd.Series) -> str:
    bucket = str(row.get("watchlist_bucket") or "")
    result_bucket = str(row.get("quarterly_result_bucket") or "result neutral")
    valuation_bucket = str(row.get("valuation_history_bucket") or "valuation neutral")
    valuation_reason = str(row.get("valuation_reason") or "").strip()
    parts: list[str] = []
    if bucket == "D2_AVOID_RED_FLAG":
        return f"Fundamental red flag: {str(row.get('red_flags') or 'Reject tier')}"
    if bucket == "D1_RESULT_DOWNTURN":
        return f"Deteriorating quarterly result + {valuation_bucket}"
    if result_bucket and result_bucket != "IGNORE":
        parts.append(result_bucket.replace("_", " ").title())
    if valuation_reason:
        parts.append(valuation_reason)
    elif valuation_bucket:
        parts.append(valuation_bucket.replace("_", " ").title())
    if _num(pd.DataFrame([row]), "composite_score").iloc[0] >= 70:
        parts.append("strong RS")
    if _num(pd.DataFrame([row]), "sector_strength", 50.0).iloc[0] >= 60:
        parts.append("sector strength")
    if bool(row.get("qualified")) or str(row.get("candidate_tier") or "").upper() == "A" or _float(row.get("pattern_score")) >= 75:
        parts.append("technical confirmation")
    return " + ".join(parts) if parts else "No strong result/value/technical alignment yet"


def _next_action(bucket: str) -> str:
    return {
        "F4_ACTION_CANDIDATE": "Prioritize chart review and add to action watchlist",
        "F3_FUND_VALUE_TECH_READY": "Track for breakout confirmation",
        "F2_RESULT_VALUE_ACCUMULATION": "Accumulate research; wait for technical strength",
        "F1_FUNDAMENTAL_WATCH": "Watch next result and technical setup",
        "D1_RESULT_DOWNTURN": "Avoid until result trend improves",
        "D2_AVOID_RED_FLAG": "Avoid unless special situation/catalyst",
        "ADD_TO_WATCHLIST": "Add to watchlist and review chart",
        "STUDY_ONLY": "Study fundamentals and wait for better setup",
        "TECHNICAL_ONLY_RISK": "Review manually; technical strong but fundamentals mixed",
        "AVOID_RED_FLAG": "Avoid unless special situation/catalyst",
        "IGNORE_FOR_NOW": "No action",
    }.get(bucket, "No action")


def enrich_rank_artifacts(
    *,
    rank_dir: str | Path,
    fundamental_scores: str | Path = DEFAULT_SCORES_PATH,
    fundamental_trends: str | Path | None = DEFAULT_TRENDS_PATH,
    industry_scores: str | Path | None = DEFAULT_INDUSTRY_SCORES_PATH,
    industry_trends: str | Path | None = DEFAULT_INDUSTRY_TRENDS_PATH,
    catalysts: str | Path | None = None,
    quarterly_result_scores: str | Path | None = None,
    stock_valuation_bands: str | Path | None = None,
    watchlist_mode: str = "legacy",
    output: str | Path = DEFAULT_OUTPUT_PATH,
    run_id: str | None = None,
    top_n: int = 100,
    min_technical_score: float = 50.0,
    return_metrics: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, EnrichmentMetrics]:
    rank_dir = Path(rank_dir)
    output = _resolve_fundamentals_default(output) or Path(output)
    fundamental_scores = _resolve_fundamentals_default(fundamental_scores) or Path(fundamental_scores)
    fundamental_trends = _resolve_fundamentals_default(fundamental_trends)
    industry_scores = _resolve_fundamentals_default(industry_scores)
    industry_trends = _resolve_fundamentals_default(industry_trends)
    catalysts = _resolve_fundamentals_default(catalysts)
    quarterly_result_scores = _resolve_fundamentals_default(quarterly_result_scores)
    stock_valuation_bands = _resolve_fundamentals_default(stock_valuation_bands)
    watchlist_mode = str(watchlist_mode or "legacy").strip().lower()
    ranked = _symbol_frame(_read_ranked(rank_dir))
    rank_rows = len(ranked)
    breakout = _best_by_symbol(_read_csv_optional(rank_dir / "breakout_scan.csv"), ["breakout_score"])
    pattern = _best_by_symbol(_read_csv_optional(rank_dir / "pattern_scan.csv"), ["pattern_score"])
    fundamentals = _symbol_frame(pd.read_csv(fundamental_scores))
    if "industry" in fundamentals.columns:
        fundamentals.loc[:, "industry_key"] = fundamentals["industry"].map(normalize_industry_key)
    trends = _symbol_frame(_read_csv_optional(Path(fundamental_trends))) if fundamental_trends else pd.DataFrame(columns=["symbol"])
    catalyst_frame = _symbol_frame(_read_csv_optional(Path(catalysts))) if catalysts else pd.DataFrame(columns=["symbol"])
    result_frame = _read_symbol_latest(quarterly_result_scores, date_column="available_at")
    valuation_frame = _read_symbol_latest(stock_valuation_bands, date_column="date")

    ranked = ranked.loc[_num(ranked, "composite_score").ge(float(min_technical_score))].copy()
    merged = ranked.merge(fundamentals, on="symbol", how="left", suffixes=("", "_fundamental"))
    has_fundamental_match = merged.get("fundamental_score", pd.Series(pd.NA, index=merged.index)).notna()
    if not trends.empty:
        merged = merged.merge(trends, on="symbol", how="left", suffixes=("", "_trend"))
    if not breakout.empty:
        merged = merged.merge(breakout, on="symbol", how="left", suffixes=("", "_breakout"))
    if not pattern.empty:
        merged = merged.merge(pattern, on="symbol", how="left", suffixes=("", "_pattern"))
    has_catalyst_scores = not catalyst_frame.empty
    if has_catalyst_scores:
        merged = merged.merge(catalyst_frame, on="symbol", how="left", suffixes=("", "_catalyst"))
    if not result_frame.empty:
        merged = merged.merge(result_frame, on="symbol", how="left", suffixes=("", "_result"))
    if not valuation_frame.empty:
        merged = merged.merge(valuation_frame, on="symbol", how="left", suffixes=("", "_valuation"))

    merged = _assign_first_available(merged, "name", ["name", "name_fundamental"], "")
    merged = _assign_first_available(merged, "industry_group", ["industry_group", "industry_group_fundamental"], "")
    merged = _assign_first_available(merged, "industry", ["industry", "industry_fundamental"], "")
    merged.loc[:, "relative_strength"] = _first_available(merged, ["relative_strength", "rel_strength", "rel_strength_score"], pd.NA)
    merged.loc[:, "volume_intensity"] = _first_available(merged, ["volume_intensity", "vol_intensity", "vol_intensity_score"], pd.NA)
    merged.loc[:, "trend_persistence"] = _first_available(merged, ["trend_persistence", "trend_score", "trend_score_score"], pd.NA)
    merged.loc[:, "proximity_to_highs"] = _first_available(merged, ["proximity_to_highs", "prox_high", "prox_high_score"], pd.NA)
    merged.loc[:, "sector_strength"] = _first_available(merged, ["sector_strength", "sector_strength_score", "sector_rs_value"], pd.NA)
    merged.loc[:, "breakout_type"] = _first_available(merged, ["breakout_type", "setup_family", "taxonomy_family", "breakout_tag"], "")
    merged.loc[:, "pattern_state"] = _first_available(merged, ["pattern_state", "pattern_lifecycle_state"], "")

    for column in ("quality_score", "growth_score", "balance_sheet_score", "valuation_score", "ownership_score", "fundamental_score"):
        if column not in merged.columns:
            merged.loc[:, column] = 50.0
        merged.loc[:, column] = pd.to_numeric(merged[column], errors="coerce").fillna(50.0)
    if "fundamental_tier" not in merged.columns:
        merged.loc[:, "fundamental_tier"] = "C"
    merged.loc[:, "fundamental_tier"] = merged["fundamental_tier"].fillna("C")
    if "red_flags" not in merged.columns:
        merged.loc[:, "red_flags"] = ""
    merged = merged.assign(red_flags=merged["red_flags"].astype("object").fillna(""))
    if "hard_red_flag" not in merged.columns:
        merged.loc[:, "hard_red_flag"] = False
    if "fundamental_score_delta" not in merged.columns:
        merged.loc[:, "fundamental_score_delta"] = 0.0
    merged.loc[:, "fundamental_score_delta"] = pd.to_numeric(merged["fundamental_score_delta"], errors="coerce").fillna(0.0)
    if "fundamental_trend_label" not in merged.columns:
        merged.loc[:, "fundamental_trend_label"] = ""
    merged.loc[:, "fundamental_trend_label"] = merged["fundamental_trend_label"].fillna("")
    if "trend_reason" not in merged.columns:
        merged.loc[:, "trend_reason"] = ""
    merged.loc[:, "trend_reason"] = merged["trend_reason"].fillna("")
    for column, default in (
        ("quarterly_result_score", 50.0),
        ("valuation_history_score", 50.0),
        ("sector_strength", 50.0),
        ("breakout_pattern_score", 50.0),
    ):
        if column not in merged.columns:
            merged.loc[:, column] = default
        merged.loc[:, column] = pd.to_numeric(merged[column], errors="coerce").fillna(default)
    for column, default in (
        ("quarterly_result_bucket", "IGNORE"),
        ("quarterly_result_reason", ""),
        ("valuation_history_bucket", "FAIR_VALUE"),
        ("valuation_reason", ""),
    ):
        if column not in merged.columns:
            merged.loc[:, column] = default
        merged.loc[:, column] = merged[column].fillna(default)
    merged.loc[:, "breakout_pattern_score"] = _breakout_pattern_score(merged)
    catalyst_score = _num(merged, "catalyst_score", default=float("nan"))
    has_row_catalyst = catalyst_score.notna()
    base_score = (
        0.70 * _num(merged, "composite_score")
        + 0.15 * _num(merged, "breakout_pattern_score", 50.0)
        + 0.15 * _num(merged, "fundamental_score", 50.0)
    )
    catalyst_adjusted_score = (
        0.60 * _num(merged, "composite_score")
        + 0.15 * _num(merged, "breakout_pattern_score", 50.0)
        + 0.15 * _num(merged, "fundamental_score", 50.0)
        + 0.10 * catalyst_score.fillna(0.0)
    )
    if watchlist_mode == "fundamental_tracking":
        merged.loc[:, "final_watchlist_score"] = (
            0.35 * _num(merged, "quarterly_result_score", 50.0)
            + 0.20 * _num(merged, "valuation_history_score", 50.0)
            + 0.15 * _num(merged, "fundamental_score", 50.0)
            + 0.15 * _num(merged, "composite_score", 50.0)
            + 0.10 * _num(merged, "sector_strength", 50.0)
            + 0.05 * _num(merged, "breakout_pattern_score", 50.0)
        )
    else:
        merged.loc[:, "final_watchlist_score"] = base_score.where(~has_row_catalyst, catalyst_adjusted_score)
    value_trap = merged["fundamental_trend_label"].astype(str).str.upper().eq("VALUE_TRAP_RISK")
    if watchlist_mode != "fundamental_tracking":
        merged.loc[value_trap, "final_watchlist_score"] = merged.loc[value_trap, "final_watchlist_score"] - 7.0
    merged.loc[:, "final_watchlist_score"] = pd.to_numeric(merged["final_watchlist_score"], errors="coerce").clip(0, 100).round(2)
    merged.loc[:, "watchlist_bucket"] = _fundamental_tracking_bucket(merged) if watchlist_mode == "fundamental_tracking" else _bucket(merged)
    add_value_trap = value_trap & merged["watchlist_bucket"].eq("ADD_TO_WATCHLIST")
    merged.loc[add_value_trap, "watchlist_bucket"] = "STUDY_ONLY"
    merged.loc[:, "watchlist_reason"] = (
        merged.apply(_fundamental_tracking_reason, axis=1)
        if watchlist_mode == "fundamental_tracking"
        else merged.apply(_reason, axis=1)
    )

    industry_frame = _read_industry_scores(industry_scores)
    matched_industry_rows = 0
    missing_industry_rows = len(merged)
    industry_label_counts: dict[str, int] = {}
    if not industry_frame.empty:
        if "industry_key" not in merged.columns:
            merged.loc[:, "industry_key"] = merged.get("industry", pd.Series("", index=merged.index)).map(
                normalize_industry_key
            )
        merged.loc[:, "industry_key"] = merged["industry_key"].fillna("").astype(str)
        merged = merged.merge(
            industry_frame,
            on="industry_key",
            how="left",
            suffixes=("", "_industry"),
        )
        matched_mask = merged["industry_fundamental_score"].notna()
        matched_industry_rows = int(matched_mask.sum())
        missing_industry_rows = int((~matched_mask).sum())
    for column, default in _NEUTRAL_INDUSTRY_SCORES.items():
        if column not in merged.columns:
            merged.loc[:, column] = default
        merged.loc[:, column] = pd.to_numeric(merged[column], errors="coerce").fillna(default)
    if "industry_fundamental_label" not in merged.columns:
        merged.loc[:, "industry_fundamental_label"] = "UNKNOWN"
    merged.loc[:, "industry_fundamental_label"] = (
        merged["industry_fundamental_label"].fillna("UNKNOWN").replace("", "UNKNOWN")
    )
    warning_values = (
        merged["industry_warning"].astype("object").fillna("")
        if "industry_warning" in merged.columns
        else pd.Series("", index=merged.index, dtype="object")
    )
    merged = merged.drop(columns=["industry_warning"], errors="ignore")
    merged.loc[:, "industry_warning"] = warning_values

    industry_trend_frame = _read_industry_trends(industry_trends)
    if not industry_trend_frame.empty and "industry_key" in merged.columns:
        merged = merged.merge(
            industry_trend_frame,
            on="industry_key",
            how="left",
            suffixes=("", "_industry_trend"),
        )
    if "industry_score_delta" not in merged.columns:
        merged.loc[:, "industry_score_delta"] = 0.0
    merged.loc[:, "industry_score_delta"] = pd.to_numeric(
        merged["industry_score_delta"], errors="coerce"
    ).fillna(0.0)
    if "industry_trend_label" not in merged.columns:
        merged.loc[:, "industry_trend_label"] = pd.Series("", index=merged.index, dtype="object")
    trend_label_values = merged["industry_trend_label"].astype("object").fillna("")
    merged = merged.drop(columns=["industry_trend_label"], errors="ignore")
    merged.loc[:, "industry_trend_label"] = trend_label_values.replace("", "UNKNOWN")

    industry_label = merged["industry_fundamental_label"].astype(str)
    industry_warning = merged["industry_warning"].astype(str)
    industry_trend_label = merged["industry_trend_label"].astype(str)
    bucket_series = merged["watchlist_bucket"].astype(str)
    reason_series = merged["watchlist_reason"].astype(str)

    weak_add = industry_label.eq("WEAK_FUNDAMENTALS") & bucket_series.eq("ADD_TO_WATCHLIST")
    bucket_series = bucket_series.where(~weak_add, "STUDY_ONLY")

    def _append(mask: pd.Series, suffix: str) -> pd.Series:
        return reason_series.where(~mask, reason_series + suffix)

    quality_supportive = industry_label.eq("QUALITY_GROWTH_LEADER") & bucket_series.eq("ADD_TO_WATCHLIST")
    reason_series = _append(quality_supportive, "; industry backdrop supportive")
    reason_series = _append(industry_label.eq("EXPENSIVE_MOMENTUM"), "; sector expensive, avoid chasing extended entries")
    reason_series = _append(industry_label.eq("VALUE_ROTATION_CANDIDATE"), "; value rotation sector candidate")
    reason_series = _append(weak_add, "; weak industry fundamentals")
    reason_series = _append(industry_label.eq("DISTORTED_DATA"), "; industry data distorted, verify manually")
    reason_series = _append(industry_warning.str.contains("low_company_count", na=False), "; low company count industry")
    reason_series = _append(industry_trend_label.eq("IMPROVING"), "; industry trend improving")
    reason_series = _append(industry_trend_label.eq("DETERIORATING"), "; industry trend deteriorating")
    reason_series = _append(industry_trend_label.eq("MOMENTUM_BUILDING"), "; industry momentum building")
    reason_series = _append(industry_trend_label.eq("MOMENTUM_FADING"), "; industry momentum fading")
    reason_series = _append(industry_trend_label.eq("VALUE_TRAP_RISK"), "; industry value-trap risk")

    merged.loc[:, "watchlist_bucket"] = bucket_series
    merged.loc[:, "watchlist_reason"] = reason_series
    merged.loc[:, "next_action"] = merged["watchlist_bucket"].map(_next_action)

    industry_trend_label_counts: dict[str, int] = {}
    if not industry_frame.empty:
        industry_label_counts = {
            str(label): int(count)
            for label, count in industry_label.value_counts(dropna=False).to_dict().items()
        }
    if not industry_trend_frame.empty:
        industry_trend_label_counts = {
            str(label): int(count)
            for label, count in industry_trend_label.value_counts(dropna=False).to_dict().items()
        }

    for column in WATCHLIST_OUTPUT_COLUMNS:
        if column not in merged.columns:
            merged.loc[:, column] = pd.NA
    merged.loc[:, "_bucket_priority"] = merged["watchlist_bucket"].map(WATCHLIST_BUCKET_PRIORITY).fillna(99)
    result = (
        merged.sort_values(["_bucket_priority", "final_watchlist_score", "composite_score"], ascending=[True, False, False], na_position="last", kind="stable")
        .head(int(top_n))
        .reset_index(drop=True)
    )
    result = result[WATCHLIST_OUTPUT_COLUMNS]
    output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output, index=False)
    if run_id:
        run_output = get_domain_paths().pipeline_runs_dir / run_id / "fundamentals" / "watchlist_candidates.csv"
        run_output.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(run_output, index=False)
    if return_metrics:
        metrics = EnrichmentMetrics(
            rank_rows=int(rank_rows),
            filtered_rank_rows=int(len(ranked)),
            matched_rank_rows=int(has_fundamental_match.sum()),
            missing_fundamental_rows=int((~has_fundamental_match).sum()),
            output_rows=int(len(result)),
            watchlist_bucket_counts={
                str(bucket): int(count)
                for bucket, count in result["watchlist_bucket"].value_counts(dropna=False).to_dict().items()
            },
            matched_industry_rows=int(matched_industry_rows),
            missing_industry_rows=int(missing_industry_rows),
            industry_label_counts=industry_label_counts,
            industry_trend_label_counts=industry_trend_label_counts,
        )
        return result, metrics
    return result


def build_parser() -> argparse.ArgumentParser:
    fundamentals_dir = get_domain_paths().fundamentals_dir
    parser = argparse.ArgumentParser(description="Enrich rank artifacts with latest fundamental scores.")
    parser.add_argument("--rank-dir", required=True, help="Rank attempt directory")
    parser.add_argument(
        "--fundamental-scores",
        default=str(fundamentals_dir / "fundamental_scores_latest.csv"),
        help="Latest fundamental scores CSV",
    )
    parser.add_argument(
        "--fundamental-trends",
        default=str(fundamentals_dir / "fundamental_trends_latest.csv"),
        help="Latest fundamental trends CSV",
    )
    parser.add_argument(
        "--industry-scores",
        default=str(fundamentals_dir / "industry_fundamental_scores_latest.csv"),
        help="Latest industry fundamental scores CSV",
    )
    parser.add_argument(
        "--industry-trends",
        default=str(fundamentals_dir / "industry_fundamental_trends_latest.csv"),
        help="Latest industry fundamental trends CSV",
    )
    parser.add_argument("--catalysts", default=None, help="Optional catalyst score CSV")
    parser.add_argument("--quarterly-result-scores", default=None, help="Optional quarterly result scores CSV")
    parser.add_argument("--stock-valuation-bands", default=None, help="Optional latest stock valuation bands CSV")
    parser.add_argument("--watchlist-mode", choices=["legacy", "fundamental_tracking"], default="legacy")
    parser.add_argument(
        "--output",
        default=str(fundamentals_dir / "watchlist_candidates_latest.csv"),
        help="Enriched watchlist output CSV",
    )
    parser.add_argument("--run-id", default=None, help="Optional pipeline run id for per-run output")
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--min-technical-score", type=float, default=50.0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = enrich_rank_artifacts(
        rank_dir=args.rank_dir,
        fundamental_scores=args.fundamental_scores,
        fundamental_trends=args.fundamental_trends,
        industry_scores=args.industry_scores,
        industry_trends=args.industry_trends,
        catalysts=args.catalysts,
        quarterly_result_scores=args.quarterly_result_scores,
        stock_valuation_bands=args.stock_valuation_bands,
        watchlist_mode=args.watchlist_mode,
        output=args.output,
        run_id=args.run_id,
        top_n=args.top_n,
        min_technical_score=args.min_technical_score,
    )
    print(f"rows written: {len(result)}")
    if not result.empty:
        print(result[["symbol", "final_watchlist_score", "watchlist_bucket", "next_action"]].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
