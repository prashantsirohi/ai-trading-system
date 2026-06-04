"""Deterministic watchlist funnel for rank-stage operator candidates."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd


LEADING_QUADRANTS = {"leading", "improving"}
STAGE2_LABELS = {"strong_stage2", "stage2", "stage1_to_stage2"}
PATTERN_LIFECYCLES = {"watchlist", "confirmed"}
SCORE_VERSION = "watchlist_v2_2026_06"
REAL_MOMENTUM_TAGS = {"52W_HIGH", "WEEKLY_GAINER", "DAILY_GAINER", "UNUSUAL_VOLUME", "DELIVERY_ACCUMULATION"}
WATCHLIST_BUCKETS = {"TRIGGERED_TODAY", "CORE_MOMENTUM", "EARLY_STAGE2", "AVOID_WEAK_CONFIRMATION"}
LIQUIDITY_COLUMNS = (
    "avg_traded_value",
    "avg_traded_value_20",
    "avg_traded_value_20d",
    "average_traded_value",
    "traded_value",
    "turnover",
    "turnover_20d",
    "avg_turnover_20d",
    "dollar_volume",
)
MIN_LIQUIDITY_VALUE = 10_000_000.0
PATTERN_TAGS = {
    "cup": "CUP_HANDLE",
    "cup_with_handle": "CUP_HANDLE",
    "flat": "FLAT_BASE",
    "flat_base": "FLAT_BASE",
    "vcp": "VCP",
    "flag": "FLAG",
    "darvas": "DARVAS",
}
FINAL_COLUMNS = [
    "rank",
    "previous_rank",
    "rank_change",
    "days_on_watchlist",
    "is_new_entry",
    "symbol_id",
    "sector",
    "sector_status",
    "sector_escape_hatch",
    "stage",
    "momentum_tags",
    "setup_label",
    "technical_catalyst_summary",
    "catalyst_tags",
    "catalyst_confidence",
    "bull_case",
    "risk_flags",
    "watchlist_score",
    "composite_score",
    "action",
    "data_trust_status",
    "watchlist_reason",
    "watchlist_bucket",
    "operator_action",
    "gate_status",
    "gate_failures",
    "primary_gate_failure",
    "tradability_status",
    "liquidity_score",
    "extension_pct_sma50",
    "score_version",
]
REJECTION_COLUMNS = [
    "symbol_id",
    "rank",
    "sector",
    "sector_status",
    "stage",
    "momentum_tags",
    "setup_label",
    "gate_status",
    "gate_failures",
    "primary_gate_failure",
    "tradability_status",
    "liquidity_score",
    "extension_pct_sma50",
    "watchlist_bucket",
    "composite_score",
    "breakout_score",
    "pattern_score",
    "sector_escape_hatch",
    "score_version",
]


def _as_symbol_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
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
    output.loc[:, "symbol_id"] = output["symbol_id"].astype(str)
    return output


def _num(row: pd.Series, key: str, default: float = 0.0) -> float:
    value = pd.to_numeric(row.get(key), errors="coerce")
    if pd.isna(value):
        return default
    return float(value)


def _text(row: pd.Series, key: str, default: str = "") -> str:
    value = row.get(key, default)
    if pd.isna(value):
        return default
    return str(value).strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    if isinstance(value, (int, float)):
        return float(value) > 0
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "qualified"}


def _join_best(left: pd.DataFrame, right: pd.DataFrame, *, score_cols: list[str], suffix: str) -> pd.DataFrame:
    if left.empty or right.empty:
        return left.copy()
    ranked = right.copy()
    for column in score_cols:
        if column not in ranked.columns:
            ranked.loc[:, column] = pd.NA
        ranked.loc[:, column] = pd.to_numeric(ranked[column], errors="coerce")
    ranked = ranked.sort_values([*score_cols, "symbol_id"], ascending=[False] * len(score_cols) + [True], kind="stable")
    ranked = ranked.drop_duplicates(subset=["symbol_id"], keep="first")
    drop_cols = [col for col in ranked.columns if col in left.columns and col != "symbol_id"]
    ranked = ranked.rename(columns={col: f"{col}_{suffix}" for col in drop_cols})
    return left.merge(ranked, on="symbol_id", how="left")


def _sector_lookup(sector_dash: pd.DataFrame | None) -> pd.DataFrame:
    frame = sector_dash.copy() if sector_dash is not None else pd.DataFrame()
    if frame.empty:
        return pd.DataFrame(columns=["sector", "Quadrant"])
    if "sector" not in frame.columns:
        for candidate in ("Sector", "index"):
            if candidate in frame.columns:
                frame.loc[:, "sector"] = frame[candidate]
                break
    if "sector" not in frame.columns:
        return pd.DataFrame(columns=["sector", "Quadrant"])
    if "Quadrant" not in frame.columns:
        frame.loc[:, "Quadrant"] = ""
    return frame[["sector", "Quadrant"]].drop_duplicates(subset=["sector"], keep="first")


def _classify_sector_status(row: pd.Series) -> str:
    quadrant = _text(row, "Quadrant").lower()
    if quadrant == "leading":
        return "LEADING"
    if quadrant == "improving":
        return "IMPROVING"
    if quadrant == "weakening":
        return "WEAKENING"
    if quadrant == "lagging":
        return "LAGGING"
    return "UNKNOWN"


def _classify_stage(row: pd.Series) -> str:
    label = _text(row, "stage2_label").lower()
    if label == "stage1_to_stage2":
        return "STAGE_1_TO_2"
    if label in {"strong_stage2", "stage2"}:
        return "STAGE_2"
    return "NON_STAGE2"


def _delivery_accumulation(row: pd.Series) -> bool:
    delivery = _num(row, "delivery_pct", -1.0)
    median = _num(row, "sector_median_delivery_pct", float("nan"))
    p75 = _num(row, "sector_p75_delivery_pct", float("nan"))
    median_ok = pd.notna(median) and delivery >= median + 10.0
    p75_ok = pd.notna(p75) and delivery >= p75
    return bool(delivery >= 0 and (median_ok or p75_ok))


def _momentum_tag_list(row: pd.Series) -> list[str]:
    return [tag.strip() for tag in str(row.get("momentum_tags", "")).split(",") if tag.strip()]


def _has_real_momentum(row: pd.Series) -> bool:
    return any(tag in REAL_MOMENTUM_TAGS for tag in _momentum_tag_list(row))


def _extension_pct(row: pd.Series) -> float:
    close = _num(row, "close", float("nan"))
    sma50 = _num(row, "sma_50", float("nan"))
    if pd.isna(close) or pd.isna(sma50) or sma50 <= 0:
        return float("nan")
    return round(((close - sma50) / sma50) * 100.0, 2)


def _liquidity_value(row: pd.Series) -> float:
    for column in LIQUIDITY_COLUMNS:
        value = _num(row, column, float("nan"))
        if pd.notna(value):
            return value
    return float("nan")


def _tradability(row: pd.Series) -> dict[str, Any]:
    close = _num(row, "close", float("nan"))
    liquidity = _liquidity_value(row)
    if pd.notna(close) and close <= 0:
        return {"tradability_status": "FAILED", "liquidity_score": 0.0}
    if pd.isna(liquidity):
        return {"tradability_status": "UNKNOWN", "liquidity_score": pd.NA}
    score = max(0.0, min(100.0, (liquidity / MIN_LIQUIDITY_VALUE) * 100.0))
    status = "PASSED" if liquidity >= MIN_LIQUIDITY_VALUE else "FAILED"
    return {"tradability_status": status, "liquidity_score": round(score, 2)}


def _collect_momentum_tags(row: pd.Series) -> list[str]:
    tags: list[str] = []
    if _num(row, "near_52w_high_pct", 999.0) <= 5.0:
        tags.append("52W_HIGH")
    if _num(row, "return_5") >= 5.0:
        tags.append("WEEKLY_GAINER")
    daily = _num(row, "return_1")
    if 3.0 <= daily <= 12.0:
        tags.append("DAILY_GAINER")
    if _num(row, "volume_ratio") >= 1.5:
        tags.append("UNUSUAL_VOLUME")
    if _delivery_accumulation(row):
        tags.append("DELIVERY_ACCUMULATION")
    if _num(row, "rank", 999999.0) <= 50.0:
        tags.append("TOP_RANKED")
    return tags


def _pattern_name(row: pd.Series) -> str:
    for key in ("pattern_name", "pattern_type", "pattern_family", "pattern_label"):
        value = _text(row, key)
        if value:
            return value
    return ""


def _classify_setup_label(row: pd.Series) -> str:
    pattern = _pattern_name(row).lower().replace(" ", "_").replace("-", "_")
    if "darvas" in pattern:
        return "DARVAS_BREAKOUT"
    if "flag" in pattern:
        return "FLAG_BREAKOUT"
    if "cup" in pattern:
        return "CUP_WITH_HANDLE"
    if _text(row, "stage2_label").lower() == "stage1_to_stage2":
        return "STAGE_1_TO_2_ACCUMULATION"
    if _num(row, "near_52w_high_pct", 999.0) <= 5.0:
        return "52W_HIGH_BREAKOUT"
    if _num(row, "volume_ratio") >= 1.5:
        return "VOLUME_BREAKOUT"
    if _num(row, "close") and _num(row, "sma_50") and abs((_num(row, "close") - _num(row, "sma_50")) / _num(row, "sma_50")) <= 0.03:
        return "PULLBACK_TO_50DMA"
    return "BASE_BREAKOUT"


def build_technical_catalyst(row: pd.Series) -> dict[str, Any]:
    tags: list[str] = []
    parts: list[str] = []
    sector_status = _text(row, "sector_status") or _classify_sector_status(row)
    stage = _classify_stage(row)
    tier = _text(row, "candidate_tier").upper()
    setup_quality = _text(row, "setup_quality").lower()

    if tier in {"A", "B"} or "breakout" in setup_quality:
        tags.append("BREAKOUT")
        parts.append(f"Tier {tier or 'qualified'} breakout")
    if _num(row, "volume_ratio") >= 1.5:
        tags.append("VOLUME_SURGE")
        parts.append(f"{_num(row, 'volume_ratio'):.1f}x volume")
    if "DELIVERY_ACCUMULATION" in str(row.get("momentum_tags", "")) or _delivery_accumulation(row):
        tags.append("DELIVERY_SPIKE")
        parts.append("delivery accumulation")
    if sector_status == "LEADING":
        tags.append("SECTOR_LEADER")
        parts.append(f"Leading {_text(row, 'sector', 'sector')} sector")
    elif sector_status == "IMPROVING":
        tags.append("SECTOR_IMPROVING")
        parts.append(f"Improving {_text(row, 'sector', 'sector')} sector")
    if stage == "STAGE_2":
        tags.append("STAGE2_TREND")
        parts.append("Stage 2 trend")
    elif stage == "STAGE_1_TO_2":
        tags.append("STAGE_1_TO_2")
        parts.append("Stage 1 to 2 transition")
    pattern = _pattern_name(row).lower().replace(" ", "_").replace("-", "_")
    for needle, tag in PATTERN_TAGS.items():
        if needle in pattern and tag not in tags:
            tags.append(tag)
            parts.append(tag.replace("_", " ").title())
            break
    if "52W_HIGH" in str(row.get("momentum_tags", "")) and "52W_HIGH" not in tags:
        tags.append("52W_HIGH")
        parts.append("near 52-week high")

    score = min(100.0, 15.0 * len(tags) + max(_num(row, "breakout_score"), _num(row, "pattern_score")) * 0.35)
    summary = " + ".join(parts[:5]) if parts else "Technical setup passed watchlist funnel"
    return {
        "technical_catalyst_tags": tags,
        "technical_catalyst_summary": summary,
        "technical_catalyst_score": round(score, 2),
    }


def compute_watchlist_score(row: pd.Series) -> float:
    sector_status = _text(row, "sector_status")
    if sector_status == "LEADING":
        sector_score = 100.0
    elif sector_status == "IMPROVING":
        sector_score = 80.0
    elif _truthy(row.get("sector_escape_hatch")):
        sector_score = 35.0
    else:
        sector_score = 0.0
    stage_score = 100.0 if _classify_stage(row) == "STAGE_2" else 85.0 if _classify_stage(row) == "STAGE_1_TO_2" else 0.0
    momentum_count = len([tag for tag in str(row.get("momentum_tags", "")).split(",") if tag.strip()])
    momentum_score = min(100.0, momentum_count * 22.0)
    setup_score = max(_num(row, "breakout_score"), _num(row, "pattern_score"), 75.0 if _text(row, "candidate_tier").upper() in {"A", "B"} else 0.0)
    catalyst_score = _num(row, "technical_catalyst_score")
    score = sector_score * 0.25 + stage_score * 0.25 + momentum_score * 0.20 + setup_score * 0.20 + catalyst_score * 0.10
    return round(min(100.0, max(0.0, score)), 2)


def _assign_watchlist_bucket(row: pd.Series) -> str:
    composite_percentile = _num(row, "composite_percentile", 0.0)
    conviction = max(_num(row, "conviction_score"), _num(row, "breakout_score"))
    volume_ratio_20 = max(_num(row, "volume_ratio_20"), _num(row, "volume_ratio"))
    high_state = composite_percentile >= 80.0
    weak_state = composite_percentile < 50.0
    triggered = high_state and conviction >= 50.0 and not _truthy(row.get("sector_escape_hatch"))
    if triggered:
        return "TRIGGERED_TODAY"
    if _classify_stage(row) == "STAGE_1_TO_2" or _text(row, "stage2_label").lower() == "stage1_to_stage2":
        return "EARLY_STAGE2"
    if high_state:
        return "CORE_MOMENTUM"
    if volume_ratio_20 >= 1.5 and weak_state:
        return "AVOID_WEAK_CONFIRMATION"
    return "CORE_MOMENTUM" if _text(row, "sector_status") in {"LEADING", "IMPROVING"} else "AVOID_WEAK_CONFIRMATION"


def _evaluate_gate_row(row: pd.Series) -> dict[str, Any]:
    failures: list[str] = []
    if _truthy(row.get("is_quarantined")) or _text(row, "data_trust_status").lower() == "blocked":
        failures.append("DATA_TRUST")
    if _text(row, "tradability_status") == "FAILED":
        failures.append("TRADABILITY")
    if not _truthy(row.get("sector_ok")):
        failures.append("REGIME")
    if not _truthy(row.get("stage_ok")):
        failures.append("STAGE")
    if not _truthy(row.get("setup_ok")):
        failures.append("SETUP")
    if not _truthy(row.get("real_momentum_ok")):
        failures.append("MOMENTUM")
    if not _truthy(row.get("not_extended")):
        failures.append("EXTENSION")
    return {
        "gate_status": "PASSED" if not failures else "REJECTED",
        "gate_failures": ",".join(failures),
        "primary_gate_failure": failures[0] if failures else "",
    }


def build_watchlist_prefilter(
    ranked: pd.DataFrame,
    breakout: pd.DataFrame,
    pattern: pd.DataFrame,
    sector_dash: pd.DataFrame,
    *,
    top_n: int = 30,
    trust_summary: dict[str, Any] | None = None,
) -> pd.DataFrame:
    universe = _build_watchlist_universe(ranked, breakout, pattern, sector_dash, trust_summary=trust_summary)
    if universe.empty:
        return pd.DataFrame()
    accepted = universe.loc[universe["gate_status"].eq("PASSED")].copy()
    escape = universe.loc[
        universe["gate_status"].eq("REJECTED")
        & universe["gate_failures"].eq("REGIME")
        & universe["stage_ok"]
        & universe["real_momentum_ok"]
        & universe["setup_ok"]
        & universe["not_extended"]
        & universe["tradability_status"].ne("FAILED")
        & (pd.to_numeric(universe["breakout_score"], errors="coerce").fillna(0) >= 80)
        & universe.get("qualified", pd.Series(False, index=universe.index)).map(_truthy)
    ].copy()
    if not escape.empty:
        escape.loc[:, "sector_escape_hatch"] = True
        escape.loc[:, "gate_status"] = "PASSED"
        escape.loc[:, "gate_failures"] = ""
        escape.loc[:, "primary_gate_failure"] = ""
        escape = escape.sort_values(["breakout_score", "symbol_id"], ascending=[False, True], kind="stable").head(2)
    selected = pd.concat([accepted, escape], ignore_index=True)
    if selected.empty:
        return selected

    selected.loc[:, "watchlist_bucket"] = selected.apply(_assign_watchlist_bucket, axis=1)
    selected.loc[:, "operator_action"] = selected["watchlist_bucket"].map(
        {
            "TRIGGERED_TODAY": "Act Today",
            "CORE_MOMENTUM": "Study",
            "EARLY_STAGE2": "Watch",
            "AVOID_WEAK_CONFIRMATION": "Avoid",
        }
    ).fillna("Watch")
    tech = selected.apply(build_technical_catalyst, axis=1, result_type="expand")
    selected = pd.concat([selected.reset_index(drop=True), tech.reset_index(drop=True)], axis=1)
    selected.loc[:, "watchlist_score"] = selected.apply(compute_watchlist_score, axis=1)
    selected.loc[:, "score_version"] = SCORE_VERSION
    selected = selected.sort_values(
        ["watchlist_score", "composite_score", "breakout_score", "pattern_score", "symbol_id"],
        ascending=[False, False, False, False, True],
        na_position="last",
        kind="stable",
    ).head(int(top_n)).reset_index(drop=True)
    selected.loc[:, "prefilter_rank"] = range(1, len(selected) + 1)
    return selected


def build_watchlist_rejections(
    ranked: pd.DataFrame,
    breakout: pd.DataFrame,
    pattern: pd.DataFrame,
    sector_dash: pd.DataFrame,
    *,
    top_n: int = 100,
    trust_summary: dict[str, Any] | None = None,
) -> pd.DataFrame:
    universe = _build_watchlist_universe(ranked, breakout, pattern, sector_dash, trust_summary=trust_summary)
    if universe.empty:
        return pd.DataFrame(columns=REJECTION_COLUMNS)
    accepted = build_watchlist_prefilter(
        ranked,
        breakout,
        pattern,
        sector_dash,
        top_n=len(universe),
        trust_summary=trust_summary,
    )
    accepted_symbols = set(accepted.get("symbol_id", pd.Series(dtype=str)).astype(str))
    rejected = universe.loc[~universe["symbol_id"].astype(str).isin(accepted_symbols)].copy()
    rejected = rejected.loc[rejected["gate_status"].eq("REJECTED")].copy()
    if rejected.empty:
        return pd.DataFrame(columns=REJECTION_COLUMNS)
    rejected.loc[:, "score_version"] = SCORE_VERSION
    for column in REJECTION_COLUMNS:
        if column not in rejected.columns:
            rejected.loc[:, column] = ""
    rejected = rejected.sort_values(
        ["rank", "composite_score", "breakout_score", "pattern_score", "symbol_id"],
        ascending=[True, False, False, False, True],
        na_position="last",
        kind="stable",
    ).head(int(top_n)).reset_index(drop=True)
    return rejected[REJECTION_COLUMNS]


def _build_watchlist_universe(
    ranked: pd.DataFrame,
    breakout: pd.DataFrame,
    pattern: pd.DataFrame,
    sector_dash: pd.DataFrame,
    *,
    trust_summary: dict[str, Any] | None = None,
) -> pd.DataFrame:
    base = _as_symbol_frame(ranked)
    if base.empty:
        return pd.DataFrame()
    if "sector" not in base.columns:
        for candidate in ("sector_name", "Sector", "industry", "industry_name"):
            if candidate in base.columns:
                base.loc[:, "sector"] = base[candidate]
                break
    if "rank" not in base.columns:
        base.loc[:, "rank"] = range(1, len(base) + 1)

    merged = _join_best(base, _as_symbol_frame(breakout), score_cols=["breakout_score", "rel_strength_score"], suffix="breakout")
    merged = _join_best(merged, _as_symbol_frame(pattern), score_cols=["pattern_score", "pattern_priority_score"], suffix="pattern")
    sector_lookup = _sector_lookup(sector_dash)
    if "sector" in merged.columns and not sector_lookup.empty:
        merged = merged.merge(sector_lookup, on="sector", how="left")
    elif "Quadrant" not in merged.columns:
        merged.loc[:, "Quadrant"] = ""

    for source, fallback in (
        ("candidate_tier_breakout", "candidate_tier"),
        ("qualified_breakout", "qualified"),
        ("setup_quality_breakout", "setup_quality"),
        ("breakout_score_breakout", "breakout_score"),
        ("pattern_score_pattern", "pattern_score"),
        ("pattern_lifecycle_state_pattern", "pattern_lifecycle_state"),
        ("pattern_operational_tier_pattern", "pattern_operational_tier"),
        ("pattern_name_pattern", "pattern_name"),
        ("pattern_type_pattern", "pattern_type"),
        ("pattern_family_pattern", "pattern_family"),
    ):
        if source in merged.columns and fallback not in merged.columns:
            merged.loc[:, fallback] = merged[source]
    numeric_columns = (
        "breakout_score",
        "pattern_score",
        "close",
        "sma_50",
        "delivery_pct",
        "return_1",
        "return_5",
        "near_52w_high_pct",
        "volume_ratio",
        "volume_ratio_20",
        "conviction_score",
        "composite_score",
        *LIQUIDITY_COLUMNS,
    )
    for column in numeric_columns:
        if column not in merged.columns:
            merged.loc[:, column] = pd.NA
        merged.loc[:, column] = pd.to_numeric(merged[column], errors="coerce")

    quarantine_value = (trust_summary or {}).get(
        "active_quarantined_symbol_ids",
        (trust_summary or {}).get("active_quarantine_symbols", []),
    )
    if isinstance(quarantine_value, (list, tuple, set)):
        active_quarantine = {str(item) for item in quarantine_value}
    else:
        active_quarantine = set()
    merged.loc[:, "is_quarantined"] = merged["symbol_id"].astype(str).isin(active_quarantine)

    if "sector" in merged.columns:
        delivery_numeric = pd.to_numeric(merged["delivery_pct"], errors="coerce")
        sector_delivery = delivery_numeric.groupby(merged["sector"])
        sector_medians = sector_delivery.median(numeric_only=True).to_dict()
        sector_p75 = sector_delivery.quantile(0.75).to_dict()
    else:
        sector_medians = {}
        sector_p75 = {}
    merged.loc[:, "sector_status"] = merged.apply(_classify_sector_status, axis=1)
    merged.loc[:, "stage"] = merged.apply(_classify_stage, axis=1)
    merged.loc[:, "sector_median_delivery_pct"] = merged.get("sector", pd.Series("", index=merged.index)).map(sector_medians)
    merged.loc[:, "sector_p75_delivery_pct"] = merged.get("sector", pd.Series("", index=merged.index)).map(sector_p75)
    merged.loc[:, "momentum_tags"] = merged.apply(
        lambda row: ",".join(_collect_momentum_tags(row)),
        axis=1,
    )
    merged.loc[:, "setup_label"] = merged.apply(_classify_setup_label, axis=1)
    if "composite_score" in merged.columns:
        merged.loc[:, "composite_percentile"] = pd.to_numeric(merged["composite_score"], errors="coerce").rank(pct=True) * 100.0
    else:
        merged.loc[:, "composite_percentile"] = pd.NA
    tradability = merged.apply(_tradability, axis=1, result_type="expand")
    merged = pd.concat([merged.reset_index(drop=True), tradability.reset_index(drop=True)], axis=1)
    merged.loc[:, "extension_pct_sma50"] = merged.apply(_extension_pct, axis=1)

    sector_ok = merged["sector_status"].isin({"LEADING", "IMPROVING"})
    stage_ok = merged["stage"].isin({"STAGE_2", "STAGE_1_TO_2"})
    real_momentum_ok = merged.apply(_has_real_momentum, axis=1)
    breakout_ok = merged.get("candidate_tier", pd.Series("", index=merged.index)).astype(str).str.upper().isin({"A", "B"}) & merged.get("qualified", pd.Series(False, index=merged.index)).map(_truthy)
    pattern_ok = (
        pd.to_numeric(merged["pattern_score"], errors="coerce").fillna(0) >= 60
    ) & merged.get("pattern_lifecycle_state", pd.Series("", index=merged.index)).astype(str).str.lower().isin(PATTERN_LIFECYCLES) & (
        merged.get("pattern_operational_tier", pd.Series("", index=merged.index)).astype(str).str.lower() != "suppression_only"
    )
    sma50 = pd.to_numeric(merged["sma_50"], errors="coerce")
    close = pd.to_numeric(merged["close"], errors="coerce")
    not_extended = ~((sma50 > 0) & (((close - sma50) / sma50) > 0.25))
    setup_ok = breakout_ok | pattern_ok

    merged.loc[:, "sector_ok"] = sector_ok
    merged.loc[:, "stage_ok"] = stage_ok
    merged.loc[:, "real_momentum_ok"] = real_momentum_ok
    merged.loc[:, "setup_ok"] = setup_ok
    merged.loc[:, "not_extended"] = not_extended
    merged.loc[:, "sector_escape_hatch"] = False
    merged.loc[:, "watchlist_bucket"] = pd.NA
    merged.loc[:, "operator_action"] = pd.NA
    merged.loc[:, "score_version"] = SCORE_VERSION
    gate = merged.apply(_evaluate_gate_row, axis=1, result_type="expand")
    merged = pd.concat([merged.reset_index(drop=True), gate.reset_index(drop=True)], axis=1)
    return merged


def build_final_watchlist(
    prefilter: pd.DataFrame,
    catalyst_enrichment: dict[str, Any] | None = None,
    *,
    top_n: int = 15,
    data_trust_status: str = "unknown",
) -> pd.DataFrame:
    if prefilter is None or prefilter.empty:
        return pd.DataFrame(columns=FINAL_COLUMNS)
    rows = prefilter.copy()
    enrichment = catalyst_enrichment or {}
    for column in ("catalyst_tags", "catalyst_confidence", "bull_case", "risk_flags", "watchlist_reason"):
        if column not in rows.columns:
            rows.loc[:, column] = ""
    for idx, row in rows.iterrows():
        symbol_key = str(row.get("symbol_id") or "")
        record = enrichment.get(symbol_key, enrichment.get(symbol_key.upper(), {})) if isinstance(enrichment, dict) else {}
        if isinstance(record, str):
            try:
                record = json.loads(record)
            except Exception:
                record = {}
        rows.at[idx, "catalyst_tags"] = ",".join(record.get("catalyst_tags", []) or [])
        rows.at[idx, "catalyst_confidence"] = record.get("catalyst_confidence", "")
        rows.at[idx, "bull_case"] = record.get("bull_case", "")
        rows.at[idx, "risk_flags"] = ",".join(record.get("risk_flags", []) or [])
        rows.at[idx, "watchlist_reason"] = record.get("watchlist_reason", "") or row.get("technical_catalyst_summary", "")
    rows.loc[:, "action"] = rows["watchlist_score"].map(lambda score: "Study" if float(score) >= 80.0 else "Watch")
    if "operator_action" not in rows.columns:
        rows.loc[:, "operator_action"] = rows["action"]
    rows.loc[:, "data_trust_status"] = data_trust_status
    rows = rows.sort_values(
        ["watchlist_score", "composite_score", "breakout_score", "pattern_score", "symbol_id"],
        ascending=[False, False, False, False, True],
        na_position="last",
        kind="stable",
    ).head(int(top_n)).reset_index(drop=True)
    rows.loc[:, "rank"] = range(1, len(rows) + 1)
    for column in FINAL_COLUMNS:
        if column not in rows.columns:
            rows.loc[:, column] = ""
    return rows[FINAL_COLUMNS]


def validate_watchlist_candidates(frame: pd.DataFrame) -> list[str]:
    warnings: list[str] = []
    if frame is None or frame.empty:
        return warnings
    missing = [column for column in FINAL_COLUMNS if column not in frame.columns]
    if missing:
        warnings.append(f"watchlist missing required columns: {', '.join(missing)}")
    if "symbol_id" in frame.columns and frame["symbol_id"].isna().any():
        warnings.append("watchlist contains null symbol_id")
    if "symbol_id" in frame.columns:
        duplicate_symbols = frame["symbol_id"].dropna().astype(str).duplicated()
        if duplicate_symbols.any():
            warnings.append("watchlist contains duplicate symbol_id")
    if "rank" in frame.columns:
        ranks = pd.to_numeric(frame["rank"], errors="coerce")
        if ranks.duplicated().any():
            warnings.append("watchlist contains duplicate rank")
    if "watchlist_score" in frame.columns:
        scores = pd.to_numeric(frame["watchlist_score"], errors="coerce")
        if scores.isna().any() or (~scores.between(0, 100)).any():
            warnings.append("watchlist_score must be between 0 and 100")
    if "data_trust_status" in frame.columns:
        blocked = frame["data_trust_status"].astype(str).str.lower().eq("blocked")
        if blocked.any():
            warnings.append("watchlist contains blocked data_trust_status")
    if "watchlist_bucket" in frame.columns:
        buckets = frame["watchlist_bucket"].dropna().astype(str)
        invalid = buckets.ne("") & ~buckets.isin(WATCHLIST_BUCKETS)
        if invalid.any():
            warnings.append("watchlist_bucket contains unknown values")
    if "operator_action" in frame.columns:
        empty_action = frame["operator_action"].fillna("").astype(str).str.strip().eq("")
        if empty_action.any():
            warnings.append("operator_action must be non-empty")
    if "gate_status" in frame.columns:
        statuses = frame["gate_status"].dropna().astype(str)
        invalid_status = statuses.ne("") & ~statuses.isin({"PASSED", "REJECTED"})
        if invalid_status.any():
            warnings.append("gate_status contains unknown values")
    return warnings
