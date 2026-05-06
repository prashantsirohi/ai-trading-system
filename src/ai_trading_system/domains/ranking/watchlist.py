"""Deterministic watchlist funnel for rank-stage operator candidates."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd


LEADING_QUADRANTS = {"leading", "improving"}
STAGE2_LABELS = {"strong_stage2", "stage2", "stage1_to_stage2"}
PATTERN_LIFECYCLES = {"watchlist", "confirmed"}
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


def _collect_momentum_tags(row: pd.Series, *, sector_median_delivery_pct: float | None = None) -> list[str]:
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
    delivery = _num(row, "delivery_pct", -1.0)
    if sector_median_delivery_pct is not None and delivery >= sector_median_delivery_pct:
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
    if "DELIVERY_ACCUMULATION" in str(row.get("momentum_tags", "")) or _num(row, "delivery_pct") >= _num(row, "sector_median_delivery_pct", 10**9):
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
    sector_score = 100.0 if _text(row, "sector_status") == "LEADING" else 80.0 if _text(row, "sector_status") == "IMPROVING" else 50.0
    stage_score = 100.0 if _classify_stage(row) == "STAGE_2" else 85.0 if _classify_stage(row) == "STAGE_1_TO_2" else 0.0
    momentum_count = len([tag for tag in str(row.get("momentum_tags", "")).split(",") if tag.strip()])
    momentum_score = min(100.0, momentum_count * 22.0)
    setup_score = max(_num(row, "breakout_score"), _num(row, "pattern_score"), 75.0 if _text(row, "candidate_tier").upper() in {"A", "B"} else 0.0)
    catalyst_score = _num(row, "technical_catalyst_score")
    score = sector_score * 0.25 + stage_score * 0.25 + momentum_score * 0.20 + setup_score * 0.20 + catalyst_score * 0.10
    return round(min(100.0, max(0.0, score)), 2)


def build_watchlist_prefilter(
    ranked: pd.DataFrame,
    breakout: pd.DataFrame,
    pattern: pd.DataFrame,
    sector_dash: pd.DataFrame,
    *,
    top_n: int = 30,
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
    for column in ("breakout_score", "pattern_score", "close", "sma_50", "delivery_pct", "return_1", "return_5", "near_52w_high_pct", "volume_ratio", "composite_score"):
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
    if active_quarantine:
        merged = merged.loc[~merged["symbol_id"].astype(str).isin(active_quarantine)].copy()

    sector_medians = merged.groupby("sector")["delivery_pct"].median(numeric_only=True).to_dict() if "sector" in merged.columns else {}
    merged.loc[:, "sector_status"] = merged.apply(_classify_sector_status, axis=1)
    merged.loc[:, "stage"] = merged.apply(_classify_stage, axis=1)
    merged.loc[:, "sector_median_delivery_pct"] = merged.get("sector", pd.Series("", index=merged.index)).map(sector_medians)
    merged.loc[:, "momentum_tags"] = merged.apply(
        lambda row: ",".join(_collect_momentum_tags(row, sector_median_delivery_pct=_num(row, "sector_median_delivery_pct", 10**9))),
        axis=1,
    )
    merged.loc[:, "setup_label"] = merged.apply(_classify_setup_label, axis=1)

    sector_ok = merged["sector_status"].isin({"LEADING", "IMPROVING"})
    stage_ok = merged["stage"].isin({"STAGE_2", "STAGE_1_TO_2"})
    momentum_ok = merged["momentum_tags"].astype(str).str.len() > 0
    breakout_ok = merged.get("candidate_tier", pd.Series("", index=merged.index)).astype(str).str.upper().isin({"A", "B"}) & merged.get("qualified", pd.Series(False, index=merged.index)).map(_truthy)
    pattern_ok = (
        pd.to_numeric(merged["pattern_score"], errors="coerce").fillna(0) >= 60
    ) & merged.get("pattern_lifecycle_state", pd.Series("", index=merged.index)).astype(str).str.lower().isin(PATTERN_LIFECYCLES) & (
        merged.get("pattern_operational_tier", pd.Series("", index=merged.index)).astype(str).str.lower() != "suppression_only"
    )
    sma50 = pd.to_numeric(merged["sma_50"], errors="coerce")
    close = pd.to_numeric(merged["close"], errors="coerce")
    not_extended = ~((sma50 > 0) & (((close - sma50) / sma50) > 0.25))

    primary = merged.loc[sector_ok & stage_ok & momentum_ok & (breakout_ok | pattern_ok) & not_extended].copy()
    escape = merged.loc[
        (~sector_ok)
        & stage_ok
        & momentum_ok
        & not_extended
        & (pd.to_numeric(merged["breakout_score"], errors="coerce").fillna(0) >= 80)
        & merged.get("qualified", pd.Series(False, index=merged.index)).map(_truthy)
    ].copy()
    if not escape.empty:
        escape.loc[:, "sector_escape_hatch"] = True
        escape = escape.sort_values(["breakout_score", "symbol_id"], ascending=[False, True], kind="stable").head(2)
    primary.loc[:, "sector_escape_hatch"] = pd.Series(False, index=primary.index, dtype=bool)
    if escape.empty:
        escape.loc[:, "sector_escape_hatch"] = pd.Series(dtype=bool)
    selected = pd.concat([primary, escape], ignore_index=True)
    if selected.empty:
        return selected

    tech = selected.apply(build_technical_catalyst, axis=1, result_type="expand")
    selected = pd.concat([selected.reset_index(drop=True), tech.reset_index(drop=True)], axis=1)
    selected.loc[:, "watchlist_score"] = selected.apply(compute_watchlist_score, axis=1)
    selected = selected.sort_values(
        ["watchlist_score", "composite_score", "breakout_score", "pattern_score", "symbol_id"],
        ascending=[False, False, False, False, True],
        na_position="last",
        kind="stable",
    ).head(int(top_n)).reset_index(drop=True)
    selected.loc[:, "prefilter_rank"] = range(1, len(selected) + 1)
    return selected


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
        record = enrichment.get(str(row.get("symbol_id")), {}) if isinstance(enrichment, dict) else {}
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
