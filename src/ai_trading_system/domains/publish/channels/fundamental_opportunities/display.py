"""Display helpers for the fundamental opportunity report."""

from __future__ import annotations

from typing import Any

import pandas as pd


LABELS = {
    "ADD_TO_TRACKER": "Add to tracker",
    "ADD_TO_WATCHLIST": "Add to watchlist",
    "AVOID_WATCH": "Avoid / Watch",
    "BASE_CASE": "Base case",
    "BELOW_OWN_MEDIAN": "Below own median",
    "CYCLICAL_COMMODITY": "Cyclical / Commodity",
    "DATA_FAILURE": "Data failure",
    "DEEPLY_BELOW_HISTORY": "Deep discount vs history",
    "DEEP_VALUE": "Deep Value",
    "DIVIDEND_CASH_COW": "Dividend / Cash Cow",
    "EXPENSIVE_VS_HISTORY": "Expensive vs history",
    "F1_FUNDAMENTAL_WATCH": "Fundamental watch",
    "F2_RESULT_VALUE_ACCUMULATION": "Result/value accumulation",
    "F4_ACTION_CANDIDATE": "Action candidate",
    "GOOD_RESULTS_BELOW_HISTORY": "Good results, below history",
    "GOOD_RESULTS_BUT_EXPENSIVE": "Good results, expensive",
    "HIGH_GROWTH": "High Growth",
    "IMPROVING_BELOW_AVERAGE": "Improving, below own average",
    "INSUFFICIENT_HISTORY": "Limited history",
    "MANUAL_REVIEW": "Manual review",
    "NEAR_OWN_MEDIAN": "Near own median",
    "QUALITY_COMPOUNDER": "Quality Compounder",
    "REPORT_ONLY": "Report only",
    "RESULT_FAILURE": "Data failure",
    "TRACK_CLOSELY": "Track closely",
    "TURNAROUND_CANDIDATE": "Turnaround Candidate",
}


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "nat", "<na>"}


def clean_label(value: Any, *, missing: str = "-") -> str:
    if is_missing(value):
        return missing
    text = str(value).strip()
    upper = text.upper()
    return LABELS.get(upper, text.replace("_", " ").title() if "_" in text else text)


def fmt_num(value: Any, digits: int = 1, *, missing: str = "-") -> str:
    if is_missing(value):
        return missing
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return clean_label(value, missing=missing)


def fmt_pct(value: Any, digits: int = 1, *, missing: str = "N/A") -> str:
    if is_missing(value):
        return missing
    try:
        return f"{float(value):+,.{digits}f}%"
    except (TypeError, ValueError):
        return clean_label(value, missing=missing)


def fmt_bps(value: Any, *, missing: str = "N/A") -> str:
    if is_missing(value):
        return missing
    try:
        return f"{float(value):+,.0f} bps"
    except (TypeError, ValueError):
        return clean_label(value, missing=missing)


def score_band(score: Any) -> str:
    value = _float(score, 0.0)
    if value >= 70:
        return "Strong candidate"
    if value >= 60:
        return "Watchlist candidate"
    if value >= 50:
        return "Manual review"
    return "Weak / avoid"


def score_tone(score: Any) -> str:
    value = _float(score, 0.0)
    if value >= 70:
        return "strong"
    if value >= 60:
        return "watch"
    if value >= 50:
        return "review"
    return "weak"


def score_width(score: Any) -> int:
    return int(max(0, min(100, round(_float(score, 0.0)))))


def display_row(row: dict[str, Any]) -> dict[str, Any]:
    score = row.get("final_watchlist_score")
    bucket = clean_label(row.get("bucket_label") or row.get("business_bucket"))
    secondary = clean_label(row.get("secondary_bucket_tags"), missing="")
    bucket_display = f"{bucket} + {secondary}" if secondary else bucket
    return {
        **row,
        "symbol_display": clean_label(row.get("symbol")),
        "industry_display": first_clean(row, ["industry_group", "industry", "sector_name"]),
        "bucket_display": bucket_display,
        "opportunity_display": clean_label(row.get("opportunity_label")),
        "score_display": fmt_num(score, 1),
        "score_band": score_band(score),
        "score_tone": score_tone(score),
        "score_width": score_width(score),
        "growth_display": f"Rev {fmt_pct(row.get('sales_growth_3y'))} / PAT {fmt_pct(row.get('profit_growth_3y'))}",
        "margin_display": fmt_bps(row.get("opm_yoy_change_bps")),
        "valuation_display": clean_label(row.get("valuation_history_bucket")),
        "valuation_score_display": fmt_num(row.get("valuation_history_score"), 1),
        "action_display": clean_label(row.get("next_action") or row.get("watchlist_bucket")),
        "tracker_display": clean_label(row.get("tracker_status"), missing="-"),
        "reason_display": clean_label(row.get("bucket_reason")),
    }


def first_clean(row: dict[str, Any], keys: list[str], *, missing: str = "-") -> str:
    for key in keys:
        value = row.get(key)
        if not is_missing(value):
            return clean_label(value, missing=missing)
    return missing


def _float(value: Any, default: float) -> float:
    try:
        if is_missing(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


__all__ = [
    "clean_label",
    "display_row",
    "first_clean",
    "fmt_bps",
    "fmt_num",
    "fmt_pct",
    "is_missing",
    "score_band",
    "score_tone",
    "score_width",
]
