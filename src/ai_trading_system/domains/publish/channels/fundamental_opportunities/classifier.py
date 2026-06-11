"""Screenshot-based fundamental business bucket classifier."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


BUCKET_ORDER = [
    "QUALITY_COMPOUNDER",
    "HIGH_GROWTH",
    "DIVIDEND_CASH_COW",
    "TURNAROUND_CANDIDATE",
    "DEEP_VALUE",
    "CYCLICAL_COMMODITY",
    "AVOID_WATCH",
]

BUCKET_LABELS = {
    "QUALITY_COMPOUNDER": "Quality Compounder",
    "TURNAROUND_CANDIDATE": "Turnaround Candidate",
    "HIGH_GROWTH": "High Growth",
    "DIVIDEND_CASH_COW": "Dividend / Cash Cow",
    "DEEP_VALUE": "Deep Value",
    "CYCLICAL_COMMODITY": "Cyclical / Commodity",
    "AVOID_WATCH": "Avoid / Watch",
}

CYCLICAL_KEYWORDS = (
    "ALUMINIUM",
    "CEMENT",
    "CHEMICAL",
    "COAL",
    "COMMODIT",
    "COPPER",
    "FERTIL",
    "GAS",
    "IRON",
    "METAL",
    "MINING",
    "OIL",
    "PAPER",
    "PETRO",
    "POWER",
    "STEEL",
    "SUGAR",
    "TEXTILE",
)

STRONG_RESULT_BUCKETS = {
    "BLOWOUT_RESULT",
    "GREAT_RESULT",
    "RESULT_ACCELERATION",
    "MARGIN_EXPANSION",
}

BELOW_HISTORY_BUCKETS = {"DEEPLY_BELOW_HISTORY", "BELOW_OWN_MEDIAN"}
EXPENSIVE_BUCKETS = {"EXPENSIVE_VS_HISTORY"}


@dataclass(frozen=True)
class BucketCard:
    bucket: str
    label: str
    subtitle: str
    description: str
    tone: str


BUCKET_CARDS = [
    BucketCard(
        "QUALITY_COMPOUNDER",
        "Quality Compounder",
        "Consistent growth + high returns",
        "Compounding machines with high ROCE, durable growth, healthy margins, and low leverage.",
        "green",
    ),
    BucketCard(
        "TURNAROUND_CANDIDATE",
        "Turnaround Candidate",
        "Improving from a low base",
        "Recovering businesses where OPM, PAT, or sales trajectory is improving after weak years.",
        "amber",
    ),
    BucketCard(
        "HIGH_GROWTH",
        "High Growth",
        "Fast growers, margin secondary",
        "Revenue-first hypergrowth where market share and growth runway matter more than current margins.",
        "blue",
    ),
    BucketCard(
        "DIVIDEND_CASH_COW",
        "Dividend / Cash Cow",
        "Mature, high cashflow, low growth",
        "Stable cash generators with strong free cash conversion, dividends, and low capital intensity.",
        "violet",
    ),
    BucketCard(
        "DEEP_VALUE",
        "Deep Value",
        "Cheap on assets, low growth priced in",
        "Companies trading below history or asset anchors with acceptable balance sheet risk.",
        "orange",
    ),
    BucketCard(
        "CYCLICAL_COMMODITY",
        "Cyclical / Commodity",
        "Earnings tied to cycle/commodity",
        "Highly variable earnings; evaluate at cycle trough or normalized mid-cycle metrics, not peak TTM.",
        "gray",
    ),
    BucketCard(
        "AVOID_WATCH",
        "Avoid / Watch",
        "Red flags present",
        "Structural deterioration, aggressive leverage, weak cash conversion, or poor accounting quality.",
        "red",
    ),
]


def classify_fundamental_opportunities(frame: pd.DataFrame) -> pd.DataFrame:
    """Classify rows into screenshot-style buckets and tracker-compatible labels."""

    if frame is None or frame.empty:
        return _empty_output()
    source = frame.copy()
    if "symbol" not in source.columns:
        for candidate in ("symbol_id", "ticker", "NSE Code"):
            if candidate in source.columns:
                source.loc[:, "symbol"] = source[candidate]
                break
    if "symbol" not in source.columns:
        return _empty_output()
    source.loc[:, "symbol"] = source["symbol"].astype(str).str.upper().str.strip()
    source = source.loc[source["symbol"].ne("")].copy()

    rows: list[dict[str, Any]] = []
    for _, row in source.iterrows():
        classified = _classify_row(row)
        rows.append({**row.to_dict(), **classified})
    out = pd.DataFrame(rows)
    if out.empty:
        return _empty_output()
    out.loc[:, "bucket_label"] = out["business_bucket"].map(BUCKET_LABELS).fillna(out["business_bucket"])
    out.loc[:, "promote_to_tracker"] = out["business_bucket"].ne("AVOID_WATCH")
    out.loc[:, "watchlist_bucket"] = out.apply(_watchlist_bucket, axis=1)
    out.loc[:, "final_watchlist_score"] = out.apply(_final_watchlist_score, axis=1).round(2)
    out.loc[:, "next_action"] = out.apply(_next_action, axis=1)
    out.loc[:, "_bucket_rank"] = out["business_bucket"].map({bucket: idx for idx, bucket in enumerate(BUCKET_ORDER)}).fillna(99)
    out = out.sort_values(["_bucket_rank", "final_watchlist_score", "symbol"], ascending=[True, False, True], kind="stable")
    return out.drop(columns=["_bucket_rank"], errors="ignore").reset_index(drop=True)


def tracker_shortlist(classified: pd.DataFrame) -> pd.DataFrame:
    """Return the tracker-compatible shortlist CSV frame."""

    columns = [
        "symbol",
        "bucket_as_of",
        "business_bucket",
        "secondary_bucket_tags",
        "opportunity_label",
        "bucket_reason",
        "manual_review_flag",
        "watchlist_bucket",
        "final_watchlist_score",
        "next_action",
        "tracker_status",
    ]
    if classified is None or classified.empty:
        return pd.DataFrame(columns=columns)
    frame = classified.copy()
    if "promote_to_tracker" in frame.columns:
        frame = frame.loc[frame["promote_to_tracker"].fillna(False).astype(bool)].copy()
    return frame[[column for column in columns if column in frame.columns]].reset_index(drop=True)


def bucket_counts(classified: pd.DataFrame) -> dict[str, int]:
    if classified is None or classified.empty or "business_bucket" not in classified.columns:
        return {bucket: 0 for bucket in BUCKET_ORDER}
    counts = classified["business_bucket"].value_counts().to_dict()
    return {bucket: int(counts.get(bucket, 0)) for bucket in BUCKET_ORDER}


def metric_definitions() -> list[dict[str, str]]:
    return [
        {"metric": "Revenue Growth (3yr CAGR)", "rationale": "Captures sustained demand growth and filters one-off years.", "type": "threshold"},
        {"metric": "PAT Growth (3yr CAGR)", "rationale": "Checks whether growth converts into shareholder earnings.", "type": "threshold"},
        {"metric": "OPM (Operating Profit Margin)", "rationale": "Level shows moat; trend shows whether economics are widening or eroding.", "type": "level + trend"},
        {"metric": "PAT Margin", "rationale": "Net conversion: how much revenue reaches shareholders.", "type": "threshold"},
        {"metric": "ROCE", "rationale": "Primary measure of capital efficiency and business quality.", "type": "threshold"},
        {"metric": "ROE", "rationale": "Shareholder return on equity; read alongside leverage.", "type": "threshold"},
        {"metric": "Debt / Equity", "rationale": "High D/E amplifies losses in down cycles; low leverage adds resilience.", "type": "upper bound"},
        {"metric": "Interest Coverage", "rationale": "Debt service ability; unavailable rows are not guessed.", "type": "lower bound"},
        {"metric": "FCF / PAT", "rationale": "Checks whether reported PAT converts into real cash.", "type": "ratio"},
        {"metric": "EV / EBITDA", "rationale": "Capital-structure-neutral valuation; use normalized readings for cyclicals.", "type": "valuation"},
        {"metric": "P/B Ratio", "rationale": "Asset-base floor; useful for capital-heavy and financial companies.", "type": "valuation"},
        {"metric": "Dividend Yield / Payout", "rationale": "Identifies mature income-generating businesses.", "type": "income filter"},
    ]


def bucket_matrix() -> list[dict[str, str]]:
    return [
        {"bucket": "Quality Compounder", "rev_growth": ">15% CAGR", "opm": ">18% stable", "roce": ">20%", "debt": "<0.5", "pat_trend": "Growing", "fcf": "High", "valuation": "P/E / PEG"},
        {"bucket": "High Growth", "rev_growth": ">30% TTM", "opm": "Rising, any level", "roce": "Any", "debt": "Any", "pat_trend": "Growing/improving", "fcf": "Moderate", "valuation": "P/S / EV/Sales"},
        {"bucket": "Dividend / Cash Cow", "rev_growth": "5-12%", "opm": ">20% stable", "roce": "15-20%", "debt": "<0.3", "pat_trend": "Stable", "fcf": "Very high", "valuation": "Dividend / FCF yield"},
        {"bucket": "Turnaround", "rev_growth": "Reviving >10%", "opm": "Expanding", "roce": "Improving", "debt": "Declining", "pat_trend": "Was negative, now +", "fcf": "Improving", "valuation": "EV/EBITDA"},
        {"bucket": "Deep Value", "rev_growth": "Flat to low", "opm": "Decent/stable", "roce": ">8%", "debt": "<1x", "pat_trend": "Flat/slight growth", "fcf": "Moderate", "valuation": "P/B < 1.5x"},
        {"bucket": "Cyclical", "rev_growth": "Volatile", "opm": "High swing", "roce": "Variable", "debt": "<2x at trough", "pat_trend": "Volatile", "fcf": "Variable", "valuation": "Normalized EV/EBITDA"},
        {"bucket": "Avoid", "rev_growth": "Declining", "opm": "Compressing", "roce": "Low/falling", "debt": ">2x", "pat_trend": "Declining", "fcf": "Low (CFO < PAT)", "valuation": "Irrelevant"},
    ]


def _classify_row(row: pd.Series) -> dict[str, Any]:
    reasons: list[str] = []
    secondary: list[str] = []
    manual_review = False

    avoid, avoid_reasons = _avoid_signal(row)
    if avoid:
        return _result("AVOID_WATCH", avoid_reasons, [], _opportunity_label(row), True)

    rules = [
        ("QUALITY_COMPOUNDER", _quality_signal(row)),
        ("HIGH_GROWTH", _high_growth_signal(row)),
        ("DIVIDEND_CASH_COW", _cash_cow_signal(row)),
        ("TURNAROUND_CANDIDATE", _turnaround_signal(row)),
        ("DEEP_VALUE", _deep_value_signal(row)),
        ("CYCLICAL_COMMODITY", _cyclical_signal(row)),
    ]
    matched = [(bucket, why) for bucket, why in rules if why]
    if matched:
        bucket, reasons = matched[0]
        secondary = [BUCKET_LABELS[item[0]] for item in matched[1:]]
    else:
        bucket = "AVOID_WATCH"
        reasons = ["No bucket rule passed; manual review required"]
        manual_review = True

    if _cyclical_signal(row) and bucket != "CYCLICAL_COMMODITY":
        secondary.append("Cyclical / Commodity")
        manual_review = True
    if bool(_bool(row.get("low_base_flag"))):
        secondary.append("Low base")
        manual_review = True
    if _text(row.get("valuation_history_bucket")).upper() == "INSUFFICIENT_HISTORY":
        secondary.append("Insufficient valuation history")
        manual_review = True
    return _result(bucket, reasons, secondary, _opportunity_label(row), manual_review)


def _result(
    bucket: str,
    reasons: list[str],
    secondary: list[str],
    opportunity: str,
    manual_review: bool,
) -> dict[str, Any]:
    deduped_secondary = list(dict.fromkeys([item for item in secondary if item]))
    return {
        "business_bucket": bucket,
        "secondary_bucket_tags": ", ".join(deduped_secondary),
        "opportunity_label": opportunity,
        "bucket_reason": "; ".join(reasons),
        "manual_review_flag": bool(manual_review),
    }


def _avoid_signal(row: pd.Series) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if _bool(row.get("hard_red_flag")):
        reasons.append("hard red flag")
    if _num(row.get("debt_to_equity")) > 2:
        reasons.append("D/E > 2x")
    if _num(row.get("roce"), 50) < 8:
        reasons.append("ROCE < 8%")
    if _num(row.get("roe"), 50) < 5:
        reasons.append("ROE < 5%")
    if _num(row.get("profit_growth_3y"), 0) < 0 and _num(row.get("sales_growth_3y"), 0) < 0:
        reasons.append("sales and PAT declining")
    if _num(row.get("cash_from_operations_last_year"), 0) < 0 and _num(row.get("free_cash_flow_last_year"), 0) < 0:
        reasons.append("CFO and FCF negative")
    if _text(row.get("quarterly_result_bucket")).upper() == "DETERIORATING":
        reasons.append("deteriorating quarterly result")
    if _opm_trend(row) < -5 and _num(row.get("profit_growth_3y"), 0) < 0:
        reasons.append("margin compression with weak PAT trend")
    return bool(reasons), reasons


def _quality_signal(row: pd.Series) -> list[str]:
    roce = _num(row.get("roce"), _score_proxy(row, "quality_score", 0, 100, 5, 30))
    debt = _num(row.get("debt_to_equity"), 0.5)
    opm = _num(row.get("opm"), _num(row.get("opm_pct")))
    sales_growth = _num(row.get("sales_growth_3y"), _num(row.get("sales_4q_cagr"), 0) * 100)
    profit_growth = _num(row.get("profit_growth_3y"), _num(row.get("profit_4q_cagr"), 0) * 100)
    fcf_quality = _fcf_pat(row)
    if roce > 20 and debt < 0.5 and opm >= 18 and sales_growth >= 15 and profit_growth >= 10 and fcf_quality >= 0.7:
        return ["ROCE > 20%", "low leverage", "stable growth and cash conversion"]
    return []


def _high_growth_signal(row: pd.Series) -> list[str]:
    sales_growth = max(
        _num(row.get("sales_growth_3y"), 0),
        _num(row.get("sales_growth"), 0),
        _num(row.get("sales_yoy_pct"), 0),
    )
    profit_growth = max(_num(row.get("profit_growth_3y"), 0), _num(row.get("profit_yoy_pct"), 0))
    if sales_growth > 30 and profit_growth >= 0 and _opm_trend(row) >= 0:
        return ["revenue growth > 30%", "PAT/margin trend not deteriorating"]
    return []


def _cash_cow_signal(row: pd.Series) -> list[str]:
    sales_growth = _num(row.get("sales_growth_3y"), 0)
    opm = _num(row.get("opm"), _num(row.get("opm_pct")))
    debt = _num(row.get("debt_to_equity"), 1)
    dividend = max(_num(row.get("dividend_yield"), 0), _num(row.get("dividend_payout_pct"), 0) / 20)
    if 5 <= sales_growth <= 12 and opm >= 20 and debt < 0.3 and _fcf_pat(row) >= 0.9 and dividend > 0:
        return ["stable growth", "high FCF quality", "dividend evidence"]
    return []


def _turnaround_signal(row: pd.Series) -> list[str]:
    sales_reviving = max(_num(row.get("sales_yoy_pct"), 0), _num(row.get("sales_qoq_pct"), 0), _num(row.get("sales_growth"), 0)) > 10
    profit_improving = max(_num(row.get("profit_yoy_pct"), 0), _num(row.get("profit_qoq_pct"), 0), _num(row.get("yoy_quarterly_profit_growth"), 0)) > 0
    opm_expanding = _opm_trend(row) > 1 or _num(row.get("opm_yoy_change_bps"), 0) >= 100
    q_bucket = _text(row.get("quarterly_result_bucket")).upper()
    if (sales_reviving and profit_improving and opm_expanding) or q_bucket in {"TURNAROUND", "MARGIN_EXPANSION", "RESULT_ACCELERATION"}:
        return ["reviving sales/PAT", "OPM trend expanding"]
    return []


def _deep_value_signal(row: pd.Series) -> list[str]:
    valuation_bucket = _text(row.get("valuation_history_bucket")).upper()
    pb = _num(row.get("price_to_book"), _num(row.get("pb"), 99))
    debt = _num(row.get("debt_to_equity"), 99)
    roce = _num(row.get("roce"), _score_proxy(row, "quality_score", 0, 100, 5, 30))
    if (valuation_bucket in BELOW_HISTORY_BUCKETS or pb < 1.5 or _num(row.get("pb_vs_5y_median_pct"), 99) < -10) and debt < 1 and roce > 8:
        return ["below-history or asset valuation", "acceptable balance sheet"]
    return []


def _cyclical_signal(row: pd.Series) -> list[str]:
    text = f"{_text(row.get('industry_group'))} {_text(row.get('industry'))} {_text(row.get('sector_name'))}".upper()
    if any(keyword in text for keyword in CYCLICAL_KEYWORDS):
        return ["cyclical industry; use normalized/mid-cycle valuation"]
    return []


def _opportunity_label(row: pd.Series) -> str:
    q_bucket = _text(row.get("quarterly_result_bucket")).upper()
    valuation_bucket = _text(row.get("valuation_history_bucket")).upper()
    expensive = valuation_bucket in EXPENSIVE_BUCKETS or max(_num(row.get("pe_pctile_5y"), 0), _num(row.get("ps_pctile_5y"), 0), _num(row.get("pb_pctile_5y"), 0)) >= 80
    below_history = valuation_bucket in BELOW_HISTORY_BUCKETS or min(
        _num(row.get("pe_vs_5y_median_pct"), 99),
        _num(row.get("ps_vs_5y_median_pct"), 99),
        _num(row.get("pb_vs_5y_median_pct"), 99),
    ) < 0
    if q_bucket in STRONG_RESULT_BUCKETS and expensive:
        return "GOOD_RESULTS_BUT_EXPENSIVE"
    if q_bucket in STRONG_RESULT_BUCKETS and below_history:
        return "GOOD_RESULTS_BELOW_HISTORY"
    if (q_bucket in {"RESULT_ACCELERATION", "MARGIN_EXPANSION", "TURNAROUND"} or _opm_trend(row) > 1) and below_history:
        return "IMPROVING_BELOW_AVERAGE"
    if valuation_bucket == "INSUFFICIENT_HISTORY" or _cyclical_signal(row) or _bool(row.get("low_base_flag")):
        return "MANUAL_REVIEW"
    return "BASE_CASE"


def _watchlist_bucket(row: pd.Series) -> str:
    if row.get("business_bucket") == "AVOID_WATCH":
        return ""
    q_score = _num(row.get("quarterly_result_score"), 50)
    opportunity = _text(row.get("opportunity_label"))
    bucket = _text(row.get("business_bucket"))
    if q_score >= 80 and opportunity in {"GOOD_RESULTS_BELOW_HISTORY", "IMPROVING_BELOW_AVERAGE"}:
        return "F4_ACTION_CANDIDATE"
    if opportunity in {"GOOD_RESULTS_BELOW_HISTORY", "IMPROVING_BELOW_AVERAGE"} or bucket == "DEEP_VALUE":
        return "F2_RESULT_VALUE_ACCUMULATION"
    return "F1_FUNDAMENTAL_WATCH"


def _final_watchlist_score(row: pd.Series) -> float:
    if row.get("business_bucket") == "AVOID_WATCH":
        return 0.0
    q = _num(row.get("quarterly_result_score"), 50)
    v = _num(row.get("valuation_history_score"), 50)
    quality = _num(row.get("quality_score"), _num(row.get("roce"), 15) * 3)
    growth = _num(row.get("growth_score"), max(_num(row.get("sales_growth_3y"), 0), _num(row.get("profit_growth_3y"), 0)) * 2)
    manual_penalty = 8 if _bool(row.get("manual_review_flag")) else 0
    return max(0.0, min(100.0, 0.30 * q + 0.25 * v + 0.25 * quality + 0.20 * growth - manual_penalty))


def _next_action(row: pd.Series) -> str:
    if row.get("business_bucket") == "AVOID_WATCH":
        return "REPORT_ONLY"
    if row.get("watchlist_bucket") == "F4_ACTION_CANDIDATE":
        return "TRACK_CLOSELY"
    if _bool(row.get("manual_review_flag")):
        return "MANUAL_REVIEW"
    return "ADD_TO_TRACKER"


def _opm_trend(row: pd.Series) -> float:
    if pd.notna(row.get("opm_yoy_change_bps")):
        return _num(row.get("opm_yoy_change_bps")) / 100.0
    if pd.notna(row.get("opm_qoq_change_bps")):
        return _num(row.get("opm_qoq_change_bps")) / 100.0
    if pd.notna(row.get("opm")) and pd.notna(row.get("opm_last_year")):
        return _num(row.get("opm")) - _num(row.get("opm_last_year"))
    return 0.0


def _fcf_pat(row: pd.Series) -> float:
    if pd.notna(row.get("fcf_pat")):
        return _num(row.get("fcf_pat"), 0)
    fcf = _num(row.get("free_cash_flow_last_year"), _num(row.get("fcf"), 0))
    pat = _num(row.get("net_profit_cr"), _num(row.get("pat"), _num(row.get("profit_after_tax"), 0)))
    if pat == 0:
        return 0.0
    return fcf / abs(pat)


def _score_proxy(row: pd.Series, column: str, low_score: float, high_score: float, low_value: float, high_value: float) -> float:
    score = _num(row.get(column), 50)
    score = min(max(score, low_score), high_score)
    return low_value + ((score - low_score) / (high_score - low_score)) * (high_value - low_value)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _text(value).lower()
    return text in {"1", "true", "t", "yes", "y"}


def _text(value: Any) -> str:
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value or "").strip()


def _empty_output() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "business_bucket",
            "bucket_label",
            "secondary_bucket_tags",
            "opportunity_label",
            "bucket_reason",
            "manual_review_flag",
            "watchlist_bucket",
            "final_watchlist_score",
            "next_action",
            "promote_to_tracker",
        ]
    )


__all__ = [
    "BUCKET_CARDS",
    "BUCKET_LABELS",
    "BUCKET_ORDER",
    "bucket_counts",
    "bucket_matrix",
    "classify_fundamental_opportunities",
    "metric_definitions",
    "tracker_shortlist",
]
