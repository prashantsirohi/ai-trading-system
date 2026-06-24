"""Pure builders for the operator-facing Google Sheets daily report."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd


CONFIRMED_BREAKOUT_COLUMNS = [
    "Status",
    "Priority",
    "Symbol",
    "Sector",
    "Sector Status",
    "Setup",
    "Candidate Tier",
    "Composite Score",
    "Breakout Score",
    "Breakout Type",
    "Breakout Level",
    "Close",
    "Distance To Breakout %",
    "Volume Ratio",
    "Delivery %",
    "RS 20D",
    "RS 60D",
    "Stage",
    "Blocked By",
    "Reason",
    "Risk Note",
    "Links",
]

PATTERN_WATCHLIST_COLUMNS = [
    "Status",
    "Priority",
    "Symbol",
    "Sector",
    "Sector Status",
    "Setup",
    "Stage",
    "Watchlist Score",
    "Composite Score",
    "Pattern Score",
    "Breakout Score",
    "Breakout Level",
    "Close",
    "Distance To Breakout %",
    "Volume Ratio",
    "Delivery %",
    "RS 20D",
    "RS 60D",
    "Previous Rank",
    "Rank Change",
    "Days On List",
    "New Entry",
    "Blocked By",
    "Reason",
    "Risk Note",
    "Links",
]

TOP_RANKED_COLUMNS = [
    "Rank",
    "Symbol",
    "Sector",
    "Sector Status",
    "Composite Score",
    "Close",
    "Volume Ratio",
    "Delivery %",
    "RS 20D",
    "RS 60D",
    "Stage",
    "Blocked By",
    "Reason",
    "Risk Note",
    "Links",
]

OPTIONAL_COLUMNS = {
    "ranked_signals": [
        "symbol_id",
        "symbol",
        "sector",
        "sector_name",
        "composite_score",
        "close",
        "volume_ratio_20",
        "volume_ratio",
        "delivery_pct",
        "rel_strength_20d",
        "rel_strength_60d",
        "rel_strength_score",
        "sma_20",
        "sma_50",
        "sma_200",
        "sma50_slope_20d_pct",
        "sma200_slope_20d_pct",
        "near_52w_high_pct",
        "stage2_label",
        "previous_rank_position",
        "rank_delta",
    ],
    "breakout_scan": [
        "symbol_id",
        "qualified",
        "breakout_state",
        "candidate_tier",
        "breakout_score",
        "breakout_level",
        "prior_range_high",
        "taxonomy_family",
        "setup_family",
        "breakout_tag",
        "volume_ratio_20",
        "volume_ratio",
        "delivery_pct_today",
        "rel_strength_score",
    ],
    "pattern_scan": [
        "symbol_id",
        "pattern_family",
        "pattern_state",
        "pattern_score",
        "breakout_level",
        "watchlist_trigger_level",
        "volume_ratio_20",
        "rel_strength_score",
        "stage2_label",
    ],
    "stock_scan": [
        "symbol_id",
        "sector",
        "sector_name",
        "close",
        "volume_ratio_20",
        "delivery_pct",
        "rel_strength_20d",
        "rel_strength_60d",
        "sma_20",
        "sma_50",
        "sma_200",
        "sma50_slope_20d_pct",
        "near_52w_high_pct",
        "stage2_label",
    ],
    "watchlist_candidates": [
        "symbol_id",
        "symbol",
        "rank",
        "watchlist_score",
        "days_on_watchlist",
        "days_on_list",
        "previous_rank",
        "rank_change",
        "is_new_entry",
        "momentum_tags",
        "watchlist_reason",
        "risk_flags",
    ],
}


@dataclass(frozen=True)
class MarketContext:
    run_date: str = ""
    trust_status: str = "unknown"
    market_state: str = ""
    direction_bias: str = ""
    action: str = ""
    allowed_exposure: float | None = None
    regime_phase: str = ""
    breadth_50dma: Any = ""
    breadth_200dma: Any = ""
    breadth_velocity: str = ""
    latest_validated_date: Any = ""
    active_quarantine_count: Any = ""
    fallback_ratio_latest: Any = ""
    qualified_breakouts: int = 0
    pattern_setups: int = 0
    watchlist_candidates: int = 0
    sectors_scanned: int = 0
    trust_notes: str = ""


@dataclass(frozen=True)
class DailyReportBuildResult:
    sections: list[tuple[str, pd.DataFrame]]
    metadata: dict[str, Any] = field(default_factory=dict)


def build_daily_report_sections(
    *,
    payload: dict[str, Any] | None = None,
    run_date: str | None = None,
    ranked_df: pd.DataFrame | None = None,
    breakout_df: pd.DataFrame | None = None,
    pattern_df: pd.DataFrame | None = None,
    stock_scan_df: pd.DataFrame | None = None,
    sector_df: pd.DataFrame | None = None,
    watchlist_df: pd.DataFrame | None = None,
    prior_watchlist_df: pd.DataFrame | None = None,
    rank_summary: dict[str, Any] | None = None,
    rank_artifact_uri: str | None = None,
    run_id: str | None = None,
    publish_timestamp: str | None = None,
) -> DailyReportBuildResult:
    payload = payload or {}
    context = _market_context(
        payload=payload,
        run_date=run_date,
        ranked_df=_frame(ranked_df),
        breakout_df=_frame(breakout_df),
        pattern_df=_frame(pattern_df),
        sector_df=_frame(sector_df),
        watchlist_df=_frame(watchlist_df),
        rank_summary=rank_summary or {},
    )
    missing = _missing_optional_columns(
        ranked_signals=_frame(ranked_df),
        breakout_scan=_frame(breakout_df),
        pattern_scan=_frame(pattern_df),
        stock_scan=_frame(stock_scan_df),
        watchlist_candidates=_frame(watchlist_df),
    )
    previous_symbols = _symbols(_frame(prior_watchlist_df))
    symbol_rows = _merged_symbol_rows(
        ranked_df=_frame(ranked_df),
        breakout_df=_frame(breakout_df),
        pattern_df=_frame(pattern_df),
        stock_scan_df=_frame(stock_scan_df),
        sector_df=_frame(sector_df),
        watchlist_df=_frame(watchlist_df),
    )
    confirmed = build_confirmed_breakouts(symbol_rows, context)
    watchlist = build_pattern_watchlist(symbol_rows, context, previous_symbols=previous_symbols, previous_available=prior_watchlist_df is not None and not _frame(prior_watchlist_df).empty)
    top_ranked = build_top_ranked(symbol_rows, context)
    banner = build_market_decision_banner(context)
    summary = build_daily_summary(context)
    diagnostics = build_diagnostics_frame(
        run_id=run_id,
        rank_artifact_uri=rank_artifact_uri,
        publish_timestamp=publish_timestamp,
        ranked_rows=len(_frame(ranked_df)),
        breakout_rows=len(_frame(breakout_df)),
        pattern_rows=len(_frame(pattern_df)),
        confirmed_rows=0 if _is_fallback_confirmed(confirmed) else len(confirmed),
        watchlist_rows=len(watchlist),
        missing_optional_columns=missing,
        trust_notes=context.trust_notes,
    )
    footer = _main_footer(context, diagnostics)
    sections = [
        ("TOP MARKET DECISION BANNER", banner),
        ("DAILY SUMMARY", summary),
        ("CONFIRMED BREAKOUTS", confirmed),
        ("STUDY WATCHLIST TOP 10", watchlist.head(10).reset_index(drop=True)),
        ("TOP RANKED", top_ranked),
        ("FOOTER", footer),
    ]
    metadata = {
        "confirmed_breakout_rows": 0 if _is_fallback_confirmed(confirmed) else len(confirmed),
        "pattern_watchlist_rows": len(watchlist),
        "top_ranked_rows": len(top_ranked),
        "missing_optional_columns": missing,
        "operator_message": _operator_message(context),
        "diagnostics": diagnostics.to_dict(orient="records"),
    }
    return DailyReportBuildResult(sections=sections, metadata=metadata)


def build_market_decision_banner(context: MarketContext) -> pd.DataFrame:
    action = context.action or ("defensive_trim" if _is_weak_regime(context) else "review")
    return pd.DataFrame(
        [
            {
                "Metric 1": "Today Action",
                "Value 1": action,
                "Metric 2": "Allowed Exposure",
                "Value 2": _blank_number(context.allowed_exposure),
                "Metric 3": "Regime Phase",
                "Value 3": context.regime_phase,
                "Metric 4": "Breadth Velocity",
                "Value 4": context.breadth_velocity,
            },
            {
                "Metric 1": "Trust Status",
                "Value 1": context.trust_status,
                "Metric 2": "Qualified Breakouts",
                "Value 2": context.qualified_breakouts,
                "Metric 3": "Pattern Setups",
                "Value 3": context.pattern_setups,
                "Metric 4": "Watchlist Candidates",
                "Value 4": context.watchlist_candidates,
            },
            {
                "Metric 1": "Run Date",
                "Value 1": context.run_date,
                "Metric 2": "Latest Validated Date",
                "Value 2": context.latest_validated_date,
                "Metric 3": "Market State",
                "Value 3": context.market_state,
                "Metric 4": "Direction Bias",
                "Value 4": context.direction_bias,
            },
            {
                "Metric 1": "Operator Message",
                "Value 1": _operator_message(context),
                "Metric 2": "",
                "Value 2": "",
                "Metric 3": "",
                "Value 3": "",
                "Metric 4": "",
                "Value 4": "",
            },
        ]
    )


def build_daily_summary(context: MarketContext) -> pd.DataFrame:
    rows = [
        ("Run Date", context.run_date),
        ("Trust", context.trust_status),
        ("Latest Validated Date", context.latest_validated_date),
        ("Active Quarantine Count", context.active_quarantine_count),
        ("Fallback Ratio Latest", context.fallback_ratio_latest),
        ("Breadth > 50DMA", context.breadth_50dma),
        ("Breadth > 200DMA", context.breadth_200dma),
        ("Market State", context.market_state),
        ("Breadth Velocity", context.breadth_velocity),
        ("Direction Bias", context.direction_bias),
        ("Allowed Exposure", _blank_number(context.allowed_exposure)),
        ("Regime Phase", context.regime_phase),
        ("Qualified Breakouts", context.qualified_breakouts),
        ("Pattern Setups", context.pattern_setups),
        ("Watchlist Candidates", context.watchlist_candidates),
        ("Sectors Scanned", context.sectors_scanned),
    ]
    return pd.DataFrame([{"Metric": k, "Value": "" if _is_missing(v) else v} for k, v in rows])


def build_confirmed_breakouts(rows: list[dict[str, Any]], context: MarketContext) -> pd.DataFrame:
    confirmed = [row for row in rows if _is_qualified(row)]
    if not confirmed:
        tags = _empty_breakout_reason_tags(context)
        return pd.DataFrame(
            [
                {
                    "Status": "No confirmed / trade-qualified breakouts today.",
                    "Blocked By": ", ".join(tags),
                    "Reason": "No trade entries",
                    "Risk Note": "; ".join(_empty_breakout_reasons(context)),
                }
            ],
            columns=CONFIRMED_BREAKOUT_COLUMNS,
        )
    out = []
    for idx, row in enumerate(confirmed, start=1):
        risk_note = generate_risk_note(row, context)
        out.append(
            {
                "Status": determine_status(row, context),
                "Priority": idx,
                "Symbol": _symbol(row),
                "Sector": _first(row, ["sector", "sector_name", "Sector"]),
                "Sector Status": _sector_status(row),
                "Setup": _setup(row),
                "Candidate Tier": _first(row, ["candidate_tier"]),
                "Composite Score": _round(_num(_first(row, ["composite_score"]), None)),
                "Breakout Score": _round(_num(_first(row, ["breakout_score"]), None)),
                "Breakout Type": _first(row, ["breakout_type", "taxonomy_family", "setup_family", "breakout_tag"]),
                "Breakout Level": _round(_breakout_level(row)),
                "Close": _round(_num(_first(row, ["close"]), None)),
                "Distance To Breakout %": _round(_distance_to_breakout(row)),
                "Volume Ratio": _round(_num(_first(row, ["volume_ratio_20", "volume_ratio"]), None)),
                "Delivery %": _round(_num(_first(row, ["delivery_pct", "delivery_pct_today", "delivery_pct_20d_avg"]), None)),
                "RS 20D": _round(_num(_first(row, ["rel_strength_20d", "rel_strength_score"]), None)),
                "RS 60D": _round(_num(_first(row, ["rel_strength_60d"]), None)),
                "Stage": classify_stage(row),
                "Blocked By": blocked_by_flags(row, context, risk_note=risk_note),
                "Reason": generate_reason(row, context),
                "Risk Note": risk_note,
                "Links": symbol_links(_symbol(row)),
            }
        )
    frame = pd.DataFrame(out, columns=CONFIRMED_BREAKOUT_COLUMNS)
    return frame.sort_values(["Breakout Score", "Composite Score", "Symbol"], ascending=[False, False, True], na_position="last", kind="stable").reset_index(drop=True)


def build_pattern_watchlist(
    rows: list[dict[str, Any]],
    context: MarketContext,
    *,
    previous_symbols: set[str] | None = None,
    previous_available: bool = False,
) -> pd.DataFrame:
    previous_symbols = previous_symbols or set()
    candidates = [row for row in rows if not _is_qualified(row) and _has_setup(row)]
    out = []
    for idx, row in enumerate(candidates, start=1):
        days = _days_on_list(row)
        risk_note = generate_risk_note(row, context)
        out.append(
            {
                "Status": determine_status(row, context),
                "Priority": _priority(row, idx),
                "Symbol": _symbol(row),
                "Sector": _first(row, ["sector", "sector_name", "Sector"]),
                "Sector Status": _sector_status(row),
                "Setup": _setup(row),
                "Stage": classify_stage(row),
                "Watchlist Score": compute_watchlist_score(row, context),
                "Composite Score": _round(_num(_first(row, ["composite_score"]), None)),
                "Pattern Score": _round(_num(_first(row, ["pattern_score", "pattern_priority_score"]), None)),
                "Breakout Score": _round(_num(_first(row, ["breakout_score"]), None)),
                "Breakout Level": _round(_breakout_level(row)),
                "Close": _round(_num(_first(row, ["close"]), None)),
                "Distance To Breakout %": _round(_distance_to_breakout(row)),
                "Volume Ratio": _round(_num(_first(row, ["volume_ratio_20", "volume_ratio"]), None)),
                "Delivery %": _round(_num(_first(row, ["delivery_pct", "delivery_pct_today", "delivery_pct_20d_avg"]), None)),
                "RS 20D": _round(_num(_first(row, ["rel_strength_20d", "rel_strength_score"]), None)),
                "RS 60D": _round(_num(_first(row, ["rel_strength_60d"]), None)),
                "Previous Rank": _blank_number(_first(row, ["previous_rank", "previous_rank_position"])),
                "Rank Change": _blank_number(_first(row, ["rank_change", "rank_delta"])),
                "Days On List": days,
                "New Entry": compute_new_entry(_symbol(row), previous_symbols if previous_available else None, days),
                "Blocked By": blocked_by_flags(row, context, risk_note=risk_note),
                "Reason": generate_reason(row, context),
                "Risk Note": risk_note,
                "Links": symbol_links(_symbol(row)),
            }
        )
    if not out:
        return pd.DataFrame(columns=PATTERN_WATCHLIST_COLUMNS)
    frame = pd.DataFrame(out, columns=PATTERN_WATCHLIST_COLUMNS)
    return frame.sort_values(["Watchlist Score", "Composite Score", "Symbol"], ascending=[False, False, True], na_position="last", kind="stable").reset_index(drop=True)


def build_top_ranked(rows: list[dict[str, Any]], context: MarketContext, *, limit: int = 10) -> pd.DataFrame:
    ranked = [row for row in rows if not _is_missing(_first(row, ["composite_score"]))]
    out = []
    for idx, row in enumerate(ranked, start=1):
        risk_note = generate_risk_note(row, context)
        out.append(
            {
                "Rank": _priority(row, idx),
                "Symbol": _symbol(row),
                "Sector": _first(row, ["sector", "sector_name", "Sector"]),
                "Sector Status": _sector_status(row),
                "Composite Score": _round(_num(_first(row, ["composite_score"]), None)),
                "Close": _round(_num(_first(row, ["close"]), None)),
                "Volume Ratio": _round(_num(_first(row, ["volume_ratio_20", "volume_ratio"]), None)),
                "Delivery %": _round(_num(_first(row, ["delivery_pct", "delivery_pct_today", "delivery_pct_20d_avg"]), None)),
                "RS 20D": _round(_num(_first(row, ["rel_strength_20d", "rel_strength_score"]), None)),
                "RS 60D": _round(_num(_first(row, ["rel_strength_60d"]), None)),
                "Stage": classify_stage(row),
                "Blocked By": blocked_by_flags(row, context, risk_note=risk_note),
                "Reason": _rank_reason(row),
                "Risk Note": risk_note,
                "Links": symbol_links(_symbol(row)),
            }
        )
    if not out:
        return pd.DataFrame(columns=TOP_RANKED_COLUMNS)
    frame = pd.DataFrame(out, columns=TOP_RANKED_COLUMNS)
    return frame.sort_values(["Rank", "Composite Score", "Symbol"], ascending=[True, False, True], na_position="last", kind="stable").head(limit).reset_index(drop=True)


def compute_watchlist_score(row: dict[str, Any] | pd.Series, market_context: MarketContext | dict[str, Any]) -> float:
    record = dict(row)
    context = market_context if isinstance(market_context, MarketContext) else MarketContext(**{k: v for k, v in market_context.items() if k in MarketContext.__dataclass_fields__})
    composite = _score(_first(record, ["composite_score"]))
    setup_quality = _setup_quality_score(record)
    sector_strength = _sector_strength_score(_sector_status(record))
    volume_confirmation = _volume_confirmation_score(_num(_first(record, ["volume_ratio_20", "volume_ratio"]), None))
    proximity = _proximity_score(record)
    persistence = _persistence_score(_days_on_list(record))
    score = (
        0.35 * composite
        + 0.20 * setup_quality
        + 0.15 * sector_strength
        + 0.15 * volume_confirmation
        + 0.10 * proximity
        + 0.05 * persistence
    )
    regime = context.regime_phase.lower()
    if "bear" in regime or "stage 4" in regime or "stage4" in regime:
        score *= 0.70
    if str(context.breadth_velocity).lower() == "very_negative":
        score *= 0.85
    trust = context.trust_status.lower()
    if trust == "degraded":
        score *= 0.90
    elif trust == "blocked":
        score *= 0.50
    return round(max(0.0, min(100.0, score)), 2)


def classify_stage(row: dict[str, Any] | pd.Series) -> str:
    record = dict(row)
    existing = str(_first(record, ["stage", "stage_label", "weekly_stage_label", "stage2_label"]) or "").strip()
    if existing:
        lowered = existing.lower()
        if lowered in {"stage2", "stage_2", "strong_stage2"}:
            return "Stage 2 / Uptrend"
        if lowered in {"stage1_to_stage2", "stage_1_to_2", "s1_to_s2"}:
            return "Base"
        if lowered not in {"nan", "none", "non_stage2", "unknown"}:
            return existing.replace("_", " ").title()
    close = _num(_first(record, ["close"]), None)
    sma20 = _num(_first(record, ["sma_20", "sma20"]), None)
    sma50 = _num(_first(record, ["sma_50", "sma50"]), None)
    sma200 = _num(_first(record, ["sma_200", "sma200"]), None)
    slope50 = _num(_first(record, ["sma50_slope_20d_pct"]), None)
    near_high = _num(_first(record, ["near_52w_high_pct"]), None)
    rs = _num(_first(record, ["rel_strength_score", "rel_strength_20d", "rel_strength_60d"]), None)
    if close is None or sma200 is None:
        return "Unknown"
    if close < sma200 and (slope50 is not None and slope50 < 0) and (rs is None or rs < 50):
        return "Stage 4 / Downtrend"
    if close < sma200:
        return "Weak / Below 200DMA"
    if sma50 is not None and close > sma50 and close > sma200 and (slope50 is None or slope50 > 0) and (near_high is None or near_high <= 15):
        return "Stage 2 / Uptrend"
    extension = None if sma50 in {None, 0} else ((close / sma50) - 1.0) * 100.0
    if extension is not None and extension > 20:
        return "Overextended"
    if close > sma200 and near_high is not None and near_high <= 20 and _has_setup(record):
        return "Base"
    if sma20 is not None and sma50 is not None and close > sma200 and min(abs(close - sma20), abs(close - sma50)) / close <= 0.04:
        return "Pullback"
    return "Unknown"


def determine_status(row: dict[str, Any] | pd.Series, market_context: MarketContext | dict[str, Any]) -> str:
    record = dict(row)
    context = market_context if isinstance(market_context, MarketContext) else MarketContext(**{k: v for k, v in market_context.items() if k in MarketContext.__dataclass_fields__})
    score = compute_watchlist_score(record, context)
    trust = context.trust_status.lower()
    weak_regime = _is_weak_regime(context)
    if trust == "blocked":
        return "BLOCKED_BY_TRUST"
    if _has_avoid_risk(record):
        return "AVOID_RISK"
    if _is_qualified(record) and not weak_regime and trust == "trusted":
        return "TRADE_READY"
    if weak_regime:
        return "BLOCKED_BY_REGIME"
    if trust == "degraded" and not _is_qualified(record):
        return "BLOCKED_BY_TRUST" if score < 55 else "STUDY_ONLY"
    if _is_qualified(record):
        return "STUDY_ONLY"
    sector = _sector_status(record).lower()
    if sector == "leading" and score >= 70:
        return "WATCH"
    if sector == "improving" and score >= 75:
        return "WATCH"
    if _has_setup(record):
        return "STUDY_ONLY"
    return "NO_ACTION"


def generate_reason(row: dict[str, Any] | pd.Series, market_context: MarketContext | dict[str, Any]) -> str:
    record = dict(row)
    sector = _sector_status(record)
    setup = _setup(record)
    score = _num(_first(record, ["composite_score"]), None)
    parts = []
    if sector:
        parts.append(f"{sector} sector")
    if setup:
        parts.append(f"{setup} setup")
    if score is not None and score >= 80:
        parts.append("high composite score")
    distance = _distance_to_breakout(record)
    if distance is not None and abs(distance) <= 3:
        parts.append("near breakout trigger")
    if not _is_qualified(record):
        parts.append("pattern setup only")
    return " + ".join(parts) if parts else "Setup retained for operator review"


def _rank_reason(row: dict[str, Any]) -> str:
    parts = []
    sector = _sector_status(row)
    score = _num(_first(row, ["composite_score"]), None)
    stage = classify_stage(row)
    if score is not None and score >= 85:
        parts.append("high composite score")
    elif score is not None:
        parts.append("ranked by composite score")
    if sector:
        parts.append(f"{sector} sector")
    if stage and stage != "Unknown":
        parts.append(stage)
    return " + ".join(parts) if parts else "Ranked signal"


def generate_risk_note(row: dict[str, Any] | pd.Series, market_context: MarketContext | dict[str, Any]) -> str:
    record = dict(row)
    context = market_context if isinstance(market_context, MarketContext) else MarketContext(**{k: v for k, v in market_context.items() if k in MarketContext.__dataclass_fields__})
    notes = []
    if _is_weak_regime(context):
        notes.append("Bear regime; position size reduced")
    if context.trust_status.lower() in {"degraded", "blocked"}:
        notes.append(f"Trust {context.trust_status}; verify data before action")
    if _sector_status(record).lower() == "lagging":
        notes.append("Lagging sector")
    if _num(_first(record, ["volume_ratio_20", "volume_ratio"]), None) is None:
        notes.append("Volume confirmation missing")
    distance = _distance_to_breakout(record)
    if distance is not None and abs(distance) > 8:
        notes.append("Too far from breakout level")
    stage = classify_stage(record)
    if "Overextended" in stage:
        notes.append("Overextended")
    if "Below 200DMA" in stage or "Stage 4" in stage:
        notes.append("Below 200DMA")
    if _is_illiquid_or_missing(record):
        notes.append("Illiquid / avoid")
    risk_flags = str(_first(record, ["risk_flags", "risk_flag", "trap_flag", "trap_category", "drop_reason"]) or "").strip()
    if risk_flags:
        notes.append(risk_flags.replace("_", " "))
    return "; ".join(dict.fromkeys(notes)) if notes else "Clean setup"


def blocked_by_flags(
    row: dict[str, Any] | pd.Series,
    market_context: MarketContext | dict[str, Any],
    *,
    risk_note: str | None = None,
) -> str:
    record = dict(row)
    context = market_context if isinstance(market_context, MarketContext) else MarketContext(**{k: v for k, v in market_context.items() if k in MarketContext.__dataclass_fields__})
    note = (risk_note or generate_risk_note(record, context)).lower()
    source = " ".join(
        str(value).lower()
        for value in [
            note,
            _first(record, ["risk_flags", "risk_flag", "trap_flag", "trap_category", "drop_reason"]),
            _sector_status(record),
        ]
        if not _is_missing(value)
    )
    flags: list[str] = []
    if _is_weak_regime(context) or "bear regime" in source or "stage 4" in source:
        flags.append("REGIME")
    if context.trust_status.lower() in {"degraded", "blocked"} or "trust" in source:
        flags.append("TRUST")
    if "too far" in source or "distance" in source or "breakout level" in source:
        flags.append("DISTANCE")
    if "illiquid" in source or "liquidity" in source:
        flags.append("LIQUIDITY")
    if "volume confirmation missing" in source or "weak volume" in source:
        flags.append("VOLUME")
    if "lagging" in source or "weak sector" in source:
        flags.append("SECTOR")
    if "stale" in source or "days stale" in source:
        flags.append("STALE")
    if "trap" in source:
        flags.append("TRAP")
    return ", ".join(dict.fromkeys(flags))


def symbol_links(symbol: Any) -> str:
    safe = str(symbol or "").strip().upper()
    if not safe:
        return ""
    escaped = safe.replace('"', '""')
    return (
        f'=HYPERLINK("https://www.tradingview.com/chart/?symbol=NSE:{escaped}","Chart")'
        f' & " | " & HYPERLINK("https://www.screener.in/company/{escaped}/","Screener")'
    )


def compute_new_entry(symbol: Any, previous_symbols: set[str] | None, days_on_list: Any) -> str:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return ""
    if previous_symbols is not None:
        return "NEW" if sym not in {str(item).strip().upper() for item in previous_symbols} else ""
    days = _num(days_on_list, None)
    return "NEW" if days == 1 else ""


def _market_context(
    *,
    payload: dict[str, Any],
    run_date: str | None,
    ranked_df: pd.DataFrame,
    breakout_df: pd.DataFrame,
    pattern_df: pd.DataFrame,
    sector_df: pd.DataFrame,
    watchlist_df: pd.DataFrame,
    rank_summary: dict[str, Any],
) -> MarketContext:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    trust = payload.get("data_trust") if isinstance(payload.get("data_trust"), dict) else {}
    direction = payload.get("market_direction") if isinstance(payload.get("market_direction"), dict) else {}
    regime = payload.get("market_regime_phase") if isinstance(payload.get("market_regime_phase"), dict) else {}
    trust_summary = rank_summary.get("trust_summary") if isinstance(rank_summary.get("trust_summary"), dict) else {}
    qualified = _qualified_count(breakout_df)
    return MarketContext(
        run_date=str(run_date or summary.get("run_date") or rank_summary.get("run_date") or ""),
        trust_status=str(summary.get("data_trust_status") or trust.get("status") or rank_summary.get("data_trust_status") or trust_summary.get("status") or "unknown"),
        market_state=str(summary.get("market_state") or direction.get("market_state") or direction.get("state") or ""),
        direction_bias=str(summary.get("direction_bias") or direction.get("direction_bias") or direction.get("bias") or ""),
        action=str(summary.get("action") or direction.get("action") or ""),
        allowed_exposure=_num(summary.get("allowed_exposure") or direction.get("allowed_exposure"), None),
        regime_phase=str(summary.get("market_regime_phase") or regime.get("phase_label") or regime.get("regime_phase") or summary.get("market_stage") or ""),
        breadth_50dma=summary.get("breadth_50dma") or summary.get("breadth_above_50dma") or direction.get("breadth_50dma") or "",
        breadth_200dma=summary.get("breadth_200dma") or summary.get("breadth_above_200dma") or direction.get("breadth_200dma") or "",
        breadth_velocity=str(summary.get("breadth_velocity") or direction.get("breadth_velocity") or (regime.get("driven_by") or {}).get("breadth_velocity_bucket") or ""),
        latest_validated_date=trust.get("latest_validated_date") or trust_summary.get("latest_validated_date") or "",
        active_quarantine_count=trust.get("active_quarantine_count") or trust_summary.get("active_quarantine_count") or trust_summary.get("active_quarantined_dates") or "",
        fallback_ratio_latest=trust.get("fallback_ratio_latest") or trust_summary.get("fallback_ratio_latest") or "",
        qualified_breakouts=qualified,
        pattern_setups=int(len(pattern_df)) if not pattern_df.empty else int(summary.get("pattern_setups_count") or summary.get("pattern_setups") or 0),
        watchlist_candidates=int(len(watchlist_df)) if not watchlist_df.empty else int(summary.get("watchlist_candidates") or len(ranked_df.head(15))),
        sectors_scanned=int(len(sector_df)) if not sector_df.empty else int(summary.get("sector_count") or 0),
        trust_notes=str(trust.get("warning") or trust.get("notes") or summary.get("trust_warning") or ""),
    )


def _merged_symbol_rows(
    *,
    ranked_df: pd.DataFrame,
    breakout_df: pd.DataFrame,
    pattern_df: pd.DataFrame,
    stock_scan_df: pd.DataFrame,
    sector_df: pd.DataFrame,
    watchlist_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for name, frame, sort_cols in [
        ("ranked", ranked_df, ["rank", "composite_score"]),
        ("stock", stock_scan_df, ["rank", "composite_score"]),
        ("pattern", pattern_df, ["pattern_priority_score", "pattern_score"]),
        ("breakout", breakout_df, ["breakout_score", "candidate_tier"]),
        ("watchlist", watchlist_df, ["rank", "watchlist_score"]),
    ]:
        for row in _best_rows(frame, sort_cols):
            symbol = _symbol(row)
            if not symbol:
                continue
            current = merged.setdefault(symbol, {"symbol_id": symbol})
            for key, value in row.items():
                if _is_missing(value):
                    continue
                target_key = key if key not in current or _is_missing(current.get(key)) else f"{name}_{key}"
                if target_key not in current or _is_missing(current.get(target_key)):
                    current[target_key] = value
    sector_lookup = _sector_lookup(sector_df)
    for row in merged.values():
        sector = str(_first(row, ["sector", "sector_name", "Sector"]) or "").strip()
        if sector and "sector_status" not in row:
            row["sector_status"] = sector_lookup.get(sector, "")
    return list(merged.values())


def _best_rows(frame: pd.DataFrame, sort_cols: list[str]) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    df = frame.copy()
    symbol_col = _symbol_col(df)
    if symbol_col is None:
        return []
    df.loc[:, "_symbol_key"] = df[symbol_col].fillna("").astype(str).str.strip().str.upper()
    df = df.loc[df["_symbol_key"].ne("")]
    ascending = []
    actual_cols = []
    for col in sort_cols:
        if col not in df.columns:
            continue
        actual_cols.append(col)
        ascending.append(True if col in {"rank", "candidate_tier"} else False)
        if col != "candidate_tier":
            df.loc[:, col] = pd.to_numeric(df[col], errors="coerce")
    if actual_cols:
        df = df.sort_values(actual_cols + ["_symbol_key"], ascending=ascending + [True], na_position="last", kind="stable")
    return [row.drop(labels=["_symbol_key"], errors="ignore").to_dict() for _, row in df.drop_duplicates("_symbol_key", keep="first").iterrows()]


def build_diagnostics_frame(**kwargs: Any) -> pd.DataFrame:
    missing = kwargs.pop("missing_optional_columns", {})
    rows = [
        ("Source run_id", kwargs.get("run_id")),
        ("Rank artifact path", kwargs.get("rank_artifact_uri")),
        ("Publish timestamp", kwargs.get("publish_timestamp") or datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
        ("Rows in ranked_signals", kwargs.get("ranked_rows")),
        ("Rows in breakout_scan", kwargs.get("breakout_rows")),
        ("Rows in pattern_scan", kwargs.get("pattern_rows")),
        ("Rows in final confirmed breakouts", kwargs.get("confirmed_rows")),
        ("Rows in final pattern watchlist", kwargs.get("watchlist_rows")),
        ("Missing optional columns", "; ".join(f"{name}: {', '.join(cols)}" for name, cols in missing.items() if cols)),
        ("Trust notes", kwargs.get("trust_notes")),
    ]
    return pd.DataFrame([{"Metric": k, "Value": "" if _is_missing(v) else v} for k, v in rows])


def _main_footer(context: MarketContext, diagnostics: pd.DataFrame) -> pd.DataFrame:
    diag_note = "Diagnostics available in Diagnostics tab."
    feedback_note = "Model feedback available in Model_Feedback tab."
    run_id = ""
    published = ""
    if not diagnostics.empty:
        lookup = {str(row.get("Metric")): row.get("Value") for _, row in diagnostics.iterrows()}
        run_id = str(lookup.get("Source run_id") or "")
        published = str(lookup.get("Publish timestamp") or "")
    return pd.DataFrame(
        [
            {
                "Run Date": context.run_date,
                "Run ID": run_id,
                "Published": published,
                "Diagnostics": diag_note,
                "Model Feedback": feedback_note,
            }
        ]
    )


def _operator_message(context: MarketContext) -> str:
    parts = []
    weak_regime = _is_weak_regime(context)
    defensive = context.allowed_exposure is not None and context.allowed_exposure <= 0.15
    stance = "Defensive" if weak_regime or defensive else "Constructive" if context.qualified_breakouts else "Neutral"
    parts.append(f"Today's stance: {stance}.")
    if context.qualified_breakouts == 0 and weak_regime:
        parts.append("No trade-qualified breakouts today.")
    elif context.qualified_breakouts == 0:
        parts.append("No qualified breakouts.")
    if context.trust_status.lower() in {"degraded", "blocked"}:
        parts.append("Data trust is not fully clean.")
    if context.breadth_velocity.lower() == "very_negative":
        parts.append("Breadth momentum is weak.")
    if defensive:
        parts.append("Maintain defensive / low exposure.")
    if context.qualified_breakouts == 0:
        parts.append("Use shortlist for study only until conditions improve.")
    return " ".join(parts)


def _empty_breakout_reason_tags(context: MarketContext) -> list[str]:
    tags: list[str] = []
    if _is_weak_regime(context):
        tags.append("REGIME")
    if str(context.breadth_velocity).lower() == "very_negative":
        tags.append("BREADTH")
    if context.trust_status.lower() in {"degraded", "blocked"}:
        tags.append("TRUST")
    tags.append("NO_CONFIRMATION")
    return tags


def _empty_breakout_reasons(context: MarketContext) -> list[str]:
    reasons: list[str] = []
    if "bear" in context.regime_phase.lower() or "stage 4" in context.regime_phase.lower():
        reasons.append("Bear regime")
    if str(context.breadth_velocity).lower() == "very_negative":
        reasons.append("Very negative breadth velocity")
    if context.trust_status.lower() in {"degraded", "blocked"}:
        reasons.append(f"Trust {context.trust_status}")
    reasons.append("No qualified breakout confirmation")
    return reasons


def _missing_optional_columns(**frames: pd.DataFrame) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    for name, expected in OPTIONAL_COLUMNS.items():
        frame = frames.get(name, pd.DataFrame())
        if frame is None or frame.empty:
            missing[name] = list(expected)
        else:
            missing[name] = [col for col in expected if col not in frame.columns]
    return {name: cols for name, cols in missing.items() if cols}


def _is_fallback_confirmed(frame: pd.DataFrame) -> bool:
    return len(frame) == 1 and str(frame.iloc[0].get("Status", "")).startswith("No confirmed")


def _qualified_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    return int(sum(_is_qualified(row.to_dict()) for _, row in frame.iterrows()))


def _is_qualified(row: dict[str, Any]) -> bool:
    qualified = str(_first(row, ["qualified", "breakout_qualified"]) or "").strip().lower()
    state = str(_first(row, ["breakout_state"]) or "").strip().lower()
    return qualified in {"1", "true", "t", "yes", "y", "qualified"} or state == "qualified"


def _has_setup(row: dict[str, Any]) -> bool:
    return any(not _is_missing(_first(row, names)) for names in (["pattern_family", "pattern_name"], ["breakout_score"], ["pattern_score"], ["setup_label"], ["taxonomy_family"]))


def _is_illiquid_or_missing(row: dict[str, Any]) -> bool:
    liquidity = _num(_first(row, ["liquidity_score"]), None)
    volume = _num(_first(row, ["volume_ratio_20", "volume_ratio"]), None)
    close = _num(_first(row, ["close"]), None)
    if close is None:
        return True
    if liquidity is not None and liquidity < 25:
        return True
    return volume is not None and volume < 0.5


def _has_avoid_risk(row: dict[str, Any]) -> bool:
    risk_text = " ".join(
        str(value).lower()
        for value in [
            _first(row, ["risk_flags", "risk_flag", "trap_flag", "trap_category", "drop_reason"]),
            _first(row, ["liquidity_bucket", "liquidity_status"]),
            _sector_status(row),
        ]
        if not _is_missing(value)
    )
    if any(token in risk_text for token in ("illiquid", "avoid", "trap", "stale", "too extended", "overextended", "weak volume", "lagging")):
        return True
    distance = _distance_to_breakout(row)
    if distance is not None and abs(distance) > 8:
        return True
    return _is_illiquid_or_missing(row)


def _is_weak_regime(context: MarketContext) -> bool:
    regime = context.regime_phase.lower()
    breadth = str(context.breadth_velocity or "").lower()
    return (
        "bear" in regime
        or "stage 4" in regime
        or "stage4" in regime
        or breadth == "very_negative"
        or (context.allowed_exposure is not None and context.allowed_exposure <= 0.15)
    )


def _setup_quality_score(row: dict[str, Any]) -> float:
    pattern = _num(_first(row, ["pattern_score", "pattern_priority_score"]), None)
    if pattern is not None:
        return pattern
    breakout = _num(_first(row, ["breakout_score"]), None)
    if breakout is not None:
        return breakout
    setup = _setup(row).lower()
    if "vcp" in setup:
        return 85
    if "darvas" in setup:
        return 80
    if "flag" in setup:
        return 75
    if "cup" in setup or "handle" in setup:
        return 80
    if "52" in setup:
        return 90
    return 50


def _sector_strength_score(status: str) -> float:
    return {"leading": 90, "improving": 75, "neutral": 55, "weakening": 40, "lagging": 25}.get(str(status).lower(), 50)


def _volume_confirmation_score(volume_ratio: float | None) -> float:
    if volume_ratio is None:
        return 50
    if volume_ratio >= 2.0:
        return 95
    if volume_ratio >= 1.5:
        return 80
    if volume_ratio >= 1.2:
        return 65
    if volume_ratio >= 1.0:
        return 50
    return 35


def _proximity_score(row: dict[str, Any]) -> float:
    distance = _distance_to_breakout(row)
    if distance is None:
        return 50
    if distance <= 0 and _is_qualified(row):
        return 95
    absolute = abs(distance)
    if absolute <= 1:
        return 95
    if absolute <= 3:
        return 85
    if absolute <= 5:
        return 70
    if absolute <= 8:
        return 55
    return 35


def _persistence_score(days: int | None) -> float:
    if days is None:
        return 50
    if 1 <= days <= 3:
        return 80
    if 4 <= days <= 10:
        return 65
    return 45


def _distance_to_breakout(row: dict[str, Any]) -> float | None:
    level = _breakout_level(row)
    close = _num(_first(row, ["close"]), None)
    if level in {None, 0} or close is None:
        return None
    return ((level - close) / close) * 100.0


def _breakout_level(row: dict[str, Any]) -> float | None:
    return _num(_first(row, ["breakout_level", "watchlist_trigger_level", "prior_range_high", "pivot_price", "resistance_level"]), None)


def _priority(row: dict[str, Any], default: int) -> int:
    value = _num(_first(row, ["rank", "priority", "breakout_rank", "pattern_priority_rank", "pattern_rank"]), None)
    return int(value) if value is not None else default


def _days_on_list(row: dict[str, Any]) -> int | None:
    value = _num(_first(row, ["days_on_watchlist", "days_on_list", "Days On List"]), None)
    return int(value) if value is not None else None


def _sector_status(row: dict[str, Any]) -> str:
    value = _first(row, ["sector_status", "Quadrant", "quadrant", "sector_quadrant"])
    text = str(value or "").strip()
    if text.upper() in {"LEADING", "IMPROVING", "NEUTRAL", "WEAKENING", "LAGGING"}:
        return text.title()
    return text


def _setup(row: dict[str, Any]) -> str:
    value = _first(row, ["setup_label", "pattern_family", "pattern_name", "taxonomy_family", "setup_family", "breakout_tag", "execution_label"])
    text = str(value or "").strip().replace("_", " ")
    lowered = text.lower()
    if "darvas" in lowered:
        return "Darvas"
    if "vcp" in lowered:
        return "VCP"
    if "flag" in lowered:
        return "Flag"
    if "cup" in lowered:
        return "Cup/Handle"
    if "52" in lowered:
        return "52W breakout"
    return text.title()


def _sector_lookup(frame: pd.DataFrame) -> dict[str, str]:
    if frame is None or frame.empty:
        return {}
    sector_col = next((col for col in ["Sector", "sector", "sector_name"] if col in frame.columns), None)
    status_col = next((col for col in ["Quadrant", "quadrant", "sector_status", "status"] if col in frame.columns), None)
    if sector_col is None or status_col is None:
        return {}
    return {str(row[sector_col]).strip(): str(row[status_col]).strip() for _, row in frame.iterrows()}


def _symbols(frame: pd.DataFrame) -> set[str]:
    if frame is None or frame.empty:
        return set()
    col = _symbol_col(frame)
    if col is None:
        return set()
    return {str(value).strip().upper() for value in frame[col].dropna() if str(value).strip()}


def _symbol_col(frame: pd.DataFrame) -> str | None:
    return next((col for col in ["symbol_id", "symbol", "Symbol", "ticker"] if col in frame.columns), None)


def _symbol(row: dict[str, Any]) -> str:
    return str(_first(row, ["symbol_id", "symbol", "Symbol", "ticker"]) or "").strip().upper()


def _first(row: dict[str, Any], names: list[str]) -> Any:
    for name in names:
        value = row.get(name)
        if not _is_missing(value):
            return value
    return ""


def _frame(value: Any) -> pd.DataFrame:
    return value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _score(value: Any) -> float:
    return max(0.0, min(100.0, _num(value, 0.0) or 0.0))


def _num(value: Any, default: float | None = 0.0) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return default
    return float(number)


def _round(value: float | None, places: int = 2) -> Any:
    return "" if value is None else round(float(value), places)


def _blank_number(value: Any) -> Any:
    number = _num(value, None)
    if number is None:
        return ""
    return int(number) if float(number).is_integer() else round(float(number), 2)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(value, str) and value.strip().lower() in {"", "nan", "none", "nat"}
