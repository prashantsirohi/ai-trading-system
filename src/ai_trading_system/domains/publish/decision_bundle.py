"""Decision-oriented publish bundle shared by Sheets and Telegram."""

from __future__ import annotations

from dataclasses import dataclass, field
from html import escape
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class PublishDecisionBundle:
    run_summary: pd.DataFrame
    sector_leaders: pd.DataFrame
    market_moves: pd.DataFrame
    top_ranked: pd.DataFrame
    pattern_setups: pd.DataFrame
    failed_breakouts: pd.DataFrame
    event_summary: dict[str, Any]
    watchlist_candidates: pd.DataFrame
    telegram_digest: str
    event_log: pd.DataFrame = field(default_factory=pd.DataFrame)
    publish_log: pd.DataFrame = field(default_factory=pd.DataFrame)


def build_publish_decision_bundle(
    *,
    run_date: str,
    ranked_signals: pd.DataFrame | None = None,
    breakout_scan: pd.DataFrame | None = None,
    pattern_scan: pd.DataFrame | None = None,
    stock_scan: pd.DataFrame | None = None,
    sector_dashboard: pd.DataFrame | None = None,
    event_frame: pd.DataFrame | None = None,
    breadth_frame: pd.DataFrame | None = None,
    watchlist_frame: pd.DataFrame | None = None,
    trust_status: str = "unknown",
    failed_breakouts: pd.DataFrame | None = None,
    insight_text: str | None = None,
    market_direction: dict[str, Any] | None = None,
    market_regime_phase: dict[str, Any] | None = None,
) -> PublishDecisionBundle:
    ranked = _frame(ranked_signals)
    breakout = _frame(breakout_scan)
    patterns = _frame(pattern_scan)
    sectors = _frame(sector_dashboard)
    events = _frame(event_frame)
    breadth = _frame(breadth_frame)
    watchlist = _shape_watchlist(_frame(watchlist_frame), patterns, ranked, sectors, run_date)
    sector_leaders = _shape_sector_leaders(sectors)
    top_ranked = _shape_top_ranked(ranked)
    market_moves = _shape_market_moves(ranked)
    pattern_setups = _shape_patterns(patterns, sectors)
    failed = _shape_failed_breakouts(_frame(failed_breakouts))
    event_summary = _shape_event_summary(events, watchlist, ranked)
    run_summary = _shape_run_summary(
        run_date=run_date,
        trust_status=trust_status,
        breadth=breadth,
        sectors=sectors,
        ranked=ranked,
        breakout=breakout,
        patterns=patterns,
        watchlist=watchlist,
        events=events,
        event_summary=event_summary,
        market_direction=market_direction or {},
        market_regime_phase=market_regime_phase or {},
    )
    event_log = _shape_event_log(events)
    publish_log = _shape_publish_log(
        run_date=run_date,
        trust_status=trust_status,
        insight_text=insight_text,
        run_summary=run_summary,
    )
    telegram_digest = _render_telegram_digest(
        run_date=run_date,
        trust_status=trust_status,
        run_summary=run_summary,
        sector_leaders=sector_leaders,
        pattern_setups=pattern_setups,
        watchlist=watchlist,
        event_summary=event_summary,
        qualified_breakouts=_qualified_breakout_count(breakout),
        market_direction=market_direction or {},
    )
    return PublishDecisionBundle(
        run_summary=run_summary,
        sector_leaders=sector_leaders,
        market_moves=market_moves,
        top_ranked=top_ranked,
        pattern_setups=pattern_setups,
        failed_breakouts=failed,
        event_summary=event_summary,
        watchlist_candidates=watchlist,
        telegram_digest=telegram_digest,
        event_log=event_log,
        publish_log=publish_log,
    )


def _frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if isinstance(value, list):
        return pd.DataFrame(value)
    return pd.DataFrame()


def _column(df: pd.DataFrame, names: list[str], default: Any = "") -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series(default, index=df.index)


def _num(value: Any, default: float = 0.0) -> float:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return default
    return float(number)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    missing = pd.isna(value)
    if isinstance(missing, bool) and missing:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _display_stage(value: Any) -> str:
    text = _clean_text(value).lower()
    if text in {"stage_1_to_2", "stage1_to_stage2", "s1_to_s2"}:
        return "Stage 1->2"
    if text in {"stage_2", "stage2", "strong_stage2", "s2"}:
        return "Stage 2"
    return _clean_text(value)


def _display_setup(value: Any) -> str:
    text = _clean_text(value).lower()
    if "darvas" in text:
        return "Darvas"
    if "flag" in text:
        return "Flag"
    if "cup" in text:
        return "Cup"
    if "ipo" in text:
        return "IPO Base"
    if "base" in text:
        return "Base"
    return _clean_text(value).replace("_", " ").title()


def _sector_status(value: Any) -> str:
    text = _clean_text(value).upper()
    if text in {"LEADING", "IMPROVING", "LAGGING", "WEAKENING"}:
        return text.title()
    return _clean_text(value)


def _shape_watchlist(
    df: pd.DataFrame,
    patterns: pd.DataFrame,
    ranked: pd.DataFrame,
    sectors: pd.DataFrame,
    run_date: str,
) -> pd.DataFrame:
    columns = [
        "Status",
        "Priority",
        "Symbol",
        "Sector",
        "Sector Status",
        "Stage",
        "Watchlist Score",
        "Composite Score",
        "Previous Rank",
        "Rank Change",
        "Days On List",
        "New Entry",
        "Tags",
        "Setup",
        "Trigger Price",
        "Current Close",
        "Entry Zone",
        "Stop Zone",
        "Reason",
        "Event Catalyst",
        "LLM Catalyst",
        "Risk Flag",
        "Last Seen",
        "Added Date",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)

    pattern_lookup = _best_pattern_lookup(patterns)
    ranked_lookup = _ranked_lookup(ranked)
    sector_lookup = _sector_lookup(sectors)
    rows: list[dict[str, Any]] = []
    sorted_df = df.copy()
    if "rank" in sorted_df.columns:
        sorted_df = sorted_df.sort_values("rank", ascending=True, na_position="last", kind="stable")
    elif "watchlist_score" in sorted_df.columns:
        sorted_df = sorted_df.sort_values("watchlist_score", ascending=False, na_position="last", kind="stable")
    for idx, row in sorted_df.head(15).iterrows():
        symbol = _clean_text(row.get("symbol_id") or row.get("symbol") or row.get("Symbol"))
        pattern = pattern_lookup.get(symbol, {})
        rank_row = ranked_lookup.get(symbol, {})
        sector = _clean_text(row.get("sector") or rank_row.get("sector_name") or rank_row.get("sector"))
        sector_status = _sector_status(row.get("sector_status") or sector_lookup.get(sector, ""))
        tags = _clean_text(row.get("momentum_tags")).replace("_", " ").title()
        setup = _display_setup(row.get("setup_label") or pattern.get("pattern_family") or pattern.get("pattern_name"))
        reason = _clean_text(row.get("watchlist_reason") or row.get("technical_catalyst_summary"))
        if not reason:
            reason = _reason_from_parts(tags, sector_status, _display_stage(row.get("stage")))
        priority = int(_num(row.get("rank"), len(rows) + 1))
        rows.append(
            {
                "Status": _clean_text(row.get("action")) or "Study",
                "Priority": priority,
                "Symbol": symbol,
                "Sector": sector,
                "Sector Status": sector_status,
                "Stage": _display_stage(row.get("stage") or row.get("stage2_label")),
                "Watchlist Score": round(_num(row.get("watchlist_score")), 2),
                "Composite Score": round(_num(row.get("composite_score") or rank_row.get("composite_score")), 2),
                "Previous Rank": _blank_number(row.get("previous_rank")),
                "Rank Change": _blank_number(row.get("rank_change")),
                "Days On List": int(_num(row.get("days_on_watchlist"), 1)),
                "New Entry": bool(str(row.get("is_new_entry", "")).strip().lower() in {"true", "1", "yes"}),
                "Tags": tags,
                "Setup": setup,
                "Trigger Price": _blank_number(pattern.get("breakout_level") or pattern.get("watchlist_trigger_level")),
                "Current Close": _blank_number(row.get("close") or rank_row.get("close")),
                "Entry Zone": "",
                "Stop Zone": _blank_number(pattern.get("invalidation_price")),
                "Reason": reason,
                "Event Catalyst": _clean_text(row.get("catalyst_tags")),
                "LLM Catalyst": _clean_text(row.get("bull_case")),
                "Risk Flag": _clean_text(row.get("risk_flags")),
                "Last Seen": run_date,
                "Added Date": run_date,
            }
        )
    return pd.DataFrame(rows, columns=columns).fillna("")


def _best_pattern_lookup(patterns: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if patterns.empty:
        return {}
    df = patterns.copy()
    if "symbol_id" not in df.columns:
        return {}
    for column in ["pattern_priority_score", "pattern_score", "volume_ratio_20"]:
        if column not in df.columns:
            df[column] = pd.NA
        df.loc[:, column] = pd.to_numeric(df[column], errors="coerce")
    df = df.sort_values(
        ["pattern_priority_score", "pattern_score", "volume_ratio_20", "symbol_id"],
        ascending=[False, False, False, True],
        na_position="last",
        kind="stable",
    )
    return {str(row["symbol_id"]): row.to_dict() for _, row in df.drop_duplicates("symbol_id").iterrows()}


def _ranked_lookup(ranked: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if ranked.empty or "symbol_id" not in ranked.columns:
        return {}
    return {str(row["symbol_id"]): row.to_dict() for _, row in ranked.drop_duplicates("symbol_id").iterrows()}


def _sector_lookup(sectors: pd.DataFrame) -> dict[str, str]:
    if sectors.empty:
        return {}
    sector_col = "Sector" if "Sector" in sectors.columns else "sector" if "sector" in sectors.columns else None
    quadrant_col = "Quadrant" if "Quadrant" in sectors.columns else "quadrant" if "quadrant" in sectors.columns else None
    if not sector_col or not quadrant_col:
        return {}
    return {_clean_text(row.get(sector_col)): _clean_text(row.get(quadrant_col)) for _, row in sectors.iterrows()}


def _blank_number(value: Any) -> Any:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return ""
    if float(number).is_integer():
        return int(number)
    return round(float(number), 2)


def _reason_from_parts(tags: str, sector_status: str, stage: str) -> str:
    parts = [part for part in [tags, sector_status + " sector" if sector_status else "", stage] if part]
    return " + ".join(parts)


def _shape_sector_leaders(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["Rank", "Sector", "RS", "Momentum", "Quadrant"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame(
        {
            "Rank": _column(df, ["RS_rank", "rank", "Rank"]),
            "Sector": _column(df, ["Sector", "sector"]),
            "RS": pd.to_numeric(_column(df, ["RS", "rs"], 0), errors="coerce").round(2),
            "Momentum": pd.to_numeric(_column(df, ["Momentum", "momentum"], 0), errors="coerce").round(2),
            "Quadrant": _column(df, ["Quadrant", "quadrant"]),
        }
    )
    out = out.loc[out["Quadrant"].astype(str).str.lower().isin({"leading", "improving"})].copy()
    return out.sort_values(["Rank", "RS"], ascending=[True, False], na_position="last", kind="stable").reset_index(drop=True)


def _shape_top_ranked(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["Symbol", "Sector", "composite_score", "Close", "Stage"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame(
        {
            "Symbol": _column(df, ["symbol_id", "symbol"]),
            "Sector": _column(df, ["sector_name", "sector"]),
            "composite_score": pd.to_numeric(_column(df, ["composite_score"], 0), errors="coerce").round(2),
            "Close": pd.to_numeric(_column(df, ["close"], 0), errors="coerce").round(2),
            "Stage": _column(df, ["stage2_label", "stage"]),
        }
    )
    return out.sort_values("composite_score", ascending=False, na_position="last", kind="stable").head(25).reset_index(drop=True)


def _shape_market_moves(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["Symbol", "Sector", "market_move_score", "Return5", "Return20", "Delivery", "VolZ"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame(
        {
            "Symbol": _column(df, ["symbol_id", "symbol"]),
            "Sector": _column(df, ["sector_name", "sector"]),
            "Return5": pd.to_numeric(_column(df, ["return_5"], 0), errors="coerce"),
            "Return20": pd.to_numeric(_column(df, ["return_20"], 0), errors="coerce"),
            "Delivery": pd.to_numeric(_column(df, ["delivery_pct"], 0), errors="coerce"),
            "VolZ": pd.to_numeric(_column(df, ["volume_zscore_20"], 0), errors="coerce"),
        }
    )
    out.loc[:, "market_move_score"] = (out["Return5"].fillna(0) * 2.0 + out["Delivery"].fillna(0) * 0.2 + out["VolZ"].fillna(0) * 5.0).round(2)
    return out[columns].sort_values("market_move_score", ascending=False, na_position="last", kind="stable").head(25).reset_index(drop=True)


def _shape_patterns(patterns: pd.DataFrame, sectors: pd.DataFrame) -> pd.DataFrame:
    columns = ["Symbol", "Pattern", "State", "Tier", "Trigger", "VolRatio", "Stage", "Sector", "Use", "pattern_score"]
    if patterns.empty:
        return pd.DataFrame(columns=columns)
    sector_lookup = _sector_lookup(sectors)
    out = pd.DataFrame(
        {
            "Symbol": _column(patterns, ["symbol_id", "symbol"]),
            "Pattern": _column(patterns, ["pattern_family", "pattern_name", "pattern_type"]),
            "State": _column(patterns, ["pattern_state", "pattern_lifecycle_state"]),
            "Tier": _column(patterns, ["pattern_operational_tier"]),
            "Trigger": pd.to_numeric(_column(patterns, ["breakout_level", "watchlist_trigger_level"], 0), errors="coerce").round(2),
            "VolRatio": pd.to_numeric(_column(patterns, ["volume_ratio_20", "breakout_volume_ratio"], 0), errors="coerce").round(2),
            "Stage": _column(patterns, ["stage2_label", "stage"]),
            "Sector": _column(patterns, ["sector", "sector_name"], ""),
            "pattern_score": pd.to_numeric(_column(patterns, ["pattern_score"], 0), errors="coerce").round(2),
        }
    )
    if out["Sector"].astype(str).str.strip().eq("").all() and sector_lookup:
        out.loc[:, "Sector"] = ""
    out.loc[:, "Use"] = out.apply(_pattern_use, axis=1)
    out.loc[:, "_state_tier_rank"] = out.apply(_pattern_state_tier_rank, axis=1)
    out.loc[:, "_stage_rank"] = out["Stage"].astype(str).str.lower().map({"strong_stage2": 0, "stage2": 1, "stage1_to_stage2": 2}).fillna(9)
    return (
        out.sort_values(
            ["_state_tier_rank", "VolRatio", "_stage_rank", "pattern_score", "Symbol"],
            ascending=[True, False, True, False, True],
            na_position="last",
            kind="stable",
        )
        .drop(columns=["_state_tier_rank", "_stage_rank"])
        .head(25)
        .reset_index(drop=True)
    )


def _pattern_state_tier_rank(row: pd.Series) -> int:
    state = _clean_text(row.get("State")).lower()
    tier = _clean_text(row.get("Tier")).lower()
    order = {
        ("confirmed", "tier_1"): 0,
        ("confirmed", "tier_2"): 1,
        ("watchlist", "tier_1"): 2,
        ("watchlist", "tier_2"): 3,
    }
    return order.get((state, tier), 9)


def _pattern_use(row: pd.Series) -> str:
    state = _clean_text(row.get("State")).lower()
    tier = _clean_text(row.get("Tier")).lower()
    if state == "confirmed" and tier == "tier_1":
        return "Watchlist"
    if state == "confirmed":
        return "Study"
    return "Monitor"


def _shape_failed_breakouts(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["Symbol", "Sector", "Trigger", "Close", "DropPct", "Tier"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame(
        {
            "Symbol": _column(df, ["symbol_id", "symbol"]),
            "Sector": _column(df, ["sector_name", "sector"]),
            "Trigger": pd.to_numeric(_column(df, ["trigger_level"], 0), errors="coerce").round(2),
            "Close": pd.to_numeric(_column(df, ["current_close", "close"], 0), errors="coerce").round(2),
            "DropPct": pd.to_numeric(_column(df, ["drop_pct"], 0), errors="coerce").round(2),
            "Tier": _column(df, ["trigger_tier", "tier"]),
        }
    )
    return out.reset_index(drop=True)


def _shape_event_summary(events: pd.DataFrame, watchlist: pd.DataFrame, ranked: pd.DataFrame) -> dict[str, Any]:
    material_count = 0
    medium_count = 0
    low_info_count = 0
    rows: list[dict[str, Any]] = []
    event_symbols: set[str] = set()
    if not events.empty:
        for _, row in events.iterrows():
            symbol = _clean_text(row.get("symbol") or row.get("symbol_id"))
            event_symbols.add(symbol)
            category = _clean_text(row.get("category") or row.get("top_category") or row.get("trigger_type"))
            materiality = _clean_text(row.get("materiality_label") or row.get("severity")).lower()
            tier = _clean_text(row.get("tier")).upper()
            if materiality in {"critical", "high", "important"} or tier == "A":
                material_count += 1
                level = "material"
            elif "bulk" in category.lower() or tier == "B" or materiality == "medium":
                medium_count += 1
                level = "medium"
            else:
                low_info_count += 1
                level = "low-info"
            rows.append({"Level": level, "Symbol": symbol, "Category": category, "Title": _clean_text(row.get("title"))})
    technical_symbols = set(watchlist.get("Symbol", pd.Series(dtype=str)).astype(str))
    if ranked is not None and not ranked.empty and "symbol_id" in ranked.columns:
        technical_symbols |= set(ranked.head(25)["symbol_id"].astype(str))
    overlaps = sorted(symbol for symbol in event_symbols & technical_symbols if symbol)
    summary_frame = pd.DataFrame(
        [
            {"Metric": "Events", "Value": int(len(events))},
            {"Metric": "Material events", "Value": int(material_count)},
            {"Metric": "Medium events", "Value": int(medium_count)},
            {"Metric": "Low-info events", "Value": int(low_info_count)},
            {"Metric": "Symbols with event + technical overlap", "Value": ", ".join(overlaps)},
        ]
    )
    return {
        "total_events": int(len(events)),
        "material_events": int(material_count),
        "medium_events": int(medium_count),
        "low_info_events": int(low_info_count),
        "overlap_symbols": overlaps,
        "summary_frame": summary_frame,
        "material_frame": pd.DataFrame(rows).loc[lambda frame: frame["Level"].isin(["material", "medium"])].head(25)
        if rows
        else pd.DataFrame(columns=["Level", "Symbol", "Category", "Title"]),
    }


def _shape_event_log(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=["Symbol", "Category", "Materiality", "Tier", "Title", "Event Hash"])
    return pd.DataFrame(
        {
            "Symbol": _column(events, ["symbol", "symbol_id"]),
            "Category": _column(events, ["category", "top_category", "trigger_type"]),
            "Materiality": _column(events, ["materiality_label", "severity"]),
            "Tier": _column(events, ["tier"]),
            "Title": _column(events, ["title"]),
            "Event Hash": _column(events, ["event_hash"]),
        }
    ).fillna("")


def _shape_publish_log(
    *,
    run_date: str,
    trust_status: str,
    insight_text: str | None,
    run_summary: pd.DataFrame,
) -> pd.DataFrame:
    rows = [
        {"Run Date": run_date, "Key": "trust_status", "Value": trust_status},
    ]
    if insight_text:
        rows.append({"Run Date": run_date, "Key": "insight_telegram_summary_raw", "Value": insight_text})
    for column in run_summary.columns:
        rows.append({"Run Date": run_date, "Key": column, "Value": run_summary.iloc[0].get(column)})
    return pd.DataFrame(rows).fillna("")


def _latest_breadth_value(breadth: pd.DataFrame) -> Any:
    if breadth.empty:
        return ""
    if "Date" in breadth.columns:
        breadth = breadth.sort_values("Date", ascending=True, kind="stable")
    value = breadth.iloc[-1].get("PctAbove200", breadth.iloc[-1].get("pct_above_200", ""))
    number = pd.to_numeric(value, errors="coerce")
    return "" if pd.isna(number) else round(float(number), 2)


def _qualified_breakout_count(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    if "breakout_state" in df.columns:
        return int(df["breakout_state"].astype(str).str.lower().eq("qualified").sum())
    if "qualified" in df.columns:
        return int(df["qualified"].astype(str).str.lower().isin({"true", "1", "yes", "qualified"}).sum())
    return int(len(df))


def _shape_run_summary(
    *,
    run_date: str,
    trust_status: str,
    breadth: pd.DataFrame,
    sectors: pd.DataFrame,
    ranked: pd.DataFrame,
    breakout: pd.DataFrame,
    patterns: pd.DataFrame,
    watchlist: pd.DataFrame,
    events: pd.DataFrame,
    event_summary: dict[str, Any],
    market_direction: dict[str, Any],
    market_regime_phase: dict[str, Any],
) -> pd.DataFrame:
    direction = market_direction or {}
    phase_fields = _phase_summary_fields(market_regime_phase)
    return pd.DataFrame(
        [
            {
                "Daily Market Insight": run_date,
                "Trust": trust_status,
                "Breadth > 200DMA": _latest_breadth_value(breadth),
                "Market State": direction.get("market_state", ""),
                "Breadth Velocity": direction.get("breadth_velocity", ""),
                "Direction Bias": direction.get("direction_bias", ""),
                "Action": direction.get("action", ""),
                "Allowed Exposure": direction.get("allowed_exposure", ""),
                **phase_fields,
                "Sectors scanned": int(len(sectors)),
                "Top ranked": int(min(len(ranked), 25)),
                "Qualified breakouts": _qualified_breakout_count(breakout),
                "Pattern setups": int(len(patterns)),
                "Watchlist candidates": int(len(watchlist)),
                "Events": f"{int(len(events))} total / {int(event_summary.get('material_events', 0))} material shown",
            }
        ]
    )


def _phase_summary_fields(market_regime_phase: dict[str, Any] | None) -> dict[str, Any]:
    phase = market_regime_phase if isinstance(market_regime_phase, dict) else {}
    driven_by = phase.get("driven_by") if isinstance(phase.get("driven_by"), dict) else {}
    s2_pct = driven_by.get("s2_pct")
    try:
        s2_text = f"{float(s2_pct):.0%}"
    except (TypeError, ValueError):
        s2_text = ""
    return {
        "Regime Phase": phase.get("phase_label", ""),
        "Regime Phase Emoji": phase.get("phase_emoji", ""),
        "Regime Phase S2 Breadth": s2_text,
        "Regime Phase Market Stage": driven_by.get("market_stage", ""),
        "Regime Phase Velocity": driven_by.get("breadth_velocity_bucket", ""),
    }


def _render_telegram_digest(
    *,
    run_date: str,
    trust_status: str,
    run_summary: pd.DataFrame,
    sector_leaders: pd.DataFrame,
    pattern_setups: pd.DataFrame,
    watchlist: pd.DataFrame,
    event_summary: dict[str, Any],
    qualified_breakouts: int,
    market_direction: dict[str, Any],
) -> str:
    row = run_summary.iloc[0].to_dict() if not run_summary.empty else {}
    breadth = row.get("Breadth > 200DMA") or "n/a"
    leading = sector_leaders.loc[sector_leaders["Quadrant"].astype(str).str.lower().eq("leading"), "Sector"].astype(str).head(6).tolist() if not sector_leaders.empty else []
    improving = sector_leaders.loc[sector_leaders["Quadrant"].astype(str).str.lower().eq("improving"), "Sector"].astype(str).head(4).tolist() if not sector_leaders.empty else []
    lines = [
        f"<b>Daily Market Insight</b> | {escape(str(run_date))}",
        f"Trust: <b>{escape(str(trust_status))}</b> | Breadth &gt;200DMA: <b>{escape(str(breadth))}%</b>",
        _market_direction_digest_line(market_direction),
        f"Breakouts: <b>{qualified_breakouts}</b> qualified | Patterns: <b>{len(pattern_setups)}</b> | Watchlist: <b>{len(watchlist)}</b>",
        "",
        "<b>Leading / Improving Sectors</b>",
        "Leading: " + escape(", ".join(leading) if leading else "n/a"),
        "Improving: " + escape(", ".join(improving) if improving else "n/a"),
        "",
        "<b>Today's Study List</b>",
    ]
    if watchlist.empty:
        lines.append("No watchlist candidates available.")
    else:
        for _, candidate in watchlist.head(5).iterrows():
            lines.append(
                "{priority}) <b>{symbol}</b> - {score:.1f} | {sector} | {stage} | {setup}".format(
                    priority=int(_num(candidate.get("Priority"), 0)),
                    symbol=escape(str(candidate.get("Symbol", ""))),
                    score=_num(candidate.get("Watchlist Score"), 0.0),
                    sector=escape(str(candidate.get("Sector", ""))),
                    stage=escape(str(candidate.get("Stage", ""))),
                    setup=escape(str(candidate.get("Setup", ""))),
                )
            )
            reason = _clean_text(candidate.get("Reason"))
            if reason:
                lines.append(f"   Why: {escape(reason)}")
            lines.append("")
    overlaps = list(event_summary.get("overlap_symbols") or [])
    if overlaps:
        lines.extend(["<b>Event + Technical Watch</b>", escape(", ".join(overlaps[:8])), ""])
    if qualified_breakouts == 0:
        lines.extend(
            [
                "<b>Note</b>",
                "No qualified breakouts today. Treat pattern setups as study/watchlist, not auto-entry.",
            ]
        )
    return "\n".join(lines).strip()


def _market_direction_digest_line(direction: dict[str, Any]) -> str:
    if not direction:
        return "Market Direction: <b>n/a</b>"
    try:
        exposure = f"{float(direction.get('allowed_exposure')) * 100:.0f}%"
    except (TypeError, ValueError):
        exposure = "n/a"
    return (
        "Market Direction: "
        f"<b>{escape(str(direction.get('direction_bias') or 'n/a'))}</b>"
        f" | State: <b>{escape(str(direction.get('market_state') or 'n/a'))}</b>"
        f" | Velocity: <b>{escape(str(direction.get('breadth_velocity') or 'n/a'))}</b>"
        f" | Action: <b>{escape(str(direction.get('action') or 'n/a'))}</b>"
        f" | Exposure: <b>{escape(exposure)}</b>"
    )


__all__ = ["PublishDecisionBundle", "build_publish_decision_bundle"]
