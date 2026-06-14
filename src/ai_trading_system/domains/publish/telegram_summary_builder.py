"""Telegram summary rendering for publish stage."""

from __future__ import annotations

from html import escape
from typing import Any, Mapping, Optional

import pandas as pd

from ai_trading_system.domains.publish.channels.weekly_pdf import metrics as weekly_metrics
from ai_trading_system.domains.publish.channels.watchlist_digest import render_watchlist_telegram


def build_telegram_summary(*, run_date: str, datasets: Mapping[str, Any]) -> str:
    """Build the compact Telegram market tearsheet."""
    bundle = datasets.get("decision_bundle")
    if bundle is not None and getattr(bundle, "telegram_digest", None):
        return _append_fundamental_pulse(str(bundle.telegram_digest), datasets)
    dashboard = datasets.get("dashboard_payload") or {}
    summary = dashboard.get("summary", {})
    data_trust = dashboard.get("data_trust", {}) or {}
    ranked_df = _sorted_ranked_signals(_as_frame(datasets.get("ranked_signals")))
    full_ranked_df = _as_frame(datasets.get("ranked_signals_full", datasets.get("ranked_signals")))
    stage2_summary = dict(datasets.get("stage2_summary") or {})
    breakout_df = _sorted_breakouts(_as_frame(datasets.get("breakout_scan")))
    watchlist_df = _as_frame(datasets.get("watchlist_candidates"))
    sector_df = _sorted_sector_dashboard(_as_frame(datasets.get("sector_dashboard")))
    prior_ranked_df = _as_frame(datasets.get("prior_ranked_signals"))
    prior_breakouts_per_run = datasets.get("prior_breakouts_per_run") or []
    move_df = weekly_metrics.volume_delivery_movers(full_ranked_df, n=5)
    shocker_df = weekly_metrics.unusual_volume_shockers(full_ranked_df, n=5)
    rank_improvers, _rank_decliners = weekly_metrics.compute_rank_movers(full_ranked_df, prior_ranked_df, top_n=5)
    failed_df = weekly_metrics.detect_failed_breakouts(
        breakout_df,
        prior_breakouts_per_run,
        full_ranked_df,
        top_n=5,
    )

    top_symbol = summary.get("top_symbol")
    if not top_symbol and not ranked_df.empty and "symbol_id" in ranked_df.columns:
        top_symbol = ranked_df.iloc[0]["symbol_id"]
    top_sector = summary.get("top_sector")
    if not top_sector and not sector_df.empty and "Sector" in sector_df.columns:
        top_sector = sector_df.iloc[0]["Sector"]

    lines = [
        f"<b>Daily Market Tearsheet</b> | {escape(str(summary.get('run_date', run_date)))}",
        f"Top symbol: <b>{escape(str(top_symbol or 'n/a'))}</b> | Top sector: <b>{escape(str(top_sector or 'n/a'))}</b>",
        f"Universe ranked: <b>{len(ranked_df)}</b> | Breakouts: <b>{len(breakout_df)}</b> | Sectors: <b>{len(sector_df)}</b>",
    ]
    lines.append(
        "Data trust: "
        f"<b>{escape(str(summary.get('data_trust_status', data_trust.get('status', 'unknown'))))}</b>"
        f" | Latest trade: <b>{escape(str(summary.get('latest_trade_date', data_trust.get('latest_trade_date', 'n/a'))))}</b>"
        f" | Latest validated: <b>{escape(str(summary.get('latest_validated_date', data_trust.get('latest_validated_date', 'n/a'))))}</b>"
    )
    market_direction_line = _format_market_direction_line(dashboard)
    if market_direction_line:
        lines.append(market_direction_line)
    regime_phase_line = _format_market_regime_phase(dashboard if isinstance(dashboard, Mapping) else {})
    if regime_phase_line:
        lines.append(regime_phase_line)
    trust_notes: list[str] = []
    quarantined_dates = list(data_trust.get("active_quarantined_dates") or [])
    if quarantined_dates:
        trust_notes.append(f"Quarantined: {', '.join(escape(str(item)) for item in quarantined_dates[:3])}")
    fallback_ratio = float(data_trust.get("fallback_ratio_latest", 0.0) or 0.0)
    if fallback_ratio > 0:
        trust_notes.append(f"Fallback ratio: {fallback_ratio * 100:.1f}%")
    if trust_notes:
        lines.append("Trust notes: " + " | ".join(trust_notes))
    stage2_line = _format_stage2_line(stage2_summary, ranked_df)
    if stage2_line:
        lines.append(stage2_line)
    lines.extend(["", "<b>Market Moves Snapshot</b>"])
    lines.extend(_format_move_block("P+V+D", move_df, _format_move_line))
    lines.extend(_format_move_block("Volume shock", shocker_df, _format_shocker_line))
    lines.extend(_format_move_block("Rank climber", rank_improvers, _format_rank_climber_line))
    lines.extend(_format_move_block("Failed risk", failed_df, _format_failed_breakout_line))
    event_warning = datasets.get("event_freshness_warning")
    if event_warning:
        lines.append(f"Events: {escape(str(event_warning))}")
    event_lines = _format_important_events(datasets)
    if event_lines:
        lines.extend(["", "<b>Important Events</b>"])
        lines.extend(event_lines)
    investigator_lines = _format_investigator_sections(datasets)
    if investigator_lines:
        lines.extend(["", *investigator_lines])
    if not watchlist_df.empty:
        lines.extend(["", render_watchlist_telegram(watchlist_df, top_n=10)])
    lines.extend(["", "<b>Top 10 Sectors</b>"])

    if sector_df.empty:
        lines.append("No sector data available.")
    else:
        for _, row in sector_df.head(10).iterrows():
            lines.append(_format_sector_line(row))

    lines.extend(["", "<b>Top 10 Breakouts</b>"])
    if breakout_df.empty:
        lines.append("No breakouts today.")
    else:
        for idx, (_, row) in enumerate(breakout_df.head(10).iterrows(), start=1):
            lines.append(_format_breakout_line(idx, row))

    lines.extend(["", "<b>Top 10 Ranked Stocks</b>"])
    if ranked_df.empty:
        lines.append("No ranked stocks available.")
    else:
        for idx, (_, row) in enumerate(ranked_df.head(10).iterrows(), start=1):
            lines.append(_format_ranked_line(idx, row))

    return _append_fundamental_pulse("\n".join(lines), datasets)


def render_fundamental_pulse(datasets: Mapping[str, Any]) -> str:
    payload = datasets.get("fundamental_dashboard_payload") or {}
    dashboard = datasets.get("dashboard_payload") or {}
    if not isinstance(payload, Mapping):
        payload = {}
    if not payload and isinstance(dashboard, Mapping):
        payload = dashboard.get("fundamentals") or {}
    universe = payload.get("universe") if isinstance(payload.get("universe"), Mapping) else {}
    sector_df = _first_frame(datasets, "sector_earnings_latest", "sector_earnings_leadership")
    great_df = _first_frame(datasets, "great_results_latest", "great_results")
    turn_df = _first_frame(datasets, "turnaround_candidates_latest", "turnaround_candidates")
    comp_df = _first_frame(datasets, "compounder_candidates_latest", "compounder_candidates")
    if not universe and sector_df.empty and great_df.empty and turn_df.empty and comp_df.empty:
        return ""
    pe = _format_decimal(universe.get("pe_ttm"), 1)
    pe_200 = _format_decimal(universe.get("pe_200dma"), 1)
    percentile = _format_decimal(universe.get("pe_percentile_5y"), 0)
    zone = escape(str(universe.get("valuation_zone") or "n/a"))
    sectors = ", ".join(escape(item) for item in _top_sector_names(sector_df, 3)) or "n/a"
    great = ", ".join(escape(item) for item in _top_symbols(great_df, 3)) or "n/a"
    turnarounds = ", ".join(escape(item) for item in _top_symbols(turn_df, 3)) or "n/a"
    compounders = ", ".join(escape(item) for item in _top_symbols(comp_df, 3)) or "n/a"
    return "\n".join(
        [
            "<b>Fundamental Pulse</b>",
            f"Universe PE: <b>{pe}</b> | PE 200DMA: <b>{pe_200}</b> | PE 5Y %ile: <b>{percentile}</b> | Zone: <b>{zone}</b>",
            f"Top earnings sectors: {sectors}",
            f"Great results: {great}",
            f"Turnarounds: {turnarounds}",
            f"Compounders: {compounders}",
        ]
    )


def _append_fundamental_pulse(message: str, datasets: Mapping[str, Any]) -> str:
    pulse = render_fundamental_pulse(datasets)
    if not pulse:
        return message
    if "Fundamental Pulse" in message:
        return message
    return message.rstrip() + "\n\n" + pulse


def _format_investigator_sections(datasets: Mapping[str, Any]) -> list[str]:
    scores = _as_frame(datasets.get("investigator_scores"))
    repeat = _as_frame(datasets.get("investigator_repeat_tracker"))
    traps = _as_frame(datasets.get("investigator_trap_log"))
    archive = _as_frame(datasets.get("investigator_archive"))
    if scores.empty and repeat.empty and traps.empty and archive.empty:
        return []
    lines = ["<b>Stock Investigator</b>"]
    high = scores.loc[scores.get("verdict", pd.Series(dtype=str)).astype(str).eq("HIGH_CONVICTION")] if not scores.empty else pd.DataFrame()
    if high.empty:
        lines.append("High Conviction: none")
    else:
        lines.append("High Conviction: " + ", ".join(_symbol_score_items(high, "final_score", 5)))
    if not repeat.empty and "high_priority_repeat" in repeat.columns:
        repeat_rows = repeat.loc[repeat["high_priority_repeat"].astype(str).str.lower().isin({"true", "1"})]
    else:
        repeat_rows = pd.DataFrame()
    lines.append("Repeat Accumulation: " + (", ".join(_symbol_score_items(repeat_rows, "repeat_score", 5)) if not repeat_rows.empty else "none"))
    sector_rows = scores.loc[scores.get("sector_rotation_active", pd.Series(False, index=scores.index)).astype(str).str.lower().isin({"true", "1"})] if not scores.empty else pd.DataFrame()
    lines.append("Sector Rotation: " + (", ".join(_top_symbols(sector_rows, 5)) if not sector_rows.empty else "none"))
    lines.append("Trap List: " + (", ".join(_top_symbols(traps, 5)) if not traps.empty else "none"))
    lines.append(f"Dropped/Archived: <b>{len(archive)}</b>")
    return lines


def _symbol_score_items(frame: pd.DataFrame, score_column: str, limit: int) -> list[str]:
    if frame.empty:
        return []
    df = frame.copy()
    if score_column in df.columns:
        df.loc[:, score_column] = pd.to_numeric(df[score_column], errors="coerce")
        df = df.sort_values(score_column, ascending=False, na_position="last", kind="stable")
    symbol_col = next((col for col in ("symbol_id", "symbol") if col in df.columns), None)
    if symbol_col is None:
        return []
    items = []
    for _, row in df.head(limit).iterrows():
        symbol = escape(str(row.get(symbol_col) or ""))
        score = row.get(score_column)
        try:
            items.append(f"{symbol}({float(score):.0f})")
        except (TypeError, ValueError):
            items.append(symbol)
    return items


def _as_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    return pd.DataFrame()


def _first_frame(datasets: Mapping[str, Any], *names: str) -> pd.DataFrame:
    for name in names:
        frame = _as_frame(datasets.get(name))
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _top_symbols(frame: pd.DataFrame, limit: int) -> list[str]:
    if frame.empty:
        return []
    df = frame.copy()
    if "insight_score" in df.columns:
        df = df.sort_values("insight_score", ascending=False, na_position="last")
    column = next((col for col in ("symbol", "symbol_id") if col in df.columns), None)
    if column is None:
        return []
    return [str(value) for value in df[column].dropna().astype(str).head(limit).tolist()]


def _top_sector_names(frame: pd.DataFrame, limit: int) -> list[str]:
    if frame.empty:
        return []
    df = frame.copy()
    if "sector_fundamental_score" in df.columns:
        df = df.sort_values("sector_fundamental_score", ascending=False, na_position="last")
    column = next((col for col in ("sector_name", "sector", "Sector") if col in df.columns), None)
    if column is None:
        return []
    return [str(value) for value in df[column].dropna().astype(str).head(limit).tolist()]


def _format_market_direction_line(dashboard: Mapping[str, Any]) -> str:
    summary = dashboard.get("summary", {}) if isinstance(dashboard, Mapping) else {}
    direction = dashboard.get("market_direction", {}) if isinstance(dashboard, Mapping) else {}
    if not isinstance(direction, Mapping):
        direction = {}
    if not isinstance(summary, Mapping):
        summary = {}
    state = direction.get("market_state") or summary.get("market_regime")
    velocity = direction.get("breadth_velocity") or summary.get("breadth_velocity_bucket")
    bias = direction.get("direction_bias") or summary.get("direction_bias")
    action = direction.get("action") or summary.get("direction_action")
    exposure = direction.get("allowed_exposure", summary.get("allowed_exposure"))
    if not any([state, velocity, bias, action, exposure is not None]):
        return ""
    try:
        exposure_text = f"{float(exposure) * 100:.0f}%"
    except (TypeError, ValueError):
        exposure_text = "n/a"
    return (
        "Market Direction: "
        f"<b>{escape(str(bias or 'n/a'))}</b>"
        f" | State: <b>{escape(str(state or 'n/a'))}</b>"
        f" | Velocity: <b>{escape(str(velocity or 'n/a'))}</b>"
        f" | Action: <b>{escape(str(action or 'n/a'))}</b>"
        f" | Exposure: <b>{escape(exposure_text)}</b>"
    )


def _format_market_regime_phase(payload: Mapping[str, Any]) -> str | None:
    phase = payload.get("market_regime_phase")
    if not isinstance(phase, Mapping):
        return None

    emoji = str(phase.get("phase_emoji") or "⚪")
    label = str(phase.get("phase_label") or "Unknown")
    driven_by = phase.get("driven_by") if isinstance(phase.get("driven_by"), Mapping) else {}

    raw_regime = driven_by.get("regime")
    velocity = driven_by.get("breadth_velocity_bucket")
    s2_pct = driven_by.get("s2_pct")

    try:
        s2_text = f"{float(s2_pct):.0%}"
    except (TypeError, ValueError):
        s2_text = "—"

    return (
        f"{escape(emoji)} <b>{escape(label)}</b>\n"
        f"  raw regime: <code>{escape(str(raw_regime or '—'))}</code> | "
        f"velocity: <code>{escape(str(velocity or '—'))}</code> | "
        f"S2 breadth: <code>{escape(s2_text)}</code>"
    )


def _sorted_sector_dashboard(sector_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if sector_df is None or sector_df.empty:
        return pd.DataFrame()
    df = sector_df.copy()
    if "RS_rank" in df.columns:
        return df.sort_values(["RS_rank", "RS"], ascending=[True, False], na_position="last")
    if "RS" in df.columns:
        return df.sort_values("RS", ascending=False, na_position="last")
    return df


def _sorted_ranked_signals(ranked_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if ranked_df is None or ranked_df.empty:
        return pd.DataFrame()
    df = ranked_df.copy()
    if "composite_score" in df.columns:
        return df.sort_values("composite_score", ascending=False, na_position="last")
    return df


def _sorted_breakouts(breakout_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if breakout_df is None or breakout_df.empty:
        return pd.DataFrame()
    df = breakout_df.copy()
    sort_columns = [
        column
        for column in ["breakout_rank", "breakout_score", "setup_quality", "symbol_id"]
        if column in df.columns
    ]
    if sort_columns:
        ascending = [
            False
            if column in {"breakout_score", "setup_quality"}
            else True
            for column in sort_columns
        ]
        return df.sort_values(sort_columns, ascending=ascending, na_position="last")
    return df


def _format_sector_line(row: pd.Series) -> str:
    sector = escape(str(row.get("Sector", "n/a")))
    rs_rank = _format_int(row.get("RS_rank"))
    rs = _format_decimal(row.get("RS"), 2)
    momentum = _format_signed_decimal(row.get("Momentum"), 2)
    quadrant = escape(str(row.get("Quadrant", "n/a")))
    return f"{rs_rank}. {sector} | RS {rs} | Mom {momentum} | {quadrant}"


def _format_breakout_line(index: int, row: pd.Series) -> str:
    symbol = escape(str(row.get("symbol_id", "n/a")))
    sector = escape(str(row.get("sector", "n/a")))
    setup = escape(str(row.get("taxonomy_family") or row.get("setup_family") or row.get("execution_label") or "setup"))
    tag = escape(str(row.get("breakout_tag", "n/a")))
    score = _format_int(row.get("breakout_score"))
    state = escape(str(row.get("breakout_state") or "watchlist"))
    tier = escape(str(row.get("candidate_tier") or "n/a"))
    reason = str(row.get("filter_reason") or "").strip()
    if not reason:
        reason = str(row.get("symbol_trend_reasons") or "").strip()
    reason_short = " | " + escape(",".join(reason.split(",")[:2])) if reason and state != "qualified" else ""
    return f"{index}. {symbol} | {sector} | {setup} | Tier {tier} | Score {score} | {state} | {tag}{reason_short}"


def _format_ranked_line(index: int, row: pd.Series) -> str:
    symbol = escape(str(row.get("symbol_id", "n/a")))
    sector = escape(str(row.get("sector_name", row.get("sector", "n/a"))))
    score = _format_decimal(row.get("composite_score"), 1)
    close = _format_decimal(row.get("close"), 2)
    rs = _format_decimal(row.get("rel_strength_score"), 1)
    confidence = _format_decimal(row.get("publish_confidence"), 2)
    signal_class = escape(str(row.get("signal_classification", "n/a")))
    return (
        f"{index}. {symbol} | {sector} | Score {score} | Close {close} | RS {rs}"
        f" | Conf {confidence} | {signal_class}"
    )


def _format_move_block(title: str, df: pd.DataFrame, formatter) -> list[str]:
    if df is None or df.empty:
        return [f"{title}: n/a"]
    return [f"{title}: " + " ; ".join(formatter(row) for _, row in df.head(3).iterrows())]


def _format_move_line(row: pd.Series) -> str:
    return (
        f"{escape(str(row.get('symbol_id', 'n/a')))} "
        f"{_format_pct_points(row.get('return_5'))} "
        f"Del {_format_decimal(row.get('delivery_pct'), 0)} "
        f"VolZ {_format_decimal(row.get('volume_zscore_20'), 1)}"
    )


def _format_shocker_line(row: pd.Series) -> str:
    return (
        f"{escape(str(row.get('symbol_id', 'n/a')))} "
        f"VolZ {_format_decimal(row.get('volume_zscore_20'), 1)} "
        f"Del {_format_decimal(row.get('delivery_pct'), 0)} "
        f"5d {_format_pct_points(row.get('return_5'))}"
    )


def _format_rank_climber_line(row: pd.Series) -> str:
    return (
        f"{escape(str(row.get('symbol_id', 'n/a')))} "
        f"RankΔ {_format_signed_int(row.get('rank_change'))} "
        f"ScoreΔ {_format_signed_decimal(row.get('score_change'), 1)}"
    )


def _format_failed_breakout_line(row: pd.Series) -> str:
    return (
        f"{escape(str(row.get('symbol_id', 'n/a')))} "
        f"{_format_decimal(row.get('drop_pct'), 1)}% "
        f"below {escape(str(row.get('trigger_tier') or 'tier n/a'))}"
    )


def _format_important_events(datasets: Mapping[str, Any], *, limit: int = 5) -> list[str]:
    snapshot = datasets.get("market_events_snapshot") or {}
    events = list(snapshot.get("events") or [])
    if not events:
        return []
    severity_rank = {"critical": 4, "high": 3, "medium": 2, "low": 1, None: 0}
    events.sort(
        key=lambda row: (
            severity_rank.get(row.get("materiality_label"), 0),
            float(row.get("importance_score") or 0.0),
        ),
        reverse=True,
    )
    lines: list[str] = []
    for row in events[:limit]:
        symbol = escape(str(row.get("symbol") or "n/a"))
        category = escape(str(row.get("category") or "event"))
        title = escape(str(row.get("title") or row.get("summary") or ""))
        tier = escape(str(row.get("tier") or "n/a"))
        mat = escape(str(row.get("materiality_label") or "neutral"))
        age = row.get("freshness_days")
        age_text = f" | {int(age)}d" if isinstance(age, int) else ""
        lines.append(f"{symbol} | {category} | Tier {tier} | {mat}{age_text} | {title[:120]}")
    return lines


def _format_decimal(value: Any, places: int = 2) -> str:
    if pd.isna(value):
        return "n/a"
    try:
        return f"{float(value):.{places}f}"
    except (TypeError, ValueError):
        return "n/a"


def _format_signed_decimal(value: Any, places: int = 2) -> str:
    if pd.isna(value):
        return "n/a"
    try:
        return f"{float(value):+.{places}f}"
    except (TypeError, ValueError):
        return "n/a"


def _format_int(value: Any) -> str:
    if pd.isna(value):
        return "-"
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return "-"


def _format_signed_int(value: Any) -> str:
    if pd.isna(value):
        return "-"
    try:
        return f"{int(value):+d}"
    except (TypeError, ValueError):
        return "-"


def _format_pct_points(value: Any, places: int = 1) -> str:
    if pd.isna(value):
        return "n/a"
    try:
        return f"{float(value):.{places}f}%"
    except (TypeError, ValueError):
        return "n/a"


def _format_stage2_line(stage2_summary: Mapping[str, Any], ranked_df: pd.DataFrame) -> str:
    uptrend_count = int(stage2_summary.get("uptrend_count") or 0)
    counts_by_label = dict(stage2_summary.get("counts_by_label") or {})
    if uptrend_count == 0 and not counts_by_label and (ranked_df is None or ranked_df.empty):
        return ""

    if not counts_by_label and ranked_df is not None and not ranked_df.empty and "stage2_label" in ranked_df.columns:
        labels = ranked_df["stage2_label"].fillna("unknown").astype(str)
        counts_by_label = {str(key): int(value) for key, value in labels.value_counts().to_dict().items()}
    if uptrend_count == 0 and ranked_df is not None and not ranked_df.empty and "is_stage2_uptrend" in ranked_df.columns:
        uptrend_count = int(ranked_df["is_stage2_uptrend"].fillna(False).astype(bool).sum())

    top_labels = ", ".join(
        f"{escape(str(label))}:{int(count)}" for label, count in list(counts_by_label.items())[:3]
    )
    if not top_labels:
        top_labels = "n/a"
    return f"Stage2: <b>{uptrend_count}</b> uptrend | labels {top_labels}"
