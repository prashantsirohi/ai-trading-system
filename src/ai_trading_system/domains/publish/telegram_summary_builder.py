"""Telegram summary rendering for publish stage."""

from __future__ import annotations

from html import escape
from typing import Any, Mapping, Optional

import pandas as pd

from ai_trading_system.domains.publish.channels.weekly_pdf import metrics as weekly_metrics


def build_telegram_summary(*, run_date: str, datasets: Mapping[str, Any]) -> str:
    """Build the compact Telegram market tearsheet."""
    dashboard = datasets.get("dashboard_payload") or {}
    summary = dashboard.get("summary", {})
    data_trust = dashboard.get("data_trust", {}) or {}
    ranked_df = _sorted_ranked_signals(_as_frame(datasets.get("ranked_signals")))
    full_ranked_df = _as_frame(datasets.get("ranked_signals_full", datasets.get("ranked_signals")))
    stage2_summary = dict(datasets.get("stage2_summary") or {})
    breakout_df = _sorted_breakouts(_as_frame(datasets.get("breakout_scan")))
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

    return "\n".join(lines)


def _as_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    return pd.DataFrame()


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
