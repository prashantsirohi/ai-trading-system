"""Render watchlist candidates for local and Telegram publish channels."""

from __future__ import annotations

from html import escape

import pandas as pd


def _as_tags(value: object) -> str:
    text = str(value or "").strip()
    return ", ".join(part.strip() for part in text.split(",") if part.strip())


def _history_badge(row: pd.Series) -> str:
    is_new = str(row.get("is_new_entry", "")).strip().lower() in {"true", "1", "yes"}
    if is_new:
        return "NEW"
    change = row.get("rank_change")
    try:
        change_int = int(float(change))
    except (TypeError, ValueError):
        change_int = 0
    days = row.get("days_on_watchlist")
    try:
        days_int = int(float(days))
    except (TypeError, ValueError):
        days_int = 1
    move = f"+{change_int}" if change_int > 0 else str(change_int) if change_int < 0 else "0"
    return f"{move}, {days_int}d"


def render_watchlist_markdown(df: pd.DataFrame) -> str:
    lines = ["# Watchlist Candidates", ""]
    if df is None or df.empty:
        lines.append("No watchlist candidates available.")
        return "\n".join(lines)
    lines.append("| Rank | Move | Symbol | Sector | Stage | Score | Action | Reason |")
    lines.append("| ---: | --- | --- | --- | --- | ---: | --- | --- |")
    for _, row in df.head(15).iterrows():
        lines.append(
            "| {rank} | {move} | {symbol} | {sector} | {stage} | {score:.2f} | {action} | {reason} |".format(
                rank=int(row.get("rank", 0) or 0),
                move=_history_badge(row),
                symbol=str(row.get("symbol_id", "")),
                sector=str(row.get("sector", "")),
                stage=str(row.get("stage", "")),
                score=float(row.get("watchlist_score", 0.0) or 0.0),
                action=str(row.get("action", "")),
                reason=str(row.get("watchlist_reason") or row.get("technical_catalyst_summary") or "").replace("|", "/"),
            )
        )
    return "\n".join(lines)


def render_watchlist_telegram(df: pd.DataFrame, *, top_n: int = 10) -> str:
    if df is None or df.empty:
        return "<b>Watchlist Candidates</b>\nNo watchlist candidates available."
    lines = ["<b>Watchlist Candidates</b>"]
    for _, row in df.head(top_n).iterrows():
        tags = _as_tags(row.get("momentum_tags"))
        reason = row.get("watchlist_reason") or row.get("technical_catalyst_summary") or ""
        badge = _history_badge(row)
        lines.append(
            "#{rank} <b>{symbol}</b> {score:.1f} | {badge} | {action} | {stage} | {tags} | {reason}".format(
                rank=int(row.get("rank", 0) or 0),
                symbol=escape(str(row.get("symbol_id", ""))),
                score=float(row.get("watchlist_score", 0.0) or 0.0),
                badge=escape(badge),
                action=escape(str(row.get("action", ""))),
                stage=escape(str(row.get("stage", ""))),
                tags=escape(tags or "no tags"),
                reason=escape(str(reason)),
            )
        )
    return "\n".join(lines)
