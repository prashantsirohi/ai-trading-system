"""Signal urgency classification for publish outputs."""

from __future__ import annotations


def classify_signal(row: dict) -> str:
    """Classify signal urgency from composite score."""
    score = float(row.get("composite_score") or 0)
    if score >= 85:
        return "actionable"
    if score >= 65:
        return "watchlist"
    return "informational"
