"""Contracts for lightweight catalyst analysis."""

from __future__ import annotations

CATALYST_TYPES = [
    "CAPEX",
    "DEMERGER",
    "VALUE_UNLOCK",
    "ORDER_WIN",
    "MARGIN_EXPANSION",
    "SECTOR_TAILWIND",
    "DEBT_REDUCTION",
    "PROMOTER_BUYING",
    "UNDERVALUATION",
    "RESULTS_BREAKOUT",
]

CATALYST_OUTPUT_COLUMNS = [
    "symbol",
    "catalyst_score",
    "catalyst_type",
    "catalyst_summary",
    "evidence_source",
    "confidence",
]

EVIDENCE_TEXT_COLUMNS = [
    "catalyst_summary",
    "summary",
    "text",
    "snippet",
    "headline",
    "note",
]
