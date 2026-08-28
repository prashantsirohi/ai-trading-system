"""Hand-maintained agent-facing contract for private journal evidence."""

from __future__ import annotations

from typing import Any


SURFACES: dict[str, dict[str, Any]] = {
    "trade_journal": {
        "surface": "trade_journal",
        "tool": "all journal MCP tools",
        "store": "trade_journal.duckdb",
        "tables": [
            "journal_analysis_run", "portfolio_reconstruction", "trade_episode",
            "journal_fill", "trade_evaluation", "portfolio_evaluation",
            "portfolio_risk_snapshot", "portfolio_reconciliation",
            "portfolio_reconciliation_item", "journal_dq_issue",
        ],
        "grain": "account-scoped journal evidence; grain varies by tool",
        "as_of_support": "EXACT",
        "notes": [
            "The server is pinned to one account at startup and never enumerates accounts.",
            "portfolio_as_of is the economic cutoff; known_at is the recording-time cutoff.",
            "Scope labels and trust status must remain attached to portfolio claims.",
            "Gross P&L excludes charges; holdings_only and securities_only are not NAV returns.",
        ],
        "columns": [
            {"name": "portfolio_as_of", "type": "date", "meaning": "Economic date described by the result.", "units": None},
            {"name": "known_at", "type": "timestamp", "meaning": "Latest recording/completion time admitted by the query.", "units": None},
            {"name": "scope", "type": "string", "meaning": "gross, securities_only, or holdings_only claim boundary.", "units": None},
            {"name": "trust_status", "type": "string", "meaning": "Persisted evidence trust classification; never reinterpreted by MCP.", "units": None},
            {"name": "logic_version", "type": "string", "meaning": "Version of the journal analysis logic that produced the evidence.", "units": None},
            {"name": "analysis_run_id", "type": "string", "meaning": "Opaque immutable analysis version identifier.", "units": None},
            {"name": "episode_id", "type": "string", "meaning": "Opaque zero-to-zero trade episode identifier.", "units": None},
            {"name": "quantity", "type": "decimal", "meaning": "Accepted security quantity; inventory deficits are not fabricated as shorts.", "units": "shares"},
            {"name": "fifo_cost", "type": "decimal", "meaning": "FIFO cost basis when trusted.", "units": "INR"},
            {"name": "weighted_average_cost", "type": "decimal", "meaning": "Weighted-average cost basis when trusted.", "units": "INR"},
            {"name": "realised_gross_pnl", "type": "decimal", "meaning": "Realised P&L before charges and taxes.", "units": "INR"},
            {"name": "score_status", "type": "string", "meaning": "Persisted scoring coverage state; insufficient evidence is not a failed score.", "units": None},
            {"name": "classification", "type": "string", "meaning": "Persisted process or reconciliation classification.", "units": None},
        ],
    }
}


def describe_schema(surface: str | None = None) -> dict[str, Any]:
    if surface is None:
        return {"surfaces": [dict(SURFACES["trade_journal"])]}
    key = str(surface).strip().lower()
    if key not in SURFACES:
        raise ValueError(f"Unknown journal MCP surface: {surface!r}")
    return dict(SURFACES[key])


__all__ = ["SURFACES", "describe_schema"]
