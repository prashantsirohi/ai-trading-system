"""Non-actionable sheet rows from breakout and pattern evidence."""
from __future__ import annotations

import pandas as pd

COLUMNS = ["Session", "Exchange", "Symbol", "Evidence", "Type", "State", "Score", "Signal date", "Freshness", "Context", "Source"]


def build_breakout_pattern_review(breakouts: pd.DataFrame, patterns: pd.DataFrame, *, session: str) -> pd.DataFrame:
    rows = []
    for frame, kind in ((breakouts, "Breakout"), (patterns, "Pattern")):
        for item in frame.to_dict("records"):
            def text(value):
                return "" if value is None or pd.isna(value) else str(value)
            symbol = text(item.get("symbol_id")).strip().upper()
            exchange = text(item.get("exchange")).strip().upper()
            if not symbol or not exchange:
                raise ValueError("review signal missing exchange or symbol")
            score = item.get("pattern_priority_score") if kind == "Pattern" else item.get("breakout_score")
            if kind == "Pattern" and (score is None or pd.isna(score)):
                score = item.get("pattern_score")
            if kind == "Pattern" and text(item.get("r1a_evidence_class")) == "suppression_only":
                score = None
            family = text(item.get("taxonomy_family")) or text(item.get("setup_family"))
            rows.append({
                "Session": session, "Exchange": exchange, "Symbol": symbol, "Evidence": kind,
                "Type": text(item.get("pattern_family")) if kind == "Pattern" else family,
                "State": text(item.get("pattern_state") if kind == "Pattern" else item.get("review_status")),
                "Score": pd.to_numeric(score, errors="coerce"),
                "Signal date": text(item.get("signal_date")) if kind == "Pattern" else session,
                "Freshness": text(item.get("evidence_origin")) if kind == "Pattern" else "current session",
                "Context": text(item.get("r1a_evidence_class") if kind == "Pattern" else item.get("market_stage")),
                "Source": "pattern_lane_scan" if kind == "Pattern" else "breakout_review_scan",
            })
    result = pd.DataFrame(rows, columns=COLUMNS)
    if not result.empty:
        result = result.sort_values(["Evidence", "Score", "Exchange", "Symbol", "Type"], ascending=[True, False, True, True, True], na_position="last", kind="stable").reset_index(drop=True)
    return result
