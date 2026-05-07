"""Rule-based catalyst analysis for a small candidate universe."""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

from ai_trading_system.domains.catalysts.contracts import (
    CATALYST_OUTPUT_COLUMNS,
    CATALYST_TYPES,
    EVIDENCE_TEXT_COLUMNS,
)
from ai_trading_system.domains.fundamentals.enrich_rank import normalize_symbol


KEYWORD_TYPES: list[tuple[str, tuple[str, ...]]] = [
    ("CAPEX", ("capex", "capacity expansion", "greenfield", "brownfield")),
    ("DEMERGER", ("demerger", "spin off", "spin-off", "scheme of arrangement")),
    ("VALUE_UNLOCK", ("value unlock", "asset sale", "monetisation", "monetization")),
    ("ORDER_WIN", ("order win", "contract", "letter of award", "loa")),
    ("MARGIN_EXPANSION", ("margin expansion", "operating leverage", "ebitda margin")),
    ("SECTOR_TAILWIND", ("sector tailwind", "policy support", "demand recovery")),
    ("DEBT_REDUCTION", ("debt reduction", "deleveraging", "repayment", "net debt")),
    ("PROMOTER_BUYING", ("promoter buying", "promoter purchase", "insider buying")),
    ("UNDERVALUATION", ("undervalued", "cheap valuation", "rerating", "valuation gap")),
    ("RESULTS_BREAKOUT", ("results breakout", "profit jump", "revenue growth", "earnings beat")),
]


def analyze_catalysts(candidates: Iterable[str], evidence: pd.DataFrame | None = None) -> pd.DataFrame:
    """Score catalyst evidence for the supplied candidates only."""

    candidate_order = [symbol for symbol in (normalize_symbol(value) for value in candidates) if symbol]
    if evidence is None or evidence.empty or not candidate_order:
        return pd.DataFrame(columns=CATALYST_OUTPUT_COLUMNS)

    frame = evidence.copy()
    if "symbol" not in frame.columns:
        return pd.DataFrame(columns=CATALYST_OUTPUT_COLUMNS)
    frame.loc[:, "symbol"] = frame["symbol"].map(normalize_symbol)
    frame = frame.loc[frame["symbol"].isin(set(candidate_order))]
    if frame.empty:
        return pd.DataFrame(columns=CATALYST_OUTPUT_COLUMNS)

    rows: list[dict[str, object]] = []
    for symbol in candidate_order:
        symbol_evidence = frame.loc[frame["symbol"].eq(symbol)]
        if symbol_evidence.empty:
            continue
        row = _best_evidence_row(symbol, symbol_evidence)
        rows.append(row)

    output = pd.DataFrame(rows, columns=CATALYST_OUTPUT_COLUMNS)
    if output.empty:
        return pd.DataFrame(columns=CATALYST_OUTPUT_COLUMNS)
    output.loc[:, "catalyst_score"] = pd.to_numeric(output["catalyst_score"], errors="coerce").fillna(0).clip(0, 100).round(2)
    output.loc[:, "confidence"] = pd.to_numeric(output["confidence"], errors="coerce").fillna(0).clip(0, 1).round(2)
    return output[CATALYST_OUTPUT_COLUMNS]


def apply_catalyst_adjustment(watchlist: pd.DataFrame, catalysts: pd.DataFrame | None) -> pd.DataFrame:
    """Apply the catalyst-aware final score formula where catalyst data exists."""

    if watchlist is None or watchlist.empty or catalysts is None or catalysts.empty:
        return watchlist.copy() if watchlist is not None else pd.DataFrame()
    output = watchlist.copy()
    output.loc[:, "symbol"] = output["symbol"].map(normalize_symbol)
    output = output.drop(columns=[column for column in CATALYST_OUTPUT_COLUMNS if column != "symbol"], errors="ignore")
    catalyst_frame = catalysts.copy()
    catalyst_frame.loc[:, "symbol"] = catalyst_frame["symbol"].map(normalize_symbol)
    output = output.merge(catalyst_frame[CATALYST_OUTPUT_COLUMNS], on="symbol", how="left")
    has_catalyst = pd.to_numeric(output.get("catalyst_score"), errors="coerce").notna()
    output.loc[has_catalyst, "final_watchlist_score"] = (
        0.60 * _num(output, "composite_score")
        + 0.15 * _num(output, "breakout_pattern_score", 50.0)
        + 0.15 * _num(output, "fundamental_score", 50.0)
        + 0.10 * _num(output, "catalyst_score")
    ).loc[has_catalyst].clip(0, 100).round(2)
    return output


def _best_evidence_row(symbol: str, evidence: pd.DataFrame) -> dict[str, object]:
    scored = []
    for _, row in evidence.iterrows():
        text = _evidence_text(row)
        catalyst_type = _catalyst_type(row, text)
        confidence = _confidence(row, text)
        score = _score(catalyst_type, confidence, text)
        scored.append((score, confidence, catalyst_type, text, row))
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    score, confidence, catalyst_type, text, row = scored[0]
    source = str(row.get("evidence_source") or row.get("source") or row.get("source_path") or "artifact")
    return {
        "symbol": symbol,
        "catalyst_score": score,
        "catalyst_type": catalyst_type,
        "catalyst_summary": _shorten(text),
        "evidence_source": source,
        "confidence": confidence,
    }


def _evidence_text(row: pd.Series) -> str:
    for column in EVIDENCE_TEXT_COLUMNS:
        value = row.get(column)
        if pd.notna(value) and str(value).strip():
            return re.sub(r"\s+", " ", str(value).strip())
    return ""


def _catalyst_type(row: pd.Series, text: str) -> str:
    provided = str(row.get("catalyst_type") or "").strip().upper()
    if provided in CATALYST_TYPES:
        return provided
    lower = text.lower()
    for catalyst_type, keywords in KEYWORD_TYPES:
        if any(keyword in lower for keyword in keywords):
            return catalyst_type
    return "SECTOR_TAILWIND"


def _confidence(row: pd.Series, text: str) -> float:
    raw = row.get("confidence")
    try:
        value = float(raw)
        if value > 1:
            value = value / 100
        return max(0.0, min(1.0, value))
    except (TypeError, ValueError):
        pass
    if len(text) >= 80:
        return 0.7
    if text:
        return 0.55
    return 0.0


def _score(catalyst_type: str, confidence: float, text: str) -> float:
    base = {
        "ORDER_WIN": 78,
        "RESULTS_BREAKOUT": 78,
        "MARGIN_EXPANSION": 74,
        "DEBT_REDUCTION": 72,
        "CAPEX": 70,
        "DEMERGER": 70,
        "VALUE_UNLOCK": 70,
        "PROMOTER_BUYING": 68,
        "SECTOR_TAILWIND": 62,
        "UNDERVALUATION": 58,
    }.get(catalyst_type, 55)
    if not text:
        return 0.0
    return round(base * max(0.25, confidence), 2)


def _shorten(text: str, limit: int = 220) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3].rstrip()}..."


def _num(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default).astype(float)
