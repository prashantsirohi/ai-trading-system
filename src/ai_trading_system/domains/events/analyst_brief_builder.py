"""Build deterministic analyst reason cards before LLM synthesis."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


NEGATIVE_EVENT_CATEGORIES = {
    "regulatory_legal",
    "rating_downgrade",
    "insider_sell",
}

POSITIVE_EVENT_CATEGORIES = {
    "bulk_deal",
    "block_deal",
    "capex_expansion",
    "major_order_win",
    "rating_upgrade",
    "buyback",
    "insider_buy",
}


def build_analyst_brief(packet: dict[str, Any]) -> dict[str, Any]:
    """Return symbol-level reason cards from technical and event facts."""

    rank_rows = list(((packet.get("rank") or {}).get("top_ranked") or []))
    breakout_rows = list(((packet.get("breakouts") or {}).get("candidates") or []))
    pattern_rows = list(((packet.get("patterns") or {}).get("candidates") or []))
    event_rows = list(((packet.get("market_intel") or {}).get("top_events") or []))
    event_rows.extend(list(((packet.get("market_intel") or {}).get("ranked_stock_events") or [])))
    event_rows = _dedupe_events(event_rows)

    rank_by_symbol = {_symbol(row): {**row, "rank": idx} for idx, row in enumerate(rank_rows, start=1) if _symbol(row)}
    breakout_by_symbol = _best_by_score(breakout_rows, "breakout_score")
    pattern_by_symbol = _best_by_score(pattern_rows, "pattern_score")
    events_by_symbol = _group_events(event_rows)

    symbols = _ordered_symbols(rank_rows, event_rows)
    cards = [
        _build_symbol_card(
            symbol=symbol,
            run_date=str(packet.get("run_date") or ""),
            rank_row=rank_by_symbol.get(symbol) or {},
            breakout_row=breakout_by_symbol.get(symbol) or {},
            pattern_row=pattern_by_symbol.get(symbol) or {},
            events=events_by_symbol.get(symbol) or [],
        )
        for symbol in symbols[:40]
    ]

    sector_cards = _build_sector_cards(packet, cards)
    return {
        "run_id": packet.get("run_id"),
        "run_date": packet.get("run_date"),
        "report_type": packet.get("report_type"),
        "data_trust": packet.get("data_trust") or {},
        "market_intel_status": (packet.get("market_intel") or {}).get("market_intel_status"),
        "market_regime": packet.get("market_regime") or {},
        "symbol_cards": cards,
        "sector_reason_cards": sector_cards,
        "score_definitions": {
            "event_confluence_score": "event materiality, trust, recency, and technical confirmation",
            "event_risk_score": "negative filing/sentiment and risk flag intensity",
            "event_recency_score": "100 for fresh same-day events, decaying to 0 around 30 days",
            "technical_confirmation_score": "composite rank, relative strength, volume, delivery, and proximity to highs",
            "event_vs_price_alignment": "aligned, mixed, caution, or no_event based on event tone and technical confirmation",
        },
    }


def build_event_features_frame(brief: dict[str, Any]) -> pd.DataFrame:
    columns = [
        "symbol",
        "rank",
        "event_hash",
        "event_confluence_score",
        "event_risk_score",
        "event_recency_score",
        "event_materiality_score",
        "technical_confirmation_score",
        "event_vs_price_alignment",
        "action_bucket",
    ]
    rows = []
    for card in list(brief.get("symbol_cards") or []):
        summary = card.get("event_summary") or {}
        scores = card.get("scores") or {}
        rows.append(
            {
                "symbol": card.get("symbol"),
                "rank": card.get("rank"),
                "event_hash": summary.get("event_hash"),
                "event_confluence_score": scores.get("event_confluence_score"),
                "event_risk_score": scores.get("event_risk_score"),
                "event_recency_score": scores.get("event_recency_score"),
                "event_materiality_score": scores.get("event_materiality_score"),
                "technical_confirmation_score": scores.get("technical_confirmation_score"),
                "event_vs_price_alignment": scores.get("event_vs_price_alignment"),
                "action_bucket": card.get("deterministic_bucket"),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _build_symbol_card(
    *,
    symbol: str,
    run_date: str,
    rank_row: dict[str, Any],
    breakout_row: dict[str, Any],
    pattern_row: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    top_event = _top_event(events)
    technical_score = _technical_confirmation_score(rank_row, breakout_row, pattern_row)
    event_recency = _event_recency_score(top_event, run_date)
    event_materiality = _float(top_event.get("event_materiality_score"))
    event_risk = _event_risk_score(top_event)
    event_confluence = _weighted_score(
        [
            (event_materiality, 0.35),
            (_float(top_event.get("trust_score")), 0.20),
            (event_recency, 0.20),
            (technical_score, 0.25),
        ]
    )
    alignment = _event_price_alignment(top_event, technical_score)
    event_summary = _event_summary(top_event, run_date=run_date)
    event_summary["event_confluence_score"] = round(event_confluence, 2)

    card = {
        "symbol": symbol,
        "rank": rank_row.get("rank"),
        "technical_summary": {
            "sector": rank_row.get("sector_name") or rank_row.get("sector"),
            "composite_score": _first_number(rank_row, "composite_score", "composite_score_adjusted"),
            "rs_score": _first_number(rank_row, "relative_strength", "rel_strength", "rel_strength_score"),
            "volume_intensity": _first_number(rank_row, "volume_intensity", "vol_intensity", "volume_intensity_normalized", "vol_intensity_score"),
            "delivery_pct": _first_number(rank_row, "delivery_pct"),
            "sector_strength": _first_number(rank_row, "sector_strength", "sector_strength_score", "sector_rs_value"),
            "trend_persistence": _first_number(rank_row, "trend_persistence", "bars_in_stage"),
            "proximity_to_highs": _first_number(rank_row, "proximity_to_highs", "prox_high", "prox_high_score"),
            "near_52w_high_pct": _near_52w_high_pct(rank_row),
            "volume_zscore_20": _first_number(rank_row, "volume_zscore_20"),
            "trend_state": _trend_state(rank_row),
            "breakout_state": breakout_row.get("breakout_state") or rank_row.get("breakout_state"),
            "breakout_score": _first_number(breakout_row, "breakout_score"),
            "candidate_tier": breakout_row.get("candidate_tier"),
            "pattern_family": pattern_row.get("pattern_family"),
            "pattern_score": _first_number(pattern_row, "pattern_score"),
        },
        "event_summary": event_summary,
        "scores": {
            "event_confluence_score": round(event_confluence, 2),
            "event_risk_score": round(event_risk, 2),
            "event_recency_score": round(event_recency, 2),
            "event_materiality_score": round(event_materiality, 2),
            "technical_confirmation_score": round(technical_score, 2),
            "event_vs_price_alignment": alignment,
        },
        "interpretation_inputs": _interpretation_inputs(rank_row, breakout_row, pattern_row, top_event, alignment),
        "risks": _risks(rank_row, top_event, event_risk),
    }
    card["deterministic_bucket"] = _bucket(card)
    return card


def _event_summary(event: dict[str, Any], *, run_date: str) -> dict[str, Any]:
    insight = event.get("llm_insight") if isinstance(event.get("llm_insight"), dict) else {}
    financials = insight.get("financials") if isinstance(insight.get("financials"), dict) else {}
    return {
        "event_confluence_score": None,
        "top_event": insight.get("summary") or event.get("summary") or event.get("title"),
        "event_hash": event.get("event_hash") or event.get("raw_event_id"),
        "event_age_days": event.get("freshness_days") if event.get("freshness_days") is not None else _event_age_days(event, run_date),
        "trust_score": event.get("trust_score"),
        "importance_score": event.get("importance_score"),
        "materiality": event.get("materiality_label") or _materiality_label(event.get("event_materiality_score")),
        "event_tier": event.get("event_tier") or event.get("tier"),
        "sentiment_label": insight.get("sentiment_label") or insight.get("sentiment") or event.get("sentiment_label"),
        "sentiment_score": insight.get("sentiment_score") or event.get("sentiment_score"),
        "risk_flags": _listify(insight.get("risk_flags") or event.get("risk_flags")),
        "key_facts": _listify(insight.get("key_facts") or insight.get("key_highlights")),
        "financials": financials or _financials_from_insight(insight),
        "llm_insight": insight or None,
        "source_url": event.get("source_url"),
        "published_at": event.get("published_at") or event.get("event_date"),
    }


def _interpretation_inputs(
    rank_row: dict[str, Any],
    breakout_row: dict[str, Any],
    pattern_row: dict[str, Any],
    event: dict[str, Any],
    alignment: str,
) -> list[str]:
    items: list[str] = []
    if breakout_row.get("breakout_state") or breakout_row.get("candidate_tier"):
        items.append("Price setup has a breakout candidate signal")
    if _float(rank_row.get("delivery_pct")) >= 55:
        items.append("Delivery participation is above the preferred threshold")
    if _float(rank_row.get("volume_zscore_20")) >= 2:
        items.append("Volume is meaningfully above its 20-day baseline")
    if pattern_row.get("pattern_family"):
        items.append(f"Pattern confirmation present: {pattern_row.get('pattern_family')}")
    if event:
        items.append("Event evidence is available and should be cited by source hash")
    if alignment == "aligned":
        items.append("Event tone and price/volume confirmation are aligned")
    if not event:
        items.append("No material event evidence found in the supplied window")
    return items[:6]


def _risks(rank_row: dict[str, Any], event: dict[str, Any], event_risk: float) -> list[str]:
    risks = []
    risks.extend(_listify((event.get("llm_insight") or {}).get("risk_flags") if isinstance(event.get("llm_insight"), dict) else event.get("risk_flags")))
    if str(event.get("category") or "") in {"bulk_deal", "block_deal"}:
        risks.append("Event is deal-flow only, not necessarily long-term accumulation")
    if event_risk >= 60:
        risks.append("Negative or risk-heavy event evidence requires caution")
    if _float(rank_row.get("delivery_pct")) < 35 and _float(rank_row.get("volume_intensity") or rank_row.get("vol_intensity")) > 70:
        risks.append("Volume strength has weak delivery confirmation")
    return list(dict.fromkeys(str(r) for r in risks if r))[:5]


def _bucket(card: dict[str, Any]) -> str:
    scores = card.get("scores") or {}
    if _float(scores.get("event_risk_score")) >= 65 or scores.get("event_vs_price_alignment") == "caution":
        return "risk/caution"
    if _float(scores.get("event_confluence_score")) >= 70 and scores.get("event_vs_price_alignment") == "aligned":
        return "actionable_confluence"
    if _float(scores.get("technical_confirmation_score")) >= 70:
        return "watchlist_only"
    return "ignore/noise"


def _build_sector_cards(packet: dict[str, Any], cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sector_rows = list(((packet.get("sector_strength") or {}).get("top_sectors") or []))
    by_sector: dict[str, int] = {}
    for card in cards:
        sector = str((card.get("technical_summary") or {}).get("sector") or "")
        if sector:
            by_sector[sector] = by_sector.get(sector, 0) + 1
    out = []
    for row in sector_rows[:8]:
        sector = row.get("Sector") or row.get("sector")
        out.append(
            {
                "sector": sector,
                "rs_rank": row.get("RS_rank") or row.get("RS_rank_pct"),
                "quadrant": row.get("Quadrant"),
                "event_backed_symbol_count": by_sector.get(str(sector), 0),
            }
        )
    return out


def _ordered_symbols(rank_rows: list[dict[str, Any]], event_rows: list[dict[str, Any]]) -> list[str]:
    out = []
    for row in event_rows:
        symbol = _symbol(row)
        if symbol and symbol not in out:
            out.append(symbol)
    for row in rank_rows:
        symbol = _symbol(row)
        if symbol and symbol not in out:
            out.append(symbol)
    return out


def _group_events(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        symbol = _symbol(event)
        if symbol:
            grouped.setdefault(symbol, []).append(event)
    for rows in grouped.values():
        rows.sort(key=lambda row: (_float(row.get("event_materiality_score")), _float(row.get("importance_score"))), reverse=True)
    return grouped


def _dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    seen = set()
    for event in events:
        key = str(event.get("event_hash") or event.get("raw_event_id") or event.get("resolved_event_id") or id(event))
        if key in seen:
            continue
        seen.add(key)
        out.append(event)
    return out


def _top_event(events: list[dict[str, Any]]) -> dict[str, Any]:
    return events[0] if events else {}


def _best_by_score(rows: list[dict[str, Any]], score_key: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = _symbol(row)
        if not symbol:
            continue
        if symbol not in out or _float(row.get(score_key)) > _float(out[symbol].get(score_key)):
            out[symbol] = row
    return out


def _technical_confirmation_score(rank_row: dict[str, Any], breakout_row: dict[str, Any], pattern_row: dict[str, Any]) -> float:
    composite = _first_number(rank_row, "composite_score_adjusted", "composite_score")
    rs = _first_number(rank_row, "relative_strength", "rel_strength_score", "rel_strength")
    volume = _first_number(rank_row, "volume_intensity", "vol_intensity_score", "volume_intensity_normalized", "vol_intensity")
    delivery = _first_number(rank_row, "delivery_pct")
    prox = _first_number(rank_row, "proximity_to_highs", "prox_high_score", "prox_high")
    breakout = _first_number(breakout_row, "breakout_score")
    pattern = _first_number(pattern_row, "pattern_score")
    return _weighted_score([(composite, 0.35), (rs, 0.20), (volume, 0.15), (delivery, 0.10), (prox, 0.10), (breakout or pattern, 0.10)])


def _event_recency_score(event: dict[str, Any], run_date: str) -> float:
    if not event:
        return 0.0
    age = event.get("freshness_days")
    if age is None:
        age = _event_age_days(event, run_date)
    return max(0.0, 100.0 - min(30.0, _float(age)) * (100.0 / 30.0))


def _event_risk_score(event: dict[str, Any]) -> float:
    if not event:
        return 0.0
    insight = event.get("llm_insight") if isinstance(event.get("llm_insight"), dict) else {}
    sentiment = str(insight.get("sentiment_label") or insight.get("sentiment") or event.get("sentiment_label") or "").lower()
    risk_flags = _listify(insight.get("risk_flags") or event.get("risk_flags"))
    category = str(event.get("category") or "")
    score = min(50.0, len(risk_flags) * 15.0)
    if sentiment == "negative":
        score += 35.0
    elif _float(insight.get("sentiment_score") or event.get("sentiment_score")) < -0.25:
        score += 25.0
    if category in NEGATIVE_EVENT_CATEGORIES:
        score += 25.0
    return min(100.0, score)


def _event_price_alignment(event: dict[str, Any], technical_score: float) -> str:
    if not event:
        return "no_event"
    category = str(event.get("category") or "")
    insight = event.get("llm_insight") if isinstance(event.get("llm_insight"), dict) else {}
    sentiment = str(insight.get("sentiment_label") or insight.get("sentiment") or event.get("sentiment_label") or "").lower()
    if sentiment == "negative" or category in NEGATIVE_EVENT_CATEGORIES:
        return "caution" if technical_score >= 55 else "mixed"
    if sentiment == "positive" or category in POSITIVE_EVENT_CATEGORIES:
        return "aligned" if technical_score >= 55 else "event_only"
    return "mixed" if technical_score >= 55 else "event_only"


def _weighted_score(parts: list[tuple[Any, float]]) -> float:
    total = 0.0
    weight = 0.0
    for value, part_weight in parts:
        if value is None:
            continue
        total += max(0.0, min(100.0, _float(value))) * part_weight
        weight += part_weight
    if weight == 0:
        return 0.0
    return total / weight


def _near_52w_high_pct(row: dict[str, Any]) -> float | None:
    close = _maybe_float(row.get("close"))
    high = _maybe_float(row.get("high_52w"))
    if close is not None and high and high > 0:
        return round(max(0.0, (high - close) / high * 100.0), 2)
    value = _first_number(row, "near_52w_high_pct")
    if value is not None:
        return value
    prox = _maybe_float(row.get("prox_high"))
    if prox is not None:
        return round(max(0.0, 100.0 - prox), 2) if prox > 1 else round(max(0.0, (1.0 - prox) * 100.0), 2)
    return None


def _trend_state(row: dict[str, Any]) -> str | None:
    close = _maybe_float(row.get("close"))
    sma50 = _maybe_float(row.get("sma_50"))
    sma200 = _maybe_float(row.get("sma_200") or row.get("sma200"))
    if close is not None and sma50 is not None and sma200 is not None:
        if close >= sma50 and close >= sma200:
            return "above SMA50 and SMA200"
        if close >= sma50:
            return "above SMA50, below SMA200"
        if close >= sma200:
            return "below SMA50, above SMA200"
        return "below SMA50 and SMA200"
    return row.get("stage2_label") or row.get("weekly_stage_label")


def _financials_from_insight(insight: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "money_value_cr",
        "market_cap_pct",
        "revenue_cr",
        "pat_cr",
        "eps",
        "capex_amount_cr",
        "order_value_cr",
        "buyback_size_cr",
    ]
    return {key: insight.get(key) for key in keys if insight.get(key) is not None}


def _event_age_days(event: dict[str, Any], run_date: str) -> int | None:
    event_date = event.get("published_at") or event.get("event_date")
    if not event_date:
        return None
    try:
        event_dt = datetime.fromisoformat(str(event_date).replace("Z", "+00:00"))
        run_dt = datetime.fromisoformat(str(run_date).replace("Z", "+00:00")) if run_date else datetime.now(timezone.utc)
        if event_dt.tzinfo is None:
            event_dt = event_dt.replace(tzinfo=timezone.utc)
        if run_dt.tzinfo is None:
            run_dt = run_dt.replace(tzinfo=timezone.utc)
        return max(0, int((run_dt - event_dt).total_seconds() // 86400))
    except ValueError:
        return None


def _materiality_label(score: Any) -> str:
    value = _float(score)
    if value >= 75:
        return "high"
    if value >= 50:
        return "medium"
    if value > 0:
        return "low"
    return "none"


def _first_number(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _maybe_float(row.get(key))
        if value is not None:
            return value
    return None


def _symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or row.get("symbol_id") or "").upper()


def _listify(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, str):
        try:
            import json

            loaded = json.loads(value)
            if isinstance(loaded, list):
                return [str(item) for item in loaded if item]
        except ValueError:
            pass
        return [value]
    return [str(value)]


def _maybe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float:
    parsed = _maybe_float(value)
    return 0.0 if parsed is None else parsed
