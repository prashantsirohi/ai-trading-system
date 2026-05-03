"""Event materiality scoring for event-aware insight packets."""

from __future__ import annotations

from typing import Any, Mapping


ALERT_SCORES = {
    "critical": 100.0,
    "important": 75.0,
    "info": 45.0,
}

TIER_SCORES = {
    "A": 90.0,
    "B": 70.0,
    "C": 45.0,
    "GENERAL": 35.0,
    "IGNORE": 0.0,
}


def score_event_materiality(
    event: Mapping[str, Any],
    *,
    rank_positions: Mapping[str, int] | None = None,
    portfolio_symbols: set[str] | None = None,
    watchlist_symbols: set[str] | None = None,
    held_sectors: set[str] | None = None,
) -> dict[str, Any]:
    """Return a 0-100 trading-workflow materiality score.

    The score intentionally bridges market_intel's event importance with the
    main system's ranking/portfolio context without changing the rank itself.
    """
    symbol = _symbol(event)
    rank_positions = rank_positions or {}
    portfolio_symbols = portfolio_symbols or set()
    watchlist_symbols = watchlist_symbols or set()
    held_sectors = held_sectors or set()

    importance = _importance_component(event)
    trust = _clamp(float(event.get("trust_score") or 0.0))
    rank_relevance = _rank_relevance(symbol, rank_positions)
    portfolio_relevance = _portfolio_relevance(
        symbol,
        sector=str(event.get("sector") or ""),
        portfolio_symbols=portfolio_symbols,
        watchlist_symbols=watchlist_symbols,
        held_sectors=held_sectors,
    )
    novelty = _clamp(float(event.get("novelty_score") or event.get("novelty") or 50.0))
    score = (
        0.35 * importance
        + 0.25 * trust
        + 0.15 * rank_relevance
        + 0.15 * portfolio_relevance
        + 0.10 * novelty
    )
    return {
        "event_materiality_score": round(_clamp(score), 2),
        "components": {
            "event_importance_score": round(importance, 2),
            "event_trust_score": round(trust, 2),
            "rank_relevance": round(rank_relevance, 2),
            "portfolio_relevance": round(portfolio_relevance, 2),
            "novelty_score": round(novelty, 2),
        },
    }


def _importance_component(event: Mapping[str, Any]) -> float:
    raw = event.get("importance_score")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = 0.0
    if 0.0 <= value <= 10.0:
        value *= 10.0
    alert = ALERT_SCORES.get(str(event.get("alert_level") or "").lower())
    tier = TIER_SCORES.get(str(event.get("tier") or event.get("event_tier") or "").upper())
    candidates = [value]
    if alert is not None:
        candidates.append(alert)
    if tier is not None:
        candidates.append(tier)
    return _clamp(max(candidates))


def _rank_relevance(symbol: str, rank_positions: Mapping[str, int]) -> float:
    rank = rank_positions.get(symbol)
    if rank is None:
        return 30.0
    if rank <= 25:
        return 100.0
    if rank <= 50:
        return 80.0
    if rank <= 100:
        return 60.0
    return 30.0


def _portfolio_relevance(
    symbol: str,
    *,
    sector: str,
    portfolio_symbols: set[str],
    watchlist_symbols: set[str],
    held_sectors: set[str],
) -> float:
    if symbol in portfolio_symbols:
        return 100.0
    if symbol in watchlist_symbols:
        return 80.0
    if sector and sector in held_sectors:
        return 50.0
    return 0.0


def _symbol(event: Mapping[str, Any]) -> str:
    return str(event.get("symbol") or event.get("symbol_id") or "").upper()


def _clamp(value: float) -> float:
    return min(100.0, max(0.0, float(value)))
