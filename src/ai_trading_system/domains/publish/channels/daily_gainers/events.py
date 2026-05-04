"""Attach recent market-intel events to daily gainers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any

from ai_trading_system.integrations.market_intel_client import get_event_query_service


@dataclass(frozen=True)
class EventRow:
    symbol: str
    category: str | None
    summary: str
    event_date: datetime | None
    importance_score: float
    trust_score: float
    link: str | None
    source: str

    @classmethod
    def from_record(cls, record: Any, *, symbol: str) -> "EventRow":
        event_date = getattr(record, "event_date", None) or getattr(record, "published_at", None)
        summary = (
            getattr(record, "one_line_summary", None)
            or getattr(record, "description", None)
            or getattr(record, "title", None)
            or "Corporate event"
        )
        return cls(
            symbol=str(getattr(record, "symbol", None) or symbol),
            category=getattr(record, "primary_category", None),
            summary=str(summary),
            event_date=event_date,
            importance_score=float(getattr(record, "importance_score", 0.0) or 0.0),
            trust_score=float(getattr(record, "trust_score", 0.0) or 0.0),
            link=getattr(record, "link", None),
            source=str(getattr(record, "source", "") or ""),
        )


def attach_events(
    symbols: list[str],
    *,
    as_of: date,
    lookback_days: int = 7,
    min_trust: float = 50.0,
) -> dict[str, list[EventRow]]:
    """Return recent corporate-action/event rows keyed by symbol."""

    out: dict[str, list[EventRow]] = {symbol: [] for symbol in symbols}
    if not symbols:
        return out

    since_date = as_of - timedelta(days=int(lookback_days))
    since = datetime.combine(since_date, time.min)
    until = datetime.combine(as_of, time.max)
    try:
        svc = get_event_query_service()
    except (FileNotFoundError, ImportError):
        return out

    for symbol in symbols:
        records = svc.get_events_for_symbol(
            symbol,
            since=since,
            until=until,
            min_trust=float(min_trust),
        )
        out[symbol] = [EventRow.from_record(record, symbol=symbol) for record in records]
    return out


def event_to_dict(event: EventRow) -> dict[str, Any]:
    return {
        "symbol": event.symbol,
        "category": event.category,
        "summary": event.summary,
        "event_date": event.event_date,
        "importance_score": event.importance_score,
        "trust_score": event.trust_score,
        "link": event.link,
        "source": event.source,
    }

