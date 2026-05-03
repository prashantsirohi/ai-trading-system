"""EnrichmentService: joins triggers with corporate-action context.

For each ``Trigger`` (volume shock, bulk deal, breakout) we query
``market_intel.EventQueryService`` for the last N days of corporate events
on that symbol, optionally apply a noise filter, and emit one
``EnrichedSignal`` per trigger.

The service is deliberately small and pure — no I/O beyond the injected
query service, no logging side-effects, no orchestrator coupling. The
``events`` pipeline stage is the only caller.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Protocol

from ai_trading_system.domains.events.triggers import Trigger

logger = logging.getLogger(__name__)


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_PER_TRIGGER_EVENT_LIMIT = 10
DEFAULT_MIN_TRUST = 80.0
DEFAULT_TIERS: tuple[str, ...] = ("A", "B")


# --------------------------------------------------------------------------- protocols


class EventQuerier(Protocol):
    """Subset of market_intel.EventQueryService we depend on (duck-typed)."""

    def get_events_for_symbol(
        self,
        symbol: str,
        *,
        since: datetime,
        until: datetime | None = None,
        tiers: Iterable[str] = ("A", "B"),
        min_importance: float = 0.0,
        min_trust: float = 80.0,
        limit: int = 50,
    ) -> list[Any]:
        ...


class NoiseFilter(Protocol):
    """Composable filter chain applied to a (trigger, [events]) pair.

    Returns the kept events plus a ``suppress_reason`` (None when nothing
    was suppressed at trigger level). Phase 4 implements this; today we
    accept any callable that satisfies the shape.
    """

    def apply(
        self,
        *,
        trigger: Trigger,
        events: list[Any],
    ) -> tuple[list[Any], str | None]:
        ...


# --------------------------------------------------------------------------- output


@dataclass(frozen=True)
class EnrichedSignal:
    trigger: Trigger
    events: list[Any] = field(default_factory=list)
    materiality_label: str | None = None       # set by Phase 4 noise filter
    top_category: str | None = None
    severity: str | None = None                # low-info | medium | high
    suppressed: bool = False
    suppress_reason: str | None = None
    event_hashes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trigger": {
                "symbol": self.trigger.symbol,
                "trigger_type": self.trigger.trigger_type,
                "as_of_date": self.trigger.as_of_date.isoformat(),
                "trigger_strength": self.trigger.trigger_strength,
                "trigger_metadata": dict(self.trigger.trigger_metadata),
            },
            "events": [_event_to_dict(e) for e in self.events],
            "event_hashes": list(self.event_hashes),
            "event_count": len(self.events),
            "materiality_label": self.materiality_label,
            "top_category": self.top_category,
            "severity": self.severity,
            "suppressed": self.suppressed,
            "suppress_reason": self.suppress_reason,
        }


def _event_to_dict(event: Any) -> dict[str, Any]:
    """Serialize a market_intel ResolvedEventRecord (or any dataclass-like)."""
    if hasattr(event, "to_dict"):
        return event.to_dict()  # type: ignore[no-any-return]
    if hasattr(event, "__dataclass_fields__"):
        out = asdict(event)
        # Coerce datetime fields to ISO strings for JSON-safety
        for key, value in list(out.items()):
            if isinstance(value, datetime):
                out[key] = value.isoformat()
        return out
    return dict(event) if isinstance(event, dict) else {"value": str(event)}


def _derive_severity(
    *,
    trigger: Trigger,
    events: list[Any],
    materiality_label: str | None,
) -> str:
    """Tier-shaped severity per the plan:

      volume_shock + Tier A event   = high
      bulk_deal alone (no event)    = medium
      breakout + no event           = low-info
      anything with a critical mat. = high
      anything with no events       = severity inherited from trigger only
    """
    has_events = bool(events)
    top_tier = max(
        (getattr(e, "event_tier", "") or "" for e in events),
        default="",
    )

    if materiality_label == "critical":
        return "high"
    if trigger.trigger_type == "volume_shock" and top_tier == "A":
        return "high"
    if trigger.trigger_type == "bulk_deal" and not has_events:
        return "medium"
    if trigger.trigger_type == "breakout" and not has_events:
        return "low-info"
    if has_events and top_tier == "A":
        return "high"
    if has_events and top_tier == "B":
        return "medium"
    return "low-info"


# --------------------------------------------------------------------------- service


class EnrichmentService:
    def __init__(
        self,
        query_service: EventQuerier,
        *,
        noise_filter: NoiseFilter | None = None,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        per_trigger_event_limit: int = DEFAULT_PER_TRIGGER_EVENT_LIMIT,
        min_trust: float = DEFAULT_MIN_TRUST,
        tiers: tuple[str, ...] = DEFAULT_TIERS,
    ):
        self.query_service = query_service
        self.noise_filter = noise_filter
        self.lookback_days = lookback_days
        self.per_trigger_event_limit = per_trigger_event_limit
        self.min_trust = min_trust
        self.tiers = tiers

    def enrich(self, triggers: Iterable[Trigger]) -> list[EnrichedSignal]:
        out: list[EnrichedSignal] = []
        for trigger in triggers:
            out.append(self._enrich_one(trigger))
        return out

    def _enrich_one(self, trigger: Trigger) -> EnrichedSignal:
        as_of_dt = datetime.combine(
            trigger.as_of_date, datetime.min.time(),
        ).replace(tzinfo=timezone.utc)
        since = as_of_dt - timedelta(days=self.lookback_days)

        try:
            events = self.query_service.get_events_for_symbol(
                trigger.symbol,
                since=since,
                until=as_of_dt + timedelta(days=1),
                tiers=self.tiers,
                min_trust=self.min_trust,
                limit=self.per_trigger_event_limit,
            )
        except Exception as exc:
            logger.warning(
                "EventQueryService failed for %s: %s — emitting empty enrichment",
                trigger.symbol, exc,
            )
            events = []

        # Phase 4 noise filter (optional — wired in later)
        suppress_reason: str | None = None
        materiality_label: str | None = None
        if self.noise_filter is not None:
            events, suppress_reason = self.noise_filter.apply(
                trigger=trigger, events=events,
            )
            # Filters that compute materiality may stash it on the events list
            # (our convention: events is a list that may have a `_materiality`
            # attr or each event carries its own). We extract the strongest.
            for ev in events:
                lbl = getattr(ev, "_materiality_label", None)
                if lbl in ("critical", "high"):
                    materiality_label = lbl
                    break
                if lbl and materiality_label is None:
                    materiality_label = lbl

        # Top category by importance score (max), fallback to first event's category.
        top_category: str | None = None
        if events:
            scored = [
                (getattr(e, "importance_score", 0.0) or 0.0, e) for e in events
            ]
            scored.sort(key=lambda tup: tup[0], reverse=True)
            top_event = scored[0][1]
            top_category = getattr(top_event, "primary_category", None)

        severity = _derive_severity(
            trigger=trigger,
            events=events,
            materiality_label=materiality_label,
        )

        event_hashes = [
            h for h in (getattr(e, "event_hash", None) for e in events) if h
        ]

        suppressed = bool(suppress_reason) and not events
        return EnrichedSignal(
            trigger=trigger,
            events=list(events),
            materiality_label=materiality_label,
            top_category=top_category,
            severity=severity,
            suppressed=suppressed,
            suppress_reason=suppress_reason,
            event_hashes=event_hashes,
        )


def summarize(signals: Iterable[EnrichedSignal]) -> dict[str, Any]:
    """Build the events_summary.json artifact payload."""
    by_trigger: dict[str, int] = {}
    by_severity: dict[str, int] = {}
    by_category: dict[str, int] = {}
    by_materiality: dict[str, int] = {}
    suppressed_count = 0
    total = 0
    total_events = 0
    for sig in signals:
        total += 1
        if sig.suppressed:
            suppressed_count += 1
        total_events += len(sig.events)
        by_trigger[sig.trigger.trigger_type] = (
            by_trigger.get(sig.trigger.trigger_type, 0) + 1
        )
        if sig.severity:
            by_severity[sig.severity] = by_severity.get(sig.severity, 0) + 1
        if sig.top_category:
            by_category[sig.top_category] = by_category.get(sig.top_category, 0) + 1
        if sig.materiality_label:
            by_materiality[sig.materiality_label] = (
                by_materiality.get(sig.materiality_label, 0) + 1
            )

    return {
        "trigger_count": total,
        "event_count": total_events,
        "suppressed_count": suppressed_count,
        "by_trigger_type": by_trigger,
        "by_severity": by_severity,
        "by_top_category": by_category,
        "by_materiality": by_materiality,
    }
