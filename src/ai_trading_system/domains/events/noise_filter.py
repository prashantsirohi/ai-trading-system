"""Composable noise-reduction filter chain for the events stage.

Design
------
A ``NoiseFilter`` exposes ``.apply(trigger, events) -> (kept, suppress_reason)``.
The default chain composes seven filters in order; each may drop events,
annotate them (e.g. ``_materiality_label``), or return a ``suppress_reason``
that signals "no event survived for a meaningful reason" so the publish
layer can record it instead of silently dropping the trigger.

Filters
  1. Category whitelist     — drop categories not in the publishable list
  2. Trust gate             — drop events whose trust_score < threshold
  3. Materiality gate       — compute material_pct via market_intel.materiality
                              and drop low-materiality events for value-bearing
                              categories
  4. Time decay             — hard lookback cutoff per category, with extended
                              window for M&A / litigation / SAST
  5. Per-symbol dedup       — suppress when (symbol, top_category) was already
                              delivered in the recent past (reads
                              events_enrichment_log)
  6. Corroboration          — annotate events that appear at NSE+BSE within the
                              window with ``_corroborated=True``
  7. Universe filter        — applied upstream at trigger collection; here we
                              provide a pass-through ``UniverseFilter`` for
                              symmetry / future use

Annotations the chain may set on each event:
  - ``_materiality_label`` ∈ {low, medium, high, critical}
  - ``_material_pct``     : float | None
  - ``_corroborated``     : bool

Tier-shaped severity is computed in ``EnrichmentService._derive_severity``,
not here, so filters stay small and order-independent.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol

from ai_trading_system.domains.events.triggers import Trigger

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- types


class NoiseFilter(Protocol):
    def apply(
        self,
        *,
        trigger: Trigger,
        events: list[Any],
    ) -> tuple[list[Any], str | None]:
        ...


# --------------------------------------------------------------------------- helpers


def _get_attr(event: Any, key: str, default: Any = None) -> Any:
    """Tolerant attribute getter: supports dataclasses, dicts, and ORM rows."""
    if event is None:
        return default
    if isinstance(event, dict):
        return event.get(key, default)
    return getattr(event, key, default)


def _set_marker(event: Any, key: str, value: Any) -> None:
    """Set a marker on the event in place (works for dataclasses + dicts)."""
    if isinstance(event, dict):
        event[key] = value
    else:
        try:
            object.__setattr__(event, key, value)
        except Exception:
            # Frozen dataclasses raise; fall back to __dict__ where possible
            try:
                event.__dict__[key] = value
            except Exception:
                pass


def _get_event_datetime(event: Any) -> datetime | None:
    for key in ("event_date", "published_at"):
        value = _get_attr(event, key)
        if value is None:
            continue
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        try:
            parsed = datetime.fromisoformat(str(value))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            continue
    return None


# --------------------------------------------------------------------------- filters


@dataclass
class CategoryWhitelistFilter:
    whitelist: frozenset[str]

    def apply(self, *, trigger, events):
        kept = [e for e in events if _get_attr(e, "primary_category") in self.whitelist]
        if events and not kept:
            return [], "all_categories_off_whitelist"
        return kept, None


@dataclass
class TrustGateFilter:
    min_trust: float = 80.0

    def apply(self, *, trigger, events):
        kept = [
            e for e in events
            if float(_get_attr(e, "trust_score", 0.0) or 0.0) >= self.min_trust
        ]
        if events and not kept:
            return [], f"all_below_min_trust({self.min_trust})"
        return kept, None


@dataclass
class MaterialityFilter:
    """Drop events whose deal_value/market_cap is below the per-category gate.

    ``market_cap_provider(symbol) -> float | None`` returns market cap in INR.
    When either deal_value or market_cap is unknown we keep the event with a
    ``_materiality_label='medium'`` annotation (neutral).
    """

    market_cap_provider: Any                    # Callable[[str], float | None]
    drop_below: str = "medium"                  # 'low' | 'medium' | 'high' | 'critical'
    thresholds: dict[str, dict[str, float]] = field(default_factory=dict)

    _LABEL_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}

    def apply(self, *, trigger, events):
        try:
            from market_intel.processing.materiality import (
                extract_deal_value_inr,
                score as materiality_score,
            )
        except ImportError:  # pragma: no cover
            logger.debug("MaterialityFilter: market_intel not installed; skipping")
            return events, None

        threshold_rank = self._LABEL_RANK.get(self.drop_below, 1)
        kept: list[Any] = []
        for event in events:
            category = _get_attr(event, "primary_category") or ""
            text = " ".join(filter(None, [
                _get_attr(event, "title") or "",
                _get_attr(event, "description") or "",
            ]))
            deal_value = extract_deal_value_inr(text)
            mcap = self._safe_market_cap(_get_attr(event, "symbol"))

            mat = materiality_score(
                category=category,
                deal_value_inr=deal_value,
                market_cap_inr=mcap,
                thresholds=self._thresholds_for_score() if self.thresholds else None,
            )
            _set_marker(event, "_materiality_label", mat.label)
            _set_marker(event, "_material_pct", mat.material_pct)

            event_rank = self._LABEL_RANK.get(mat.label, 1)
            # Keep if rank ≥ threshold OR if neither value is known (label=medium
            # acts as neutral; the score() function returns medium when inputs
            # are missing).
            if event_rank >= threshold_rank:
                kept.append(event)
            elif mat.deal_value_inr is None or mat.market_cap_inr is None:
                # Unknown metrics — keep with the neutral label
                kept.append(event)
        if events and not kept:
            return [], f"all_below_materiality({self.drop_below})"
        return kept, None

    def _safe_market_cap(self, symbol: str | None) -> float | None:
        if symbol is None or self.market_cap_provider is None:
            return None
        try:
            return self.market_cap_provider(symbol)
        except Exception:
            return None

    def _thresholds_for_score(self) -> dict[str, dict[str, float]]:
        """Convert our config shape to the shape market_intel.score() expects."""
        out: dict[str, dict[str, float]] = {}
        for cat, lvls in self.thresholds.items():
            out[cat] = {
                "low": 0.0,
                "medium": float(lvls.get("medium", 0.0)),
                "high": float(lvls.get("high", 0.0)),
                "critical": float(lvls.get("critical", 0.0)),
            }
        return out


@dataclass
class TimeDecayFilter:
    routine_lookback_days: int = 30
    extended_lookback_days: int = 90
    extended_categories: frozenset[str] = field(default_factory=frozenset)
    as_of: datetime | None = None  # injected per-run by EventsStage; defaults to now

    def apply(self, *, trigger, events):
        try:
            from market_intel.processing.time_decay import is_within_lookback
        except ImportError:  # pragma: no cover
            logger.debug("TimeDecayFilter: market_intel not installed; skipping")
            return events, None

        as_of = self.as_of or datetime.now(timezone.utc)
        kept = []
        for event in events:
            event_dt = _get_event_datetime(event)
            if event_dt is None:
                # No date — assume recent rather than drop.
                kept.append(event)
                continue
            category = _get_attr(event, "primary_category") or ""
            if is_within_lookback(
                event_date=event_dt, as_of=as_of, category=category,
                routine_lookback_days=self.routine_lookback_days,
                extended_lookback_days=self.extended_lookback_days,
                extended_categories=set(self.extended_categories),
            ):
                kept.append(event)
        if events and not kept:
            return [], "all_outside_lookback"
        return kept, None


@dataclass
class PerSymbolDedupFilter:
    """Suppress when (symbol, top_category) was published in the recent past.

    Reads from the ``events_enrichment_log`` table written by EventsStage on
    previous runs. ``conn_provider() -> connection`` is duck-typed; pass the
    registry connection in tests / production.
    """

    conn_provider: Any
    lookback_days: int = 7

    def apply(self, *, trigger, events):
        if self.conn_provider is None or not events:
            return events, None
        # Pick the candidate top_category from the surviving events
        top_cat = _get_attr(events[0], "primary_category")
        for ev in events[1:]:
            score = float(_get_attr(ev, "importance_score", 0.0) or 0.0)
            best_score = float(_get_attr(events[0], "importance_score", 0.0) or 0.0)
            if score > best_score:
                top_cat = _get_attr(ev, "primary_category")
        if not top_cat:
            return events, None
        try:
            conn = self._open()
        except Exception:
            return events, None
        try:
            row = conn.execute(
                """
                SELECT 1 FROM events_enrichment_log
                WHERE symbol = ? AND top_category = ?
                  AND suppressed = FALSE
                  AND created_at >= current_timestamp - INTERVAL ? DAY
                LIMIT 1
                """,
                [trigger.symbol, top_cat, self.lookback_days],
            ).fetchone()
        except Exception as exc:
            logger.debug("PerSymbolDedupFilter: query failed (%s); skipping", exc)
            return events, None
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if row is not None:
            return [], f"per_symbol_dedup({top_cat},{self.lookback_days}d)"
        return events, None

    def _open(self):
        ctx = self.conn_provider()
        if hasattr(ctx, "__enter__"):
            return ctx.__enter__()
        return ctx


@dataclass
class CorroborationFilter:
    """Mark events that appear in two sources within the configured window.

    Uses the convention that ``source`` is e.g. ``nse_rss`` or ``bse_corp``.
    Events with ``_corroborated=True`` get downstream routing-tier boost in
    Phase 6's publish layer.
    """

    window_hours: int = 24

    def apply(self, *, trigger, events):
        from collections import defaultdict
        groups: dict[tuple[str, str], list[tuple[datetime | None, Any]]] = defaultdict(list)
        for ev in events:
            sym = _get_attr(ev, "symbol") or ""
            cat = _get_attr(ev, "primary_category") or ""
            groups[(sym, cat)].append((_get_event_datetime(ev), ev))

        for (_sym, _cat), bucket in groups.items():
            sources = {_get_attr(ev, "source") for _, ev in bucket}
            if len(sources) < 2:
                continue
            # Within-window check across distinct sources
            dated = [(dt, ev) for dt, ev in bucket if dt is not None]
            if len(dated) < 2:
                # Lacking timestamps — still mark as corroborated, conservatively
                for _, ev in bucket:
                    _set_marker(ev, "_corroborated", True)
                continue
            dated.sort(key=lambda t: t[0])  # type: ignore[arg-type]
            # Slide a window over the timestamps, mark all in any window
            corroborated_set: set[int] = set()
            for i in range(len(dated)):
                ts_i, _ = dated[i]
                for j in range(i + 1, len(dated)):
                    ts_j, _ = dated[j]
                    delta = (ts_j - ts_i).total_seconds() / 3600.0
                    if delta > self.window_hours:
                        break
                    if _get_attr(dated[i][1], "source") != _get_attr(dated[j][1], "source"):
                        corroborated_set.add(i)
                        corroborated_set.add(j)
            for idx in corroborated_set:
                _set_marker(dated[idx][1], "_corroborated", True)

        return events, None


@dataclass
class UniverseFilter:
    """Drop events whose symbol isn't in the tradable universe.

    Triggers are already universe-filtered upstream by trigger_collector, but
    we re-apply here for symmetry — the events list could in principle contain
    symbols other than the trigger's symbol if a future enrichment hits a
    related-party event.
    """

    universe: frozenset[str] | None = None

    def apply(self, *, trigger, events):
        if self.universe is None:
            return events, None
        kept = [
            e for e in events
            if (_get_attr(e, "symbol") or "").upper() in self.universe
        ]
        if events and not kept:
            return [], "all_symbols_outside_universe"
        return kept, None


# --------------------------------------------------------------------------- chain


@dataclass
class FilterChain:
    """Compose multiple filters; first non-empty suppress_reason wins."""

    filters: list[NoiseFilter] = field(default_factory=list)

    def apply(self, *, trigger, events):
        first_reason: str | None = None
        current = list(events)
        for f in self.filters:
            kept, reason = f.apply(trigger=trigger, events=current)
            if reason and first_reason is None:
                first_reason = reason
            current = kept
            if not current:
                # Short-circuit: nothing left to filter
                break
        return current, first_reason


# --------------------------------------------------------------------------- builder


_DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "platform" / "config" / "events_filters.json"
)


def load_default_config() -> dict[str, Any]:
    if not _DEFAULT_CONFIG_PATH.exists():
        return {}
    return json.loads(_DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))


def build_default_filter_chain(
    *,
    config: dict[str, Any] | None = None,
    market_cap_provider=None,
    conn_provider=None,
    universe: frozenset[str] | None = None,
    as_of: datetime | None = None,
) -> FilterChain:
    """Construct the default 7-filter chain.

    Pass ``None`` for any of the side-effecting inputs to skip those filters
    silently. The chain stays valid (empty filter list at worst).
    """
    cfg = config or load_default_config()
    filters: list[NoiseFilter] = []

    whitelist = cfg.get("category_whitelist") or []
    if whitelist:
        filters.append(CategoryWhitelistFilter(whitelist=frozenset(whitelist)))

    min_trust = float(cfg.get("min_trust_score", 80.0))
    filters.append(TrustGateFilter(min_trust=min_trust))

    if market_cap_provider is not None:
        mat_cfg = cfg.get("materiality") or {}
        filters.append(
            MaterialityFilter(
                market_cap_provider=market_cap_provider,
                drop_below=str(mat_cfg.get("drop_below", "medium")),
                thresholds=dict(mat_cfg.get("thresholds") or {}),
            )
        )

    td_cfg = cfg.get("time_decay") or {}
    filters.append(
        TimeDecayFilter(
            routine_lookback_days=int(td_cfg.get("routine_lookback_days", 30)),
            extended_lookback_days=int(td_cfg.get("extended_lookback_days", 90)),
            extended_categories=frozenset(td_cfg.get("extended_categories") or ()),
            as_of=as_of,
        )
    )

    dedup_cfg = cfg.get("per_symbol_dedup") or {}
    if dedup_cfg.get("enabled") and conn_provider is not None:
        filters.append(
            PerSymbolDedupFilter(
                conn_provider=conn_provider,
                lookback_days=int(dedup_cfg.get("lookback_days", 7)),
            )
        )

    corr_cfg = cfg.get("corroboration") or {}
    if corr_cfg.get("enabled", True):
        filters.append(
            CorroborationFilter(
                window_hours=int(corr_cfg.get("window_hours", 24)),
            )
        )

    if universe is not None:
        filters.append(UniverseFilter(universe=universe))

    return FilterChain(filters=filters)
