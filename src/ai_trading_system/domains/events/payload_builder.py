"""Format enriched event signals for downstream publish channels.

Two outputs:
  - ``format_telegram_block(signal)`` -> Markdown/HTML snippet a publish
    channel can splice into its main message
  - ``apply_events_overlay(dashboard_payload, enriched_signals)`` -> mutates
    the dashboard JSON to attach an ``events`` array per ranked symbol
    (used by the React execution console's Events tab)

Both are pure (no I/O) and can be unit-tested without touching publishing
infrastructure.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from ai_trading_system.domains.events.enrichment_service import EnrichedSignal


# --------------------------------------------------------------------------- telegram


def _trigger_caption(signal: EnrichedSignal) -> str:
    t = signal.trigger
    if t.trigger_type == "volume_shock":
        z = t.trigger_metadata.get("z_score")
        turnover = t.trigger_metadata.get("turnover_cr")
        if z is not None and turnover is not None:
            return f"Volume z={z:.1f}  (₹{turnover:,.0f}Cr turnover)"
        if z is not None:
            return f"Volume z={z:.1f}"
        return "Volume shocker"
    if t.trigger_type == "bulk_deal":
        client = t.trigger_metadata.get("client_name") or "Institutional"
        side = t.trigger_metadata.get("side", "")
        value = t.trigger_metadata.get("deal_value_cr")
        if value:
            return f"{side.title()} bulk: {client} ₹{value:.0f}Cr"
        return f"{side.title()} bulk: {client}"
    if t.trigger_type == "breakout":
        tier = t.trigger_metadata.get("tier", "")
        score = t.trigger_metadata.get("score")
        if score is not None:
            return f"Tier-{tier} breakout (score {score:.0f})"
        return f"Tier-{tier} breakout"
    return t.trigger_type


def _ago_label(event_dt: Any, *, now: datetime | None = None) -> str:
    if event_dt is None:
        return ""
    if isinstance(event_dt, str):
        try:
            event_dt = datetime.fromisoformat(event_dt)
        except ValueError:
            return ""
    if not isinstance(event_dt, datetime):
        return ""
    if event_dt.tzinfo is None:
        event_dt = event_dt.replace(tzinfo=timezone.utc)
    ref = now or datetime.now(timezone.utc)
    delta_days = max(0, int((ref - event_dt).total_seconds() // 86400))
    if delta_days == 0:
        return "today"
    if delta_days == 1:
        return "1d ago"
    return f"{delta_days}d ago"


def _event_line(event: Any, *, now: datetime | None = None) -> str:
    """One bullet line for an event in the Telegram block."""
    title = _safe(event, "title") or _safe(event, "primary_category", "event")
    category = _safe(event, "primary_category")
    age = _ago_label(_safe(event, "event_date") or _safe(event, "published_at"), now=now)
    parts = [f"• {category or 'event'}: {title.strip()}"] if category else [f"• {title.strip()}"]
    suffix_bits = []
    if age:
        suffix_bits.append(age)
    mat_label = _safe(event, "_materiality_label")
    mat_pct = _safe(event, "_material_pct")
    if mat_label in ("high", "critical") and mat_pct is not None:
        suffix_bits.append(f"MATERIAL: {mat_pct * 100:.1f}% mcap")
    elif mat_label in ("high", "critical"):
        suffix_bits.append(f"MATERIAL: {mat_label}")
    if _safe(event, "_corroborated"):
        suffix_bits.append("NSE+BSE")
    if suffix_bits:
        parts.append(" (" + ", ".join(suffix_bits) + ")")
    return "".join(parts)


def format_telegram_block(
    signal: EnrichedSignal,
    *,
    max_events: int = 4,
    now: datetime | None = None,
) -> str:
    """Render an EnrichedSignal as a multiline Telegram-friendly block.

    Example output:

        🚨 RELIANCE  Volume z=4.2  (₹1,820Cr turnover)
           📰 Corp actions:
           • capex_expansion: ₹15,000Cr Jamnagar expansion (3d ago, MATERIAL: 4.2% mcap)
           • board_meeting: 2026-05-08 capex approval expected (today)
           Trust: ✅ NSE+BSE corroborated
    """
    sym = signal.trigger.symbol
    icon = _severity_icon(signal.severity)
    head = f"{icon} {sym}  {_trigger_caption(signal)}"
    if not signal.events:
        if signal.suppress_reason:
            return f"{head}\n   (suppressed: {signal.suppress_reason})"
        return head

    lines = [head, "   📰 Corp actions:"]
    for ev in signal.events[:max_events]:
        lines.append("   " + _event_line(ev, now=now))
    overflow = len(signal.events) - max_events
    if overflow > 0:
        lines.append(f"   ... +{overflow} more")
    if any(_safe(e, "_corroborated") for e in signal.events):
        lines.append("   Trust: ✅ NSE+BSE corroborated")
    return "\n".join(lines)


def format_telegram_digest(
    signals: Iterable[EnrichedSignal],
    *,
    max_signals: int = 10,
    max_events_per_signal: int = 3,
    now: datetime | None = None,
) -> str:
    """Single Telegram message containing the top-N signals."""
    blocks = []
    for sig in list(signals)[:max_signals]:
        if sig.suppressed:
            continue
        blocks.append(
            format_telegram_block(
                sig, max_events=max_events_per_signal, now=now,
            )
        )
    if not blocks:
        return "No actionable events today."
    return "\n\n".join(blocks)


def _severity_icon(severity: str | None) -> str:
    if severity == "high":
        return "🚨"
    if severity == "medium":
        return "⚡"
    return "ℹ️"


def _safe(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


# --------------------------------------------------------------------------- dashboard


def apply_events_overlay(
    dashboard_payload: dict[str, Any] | None,
    enriched_signals: Iterable[EnrichedSignal],
    *,
    symbol_field: str = "symbol",
) -> dict[str, Any] | None:
    """Mutate ``dashboard_payload`` to attach an ``events`` array per symbol.

    Looks for any list-valued top-level field whose entries are dicts with a
    ``symbol`` key, and attaches a ``events`` list to matching rows. Also
    adds a top-level ``events_index`` summary so the React UI can drive a
    dedicated Events tab.

    No-ops when payload is None / empty.
    """
    if dashboard_payload is None:
        return None

    by_symbol: dict[str, list[dict[str, Any]]] = {}
    summary_rows: list[dict[str, Any]] = []
    for sig in enriched_signals:
        sym = sig.trigger.symbol
        signal_dict = sig.to_dict()
        by_symbol.setdefault(sym, []).append(signal_dict)
        summary_rows.append({
            "symbol": sym,
            "trigger_type": sig.trigger.trigger_type,
            "severity": sig.severity,
            "top_category": sig.top_category,
            "materiality_label": sig.materiality_label,
            "event_count": len(sig.events),
            "suppressed": sig.suppressed,
        })

    # Attach per-symbol events to any list of dicts that has a 'symbol' key
    for value in dashboard_payload.values():
        if not isinstance(value, list):
            continue
        for row in value:
            if not isinstance(row, dict):
                continue
            sym = row.get(symbol_field) or row.get("ticker")
            if sym and sym in by_symbol:
                row["events"] = list(by_symbol[sym])

    dashboard_payload["events_index"] = summary_rows
    return dashboard_payload


# --------------------------------------------------------------------------- weekly PDF


def build_events_of_the_week_section(
    enriched_signals: Iterable[EnrichedSignal],
    *,
    top_n: int = 10,
) -> dict[str, Any]:
    """Produce a structured payload for the weekly-PDF "Events of the Week"
    block. The PDF builder consumes this dict; layout is its concern.

    Output shape:
      {
        "headline_count": int,
        "by_severity": {"high": N, "medium": N, "low-info": N},
        "top_signals": [
          {
            "symbol": "RELIANCE",
            "trigger_type": "volume_shock",
            "severity": "high",
            "top_category": "capex_expansion",
            "headline": "Capex announcement: ₹15,000Cr Jamnagar expansion",
            "event_count": 3,
            "materiality_label": "high",
          }, ...
        ],
        "sector_heatmap": {"sector_or_unknown": <count>},
      }
    """
    signals = [s for s in enriched_signals if not s.suppressed]
    by_severity: dict[str, int] = {}
    sector_count: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    for sig in signals:
        if sig.severity:
            by_severity[sig.severity] = by_severity.get(sig.severity, 0) + 1
        sector = "unknown"
        for ev in sig.events:
            sector = _safe(ev, "sector") or sector
            if sector != "unknown":
                break
        sector_count[sector] = sector_count.get(sector, 0) + 1
        headline = ""
        if sig.events:
            headline = _safe(sig.events[0], "title") or sig.top_category or ""
        rows.append({
            "symbol": sig.trigger.symbol,
            "trigger_type": sig.trigger.trigger_type,
            "severity": sig.severity,
            "top_category": sig.top_category,
            "headline": headline,
            "event_count": len(sig.events),
            "materiality_label": sig.materiality_label,
        })

    # Rank by (severity, event_count) descending
    severity_rank = {"high": 3, "medium": 2, "low-info": 1, None: 0}
    rows.sort(
        key=lambda r: (severity_rank.get(r["severity"], 0), r["event_count"]),
        reverse=True,
    )
    return {
        "headline_count": len(signals),
        "by_severity": by_severity,
        "top_signals": rows[:top_n],
        "sector_heatmap": sector_count,
    }
