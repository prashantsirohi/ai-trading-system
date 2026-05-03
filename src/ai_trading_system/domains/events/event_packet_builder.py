"""Build compact event packets for the insight stage."""

from __future__ import annotations

import json
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.events.event_materiality import score_event_materiality
from ai_trading_system.integrations.market_intel_client import resolve_db_path
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext


IGNORE_CATEGORIES = {
    "nav_update",
    "newspaper_publication",
    "investor_meet",
    "agm_notice",
    "compliance_certificate",
    "loss_of_certificate",
    "analyst_call",
}


def build_event_packet(
    context: StageContext,
    *,
    ranked_df: pd.DataFrame,
    portfolio_symbols: set[str] | None = None,
    watchlist_symbols: set[str] | None = None,
) -> tuple[dict[str, Any], pd.DataFrame]:
    """Return the event packet and confluence DataFrame for the insight brain."""
    portfolio_symbols = {s.upper() for s in (portfolio_symbols or set())}
    watchlist_symbols = {s.upper() for s in (watchlist_symbols or set())}
    rank_positions = _rank_positions(ranked_df)
    held_sectors = _held_sectors(ranked_df, portfolio_symbols)

    snapshot = _read_json_artifact(context.artifact_for("events", "market_events_snapshot"))
    enrichment = _read_json_artifact(context.artifact_for("events", "events_enrichment"))
    events = _dedupe_events(_snapshot_events(snapshot), _signal_events(enrichment))
    insights = _load_llm_insights(events)

    rows: list[dict[str, Any]] = []
    ignored_count = 0
    for event in events:
        category = str(event.get("category") or event.get("primary_category") or "")
        if category in IGNORE_CATEGORIES:
            ignored_count += 1
            continue
        normalized = _normalize_event(event)
        normalized["llm_insight"] = insights.get(int(normalized["raw_event_id"])) if normalized.get("raw_event_id") else None
        normalized.update(
            score_event_materiality(
                normalized,
                rank_positions=rank_positions,
                portfolio_symbols=portfolio_symbols,
                watchlist_symbols=watchlist_symbols,
                held_sectors=held_sectors,
            )
        )
        normalized["rank_position"] = rank_positions.get(str(normalized.get("symbol") or "").upper())
        normalized["portfolio_match"] = str(normalized.get("symbol") or "").upper() in portfolio_symbols
        normalized["watchlist_match"] = str(normalized.get("symbol") or "").upper() in watchlist_symbols
        rows.append(normalized)

    rows.sort(
        key=lambda row: (
            float(row.get("event_materiality_score") or 0.0),
            float(row.get("importance_score") or 0.0),
        ),
        reverse=True,
    )
    confluence = pd.DataFrame(rows)
    counts = _counts(rows, ignored_count=ignored_count)
    event_window = _event_window(context)
    packet = {
        "market_intel_status": snapshot.get("market_intel_status") or "unknown",
        "event_window": event_window,
        "event_counts": counts,
        "critical_events": [r for r in rows if str(r.get("alert_level")) == "critical"][:10],
        "important_events": [r for r in rows if str(r.get("alert_level")) == "important"][:15],
        "portfolio_events": [r for r in rows if r.get("portfolio_match")][:15],
        "watchlist_events": [r for r in rows if r.get("watchlist_match")][:15],
        "ranked_stock_events": [r for r in rows if r.get("rank_position") is not None][:25],
        "sector_event_clusters": _sector_clusters(rows),
        "event_risk_flags": _risk_flags(rows),
        "top_events": rows[:25],
    }
    return packet, confluence


def _read_json_artifact(artifact: StageArtifact | None) -> dict[str, Any]:
    if artifact is None:
        return {}
    try:
        return json.loads(Path(artifact.uri).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _snapshot_events(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in list(snapshot.get("events") or []) if isinstance(row, dict)]


def _signal_events(enrichment: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for signal in list(enrichment.get("signals") or []):
        if not isinstance(signal, dict):
            continue
        trigger = signal.get("trigger") or {}
        signal_events = list(signal.get("events") or [])
        if not signal_events and trigger:
            out.append(_event_from_trigger(trigger, signal))
            continue
        for event in signal_events:
            if isinstance(event, dict):
                row = dict(event)
                row.setdefault("symbol", trigger.get("symbol"))
                row.setdefault("trigger_type", trigger.get("trigger_type"))
                row.setdefault("signal_severity", signal.get("severity"))
                out.append(row)
    return out


def _event_from_trigger(trigger: dict[str, Any], signal: dict[str, Any]) -> dict[str, Any]:
    trigger_type = str(trigger.get("trigger_type") or "event")
    metadata = dict(trigger.get("trigger_metadata") or {})
    symbol = str(trigger.get("symbol") or "").upper()
    trade_date = metadata.get("trade_date") or trigger.get("as_of_date")
    event_hash = _trigger_event_hash(trigger_type=trigger_type, symbol=symbol, metadata=metadata, trade_date=trade_date)
    category = "bulk_deal" if trigger_type == "bulk_deal" else trigger_type
    title = _trigger_title(trigger_type=trigger_type, symbol=symbol, metadata=metadata)
    return {
        "symbol": symbol,
        "category": category,
        "primary_category": category,
        "tier": "B" if trigger_type == "bulk_deal" else "GENERAL",
        "event_tier": "B" if trigger_type == "bulk_deal" else "GENERAL",
        "alert_level": "important" if trigger_type == "bulk_deal" else "info",
        "importance_score": _trigger_importance(trigger_type=trigger_type, metadata=metadata, strength=trigger.get("trigger_strength")),
        "trust_score": 80.0,
        "novelty_score": 50.0,
        "event_date": trade_date,
        "published_at": trade_date,
        "event_hash": event_hash,
        "source_url": metadata.get("source_url"),
        "title": title,
        "summary": title,
        "trigger_type": trigger_type,
        "trigger_metadata": metadata,
        "signal_severity": signal.get("severity"),
        "synthetic_source": "events_trigger",
        "raw_event_id": None,
        "resolved_event_id": None,
    }


def _trigger_event_hash(*, trigger_type: str, symbol: str, metadata: dict[str, Any], trade_date: Any) -> str:
    if metadata.get("deal_hash"):
        return str(metadata["deal_hash"])
    key = "|".join(
        str(part)
        for part in [
            trigger_type,
            symbol,
            trade_date or "",
            metadata.get("client_name") or "",
            metadata.get("side") or "",
            metadata.get("quantity") or "",
            metadata.get("deal_value_cr") or "",
        ]
    )
    return "trigger:" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]


def _trigger_title(*, trigger_type: str, symbol: str, metadata: dict[str, Any]) -> str:
    if trigger_type == "bulk_deal":
        side = str(metadata.get("side") or "bulk").upper()
        client = metadata.get("client_name") or "unknown client"
        value = metadata.get("deal_value_cr")
        date = metadata.get("trade_date") or "unknown date"
        value_text = f" worth {float(value):.1f} Cr" if _is_number(value) else ""
        return f"{side} bulk deal by {client}{value_text} on {date}"
    return f"{trigger_type} trigger for {symbol}"


def _trigger_importance(*, trigger_type: str, metadata: dict[str, Any], strength: Any) -> float:
    if trigger_type == "bulk_deal":
        value = metadata.get("deal_value_cr")
        if _is_number(value):
            return min(10.0, max(6.0, float(value) / 20.0))
        return 7.0
    if _is_number(strength):
        return min(10.0, max(3.0, float(strength) * 10.0))
    return 5.0


def _dedupe_events(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for group in groups:
        for event in group:
            key = str(event.get("event_hash") or event.get("raw_event_id") or event.get("resolved_event_id") or json.dumps(event, sort_keys=True, default=str))
            if key in seen:
                continue
            seen.add(key)
            out.append(event)
    return out


def _normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    category = event.get("category") or event.get("primary_category")
    return {
        "raw_event_id": _maybe_int(event.get("raw_event_id")),
        "resolved_event_id": _maybe_int(event.get("resolved_event_id")),
        "symbol": str(event.get("symbol") or event.get("symbol_id") or "").upper(),
        "company_name": event.get("company_name"),
        "title": event.get("title"),
        "summary": event.get("summary") or event.get("one_line_summary") or event.get("description"),
        "category": category,
        "tier": event.get("tier") or event.get("event_tier"),
        "alert_level": event.get("alert_level"),
        "importance_score": event.get("importance_score"),
        "trust_score": event.get("trust_score"),
        "novelty_score": event.get("novelty_score"),
        "event_date": event.get("event_date"),
        "published_at": event.get("published_at"),
        "event_hash": event.get("event_hash"),
        "source_url": event.get("source_url") or event.get("link") or event.get("attachment_url"),
        "sector": event.get("sector"),
        "risk_flags": event.get("risk_flags") or event.get("risk_flags_json") or [],
        "trigger_type": event.get("trigger_type"),
        "trigger_metadata": event.get("trigger_metadata") or {},
        "synthetic_source": event.get("synthetic_source"),
    }


def _rank_positions(ranked_df: pd.DataFrame) -> dict[str, int]:
    if ranked_df is None or ranked_df.empty:
        return {}
    symbol_col = "symbol_id" if "symbol_id" in ranked_df.columns else "symbol"
    if symbol_col not in ranked_df.columns:
        return {}
    return {
        str(symbol).upper(): idx
        for idx, symbol in enumerate(ranked_df[symbol_col].fillna("").astype(str).tolist(), start=1)
        if symbol
    }


def _held_sectors(ranked_df: pd.DataFrame, portfolio_symbols: set[str]) -> set[str]:
    if not portfolio_symbols or ranked_df is None or ranked_df.empty:
        return set()
    symbol_col = "symbol_id" if "symbol_id" in ranked_df.columns else "symbol"
    sector_col = "sector_name" if "sector_name" in ranked_df.columns else "sector"
    if symbol_col not in ranked_df.columns or sector_col not in ranked_df.columns:
        return set()
    out = set()
    for row in ranked_df.to_dict(orient="records"):
        if str(row.get(symbol_col) or "").upper() in portfolio_symbols and row.get(sector_col):
            out.add(str(row[sector_col]))
    return out


def _load_llm_insights(events: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    ids = sorted({_maybe_int(e.get("raw_event_id")) for e in events if _maybe_int(e.get("raw_event_id")) is not None})
    if not ids:
        return {}
    db_path = Path(resolve_db_path())
    if not db_path.exists():
        return {}
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT raw_event_id, model_used, provider, prompt_tokens, completion_tokens, insight_json
                FROM llm_insight
                WHERE raw_event_id IN (SELECT UNNEST(?))
                """,
                [ids],
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return {}
    out = {}
    for raw_event_id, model, provider, prompt_tokens, completion_tokens, insight_json in rows:
        try:
            payload = json.loads(insight_json) if insight_json else {}
        except (TypeError, ValueError):
            payload = {}
        payload.update(
            {
                "model_used": model,
                "provider": provider,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
            }
        )
        out[int(raw_event_id)] = payload
    return out


def _counts(rows: list[dict[str, Any]], *, ignored_count: int) -> dict[str, int]:
    return {
        "critical": sum(1 for row in rows if str(row.get("alert_level")) == "critical"),
        "important": sum(1 for row in rows if str(row.get("alert_level")) == "important"),
        "info": sum(1 for row in rows if str(row.get("alert_level") or "info") not in {"critical", "important"}),
        "ignored": int(ignored_count),
    }


def _sector_clusters(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        sector = str(row.get("sector") or "unknown")
        counts[sector] = counts.get(sector, 0) + 1
    return [{"sector": key, "event_count": value} for key, value in sorted(counts.items(), key=lambda item: item[1], reverse=True)]


def _risk_flags(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        insight = row.get("llm_insight") or {}
        flags = insight.get("risk_flags") or row.get("risk_flags") or []
        if isinstance(flags, str):
            flags = [flags]
        for flag in flags:
            out.append({"symbol": row.get("symbol"), "event_hash": row.get("event_hash"), "risk_flag": str(flag)})
    return out[:25]


def _event_window(context: StageContext) -> dict[str, str]:
    try:
        base = datetime.fromisoformat(str(context.run_date)).replace(tzinfo=timezone.utc)
    except ValueError:
        base = datetime.now(timezone.utc)
    return {
        "from": base.isoformat(),
        "to": (base + timedelta(days=1) - timedelta(seconds=1)).isoformat(),
    }


def _maybe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_number(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False
