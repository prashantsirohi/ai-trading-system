"""LLM routing and deterministic fallback for market insight reports."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

import requests


DEFAULT_ROUTES = {
    "daily_market_report": {
        "primary": "deepseek/deepseek-v4-flash",
        "fallback": "qwen/qwen3-235b-a22b-2507",
        "max_output_tokens": 1800,
        "temperature": 0.2,
    },
    "weekly_market_report": {
        "primary": "deepseek/deepseek-v4-flash",
        "fallback": "deepseek/deepseek-v4-pro",
        "max_output_tokens": 4000,
        "temperature": 0.25,
    },
}


def build_market_report(
    packet: dict[str, Any],
    *,
    project_root: Path,
    report_type: str = "daily",
) -> tuple[str, dict[str, Any]]:
    route_name = "weekly_market_report" if report_type == "weekly" else "daily_market_report"
    route = _load_routes(project_root).get(route_name, DEFAULT_ROUTES[route_name])
    api_key = os.environ.get("OPENROUTER_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        return build_deterministic_market_report(packet, report_type=report_type), {
            "status": "skipped_no_api_key",
            "route": route_name,
            "model": route.get("primary"),
            "prompt_tokens": 0,
            "completion_tokens": 0,
        }
    try:
        text, usage = _call_openrouter(packet, route=route, api_key=api_key, report_type=report_type)
        return text, {"status": "completed", "route": route_name, **usage}
    except Exception as exc:
        return build_deterministic_market_report(packet, report_type=report_type), {
            "status": "fallback_after_error",
            "route": route_name,
            "model": route.get("primary"),
            "error": str(exc),
            "prompt_tokens": 0,
            "completion_tokens": 0,
        }


def _load_routes(project_root: Path) -> dict[str, dict[str, Any]]:
    config_path = Path(os.environ.get("LLM_BRAIN_CONFIG") or project_root / "config" / "llm_brain.yaml")
    if not config_path.exists():
        return DEFAULT_ROUTES
    try:
        import yaml  # type: ignore

        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        return dict(payload.get("llm_brain") or DEFAULT_ROUTES)
    except Exception:
        return DEFAULT_ROUTES


def _call_openrouter(
    packet: dict[str, Any],
    *,
    route: dict[str, Any],
    api_key: str,
    report_type: str,
) -> tuple[str, dict[str, Any]]:
    model = str(route.get("primary") or DEFAULT_ROUTES["daily_market_report"]["primary"])
    system = (
        "You are an Indian equities market analyst. Write concise, grounded "
        "market insight from the supplied JSON only. Do not invent symbols, "
        "events, prices, forecast levels, or buy/sell guarantees."
    )
    llm_packet = _build_llm_packet(packet)
    market_intel_status = str((llm_packet.get("market_intel") or {}).get("market_intel_status") or "unknown")
    user = (
        f"Create a {report_type} market insight in markdown. Include data trust, "
        "market regime, corporate event intelligence, event + technical confluence, "
        "tomorrow/next-week watchlist, and caution list. Cite event_hash or raw_event_id "
        "for every event claim. If market_intel_status is missing, stale, or degraded, "
        f"include this exact phrase in the Data Trust section: Market intel status: {market_intel_status}. "
        "Do not use the phrase 'price target' or 'price targets'; say 'forecast levels' if needed. "
        "Keep the report under 900 words. Do not add uncited notable observations.\n\nJSON:\n"
        + json.dumps(llm_packet, default=str, ensure_ascii=False)[:30000]
    )
    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "max_tokens": int(route.get("max_output_tokens") or 1800),
            "temperature": float(route.get("temperature") or 0.2),
        },
        timeout=45,
    )
    response.raise_for_status()
    data = response.json()
    usage = data.get("usage") or {}
    max_tokens = int(route.get("max_output_tokens") or 1800)
    text = _enforce_report_contract(
        _clean_markdown_response(str(data["choices"][0]["message"]["content"])),
        packet=llm_packet,
    )
    completion_tokens = int(usage.get("completion_tokens") or 0)
    return text, {
        "model": model,
        "prompt_tokens": int(usage.get("prompt_tokens") or 0),
        "completion_tokens": completion_tokens,
        "max_output_tokens": max_tokens,
        "possible_truncation": completion_tokens >= max_tokens,
    }


def _clean_markdown_response(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
    return cleaned + ("\n" if cleaned else "")


def _enforce_report_contract(text: str, *, packet: dict[str, Any]) -> str:
    market_intel_status = str((packet.get("market_intel") or {}).get("market_intel_status") or "").lower()
    fixed = text.replace("price targets", "forecast levels").replace("price target", "forecast level")
    if market_intel_status in {"missing", "stale", "degraded"} and market_intel_status not in fixed.lower():
        fixed = f"Market intel status: {market_intel_status}\n\n" + fixed.lstrip()
    return fixed if fixed.endswith("\n") else fixed + "\n"


def _build_llm_packet(packet: dict[str, Any]) -> dict[str, Any]:
    """Return a compact event-first payload so important sections survive limits."""

    market_intel = packet.get("market_intel") or {}
    rank = packet.get("rank") or {}
    sector_strength = packet.get("sector_strength") or {}
    return _json_safe(
        {
            "run_id": packet.get("run_id"),
            "run_date": packet.get("run_date"),
            "report_type": packet.get("report_type"),
            "data_trust": packet.get("data_trust") or {},
            "data_trust_status": packet.get("data_trust_status"),
            "dq_summary": _limit_results(packet.get("dq_summary") or {}, "results", 20),
            "market_intel": {
                "market_intel_status": market_intel.get("market_intel_status"),
                "event_window": market_intel.get("event_window"),
                "event_counts": market_intel.get("event_counts") or {},
                "critical_events": [_compact_event(row) for row in list(market_intel.get("critical_events") or [])[:8]],
                "important_events": [_compact_event(row) for row in list(market_intel.get("important_events") or [])[:12]],
                "portfolio_events": [_compact_event(row) for row in list(market_intel.get("portfolio_events") or [])[:8]],
                "watchlist_events": [_compact_event(row) for row in list(market_intel.get("watchlist_events") or [])[:8]],
                "ranked_stock_events": [_compact_event(row) for row in list(market_intel.get("ranked_stock_events") or [])[:12]],
                "sector_event_clusters": list(market_intel.get("sector_event_clusters") or [])[:10],
                "event_risk_flags": list(market_intel.get("event_risk_flags") or [])[:10],
                "top_events": [_compact_event(row) for row in list(market_intel.get("top_events") or [])[:12]],
            },
            "market_regime": packet.get("market_regime") or {},
            "sector_strength": {"top_sectors": [_compact_sector(row) for row in list(sector_strength.get("top_sectors") or [])[:8]]},
            "rank": {
                "row_count": rank.get("row_count"),
                "top_ranked": [_compact_rank(row) for row in list(rank.get("top_ranked") or [])[:12]],
            },
            "breakouts": {
                "row_count": (packet.get("breakouts") or {}).get("row_count"),
                "candidates": [_compact_rank(row) for row in list((packet.get("breakouts") or {}).get("candidates") or [])[:12]],
            },
            "patterns": {
                "row_count": (packet.get("patterns") or {}).get("row_count"),
                "candidates": [_compact_pattern(row) for row in list((packet.get("patterns") or {}).get("candidates") or [])[:12]],
            },
            "portfolio": packet.get("portfolio") or {},
            "watchlist": packet.get("watchlist") or {},
        }
    )


def _limit_results(payload: dict[str, Any], key: str, limit: int) -> dict[str, Any]:
    out = dict(payload)
    if isinstance(out.get(key), list):
        out[key] = out[key][:limit]
    return out


def _compact_event(row: dict[str, Any]) -> dict[str, Any]:
    metadata = row.get("trigger_metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}
    return {
        "symbol": row.get("symbol"),
        "category": row.get("category"),
        "alert_level": row.get("alert_level"),
        "summary": row.get("summary") or row.get("title"),
        "event_hash": row.get("event_hash"),
        "raw_event_id": row.get("raw_event_id"),
        "event_materiality_score": row.get("event_materiality_score"),
        "rank_position": row.get("rank_position"),
        "side": metadata.get("side"),
        "client_name": metadata.get("client_name"),
        "deal_value_cr": metadata.get("deal_value_cr"),
        "trade_date": metadata.get("trade_date") or row.get("event_date"),
    }


def _compact_rank(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol_id": row.get("symbol_id") or row.get("symbol"),
        "sector_name": row.get("sector_name"),
        "composite_score": row.get("composite_score"),
        "rank_position": row.get("rank_position"),
        "return_5": row.get("return_5"),
        "return_20": row.get("return_20"),
        "stage2_label": row.get("stage2_label"),
        "breakout_state": row.get("breakout_state"),
    }


def _compact_sector(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "Sector": row.get("Sector") or row.get("sector"),
        "RS_rank": row.get("RS_rank"),
        "Momentum": row.get("Momentum"),
        "Quadrant": row.get("Quadrant"),
    }


def _compact_pattern(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol_id": row.get("symbol_id") or row.get("symbol"),
        "pattern_family": row.get("pattern_family"),
        "pattern_score": row.get("pattern_score"),
        "pattern_state": row.get("pattern_state"),
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def build_deterministic_market_report(packet: dict[str, Any], *, report_type: str) -> str:
    market_intel = packet.get("market_intel") or {}
    technical = packet.get("rank") or {}
    trust = packet.get("data_trust") or {}
    top_ranked = list(technical.get("top_ranked") or [])[:10]
    top_events = list(market_intel.get("top_events") or [])[:10]
    confluence = [event for event in top_events if event.get("rank_position") is not None][:8]

    lines = [
        f"# {'Weekly' if report_type == 'weekly' else 'Daily'} Market Insight",
        "",
        "## 1. Data Trust & Pipeline Health",
        f"- Trust status: {trust.get('status') or packet.get('data_trust_status') or 'unknown'}",
        "",
        "## 2. Market Regime",
        f"- Regime: {(packet.get('market_regime') or {}).get('market_regime', 'unknown')}",
        "",
        "## 3. Corporate Event Intelligence",
        f"- Market intel status: {market_intel.get('market_intel_status', 'unknown')}",
    ]
    if top_events:
        for event in top_events[:5]:
            source = event.get("event_hash") or event.get("raw_event_id") or "uncited"
            lines.append(f"- {event.get('symbol')}: {event.get('category')} - {event.get('summary') or event.get('title')} [{source}]")
    else:
        lines.append("- No critical or important event intelligence in the selected window.")
    lines.extend(["", "## 4. Top Ranked Stocks"])
    if top_ranked:
        for idx, row in enumerate(top_ranked[:10], start=1):
            lines.append(f"- {idx}. {row.get('symbol_id') or row.get('symbol')}: score {row.get('composite_score', 'n/a')}")
    else:
        lines.append("- No ranked stock artifact available.")
    lines.extend(["", "## 5. Event + Technical Confluence"])
    if confluence:
        for event in confluence:
            source = event.get("event_hash") or event.get("raw_event_id") or "uncited"
            lines.append(
                f"- {event.get('symbol')}: rank #{event.get('rank_position')} plus "
                f"{event.get('category')} event, materiality {event.get('event_materiality_score')} [{source}]"
            )
    else:
        lines.append("- No event + technical confluence found.")
    lines.extend(["", "## 6. Caution List"])
    risk_flags = list(market_intel.get("event_risk_flags") or [])[:8]
    if risk_flags:
        for flag in risk_flags:
            lines.append(f"- {flag.get('symbol')}: {flag.get('risk_flag')} [{flag.get('event_hash') or 'uncited'}]")
    else:
        lines.append("- No event risk flags in the packet.")
    return "\n".join(lines) + "\n"
