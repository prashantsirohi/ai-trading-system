"""LLM routing and deterministic fallback for market insight reports."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

import requests

from ai_trading_system.domains.events.analyst_brief_builder import build_analyst_brief


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
    synthesis, model_usage = build_market_synthesis(packet, project_root=project_root, report_type=report_type)
    analyst_brief = packet.get("analyst_brief") or build_analyst_brief(packet)
    markdown = render_market_report_markdown(
        synthesis,
        analyst_brief=analyst_brief,
        report_type=report_type,
        model_usage=model_usage,
    )
    return markdown, model_usage


def build_market_synthesis(
    packet: dict[str, Any],
    *,
    project_root: Path,
    report_type: str = "daily",
) -> tuple[dict[str, Any], dict[str, Any]]:
    route_name = "weekly_market_report" if report_type == "weekly" else "daily_market_report"
    route = _load_routes(project_root).get(route_name, DEFAULT_ROUTES[route_name])
    api_key = os.environ.get("OPENROUTER_KEY") or os.environ.get("OPENROUTER_API_KEY")
    analyst_brief = packet.get("analyst_brief") or build_analyst_brief(packet)
    if not api_key:
        return build_deterministic_synthesis(analyst_brief), {
            "status": "skipped_no_api_key",
            "route": route_name,
            "model": route.get("primary"),
            "prompt_tokens": 0,
            "completion_tokens": 0,
        }
    try:
        raw_synthesis, usage = _call_openrouter_json(
            packet,
            analyst_brief=analyst_brief,
            route=route,
            api_key=api_key,
            report_type=report_type,
        )
        synthesis = normalize_synthesis_json(raw_synthesis, analyst_brief)
        validation = validate_synthesis_json(synthesis, analyst_brief)
        if validation["status"] != "passed":
            fallback = build_deterministic_synthesis(analyst_brief)
            return fallback, {
                "status": "validation_fallback",
                "llm_status": "completed",
                "route": route_name,
                **usage,
                "llm_synthesis_raw": raw_synthesis,
                "llm_synthesis_normalized": synthesis,
                "synthesis_validation": validation,
            }
        return synthesis, {
            "status": "completed",
            "route": route_name,
            **usage,
            "llm_synthesis_raw": raw_synthesis,
            "llm_synthesis_normalized": synthesis,
            "synthesis_validation": validation,
        }
    except Exception as exc:
        return build_deterministic_synthesis(analyst_brief), {
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


def _call_openrouter_json(
    packet: dict[str, Any],
    *,
    analyst_brief: dict[str, Any],
    route: dict[str, Any],
    api_key: str,
    report_type: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    model = str(route.get("primary") or DEFAULT_ROUTES["daily_market_report"]["primary"])
    system = (
        "You are producing a trading-operations brief, not news copy. Use only "
        "the supplied analyst reason cards. Classify symbols into actionable "
        "confluence, watchlist only, risk/caution, or ignore/noise. Do not give "
        "price targets or buy/sell instructions. Return structured JSON only."
    )
    llm_packet = _build_llm_packet(packet, analyst_brief=analyst_brief)
    market_intel_status = str((llm_packet.get("market_intel") or {}).get("market_intel_status") or "unknown")
    user = (
        f"Create a {report_type} market synthesis JSON from the supplied analyst_brief. "
        "For each named symbol, explain technical setup, event catalyst, why the "
        "combination matters, invalidation risk, and whether evidence is fresh, stale, "
        "or already priced in. Use event_hash values when making event claims. "
        f"If market_intel_status is missing, stale, or degraded, mention {market_intel_status} in market_read. "
        "Return ONLY valid JSON matching this schema: "
        '{"market_read":"","top_opportunities":[{"symbol":"","setup_type":"","why_it_matters":"","evidence":[],"risk":"","action_bucket":"watch / eligible / avoid"}],"caution_list":[],"sector_rotation":[],"tomorrow_watchlist":[]}.\n\nJSON:\n'
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
    try:
        data = response.json()
    except ValueError as exc:
        raise ValueError(
            "openrouter_response_json_decode_failed: "
            f"status={response.status_code} body_snippet={_snippet(response.text)}"
        ) from exc
    usage = data.get("usage") or {}
    max_tokens = int(route.get("max_output_tokens") or 1800)
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError(
            "openrouter_response_missing_content: "
            f"top_level_keys={sorted(data.keys()) if isinstance(data, dict) else type(data).__name__}"
        ) from exc
    try:
        synthesis = _clean_json_response(str(content))
    except ValueError as exc:
        raise ValueError(
            "openrouter_content_json_decode_failed: "
            f"content_snippet={_snippet(content)}"
        ) from exc
    completion_tokens = int(usage.get("completion_tokens") or 0)
    return synthesis, {
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


def _clean_json_response(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
        if cleaned.startswith("json"):
            cleaned = cleaned[4:].strip()
    loaded = json.loads(cleaned)
    if not isinstance(loaded, dict):
        raise ValueError("LLM synthesis response must be a JSON object")
    return loaded


def _snippet(value: Any, *, limit: int = 240) -> str:
    text = str(value or "").strip().replace("\n", "\\n")
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _enforce_report_contract(text: str, *, packet: dict[str, Any]) -> str:
    market_intel_status = str((packet.get("market_intel") or {}).get("market_intel_status") or "").lower()
    fixed = text.replace("price targets", "forecast levels").replace("price target", "forecast level")
    if market_intel_status in {"missing", "stale", "degraded"} and market_intel_status not in fixed.lower():
        fixed = f"Market intel status: {market_intel_status}\n\n" + fixed.lstrip()
    return fixed if fixed.endswith("\n") else fixed + "\n"


def _build_llm_packet(packet: dict[str, Any], *, analyst_brief: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a compact event-first payload so important sections survive limits."""

    market_intel = packet.get("market_intel") or {}
    rank = packet.get("rank") or {}
    sector_strength = packet.get("sector_strength") or {}
    analyst_brief = analyst_brief or packet.get("analyst_brief") or build_analyst_brief(packet)
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
            "analyst_brief": {
                "market_intel_status": analyst_brief.get("market_intel_status"),
                "market_regime": analyst_brief.get("market_regime") or {},
                "symbol_cards": list(analyst_brief.get("symbol_cards") or [])[:20],
                "sector_reason_cards": list(analyst_brief.get("sector_reason_cards") or [])[:8],
                "score_definitions": analyst_brief.get("score_definitions") or {},
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
        "trust_score": row.get("trust_score"),
        "importance_score": row.get("importance_score"),
        "event_tier": row.get("event_tier") or row.get("tier"),
        "sentiment_label": row.get("sentiment_label"),
        "sentiment_score": row.get("sentiment_score"),
        "risk_flags": row.get("risk_flags"),
        "key_facts": row.get("key_facts"),
        "financials": row.get("financials"),
        "llm_insight": row.get("llm_insight"),
        "source_url": row.get("source_url"),
        "published_at": row.get("published_at"),
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
        "relative_strength": row.get("relative_strength") or row.get("rel_strength") or row.get("rel_strength_score"),
        "volume_intensity": row.get("volume_intensity") or row.get("vol_intensity") or row.get("volume_intensity_normalized"),
        "delivery_pct": row.get("delivery_pct"),
        "sector_strength": row.get("sector_strength") or row.get("sector_strength_score") or row.get("sector_rs_value"),
        "trend_persistence": row.get("trend_persistence") or row.get("bars_in_stage"),
        "proximity_to_highs": row.get("proximity_to_highs") or row.get("prox_high") or row.get("prox_high_score"),
        "near_52w_high_pct": row.get("near_52w_high_pct"),
        "volume_zscore_20": row.get("volume_zscore_20"),
        "breakout_score": row.get("breakout_score"),
        "candidate_tier": row.get("candidate_tier"),
        "pattern_family": row.get("pattern_family"),
        "pattern_score": row.get("pattern_score"),
        "return_5": row.get("return_5"),
        "return_20": row.get("return_20"),
        "stage2_label": row.get("stage2_label"),
        "breakout_state": row.get("breakout_state"),
    }


def _compact_sector(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "Sector": row.get("Sector") or row.get("sector"),
        "RS_rank": row.get("RS_rank") or row.get("RS_rank_pct"),
        "Momentum": row.get("Momentum"),
        "Quadrant": row.get("Quadrant"),
    }


def _compact_pattern(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol_id": row.get("symbol_id") or row.get("symbol"),
        "pattern_family": row.get("pattern_family"),
        "pattern_score": row.get("pattern_score"),
        "pattern_state": row.get("pattern_state"),
        "stage2_label": row.get("stage2_label"),
        "volume_zscore_20": row.get("volume_zscore_20"),
        "rel_strength_score": row.get("rel_strength_score"),
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def validate_synthesis_json(synthesis: dict[str, Any], analyst_brief: dict[str, Any]) -> dict[str, Any]:
    issues: list[dict[str, str]] = []
    allowed_symbols = {str(card.get("symbol") or "").upper() for card in list(analyst_brief.get("symbol_cards") or []) if card.get("symbol")}
    required = ["market_read", "top_opportunities", "caution_list", "sector_rotation", "tomorrow_watchlist"]
    for key in required:
        if key not in synthesis:
            issues.append({"rule": "missing_required_key", "detail": key})
    for key in ["top_opportunities", "caution_list", "sector_rotation", "tomorrow_watchlist"]:
        if key in synthesis and not isinstance(synthesis.get(key), list):
            issues.append({"rule": "section_must_be_list", "detail": key})
    for row in list(synthesis.get("top_opportunities") or []) + list(synthesis.get("caution_list") or []) + list(synthesis.get("tomorrow_watchlist") or []):
        if not isinstance(row, dict):
            issues.append({"rule": "list_item_must_be_object", "detail": "non-object item"})
            continue
        symbol = str(row.get("symbol") or "").upper()
        if symbol and symbol not in allowed_symbols:
            issues.append({"rule": "no_invented_symbols", "detail": symbol})
    blocked = ["must buy", "must sell", "guaranteed buy", "guaranteed sell", "price target"]
    text = json.dumps(synthesis, default=str).lower()
    for phrase in blocked:
        if phrase in text:
            issues.append({"rule": "no_buy_sell_guarantee_language", "detail": phrase})
    return {"status": "passed" if not issues else "failed", "issues": issues}


def normalize_synthesis_json(synthesis: dict[str, Any], analyst_brief: dict[str, Any]) -> dict[str, Any]:
    allowed_symbols = {
        str(card.get("symbol") or "").upper(): card
        for card in list(analyst_brief.get("symbol_cards") or [])
        if card.get("symbol")
    }
    normalized = dict(synthesis) if isinstance(synthesis, dict) else {}
    normalized["market_read"] = str(normalized.get("market_read") or "").strip()
    normalized["top_opportunities"] = _normalize_symbol_section(
        normalized.get("top_opportunities"),
        allowed_symbols=allowed_symbols,
        default_bucket="eligible",
    )
    normalized["caution_list"] = _normalize_symbol_section(
        normalized.get("caution_list"),
        allowed_symbols=allowed_symbols,
        default_bucket="avoid",
    )
    normalized["tomorrow_watchlist"] = _normalize_symbol_section(
        normalized.get("tomorrow_watchlist"),
        allowed_symbols=allowed_symbols,
        default_bucket="watch",
    )
    normalized["sector_rotation"] = _normalize_sector_section(normalized.get("sector_rotation"))
    return normalized


def build_deterministic_synthesis(analyst_brief: dict[str, Any]) -> dict[str, Any]:
    cards = list(analyst_brief.get("symbol_cards") or [])
    actionable = [card for card in cards if card.get("deterministic_bucket") == "actionable_confluence"][:8]
    watch = [card for card in cards if card.get("deterministic_bucket") == "watchlist_only"][:8]
    caution = [card for card in cards if card.get("deterministic_bucket") == "risk/caution"][:8]
    market_status = analyst_brief.get("market_intel_status") or "unknown"
    return {
        "market_read": f"Market intel status: {market_status}. Deterministic synthesis uses prepared technical and event reason cards only.",
        "top_opportunities": [_opportunity_from_card(card, default_bucket="eligible") for card in actionable],
        "caution_list": [_opportunity_from_card(card, default_bucket="avoid") for card in caution],
        "sector_rotation": list(analyst_brief.get("sector_reason_cards") or [])[:6],
        "tomorrow_watchlist": [_opportunity_from_card(card, default_bucket="watch") for card in watch],
    }


def _normalize_symbol_section(
    value: Any,
    *,
    allowed_symbols: dict[str, dict[str, Any]],
    default_bucket: str,
) -> list[dict[str, Any]]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    normalized: list[dict[str, Any]] = []
    for item in items:
        normalized_item = _normalize_symbol_item(
            item,
            allowed_symbols=allowed_symbols,
            default_bucket=default_bucket,
        )
        if normalized_item is not None:
            normalized.append(normalized_item)
    return normalized


def _normalize_symbol_item(
    item: Any,
    *,
    allowed_symbols: dict[str, dict[str, Any]],
    default_bucket: str,
) -> dict[str, Any] | None:
    if isinstance(item, dict):
        data = dict(item)
    elif isinstance(item, str):
        symbol = _extract_symbol(item, allowed_symbols)
        data = {"symbol": symbol or "", "why_it_matters": item}
    else:
        return None
    symbol = _extract_symbol(data.get("symbol") or data.get("ticker") or data.get("name") or data.get("why_it_matters"), allowed_symbols)
    if symbol:
        data["symbol"] = symbol
    evidence = data.get("evidence")
    if isinstance(evidence, str):
        evidence = [evidence]
    elif not isinstance(evidence, list):
        evidence = []
    data["evidence"] = [str(item) for item in evidence if item][:5]
    data["setup_type"] = str(data.get("setup_type") or "technical watch")
    data["why_it_matters"] = str(data.get("why_it_matters") or data.get("summary") or data.get("rationale") or "")
    data["risk"] = str(data.get("risk") or "No specific supplied risk flag.")
    bucket = str(data.get("action_bucket") or default_bucket).strip().lower()
    bucket_map = {"watchlist": "watch", "eligible": "eligible", "avoid": "avoid", "watch": "watch"}
    data["action_bucket"] = bucket_map.get(bucket, default_bucket)
    return {
        "symbol": data.get("symbol") or "",
        "setup_type": data["setup_type"],
        "why_it_matters": data["why_it_matters"],
        "evidence": data["evidence"],
        "risk": data["risk"],
        "action_bucket": data["action_bucket"],
    }


def _normalize_sector_section(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    normalized = []
    for item in items:
        if isinstance(item, dict):
            normalized.append(
                {
                    "sector": item.get("sector") or item.get("Sector") or "",
                    "quadrant": item.get("quadrant") or item.get("Quadrant") or "",
                    "rs_rank": item.get("rs_rank") or item.get("RS_rank"),
                    "event_backed_symbol_count": item.get("event_backed_symbol_count"),
                }
            )
        elif isinstance(item, str):
            normalized.append({"sector": item, "quadrant": "", "rs_rank": None, "event_backed_symbol_count": None})
    return normalized


def _extract_symbol(value: Any, allowed_symbols: dict[str, dict[str, Any]]) -> str | None:
    text = str(value or "").upper()
    if not text:
        return None
    if text in allowed_symbols:
        return text
    for symbol in allowed_symbols:
        if f"{symbol}" in text:
            return symbol
    return None


def _opportunity_from_card(card: dict[str, Any], *, default_bucket: str) -> dict[str, Any]:
    technical = card.get("technical_summary") or {}
    event = card.get("event_summary") or {}
    scores = card.get("scores") or {}
    evidence = []
    if technical.get("composite_score") is not None:
        evidence.append(f"Composite score {technical.get('composite_score')}")
    if event.get("event_hash"):
        evidence.append(f"{event.get('top_event')} [{event.get('event_hash')}]")
    evidence.extend(list(card.get("interpretation_inputs") or [])[:2])
    return {
        "symbol": card.get("symbol"),
        "setup_type": _setup_type(card),
        "why_it_matters": f"Confluence {scores.get('event_confluence_score')} with technical confirmation {scores.get('technical_confirmation_score')}.",
        "evidence": [str(item) for item in evidence if item][:5],
        "risk": "; ".join(str(item) for item in list(card.get("risks") or [])[:2]) or "No specific supplied risk flag.",
        "action_bucket": default_bucket,
    }


def _setup_type(card: dict[str, Any]) -> str:
    technical = card.get("technical_summary") or {}
    event = card.get("event_summary") or {}
    pieces = []
    if technical.get("breakout_state") or technical.get("candidate_tier"):
        pieces.append("breakout")
    if technical.get("pattern_family"):
        pieces.append(str(technical.get("pattern_family")))
    if event.get("event_hash"):
        pieces.append("catalyst confluence")
    return " + ".join(pieces) or "technical watch"


def render_market_report_markdown(
    synthesis: dict[str, Any],
    *,
    analyst_brief: dict[str, Any],
    report_type: str,
    model_usage: dict[str, Any],
) -> str:
    title = "Weekly Market Insight" if report_type == "weekly" else "Daily Market Insight"
    lines = [
        f"# {title}",
        "",
        "## 1. Data Trust & Pipeline Health",
        f"- Trust status: {(analyst_brief.get('data_trust') or {}).get('status', 'unknown')}",
        f"- Market intel status: {analyst_brief.get('market_intel_status', 'unknown')}",
    ]
    if model_usage.get("status") == "skipped_no_api_key":
        lines.append("- LLM synthesis: skipped, deterministic fallback used.")
    elif model_usage.get("status") == "fallback_after_error":
        lines.append("- LLM synthesis: failed, deterministic fallback used.")
    elif model_usage.get("status") == "validation_fallback":
        lines.append("- LLM synthesis: failed validation, deterministic fallback used.")
    else:
        lines.append("- LLM synthesis: completed from structured reason cards.")

    lines.extend(["", "## 2. Market Read", f"- {synthesis.get('market_read') or 'No market read supplied.'}", ""])
    _append_opportunity_section(lines, "## 3. Actionable Confluence", synthesis.get("top_opportunities") or [])
    _append_opportunity_section(lines, "## 4. Tomorrow Watchlist", synthesis.get("tomorrow_watchlist") or [])
    _append_opportunity_section(lines, "## 5. Caution List", synthesis.get("caution_list") or [])
    lines.extend(["", "## 6. Sector Rotation"])
    sectors = list(synthesis.get("sector_rotation") or [])
    if sectors:
        for row in sectors[:8]:
            if isinstance(row, dict):
                sector = row.get("sector") or row.get("Sector") or "unknown"
                quadrant = row.get("quadrant") or row.get("Quadrant") or "n/a"
                lines.append(f"- {sector}: {quadrant}")
            else:
                lines.append(f"- {row}")
    else:
        lines.append("- No sector rotation callout in the supplied evidence.")
    return "\n".join(lines) + "\n"


def _append_opportunity_section(lines: list[str], heading: str, rows: list[Any]) -> None:
    lines.extend([heading])
    if not rows:
        lines.append("- None from supplied evidence.")
        lines.append("")
        return
    for row in rows[:10]:
        if not isinstance(row, dict):
            lines.append(f"- {row}")
            continue
        symbol = row.get("symbol") or "UNKNOWN"
        setup = row.get("setup_type") or "setup"
        bucket = row.get("action_bucket") or "watch"
        lines.append(f"- {symbol}: {setup} ({bucket})")
        if row.get("why_it_matters"):
            lines.append(f"  Evidence read: {row.get('why_it_matters')}")
        evidence = [str(item) for item in list(row.get("evidence") or []) if item]
        if evidence:
            lines.append(f"  Evidence: {'; '.join(evidence[:4])}")
        if row.get("risk"):
            lines.append(f"  Risk: {row.get('risk')}")
    lines.append("")


def build_deterministic_market_report(packet: dict[str, Any], *, report_type: str) -> str:
    analyst_brief = packet.get("analyst_brief") or build_analyst_brief(packet)
    return render_market_report_markdown(
        build_deterministic_synthesis(analyst_brief),
        analyst_brief=analyst_brief,
        report_type=report_type,
        model_usage={"status": "deterministic"},
    )


def _legacy_deterministic_market_report(packet: dict[str, Any], *, report_type: str) -> str:
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
