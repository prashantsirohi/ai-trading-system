"""LLM catalyst enrichment for watchlist candidates.

The LLM layer only annotates fundamental/news context. It never receives raw
OHLCV and never decides the ranking order.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import requests


CATALYST_COLUMNS = ["catalyst_tags", "catalyst_confidence", "bull_case", "risk_flags", "watchlist_reason"]
CONFIDENCE_VALUES = {"HIGH", "MEDIUM", "LOW"}


class CatalystClient(Protocol):
    def complete_json(self, payload: dict[str, Any]) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class CatalystRecord:
    symbol: str
    catalyst_tags: list[str]
    catalyst_confidence: str
    bull_case: str
    risk_flags: list[str]
    watchlist_reason: str
    status: str = "completed"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class OpenRouterCatalystClient:
    def __init__(self, *, route: dict[str, Any], api_key: str):
        self.route = route
        self.api_key = api_key

    def complete_json(self, payload: dict[str, Any]) -> dict[str, Any]:
        model = str(self.route.get("primary") or "deepseek/deepseek-v4-flash")
        system = (
            "You annotate watchlist candidates with fundamental and market-intel catalysts. "
            "Use only supplied events and company context. Do not rank, score, or give buy/sell advice. "
            "Return JSON only."
        )
        user = (
            "Return a JSON object matching this exact schema: "
            '{"catalyst_tags":[],"catalyst_confidence":"HIGH|MEDIUM|LOW","bull_case":"","risk_flags":[],"watchlist_reason":""}. '
            "catalyst_tags and risk_flags must be short strings. If no meaningful fundamental catalyst exists, "
            "use LOW confidence, empty catalyst_tags, and explain that the reason remains technical.\n\n"
            + json.dumps(payload, ensure_ascii=False, default=str)
        )
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "max_tokens": int(self.route.get("max_output_tokens") or 700),
                "temperature": float(self.route.get("temperature") or 0.0),
            },
            timeout=int(self.route.get("timeout_seconds") or 45),
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return _clean_json_response(str(content))


def build_default_client(*, project_root: Path) -> CatalystClient | None:
    api_key = os.environ.get("OPENROUTER_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        return None
    route = _load_route(project_root)
    return OpenRouterCatalystClient(route=route, api_key=api_key)


def enrich_with_catalyst(
    prefilter_df: pd.DataFrame,
    *,
    market_intel: dict[str, Any] | list[dict[str, Any]] | None,
    llm_client: CatalystClient | None,
    run_date: str,
    cache_dir: Path | None = None,
) -> dict[str, Any]:
    if prefilter_df is None or prefilter_df.empty or llm_client is None:
        return {}
    events_by_symbol = _events_by_symbol(market_intel, run_date=run_date)
    cache_dir = cache_dir or Path(".watchlist_catalyst_cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    output: dict[str, Any] = {}
    for _, row in prefilter_df.iterrows():
        symbol = str(row.get("symbol_id") or "").upper()
        if not symbol:
            continue
        cache_path = cache_dir / f"{run_date}_{_safe_symbol(symbol)}.json"
        cached = _read_cache(cache_path)
        if cached:
            output[symbol] = cached
            continue
        try:
            payload = _build_symbol_context(row, events=events_by_symbol.get(symbol, []))
            record = _normalize_response(symbol, llm_client.complete_json(payload), fallback_reason=str(row.get("technical_catalyst_summary") or ""))
        except Exception as exc:
            record = CatalystRecord(
                symbol=symbol,
                catalyst_tags=[],
                catalyst_confidence="",
                bull_case="",
                risk_flags=[],
                watchlist_reason=str(row.get("technical_catalyst_summary") or ""),
                status=f"fallback_after_error: {exc}",
            )
        output[symbol] = record.to_dict()
        cache_path.write_text(json.dumps(output[symbol], indent=2, sort_keys=True, default=str), encoding="utf-8")
    return output


def _build_symbol_context(row: pd.Series, *, events: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "symbol": str(row.get("symbol_id") or ""),
        "company_name": str(row.get("company_name") or row.get("name") or ""),
        "sector": str(row.get("sector") or ""),
        "technical_context": {
            "stage": str(row.get("stage") or ""),
            "momentum_tags": [tag.strip() for tag in str(row.get("momentum_tags") or "").split(",") if tag.strip()],
            "breakout_type": str(row.get("setup_label") or ""),
            "technical_catalyst_summary": str(row.get("technical_catalyst_summary") or ""),
        },
        "market_intel": [_compact_event(event) for event in events[:8]],
    }


def _compact_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_hash": event.get("event_hash"),
        "event_date": event.get("event_date") or event.get("published_at"),
        "category": event.get("top_category") or event.get("category") or event.get("primary_category"),
        "materiality_label": event.get("materiality_label") or event.get("alert_level"),
        "severity": event.get("severity") or event.get("signal_severity"),
        "title": event.get("title"),
        "summary": event.get("summary"),
    }


def _events_by_symbol(market_intel: dict[str, Any] | list[dict[str, Any]] | None, *, run_date: str) -> dict[str, list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    if isinstance(market_intel, dict):
        for key in ("top_events", "ranked_stock_events", "watchlist_events", "signals", "events"):
            for item in list(market_intel.get(key) or []):
                if isinstance(item, dict):
                    if key == "signals":
                        events.extend(_events_from_signal(item))
                    else:
                        events.append(item)
    elif isinstance(market_intel, list):
        events = [dict(item) for item in market_intel if isinstance(item, dict)]

    cutoff = pd.Timestamp(run_date) - timedelta(days=30)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        symbol = str(event.get("symbol") or (event.get("trigger") or {}).get("symbol") or "").upper()
        if not symbol:
            continue
        event_date = pd.to_datetime(event.get("event_date") or event.get("published_at") or event.get("as_of_date"), errors="coerce")
        if pd.notna(event_date) and event_date.tz_localize(None) < cutoff:
            continue
        grouped.setdefault(symbol, []).append(event)
    return grouped


def _events_from_signal(signal: dict[str, Any]) -> list[dict[str, Any]]:
    trigger = signal.get("trigger") or {}
    symbol = trigger.get("symbol")
    rows: list[dict[str, Any]] = []
    for event in list(signal.get("events") or []):
        if isinstance(event, dict):
            row = dict(event)
            row.setdefault("symbol", symbol)
            row.setdefault("materiality_label", signal.get("materiality_label"))
            row.setdefault("top_category", signal.get("top_category"))
            row.setdefault("severity", signal.get("severity"))
            rows.append(row)
    if not rows and symbol:
        rows.append(
            {
                "symbol": symbol,
                "event_date": trigger.get("as_of_date"),
                "category": trigger.get("trigger_type"),
                "materiality_label": signal.get("materiality_label"),
                "top_category": signal.get("top_category"),
                "severity": signal.get("severity"),
                "summary": trigger.get("trigger_type"),
            }
        )
    return rows


def _normalize_response(symbol: str, payload: dict[str, Any], *, fallback_reason: str) -> CatalystRecord:
    confidence = str(payload.get("catalyst_confidence") or "").upper()
    if confidence not in CONFIDENCE_VALUES:
        confidence = "LOW" if payload.get("watchlist_reason") or payload.get("bull_case") else ""
    return CatalystRecord(
        symbol=symbol,
        catalyst_tags=[str(item)[:40] for item in list(payload.get("catalyst_tags") or [])[:6]],
        catalyst_confidence=confidence,
        bull_case=str(payload.get("bull_case") or "")[:500],
        risk_flags=[str(item)[:80] for item in list(payload.get("risk_flags") or [])[:6]],
        watchlist_reason=str(payload.get("watchlist_reason") or fallback_reason)[:500],
    )


def _read_cache(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _safe_symbol(symbol: str) -> str:
    return "".join(ch for ch in symbol if ch.isalnum() or ch in {"_", "-"}).upper()


def _load_route(project_root: Path) -> dict[str, Any]:
    config_path = Path(os.environ.get("LLM_BRAIN_CONFIG") or project_root / "config" / "llm_brain.yaml")
    if config_path.exists():
        try:
            import yaml  # type: ignore

            payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            routes = dict(payload.get("llm_brain") or {})
            if "watchlist_catalyst" in routes:
                return dict(routes["watchlist_catalyst"])
            if "event_classification_recheck" in routes:
                return dict(routes["event_classification_recheck"])
        except Exception:
            pass
    return {"primary": "qwen/qwen3-235b-a22b-2507", "fallback": "deepseek/deepseek-v4-flash", "max_output_tokens": 700, "temperature": 0.0}


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
        raise ValueError("watchlist catalyst response must be a JSON object")
    return loaded
