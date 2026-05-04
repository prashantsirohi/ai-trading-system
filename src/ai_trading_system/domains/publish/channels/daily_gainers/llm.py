"""Batched OpenRouter insight generation for the daily gainers report."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from ai_trading_system.domains.events.event_llm_router import DEFAULT_ROUTES, _load_routes
from ai_trading_system.domains.publish.channels.daily_gainers.events import EventRow

FALLBACK_SUMMARY = "LLM unavailable - see table below."


def generate_insight(
    gainers_df: pd.DataFrame,
    events_by_symbol: dict[str, list[EventRow]],
    model: str | None = None,
) -> dict[str, Any]:
    """Generate one batched LLM summary, falling back deterministically."""

    api_key = os.environ.get("OPENROUTER_KEY") or os.environ.get("OPENROUTER_API_KEY")
    resolved_model = model or _default_model()
    if not api_key:
        return _fallback("skipped_no_api_key", resolved_model)

    payload = _build_payload(gainers_df, events_by_symbol)
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": resolved_model,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are a terse Indian equities market analyst. "
                            "Use only the supplied JSON. Do not invent news, "
                            "forecasts, recommendations, or event explanations."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            "Return valid JSON only with this exact shape: "
                            '{"summary_md":"...","per_stock":{"SYM":"one-line takeaway"}}. '
                            "Keep summary_md under 220 words and each per-stock line under 28 words. "
                            "Mention corporate events only when present in the JSON.\n\n"
                            + json.dumps(payload, default=str, ensure_ascii=False)
                        ),
                    },
                ],
                "max_tokens": 1800,
                "temperature": 0.2,
            },
            timeout=45,
        )
        response.raise_for_status()
        content = str(response.json()["choices"][0]["message"]["content"])
        parsed = _parse_response(content)
        parsed.update({"status": "completed", "model": resolved_model})
        return parsed
    except Exception as exc:  # noqa: BLE001 - network and vendor payload failures share fallback
        return _fallback("fallback_after_error", resolved_model, str(exc))


def _default_model() -> str:
    route = _load_routes(_project_root()).get("daily_market_report", DEFAULT_ROUTES["daily_market_report"])
    return str(route.get("primary") or DEFAULT_ROUTES["daily_market_report"]["primary"])


def _project_root() -> Path:
    return Path(__file__).resolve().parents[6]


def _build_payload(
    gainers_df: pd.DataFrame,
    events_by_symbol: dict[str, list[EventRow]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in gainers_df.to_dict("records"):
        symbol = str(row.get("symbol_id") or "")
        rows.append(
            {
                "symbol": symbol,
                "pct_change": _json_safe(row.get("pct_change")),
                "close": _json_safe(row.get("close")),
                "volume": _json_safe(row.get("volume")),
                "events": [
                    {
                        "cat": event.category,
                        "summary": event.summary,
                        "date": event.event_date,
                        "importance": event.importance_score,
                    }
                    for event in events_by_symbol.get(symbol, [])
                ],
            }
        )
    return rows


def _parse_response(content: str) -> dict[str, Any]:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
    if not cleaned.startswith("{"):
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            cleaned = cleaned[start : end + 1]

    parsed = json.loads(cleaned)
    summary_md = parsed.get("summary_md")
    per_stock = parsed.get("per_stock")
    if not isinstance(summary_md, str) or not isinstance(per_stock, dict):
        raise ValueError("LLM response does not match daily gainers JSON contract")
    if not all(isinstance(key, str) and isinstance(value, str) for key, value in per_stock.items()):
        raise ValueError("LLM per_stock entries must be strings")
    return {"summary_md": summary_md, "per_stock": per_stock}


def _fallback(status: str, model: str | None, error: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"summary_md": FALLBACK_SUMMARY, "per_stock": {}, "status": status, "model": model}
    if error:
        out["error"] = error
    return out


def _json_safe(value: Any) -> Any:
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value

