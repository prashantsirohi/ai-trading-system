"""LLM narrative synthesis stage.

Consumes the deterministic insight packet built by ``InsightStage`` and runs
the market-report LLM with validation + deterministic fallback. All
LLM-shaped artifacts (markdown report, llm_synthesis, model_usage,
validation_report, telegram_summary) live here so an LLM provider outage
cannot block the deterministic insight stage upstream.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ai_trading_system.domains.events.event_llm_router import (
    build_deterministic_synthesis,
    build_market_synthesis,
    render_market_report_markdown,
)
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class NarrativeStage:
    """LLM-driven daily/weekly report synthesis."""

    name = "narrative"

    def run(self, context: StageContext) -> StageResult:
        report_type = str(context.params.get("insight_report_type") or "daily").lower()
        combined_packet = _read_json_artifact(context.require_artifact("insight", "combined_insight_packet"))
        analyst_brief = _read_json_artifact(context.require_artifact("insight", "analyst_brief"))

        llm_synthesis, model_usage = build_market_synthesis(
            combined_packet,
            project_root=context.project_root,
            report_type=report_type,
        )
        report_markdown = render_market_report_markdown(
            llm_synthesis,
            analyst_brief=analyst_brief,
            report_type=report_type,
            model_usage=model_usage,
        )
        validation = validate_report(report_markdown, combined_packet, model_usage=model_usage)
        if validation["status"] != "passed":
            original_usage = dict(model_usage)
            llm_synthesis = build_deterministic_synthesis(analyst_brief)
            fallback_usage = {
                **original_usage,
                "status": "validation_fallback",
                "llm_status": original_usage.get("status"),
                "llm_possible_truncation": bool(original_usage.get("possible_truncation")),
                "possible_truncation": False,
                "validation_fallback_issues": validation.get("issues") or [],
            }
            fallback_markdown = render_market_report_markdown(
                llm_synthesis,
                analyst_brief=analyst_brief,
                report_type=report_type,
                model_usage=fallback_usage,
            )
            fallback_validation = validate_report(fallback_markdown, combined_packet, model_usage=fallback_usage)
            if fallback_validation["status"] == "passed":
                report_markdown = fallback_markdown
                model_usage = fallback_usage
                validation = fallback_validation

        telegram_summary = _build_telegram_summary(report_markdown, combined_packet)

        llm_synthesis_path = context.write_json("llm_synthesis.json", llm_synthesis)
        llm_synthesis_raw_path = context.write_json(
            "llm_synthesis_raw.json",
            model_usage.get("llm_synthesis_raw") or llm_synthesis,
        )
        daily_json_path = context.write_json(
            f"{report_type}_insight.json",
            {
                "run_id": context.run_id,
                "run_date": context.run_date,
                "report_type": report_type,
                "status": validation["status"],
                "report_markdown": report_markdown,
                "market_intel": combined_packet.get("market_intel"),
                "analyst_brief": analyst_brief,
                "llm_synthesis": llm_synthesis,
                "model_usage": model_usage,
                "validation": validation,
            },
        )
        model_usage_path = context.write_json("model_usage.json", model_usage)
        validation_path = context.write_json("validation_report.json", validation)

        out_dir = context.output_dir()
        markdown_path = out_dir / f"{report_type}_insight.md"
        markdown_path.write_text(report_markdown, encoding="utf-8")
        telegram_path = out_dir / "telegram_summary.txt"
        telegram_path.write_text(telegram_summary, encoding="utf-8")

        artifacts = [
            StageArtifact.from_file("llm_synthesis", llm_synthesis_path),
            StageArtifact.from_file("llm_synthesis_raw", llm_synthesis_raw_path),
            StageArtifact.from_file(f"{report_type}_insight_json", daily_json_path),
            StageArtifact.from_file(f"{report_type}_insight_markdown", markdown_path),
            StageArtifact.from_file("telegram_summary", telegram_path),
            StageArtifact.from_file("model_usage", model_usage_path),
            StageArtifact.from_file("validation_report", validation_path),
        ]
        return StageResult(
            artifacts=artifacts,
            metadata={
                "report_type": report_type,
                "validation_status": validation["status"],
                "model_status": model_usage.get("status"),
            },
        )


def validate_report(markdown: str, packet: dict[str, Any], *, model_usage: dict[str, Any] | None = None) -> dict[str, Any]:
    allowed_symbols = _allowed_symbols(packet)
    event_sources = _event_sources(packet)
    issues: list[dict[str, str]] = []
    lower = markdown.lower()
    model_usage = model_usage or {}
    stripped = markdown.strip()
    if stripped.startswith("```") or stripped.endswith("```") or stripped.count("```") % 2 == 1:
        issues.append({"rule": "no_markdown_fence_wrappers", "detail": "report contains raw code fence markers"})
    if bool(model_usage.get("possible_truncation")):
        issues.append({"rule": "llm_output_may_be_truncated", "detail": "completion_tokens reached max_output_tokens"})
    if stripped and not stripped.endswith((".", "।", "!", "?", ")", "]")) and bool(model_usage.get("possible_truncation")):
        issues.append({"rule": "report_ends_mid_sentence", "detail": stripped[-160:]})
    market_intel_status = str((packet.get("market_intel") or {}).get("market_intel_status") or "").lower()
    if market_intel_status in {"missing", "stale", "degraded"} and market_intel_status not in lower:
        issues.append({"rule": "market_intel_status_must_show_warning", "detail": market_intel_status})
    blocked_phrases = [
        "guaranteed buy",
        "guaranteed sell",
        "sure-shot",
        "sure shot",
        "will definitely",
        "must buy",
        "must sell",
        "price target",
    ]
    for phrase in blocked_phrases:
        if phrase == "price target" and (
            "no price target" in lower
            or "no forward price target" in lower
            or "without price target" in lower
        ):
            continue
        if phrase in lower:
            issues.append({"rule": "no_buy_sell_guarantee_language", "detail": phrase})

    for token in sorted(set(re.findall(r"\b[A-Z][A-Z0-9&]{1,14}\b", markdown))):
        if token in {"NSE", "BSE", "LLM", "JSON", "PDF", "DQ", "RS", "PAT", "EPS", "ADX", "SMA", "FMCG", "BUY", "SELL"}:
            continue
        if token not in allowed_symbols and token.endswith("CR"):
            continue
        if token not in allowed_symbols and _token_only_in_cited_event_lines(markdown, token, event_sources):
            continue
        if token not in allowed_symbols and token in _symbol_like_lines(markdown):
            issues.append({"rule": "no_invented_symbols", "detail": token})

    for line in markdown.splitlines():
        if not any(str(symbol) in line for symbol in allowed_symbols):
            continue
        if any(str(src) in line for src in event_sources):
            continue
        if any(word in line.lower() for word in ["event", "capex", "result", "order", "fundraise", "legal", "buyback"]):
            issues.append({"rule": "event_claim_must_be_cited", "detail": line[:180]})

    trust_status = str((packet.get("data_trust") or {}).get("status") or "").lower()
    if trust_status in {"degraded", "blocked"} and trust_status not in lower:
        issues.append({"rule": "degraded_trust_must_show_warning", "detail": trust_status})

    return {
        "status": "passed" if not issues else "failed",
        "issues": issues,
        "allowed_symbol_count": len(allowed_symbols),
        "event_source_count": len(event_sources),
    }


def _build_telegram_summary(markdown: str, packet: dict[str, Any]) -> str:
    top_events = ((packet.get("market_intel") or {}).get("top_events") or [])[:5]
    top_ranked = (((packet.get("rank") or {}).get("top_ranked") or []))[:5]
    lines = [
        f"Daily Market Insight | {packet.get('run_date')}",
        f"Trust: {(packet.get('data_trust') or {}).get('status', 'unknown')}",
    ]
    market_intel_status = (packet.get("market_intel") or {}).get("market_intel_status")
    if market_intel_status in {"missing", "stale", "degraded"}:
        lines.append(f"Market intel: {market_intel_status}")
    if top_events:
        lines.append("Events: " + " ; ".join(f"{e.get('symbol')} {e.get('category')} [{e.get('event_hash') or e.get('raw_event_id')}]" for e in top_events))
    if top_ranked:
        lines.append("Top ranked: " + ", ".join(str(row.get("symbol_id") or row.get("symbol")) for row in top_ranked))
    return "\n".join(lines) + "\n\n" + "\n".join(markdown.splitlines()[:18])


def _read_json_artifact(artifact: StageArtifact) -> dict[str, Any]:
    try:
        return json.loads(Path(artifact.uri).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _allowed_symbols(packet: dict[str, Any]) -> set[str]:
    out = set()
    for row in ((packet.get("rank") or {}).get("top_ranked") or []):
        symbol = row.get("symbol_id") or row.get("symbol")
        if symbol:
            out.add(str(symbol).upper())
    for group in ["top_events", "critical_events", "important_events", "portfolio_events", "watchlist_events", "ranked_stock_events"]:
        for row in ((packet.get("market_intel") or {}).get(group) or []):
            symbol = row.get("symbol")
            if symbol:
                out.add(str(symbol).upper())
    for group in ["portfolio", "watchlist"]:
        for symbol in ((packet.get(group) or {}).get("symbols") or []):
            out.add(str(symbol).upper())
    return out


def _event_sources(packet: dict[str, Any]) -> set[str]:
    out = set()
    for group in ["top_events", "critical_events", "important_events", "portfolio_events", "watchlist_events", "ranked_stock_events"]:
        for row in ((packet.get("market_intel") or {}).get(group) or []):
            for key in ("event_hash", "raw_event_id", "resolved_event_id"):
                value = row.get(key)
                if value not in (None, ""):
                    out.add(str(value))
    return out


def _symbol_like_lines(markdown: str) -> set[str]:
    out = set()
    for line in markdown.splitlines():
        if any(marker in line.lower() for marker in ["rank", "event", "stock", "symbol", "watch", "caution"]):
            out.update(re.findall(r"\b[A-Z][A-Z0-9&]{1,14}\b", line))
    return out


def _token_only_in_cited_event_lines(markdown: str, token: str, event_sources: set[str]) -> bool:
    lines = [line for line in markdown.splitlines() if re.search(rf"\b{re.escape(token)}\b", line)]
    if not lines:
        return False
    return all(any(str(src) in line for src in event_sources) for line in lines)
