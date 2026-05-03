"""Event-aware daily/weekly insight stage."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.domains.events.event_llm_router import (
    build_deterministic_market_report,
    build_market_report,
)
from ai_trading_system.domains.events.event_packet_builder import build_event_packet
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class InsightStage:
    """Build technical + event intelligence packets and a publishable report."""

    name = "insight"

    def run(self, context: StageContext) -> StageResult:
        report_type = str(context.params.get("insight_report_type") or "daily").lower()
        ranked_df = _read_csv_artifact(context.artifact_for("rank", "ranked_signals"))
        breakout_df = _read_csv_artifact(context.artifact_for("rank", "breakout_scan"))
        pattern_df = _read_csv_artifact(context.artifact_for("rank", "pattern_scan"))
        sector_df = _read_csv_artifact(context.artifact_for("rank", "sector_dashboard"))
        dashboard_payload = _read_json_artifact(context.artifact_for("rank", "dashboard_payload"))
        positions_df = _read_csv_artifact(context.artifact_for("execute", "positions"))

        portfolio_symbols = _symbols_from_df(positions_df)
        watchlist_symbols = _parse_symbols(context.params.get("watchlist_symbols") or "")
        event_packet, confluence_df = build_event_packet(
            context,
            ranked_df=ranked_df,
            portfolio_symbols=portfolio_symbols,
            watchlist_symbols=watchlist_symbols,
        )
        technical_packet = _build_technical_packet(
            context,
            ranked_df=ranked_df,
            breakout_df=breakout_df,
            pattern_df=pattern_df,
            sector_df=sector_df,
            dashboard_payload=dashboard_payload,
            positions_df=positions_df,
        )
        combined_packet = {
            "run_id": context.run_id,
            "run_date": context.run_date,
            "report_type": report_type,
            **technical_packet,
            "market_intel": event_packet,
            "portfolio": {"symbols": sorted(portfolio_symbols)},
            "watchlist": {"symbols": sorted(watchlist_symbols)},
        }

        report_markdown, model_usage = build_market_report(
            combined_packet,
            project_root=context.project_root,
            report_type=report_type,
        )
        validation = validate_report(report_markdown, combined_packet, model_usage=model_usage)
        if validation["status"] != "passed":
            original_usage = dict(model_usage)
            fallback_markdown = build_deterministic_market_report(combined_packet, report_type=report_type)
            fallback_usage = {
                **original_usage,
                "status": "validation_fallback",
                "llm_status": original_usage.get("status"),
                "llm_possible_truncation": bool(original_usage.get("possible_truncation")),
                "possible_truncation": False,
                "validation_fallback_issues": validation.get("issues") or [],
            }
            fallback_validation = validate_report(fallback_markdown, combined_packet, model_usage=fallback_usage)
            if fallback_validation["status"] == "passed":
                report_markdown = fallback_markdown
                model_usage = fallback_usage
                validation = fallback_validation
        telegram_summary = _build_telegram_summary(report_markdown, combined_packet)

        technical_path = context.write_json("technical_packet.json", technical_packet)
        event_path = context.write_json("event_packet.json", event_packet)
        combined_path = context.write_json("combined_insight_packet.json", combined_packet)
        daily_json_path = context.write_json(
            f"{report_type}_insight.json",
            {
                "run_id": context.run_id,
                "run_date": context.run_date,
                "report_type": report_type,
                "status": validation["status"],
                "report_markdown": report_markdown,
                "market_intel": event_packet,
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
        confluence_path = out_dir / "event_confluence.csv"
        if confluence_df.empty:
            confluence_df = pd.DataFrame(
                columns=[
                    "symbol",
                    "category",
                    "alert_level",
                    "rank_position",
                    "event_materiality_score",
                    "event_hash",
                    "summary",
                ]
            )
        confluence_df.to_csv(confluence_path, index=False)

        artifacts = [
            StageArtifact.from_file("technical_packet", technical_path),
            StageArtifact.from_file("event_packet", event_path, row_count=len(event_packet.get("top_events") or [])),
            StageArtifact.from_file("combined_insight_packet", combined_path),
            StageArtifact.from_file(f"{report_type}_insight_json", daily_json_path),
            StageArtifact.from_file(f"{report_type}_insight_markdown", markdown_path),
            StageArtifact.from_file("telegram_summary", telegram_path),
            StageArtifact.from_file("event_confluence", confluence_path, row_count=len(confluence_df)),
            StageArtifact.from_file("model_usage", model_usage_path),
            StageArtifact.from_file("validation_report", validation_path),
        ]
        return StageResult(
            artifacts=artifacts,
            metadata={
                "report_type": report_type,
                "validation_status": validation["status"],
                "model_status": model_usage.get("status"),
                "event_count": int(sum((event_packet.get("event_counts") or {}).values())),
                "confluence_count": int(len(confluence_df)),
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


def _build_technical_packet(
    context: StageContext,
    *,
    ranked_df: pd.DataFrame,
    breakout_df: pd.DataFrame,
    pattern_df: pd.DataFrame,
    sector_df: pd.DataFrame,
    dashboard_payload: dict[str, Any],
    positions_df: pd.DataFrame,
) -> dict[str, Any]:
    summary = dashboard_payload.get("summary") or {}
    return {
        "data_trust": dashboard_payload.get("data_trust") or {},
        "dq_summary": _dq_summary(context),
        "data_trust_status": summary.get("data_trust_status"),
        "market_regime": dashboard_payload.get("market_regime") or dashboard_payload.get("regime") or {},
        "sector_strength": {"top_sectors": _records(sector_df, limit=15)},
        "rank": {"top_ranked": _records(ranked_df, limit=50), "row_count": int(len(ranked_df))},
        "breakouts": {"candidates": _records(breakout_df, limit=50), "row_count": int(len(breakout_df))},
        "patterns": {"candidates": _records(pattern_df, limit=50), "row_count": int(len(pattern_df))},
        "volume_delivery": {},
        "positions": _records(positions_df, limit=50),
    }


def _dq_summary(context: StageContext) -> dict[str, Any]:
    registry = getattr(context, "registry", None)
    if registry is None or not hasattr(registry, "connection"):
        return {}
    try:
        with registry.connection() as conn:
            rows = conn.execute(
                """
                SELECT stage_name, rule_id, severity, status, failed_count
                FROM data_quality_result
                WHERE run_id = ?
                ORDER BY evaluated_at DESC
                LIMIT 50
                """,
                [context.run_id],
            ).fetchall()
        return {
            "results": [
                {
                    "stage_name": row[0],
                    "rule_id": row[1],
                    "severity": row[2],
                    "status": row[3],
                    "failed_count": row[4],
                }
                for row in rows
            ]
        }
    except Exception:
        return {}


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


def _read_csv_artifact(artifact: StageArtifact | None) -> pd.DataFrame:
    if artifact is None:
        return pd.DataFrame()
    try:
        return pd.read_csv(Path(artifact.uri))
    except Exception:
        return pd.DataFrame()


def _read_json_artifact(artifact: StageArtifact | None) -> dict[str, Any]:
    if artifact is None:
        return {}
    try:
        return json.loads(Path(artifact.uri).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _records(df: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.head(limit).where(pd.notna(df.head(limit)), None).to_dict(orient="records")


def _symbols_from_df(df: pd.DataFrame) -> set[str]:
    if df is None or df.empty:
        return set()
    col = "symbol_id" if "symbol_id" in df.columns else "symbol"
    if col not in df.columns:
        return set()
    return {str(value).upper() for value in df[col].dropna().astype(str) if value}


def _parse_symbols(raw: str) -> set[str]:
    return {part.strip().upper() for part in str(raw or "").split(",") if part.strip()}


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
