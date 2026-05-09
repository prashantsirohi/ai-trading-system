"""Deterministic insight stage.

Builds the technical packet, event packet, analyst brief, and combined
insight packet from upstream rank/events/execute artifacts. LLM synthesis
of the market report and the telegram summary live in ``narrative`` so
LLM provider issues cannot block deterministic enrichment.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.domains.events.event_packet_builder import build_event_packet
from ai_trading_system.domains.events.analyst_brief_builder import (
    build_analyst_brief,
    build_event_features_frame,
)
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class InsightStage:
    """Build technical + event intelligence packets and the analyst brief."""

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
        analyst_brief = build_analyst_brief(combined_packet)
        combined_packet["analyst_brief"] = analyst_brief

        technical_path = context.write_json("technical_packet.json", technical_packet)
        event_path = context.write_json("event_packet.json", event_packet)
        analyst_brief_path = context.write_json("analyst_brief.json", analyst_brief)
        combined_path = context.write_json("combined_insight_packet.json", combined_packet)

        out_dir = context.output_dir()
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
        event_features_path = out_dir / "event_features.csv"
        build_event_features_frame(analyst_brief).to_csv(event_features_path, index=False)

        artifacts = [
            StageArtifact.from_file("technical_packet", technical_path),
            StageArtifact.from_file("event_packet", event_path, row_count=len(event_packet.get("top_events") or [])),
            StageArtifact.from_file("analyst_brief", analyst_brief_path, row_count=len(analyst_brief.get("symbol_cards") or [])),
            StageArtifact.from_file("combined_insight_packet", combined_path),
            StageArtifact.from_file("event_confluence", confluence_path, row_count=len(confluence_df)),
            StageArtifact.from_file("event_features", event_features_path),
        ]
        return StageResult(
            artifacts=artifacts,
            metadata={
                "report_type": report_type,
                "event_count": int(sum((event_packet.get("event_counts") or {}).values())),
                "confluence_count": int(len(confluence_df)),
            },
        )


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
