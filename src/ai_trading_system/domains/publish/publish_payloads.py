"""Payload assembly helpers for publish stage channel delivery."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict

import pandas as pd

from core.trust_confidence import attach_audit_fields
from run.stages.base import StageArtifact
from ai_trading_system.domains.publish.signal_classification import classify_signal


def apply_trust_overlay(payload: dict, trust_status: str) -> dict:
    """Attach trust status to publish payloads in an additive way."""
    output = dict(payload)
    output["trust_status"] = trust_status
    output["trust_warning"] = None
    if trust_status != "trusted":
        output["trust_warning"] = f"Trust status is {trust_status}. Review before acting."
    return output


def add_rank_diff(current_rows: list[dict], previous_rows: list[dict]) -> list[dict]:
    """Attach previous rank and delta fields for publish diff context."""
    prev_rank_map = {
        str(row.get("symbol") or row.get("symbol_id") or ""): idx + 1
        for idx, row in enumerate(previous_rows or [])
    }
    enriched = []
    for idx, row in enumerate(current_rows or [], start=1):
        symbol = str(row.get("symbol") or row.get("symbol_id") or "")
        previous_rank = prev_rank_map.get(symbol)
        enriched.append(
            {
                **row,
                "previous_rank": previous_rank,
                "rank_change": None if previous_rank is None else previous_rank - idx,
                "new_entry": previous_rank is None,
            }
        )
    return enriched


def attach_publish_explainability(row: dict) -> dict:
    """Attach publish-ready explainability fields."""
    return {
        **row,
        "why_selected": row.get("why_selected") or row.get("top_factors"),
        "key_factors": row.get("top_factors"),
        "risk_note": row.get("risk_note") or row.get("rejection_reasons"),
    }


def format_rows_for_channel(rows: list[dict], channel: str) -> dict:
    """Format publish rows for channel-specific density/structure."""
    if channel == "telegram":
        return {"rows": rows[:10], "mode": "concise"}
    if channel == "sheets":
        return {"rows": rows, "mode": "full"}
    if channel == "dashboard":
        return {"rows": rows, "mode": "structured_json"}
    return {"rows": rows, "mode": "default"}


def attach_publish_confidence(row: dict) -> dict:
    """Expose rank confidence as publish confidence."""
    return {
        **row,
        "publish_confidence": row.get("rank_confidence"),
    }


def attach_publish_metadata(row: dict, trust_status: str) -> dict:
    """Attach publish-facing trust/confidence envelope to a row."""
    payload = attach_publish_confidence(row)
    payload["trust_status"] = trust_status
    return payload


def build_publish_datasets(
    *,
    context_artifact_for: Callable[[str], StageArtifact | None],
    read_artifact: Callable[[StageArtifact], pd.DataFrame],
    read_json_artifact: Callable[[StageArtifact], Dict[str, Any]],
    ranked_signals_artifact: StageArtifact,
    run_id: str | None = None,
    stage_name: str | None = "publish",
) -> Dict[str, Any]:
    """Load publish datasets from rank-stage artifacts with compatibility defaults."""
    scan_artifact = context_artifact_for("stock_scan")
    breakout_artifact = context_artifact_for("breakout_scan")
    dashboard_artifact = context_artifact_for("sector_dashboard")
    dashboard_payload_artifact = context_artifact_for("dashboard_payload")

    ranked_df = read_artifact(ranked_signals_artifact)
    dashboard_payload = read_json_artifact(dashboard_payload_artifact) if dashboard_payload_artifact else {}
    trust_status = str(
        (dashboard_payload.get("summary", {}) or {}).get(
            "data_trust_status",
            (dashboard_payload.get("data_trust", {}) or {}).get("status", "unknown"),
        )
    )

    ranked_rows = ranked_df.to_dict(orient="records") if isinstance(ranked_df, pd.DataFrame) and not ranked_df.empty else []
    ranked_rows = [
        attach_publish_metadata(
            attach_publish_explainability(
                {
                    **row,
                    "signal_classification": classify_signal(row),
                }
            ),
            trust_status=trust_status,
        )
        for row in ranked_rows
    ]
    ranked_rows = add_rank_diff(ranked_rows, [])
    ranked_rows = [
        attach_audit_fields(
            apply_trust_overlay(row, trust_status),
            run_id=run_id,
            stage=stage_name,
            artifact_path=ranked_signals_artifact.uri,
        )
        for row in ranked_rows
    ]

    telegram_pack = format_rows_for_channel(ranked_rows, "telegram")
    sheets_pack = format_rows_for_channel(ranked_rows, "sheets")
    dashboard_pack = format_rows_for_channel(ranked_rows, "dashboard")

    return {
        "ranked_signals": ranked_df,
        "breakout_scan": read_artifact(breakout_artifact) if breakout_artifact else pd.DataFrame(),
        "stock_scan": read_artifact(scan_artifact) if scan_artifact else pd.DataFrame(),
        "sector_dashboard": read_artifact(dashboard_artifact) if dashboard_artifact else pd.DataFrame(),
        "dashboard_payload": dashboard_payload,
        "publish_rows_telegram": telegram_pack["rows"],
        "publish_rows_sheets": sheets_pack["rows"],
        "publish_rows_dashboard": dashboard_pack["rows"],
        "publish_mode_telegram": telegram_pack["mode"],
        "publish_mode_sheets": sheets_pack["mode"],
        "publish_mode_dashboard": dashboard_pack["mode"],
        "publish_trust_status": trust_status,
    }


def build_publish_metadata(
    *,
    rank_artifact: StageArtifact,
    ranked_df: pd.DataFrame,
    targets: list[dict[str, Any]],
) -> Dict[str, Any]:
    """Build publish stage metadata summary from delivery outcomes."""
    top_publish_confidence = None
    if isinstance(ranked_df, pd.DataFrame) and not ranked_df.empty and "rank_confidence" in ranked_df.columns:
        try:
            top_publish_confidence = float(ranked_df.iloc[0]["rank_confidence"])
        except (TypeError, ValueError):
            top_publish_confidence = None

    return {
        "rank_artifact_uri": rank_artifact.uri,
        "rank_artifact_hash": rank_artifact.content_hash,
        "targets": targets,
        "top_symbol": ranked_df.iloc[0]["symbol_id"] if not ranked_df.empty else None,
        "top_publish_confidence": top_publish_confidence,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
