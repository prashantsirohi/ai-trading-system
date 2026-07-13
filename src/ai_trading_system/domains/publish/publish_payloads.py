"""Payload assembly helpers for publish stage channel delivery."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd

from ai_trading_system.pipeline.contracts import attach_audit_fields
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.domains.publish.signal_classification import classify_signal


DEFAULT_FUNDAMENTAL_ARTIFACT_TYPES = frozenset(
    {
        "watchlist_candidates",
        "fundamental_summary",
        "fundamental_scores",
        "quarterly_result_scores",
        "stock_valuation_bands_latest",
        "company_growth_features",
        "company_insight_tags",
        "great_results",
        "great_results_latest",
        "turnaround_candidates",
        "turnaround_candidates_latest",
        "compounder_candidates",
        "compounder_candidates_latest",
        "sector_earnings_leadership",
        "sector_earnings_latest",
        "sector_valuation_daily",
        "sector_valuation_latest",
        "universe_valuation_daily",
        "universe_valuation_latest",
        "valuation_cycle_features",
        "valuation_cycle_latest",
        "fundamental_dashboard_payload",
    }
)


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
    fundamental_artifact_types: set[str] | frozenset[str] | None = None,
    project_root: Path | str | None = None,
    run_date: str | None = None,
) -> Dict[str, Any]:
    """Load publish datasets from rank-stage artifacts with compatibility defaults."""
    fundamental_types = set(fundamental_artifact_types or DEFAULT_FUNDAMENTAL_ARTIFACT_TYPES)
    scan_artifact = context_artifact_for("stock_scan")
    breakout_artifact = context_artifact_for("breakout_scan")
    pattern_artifact = context_artifact_for("pattern_scan")
    dashboard_artifact = context_artifact_for("sector_dashboard")
    sector_rotation_artifact = context_artifact_for("sector_rotation")
    industry_rotation_artifact = context_artifact_for("industry_rotation")
    stock_rotation_artifact = context_artifact_for("stock_rotation")
    accumulation_distribution_artifact = context_artifact_for("accumulation_distribution")
    sector_custom_indices_artifact = context_artifact_for("sector_custom_indices")
    sector_rotation_payload_artifact = context_artifact_for("sector_rotation_payload")
    watchlist_artifact = context_artifact_for("watchlist_candidates") or context_artifact_for("watchlist_final")
    dashboard_payload_artifact = context_artifact_for("dashboard_payload")
    candidate_tracker_artifact = context_artifact_for("candidate_tracker_current")

    ranked_df = read_artifact(ranked_signals_artifact)
    pattern_df = read_artifact(pattern_artifact) if pattern_artifact else pd.DataFrame()
    stage_df = read_artifact(scan_artifact) if scan_artifact else pd.DataFrame()
    decision_read_sources: list[dict[str, Any]] = []
    if project_root is not None:
        from ai_trading_system.ui.execution_api.services.readmodels.decision_reads import (
            DecisionReadError, PatternHistoryReadRepository,
            RankHistoryReadRepository, StageHistoryReadRepository,
        )

        repository_calls = {
            "rank": lambda: RankHistoryReadRepository(project_root).get_current_rankings(trade_date=run_date),
            "stage": lambda: StageHistoryReadRepository(project_root).get_current_stage_snapshot(trade_date=run_date),
            "pattern": lambda: PatternHistoryReadRepository(project_root).get_current_patterns(trade_date=run_date),
        }
        for domain, call in repository_calls.items():
            try:
                payload = call()
                frame = pd.DataFrame(payload["rows"])
                if not frame.empty:
                    if domain == "rank":
                        frame = frame.copy()
                        frame.loc[:, "rank"] = frame.get("rank_position")
                        ranked_df = frame
                    elif domain == "stage":
                        stage_df = frame
                    else:
                        pattern_df = frame
                    decision_read_sources.append(payload["metadata"])
                    continue
                reason = "DuckDB returned no rows"
            except (DecisionReadError, Exception) as exc:  # publisher remains available through explicit fallback
                reason = str(exc)
            decision_read_sources.append({
                "domain": domain, "data_source": "ARTIFACT_FALLBACK",
                "as_of_date": run_date, "model_version": None,
                "row_count": len({"rank": ranked_df, "stage": stage_df, "pattern": pattern_df}[domain]),
                "fallback_used": True, "fallback_reason": reason,
                "fallback_run_id": run_id, "error": reason,
            })
    stage2_summary = _build_stage2_summary(ranked_df)
    dashboard_payload = read_json_artifact(dashboard_payload_artifact) if dashboard_payload_artifact else {}
    fundamental_artifacts = {
        artifact_type: context_artifact_for(artifact_type)
        for artifact_type in fundamental_types
        if artifact_type not in {"watchlist_candidates", "fundamental_summary"}
    }
    fundamental_dashboard_payload = (
        read_json_artifact(fundamental_artifacts["fundamental_dashboard_payload"])
        if fundamental_artifacts.get("fundamental_dashboard_payload")
        else {}
    )
    if fundamental_dashboard_payload:
        dashboard_payload["fundamentals"] = _dashboard_fundamentals_payload(fundamental_dashboard_payload)
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
    watchlist_df = read_artifact(watchlist_artifact) if watchlist_artifact else pd.DataFrame()
    if isinstance(watchlist_df, pd.DataFrame) and not watchlist_df.empty:
        data_trust = dict(dashboard_payload.get("data_trust") or {"status": trust_status})
        watchlist_rows = [
            attach_audit_fields(
                {
                    **apply_trust_overlay(row, trust_status),
                    "data_trust": data_trust,
                },
                run_id=run_id,
                stage=stage_name,
                artifact_path=watchlist_artifact.uri if watchlist_artifact else None,
            )
            for row in watchlist_df.head(15).to_dict(orient="records")
        ]
        dashboard_payload["watchlist"] = watchlist_rows

    datasets: Dict[str, Any] = {
        "ranked_signals": ranked_df,
        "breakout_scan": read_artifact(breakout_artifact) if breakout_artifact else pd.DataFrame(),
        "pattern_scan": pattern_df,
        "stock_scan": stage_df,
        "sector_dashboard": read_artifact(dashboard_artifact) if dashboard_artifact else pd.DataFrame(),
        "sector_rotation": read_artifact(sector_rotation_artifact) if sector_rotation_artifact else pd.DataFrame(),
        "industry_rotation": read_artifact(industry_rotation_artifact) if industry_rotation_artifact else pd.DataFrame(),
        "stock_rotation": read_artifact(stock_rotation_artifact) if stock_rotation_artifact else pd.DataFrame(),
        "accumulation_distribution": read_artifact(accumulation_distribution_artifact) if accumulation_distribution_artifact else pd.DataFrame(),
        "sector_custom_indices": read_artifact(sector_custom_indices_artifact) if sector_custom_indices_artifact else pd.DataFrame(),
        "sector_rotation_payload": read_json_artifact(sector_rotation_payload_artifact) if sector_rotation_payload_artifact else {},
        "watchlist_candidates": watchlist_df,
        "candidate_tracker_current": read_artifact(candidate_tracker_artifact) if candidate_tracker_artifact else pd.DataFrame(),
        "dashboard_payload": dashboard_payload,
        "publish_rows_telegram": telegram_pack["rows"],
        "publish_rows_sheets": sheets_pack["rows"],
        "publish_rows_dashboard": dashboard_pack["rows"],
        "publish_mode_telegram": telegram_pack["mode"],
        "publish_mode_sheets": sheets_pack["mode"],
        "publish_mode_dashboard": dashboard_pack["mode"],
        "publish_trust_status": trust_status,
        "stage2_summary": stage2_summary,
        "stage2_breakdown_symbols": stage2_summary.get("top_symbols", []),
        "decision_read_source_summary": decision_read_sources,
    }
    for artifact_type, artifact in fundamental_artifacts.items():
        if artifact_type == "fundamental_dashboard_payload":
            datasets[artifact_type] = fundamental_dashboard_payload
        elif artifact_type == "fundamental_scores":
            datasets[artifact_type] = read_artifact(artifact) if artifact else pd.DataFrame()
        elif artifact is not None:
            datasets[artifact_type] = read_artifact(artifact)
        elif artifact_type not in datasets:
            datasets[artifact_type] = pd.DataFrame()
    if "fundamental_summary" in fundamental_types:
        summary_artifact = context_artifact_for("fundamental_summary")
        datasets["fundamental_summary"] = read_json_artifact(summary_artifact) if summary_artifact else {}
    return datasets


def build_publish_metadata(
    *,
    rank_artifact: StageArtifact,
    ranked_df: pd.DataFrame,
    targets: list[dict[str, Any]],
    stage2_summary: dict[str, Any] | None = None,
    stage2_breakdown_symbols: list[str] | None = None,
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
        "stage2_summary": dict(stage2_summary or {}),
        "stage2_breakdown_symbols": list(stage2_breakdown_symbols or []),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_stage2_summary(ranked_df: pd.DataFrame, *, max_symbols: int = 10) -> dict[str, Any]:
    if ranked_df is None or ranked_df.empty:
        return {
            "uptrend_count": 0,
            "counts_by_label": {},
            "top_symbols": [],
        }

    df = ranked_df.copy()
    label_counts: dict[str, int] = {}
    if "stage2_label" in df.columns:
        labels = df["stage2_label"].fillna("unknown").astype(str)
        label_counts = {str(key): int(value) for key, value in labels.value_counts().to_dict().items()}

    uptrend_count = 0
    if "is_stage2_uptrend" in df.columns:
        uptrend_count = int(df["is_stage2_uptrend"].fillna(False).astype(bool).sum())

    top_symbols: list[str] = []
    if "symbol_id" in df.columns:
        top_symbols = [str(symbol) for symbol in df["symbol_id"].dropna().astype(str).head(max_symbols).tolist()]

    return {
        "uptrend_count": uptrend_count,
        "counts_by_label": label_counts,
        "top_symbols": top_symbols,
    }


def _dashboard_fundamentals_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "summary": dict(payload.get("summary") or {}),
        "great_results": list(payload.get("great_results_top") or payload.get("top_great_results") or []),
        "turnarounds": list(payload.get("turnarounds_top") or payload.get("top_turnarounds") or []),
        "compounders": list(payload.get("compounders_top") or payload.get("top_compounders") or []),
        "sector_earnings": list(payload.get("sector_earnings_top") or payload.get("sector_earnings_leadership") or []),
        "valuation_cycle": list(payload.get("valuation_chart") or []),
        "universe": dict(payload.get("universe") or {}),
    }
