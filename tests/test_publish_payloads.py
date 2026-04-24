from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.ranking.payloads import build_dashboard_payload
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.pipeline.contracts import StageContext
from ai_trading_system.domains.publish.publish_payloads import (
    build_publish_datasets,
    build_publish_metadata,
)
from ai_trading_system.domains.publish.telegram_summary_builder import build_telegram_summary


def test_build_publish_datasets_loads_optional_artifacts_with_defaults(tmp_path: Path) -> None:
    ranked_artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri=str(tmp_path / "ranked_signals.csv"),
        content_hash="ranked-hash",
    )
    breakout_artifact = StageArtifact(
        artifact_type="breakout_scan",
        uri=str(tmp_path / "breakout_scan.csv"),
        content_hash="breakout-hash",
    )

    artifacts = {
        "breakout_scan": breakout_artifact,
    }

    def artifact_for(name: str) -> StageArtifact | None:
        return artifacts.get(name)

    def read_artifact(artifact: StageArtifact) -> pd.DataFrame:
        if artifact.artifact_type == "ranked_signals":
            return pd.DataFrame([{"symbol_id": "AAA", "composite_score": 90.0}])
        if artifact.artifact_type == "breakout_scan":
            return pd.DataFrame([{"symbol_id": "AAA", "setup_family": "range_breakout"}])
        return pd.DataFrame()

    datasets = build_publish_datasets(
        context_artifact_for=artifact_for,
        read_artifact=read_artifact,
        read_json_artifact=lambda _artifact: {"summary": {"run_date": "2026-04-16"}},
        ranked_signals_artifact=ranked_artifact,
    )

    assert not datasets["ranked_signals"].empty
    assert not datasets["breakout_scan"].empty
    assert datasets["stock_scan"].empty
    assert datasets["sector_dashboard"].empty
    assert datasets["dashboard_payload"] == {}
    assert datasets["publish_trust_status"] == "unknown"
    assert datasets["publish_mode_telegram"] == "concise"
    assert datasets["publish_mode_sheets"] == "full"
    assert datasets["publish_mode_dashboard"] == "structured_json"
    assert datasets["publish_rows_telegram"][0]["signal_classification"] == "actionable"
    assert datasets["publish_rows_telegram"][0]["publish_confidence"] is None
    assert datasets["stage2_summary"]["uptrend_count"] == 0
    assert datasets["stage2_breakdown_symbols"] == ["AAA"]


def test_build_publish_metadata_uses_top_ranked_symbol() -> None:
    ranked_artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri="/tmp/ranked_signals.csv",
        content_hash="hash-a",
    )
    ranked_df = pd.DataFrame(
        [{"symbol_id": "INFY", "rank_confidence": 0.85}, {"symbol_id": "RELIANCE", "rank_confidence": 0.70}]
    )
    targets = [{"channel": "telegram_summary", "status": "delivered"}]

    metadata = build_publish_metadata(
        rank_artifact=ranked_artifact,
        ranked_df=ranked_df,
        targets=targets,
    )

    assert metadata["rank_artifact_uri"] == "/tmp/ranked_signals.csv"
    assert metadata["rank_artifact_hash"] == "hash-a"
    assert metadata["top_symbol"] == "INFY"
    assert metadata["top_publish_confidence"] == 0.85
    assert metadata["targets"] == targets
    assert "completed_at" in metadata


def test_build_publish_datasets_adds_stage2_breakdown_and_telegram_summary_line() -> None:
    ranked_df = pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 95.0, "stage2_label": "strong_stage2", "is_stage2_uptrend": True},
            {"symbol_id": "BBB", "composite_score": 92.0, "stage2_label": "stage2", "is_stage2_uptrend": True},
            {"symbol_id": "CCC", "composite_score": 89.0, "stage2_label": "stage1_to_stage2", "is_stage2_uptrend": False},
        ]
    )
    ranked_artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri="/tmp/ranked.csv",
        content_hash="ranked-hash",
    )
    datasets = build_publish_datasets(
        context_artifact_for=lambda _name: None,
        read_artifact=lambda _artifact: ranked_df.copy(),
        read_json_artifact=lambda _artifact: {"summary": {"run_date": "2026-04-21"}},
        ranked_signals_artifact=ranked_artifact,
    )
    assert datasets["stage2_summary"]["uptrend_count"] == 2
    assert datasets["stage2_summary"]["counts_by_label"]["strong_stage2"] == 1
    assert datasets["stage2_breakdown_symbols"] == ["AAA", "BBB", "CCC"]

    message = build_telegram_summary(run_date="2026-04-21", datasets=datasets)
    assert "Stage2:" in message
    assert "strong_stage2:1" in message


def test_build_dashboard_payload_explains_empty_discoveries_when_ranked_covers_stock_scan(tmp_path: Path) -> None:
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-04-24-fixture",
        run_date="2026-04-24",
        stage_name="rank",
        attempt_number=1,
        params={},
    )
    ranked_df = pd.DataFrame(
        [
            {"symbol_id": "AAA", "rank": 1, "composite_score": 91.0},
            {"symbol_id": "BBB", "rank": 2, "composite_score": 88.0},
        ]
    )
    stock_scan_df = pd.DataFrame(
        [
            {"symbol_id": "AAA", "rank": 1, "composite_score": 91.0, "pattern_positive": True, "breakout_positive": False, "discovered_by_pattern_scan": False},
            {"symbol_id": "BBB", "rank": 2, "composite_score": 88.0, "pattern_positive": False, "breakout_positive": True, "discovered_by_pattern_scan": False},
        ]
    )

    payload = build_dashboard_payload(
        context=context,
        ranked_df=ranked_df,
        breakout_df=pd.DataFrame(),
        pattern_df=pd.DataFrame(),
        stock_scan_df=stock_scan_df,
        sector_dashboard_df=pd.DataFrame(),
        warnings=[],
        trust_summary={"status": "trusted"},
        task_status={},
    )

    summary = payload["summary"]
    assert summary["ranked_universe_covers_stock_scan"] is True
    assert summary["ranked_universe_stock_scan_coverage_pct"] == 100.0
    assert summary["discovery_visibility_reason"] == "ranked_universe_covers_stock_scan"
    assert "ranked universe already covers the full stock-scan symbol set" in summary["discovery_visibility_note"]
