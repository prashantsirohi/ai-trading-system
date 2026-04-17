from __future__ import annotations

from pathlib import Path

import pandas as pd

from analytics.data_trust import load_data_trust_summary
from run.stages.base import StageArtifact
from services.publish.publish_payloads import build_publish_datasets


def test_data_trust_summary_exposes_trust_confidence_envelope_for_missing_db(tmp_path: Path) -> None:
    summary = load_data_trust_summary(tmp_path / "missing.duckdb")

    assert summary["status"] == "missing"
    assert summary["trust_confidence"]["trust_status"] == "missing"
    assert summary["trust_confidence"]["provider_confidence"] is None


def test_publish_datasets_surface_trust_status_from_dashboard_payload(tmp_path: Path) -> None:
    ranked_artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri=str(tmp_path / "ranked_signals.csv"),
        content_hash="hash",
    )
    dashboard_payload_artifact = StageArtifact(
        artifact_type="dashboard_payload",
        uri=str(tmp_path / "dashboard_payload.json"),
        content_hash="hash-2",
    )

    def artifact_for(name: str) -> StageArtifact | None:
        if name == "dashboard_payload":
            return dashboard_payload_artifact
        return None

    def read_artifact(_artifact: StageArtifact) -> pd.DataFrame:
        return pd.DataFrame([{"symbol_id": "AAA", "composite_score": 90.0, "rank_confidence": 0.9}])

    def read_json_artifact(_artifact: StageArtifact) -> dict:
        return {
            "summary": {
                "data_trust_status": "degraded",
                "trust_confidence": {"provider_confidence": 0.7},
            }
        }

    datasets = build_publish_datasets(
        context_artifact_for=artifact_for,
        read_artifact=read_artifact,
        read_json_artifact=read_json_artifact,
        ranked_signals_artifact=ranked_artifact,
        run_id="pipeline-2026-04-17-publish",
        stage_name="publish",
    )

    row = datasets["publish_rows_telegram"][0]
    assert datasets["publish_trust_status"] == "degraded"
    assert row["trust_status"] == "degraded"
    assert "degraded" in str(row["trust_warning"])

