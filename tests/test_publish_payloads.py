from __future__ import annotations

from pathlib import Path

import pandas as pd

from run.stages.base import StageArtifact
from services.publish.publish_payloads import build_publish_datasets, build_publish_metadata


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


def test_build_publish_metadata_uses_top_ranked_symbol() -> None:
    ranked_artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri="/tmp/ranked_signals.csv",
        content_hash="hash-a",
    )
    ranked_df = pd.DataFrame([{"symbol_id": "INFY"}, {"symbol_id": "RELIANCE"}])
    targets = [{"channel": "telegram_summary", "status": "delivered"}]

    metadata = build_publish_metadata(
        rank_artifact=ranked_artifact,
        ranked_df=ranked_df,
        targets=targets,
    )

    assert metadata["rank_artifact_uri"] == "/tmp/ranked_signals.csv"
    assert metadata["rank_artifact_hash"] == "hash-a"
    assert metadata["top_symbol"] == "INFY"
    assert metadata["targets"] == targets
    assert "completed_at" in metadata
