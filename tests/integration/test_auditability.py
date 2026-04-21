from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.pipeline.contracts import attach_audit_fields
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.domains.publish.publish_payloads import build_publish_datasets


def test_attach_audit_fields_adds_lineage_metadata() -> None:
    row = attach_audit_fields(
        {"symbol_id": "AAA"},
        run_id="pipeline-2026-04-17",
        stage="publish",
        artifact_path="/tmp/ranked_signals.csv",
    )

    assert row["audit_run_id"] == "pipeline-2026-04-17"
    assert row["audit_stage"] == "publish"
    assert row["audit_artifact_path"] == "/tmp/ranked_signals.csv"


def test_publish_rows_include_audit_lineage(tmp_path: Path) -> None:
    ranked_artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri=str(tmp_path / "ranked_signals.csv"),
        content_hash="hash-a",
    )

    datasets = build_publish_datasets(
        context_artifact_for=lambda _name: None,
        read_artifact=lambda _artifact: pd.DataFrame([{"symbol_id": "AAA", "composite_score": 91.0}]),
        read_json_artifact=lambda _artifact: {"summary": {"data_trust_status": "trusted"}},
        ranked_signals_artifact=ranked_artifact,
        run_id="pipeline-2026-04-17-audit",
        stage_name="publish",
    )

    row = datasets["publish_rows_sheets"][0]
    assert row["audit_run_id"] == "pipeline-2026-04-17-audit"
    assert row["audit_stage"] == "publish"
    assert row["audit_artifact_path"] == str(tmp_path / "ranked_signals.csv")
