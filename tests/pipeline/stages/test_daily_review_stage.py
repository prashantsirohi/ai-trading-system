from dataclasses import replace
from datetime import datetime, timezone
import json

import pandas as pd
import pytest

from ai_trading_system.pipeline.contracts import (
    StageArtifact,
    StageContext,
    StageResult,
)
from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.pipeline.stages.daily_review import (
    DailyReviewStage,
    SOURCES,
    load_inputs,
)
from ai_trading_system.pipeline.stages.publish import PublishStage


@pytest.fixture
def review_context(tmp_path, monkeypatch):
    (tmp_path / "src" / "ai_trading_system").mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text("")
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    registry.create_run("review", "test", "2026-10-19")
    context = StageContext(
        tmp_path,
        tmp_path / "ohlcv.duckdb",
        "review",
        "2026-10-19",
        "daily_review",
        1,
        registry=registry,
        params={"daily_review_mode": "shadow"},
    )
    by_stage = {}
    for name, (stage, kind) in SOURCES.items():
        by_stage.setdefault(stage, []).append((name, kind))
    for stage, kinds in by_stage.items():
        stage_id = registry.start_stage("review", stage, 1)
        for name, kind in kinds:
            path = (
                tmp_path
                / "pipeline_runs"
                / "review"
                / stage
                / "attempt_1"
                / f"{kind}.{'json' if name.endswith('summary') else 'csv'}"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            metadata = {}
            if name == "final_summary":
                path.write_text(
                    json.dumps(
                        {
                            "status": "complete",
                            "shadow_only": True,
                            "decision_content_hash": "final-hash",
                            "calendar": {"status": "PASS"},
                        }
                    )
                )
                count = None
            elif name == "execution_summary":
                path.write_text(json.dumps({"summary": {"preview_only": True}}))
                count = None
            else:
                data = (
                    [{"exchange": "NSE", "symbol_id": "AAA", "composite_score": 80}]
                    if name in {"rank", "shortlist"}
                    else []
                )
                pd.DataFrame(
                    data, columns=["exchange", "symbol_id", "composite_score"]
                ).to_csv(path, index=False)
                count = len(data)
                if name in {"final_list", "final_universe"}:
                    metadata["decision_content_hash"] = "final-hash"
            registry.record_artifact(
                "review",
                stage,
                1,
                StageArtifact.from_file(
                    kind, path, count, metadata=metadata, attempt_number=1
                ),
            )
        registry.finish_stage(stage_id, "completed")
    return context


def materialize(context):
    result = DailyReviewStage().run(context)
    context.artifacts["daily_review"] = {a.artifact_type: a for a in result.artifacts}
    return result


def test_stage_exact_replay_and_publication_gate(review_context):
    first = materialize(review_context)
    before = {a.artifact_type: open(a.uri, "rb").read() for a in first.artifacts}
    second = materialize(review_context)
    assert before == {
        a.artifact_type: open(a.uri, "rb").read() for a in second.artifacts
    }
    datasets = {}
    PublishStage()._attach_daily_review_data(review_context, datasets)
    assert datasets.get("daily_review_ready"), (
        datasets,
        json.loads(open(first.artifacts[2].uri).read()),
    )
    assert datasets["daily_review_rows"].iloc[0]["symbol"] == "AAA"
    # Current pack mutation cannot refresh the existing external tab.
    artifact = review_context.artifacts["daily_review"]["daily_review_list"]
    with open(artifact.uri, "a") as stream:
        stream.write("tampered\n")
    datasets = {}
    PublishStage()._attach_daily_review_data(review_context, datasets)
    assert datasets["daily_review_state"]["status"] == "not_published"
    assert "daily_review_ready" not in datasets


def test_failed_or_unpromoted_producer_is_not_used(review_context):
    with review_context.registry._writer() as conn:
        conn.execute(
            "UPDATE pipeline_stage_run SET status='failed' WHERE run_id=? AND stage_name=?",
            ["review", "investigator"],
        )
        conn.execute(
            "UPDATE pipeline_artifact SET lifecycle_status='written' WHERE run_id=? AND stage_name=?",
            ["review", "execute"],
        )
    sources, _, _ = load_inputs(
        review_context, review_context.project_root, datetime.now(timezone.utc)
    )
    assert sources["investigator"].issue == "SOURCE_MISSING"
    assert sources["positions"].issue == "SOURCE_MISSING"


@pytest.mark.parametrize("mutation", ["future", "row_count", "hash", "session"])
def test_invalid_producer_is_explicit(review_context, mutation):
    with review_context.registry._writer() as conn:
        assignments = {
            "future": "promoted_at=TIMESTAMP '2099-01-01'",
            "row_count": "row_count=999",
            "hash": "content_hash='bad'",
        }
        if mutation == "session":
            conn.execute(
                "UPDATE pipeline_run SET run_date=? WHERE run_id=?",
                ["2026-10-18", "review"],
            )
        else:
            conn.execute(
                "UPDATE pipeline_artifact SET "
                + assignments[mutation]
                + " WHERE run_id=? AND artifact_type=?",
                ["review", "ranked_signals"],
            )
    sources, _, _ = load_inputs(
        review_context, review_context.project_root, datetime.now(timezone.utc)
    )
    assert sources["shortlist"].issue and not sources["shortlist"].rows


def test_invalid_calendar_never_publishes(review_context):
    with review_context.registry._writer() as conn:
        conn.execute(
            "UPDATE pipeline_artifact SET lifecycle_status='written' WHERE artifact_type=?",
            ["final_review_summary"],
        )
    materialize(review_context)
    datasets = {}
    PublishStage()._attach_daily_review_data(review_context, datasets)
    assert datasets["daily_review_state"]["status"] == "not_published"


def test_disabled_stage_and_normalized_order(review_context):
    assert DailyReviewStage().run(replace(review_context, params={})) == StageResult()
    from ai_trading_system.pipeline.orchestrator import PipelineOrchestrator

    orchestrator = object.__new__(PipelineOrchestrator)
    orchestrator._fundamental_scores_available = lambda _: False
    stages = orchestrator._normalize_stage_names(None, daily_review_mode="shadow")
    assert (
        stages.index("execute") < stages.index("daily_review") < stages.index("publish")
    )
    assert "daily_review" not in orchestrator._normalize_stage_names(None)
