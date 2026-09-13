import csv
import json
from pathlib import Path

from ai_trading_system.pipeline.contracts import StageContext
from ai_trading_system.pipeline.stages.final_review import (
    materialize_final_review,
    write_review_artifacts,
)
from ai_trading_system.domains.opportunities.review_projection import (
    ReviewProjection,
    REVIEW_FIELDS,
)
from ai_trading_system.pipeline.orchestrator import build_parser


def context(tmp_path):
    return StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="review",
        run_date="2026-09-11",
        stage_name="opportunities",
        attempt_number=1,
    )


def test_default_is_noop_and_cli_explicit(tmp_path):
    assert build_parser().parse_args([]).final_review_mode == "off"
    c = context(tmp_path)
    assert not materialize_final_review(c).artifacts
    assert not (tmp_path / "data").exists()


def test_failure_emits_current_empty_lists_and_failed_status(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "runtime"))
    c = context(tmp_path)
    c.params["final_review_mode"] = "shadow"
    result = materialize_final_review(c)
    assert result.metadata["final_review_status"] == "failed"
    assert {a.artifact_type for a in result.artifacts} == {
        "final_review_universe",
        "final_review_list",
        "final_review_summary",
    }
    for a in result.artifacts:
        if a.artifact_type.endswith("summary"):
            assert json.loads(Path(a.uri).read_text())["error_type"] == "ValueError"
        else:
            with Path(a.uri).open() as f:
                reader = csv.DictReader(f)
                assert reader.fieldnames == list(REVIEW_FIELDS)
                assert not list(reader)


def test_rows_with_and_without_setup_share_stable_schema(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "runtime"))
    c = context(tmp_path)
    rows = [
        {"exchange": "NSE", "symbol": "NONE"},
        {"exchange": "NSE", "symbol": "SETUP", "trigger": 100.0, "extension_pct": 1.0},
    ]
    p = ReviewProjection(
        rows, [rows[1]], {"status": "degraded", "decision_content_hash": "hash"}, ()
    )
    result = write_review_artifacts(c, p)
    assert len(result.artifacts) == 3
    with Path(result.artifacts[0].uri).open() as f:
        data = list(csv.DictReader(f))
    assert data[0]["trigger"] == "" and data[1]["trigger"] == "100.0"
