"""Final-review publication must preserve the last good pilot on invalid dates."""

import json

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.publish import PublishStage


def _context(tmp_path, calendar_status):
    list_path = tmp_path / "final_review_list.csv"
    list_path.write_text("exchange,symbol\n", encoding="utf-8")
    summary_path = tmp_path / "final_review_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "status": "degraded",
                "shadow_only": True,
                "decision_content_hash": "same-decision",
                "calendar": {"status": calendar_status},
            }
        ),
        encoding="utf-8",
    )
    return StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="weekend-run",
        run_date="2026-09-27",
        stage_name="publish",
        attempt_number=1,
        params={"final_review_mode": "shadow"},
        artifacts={
            "opportunities": {
                "final_review_list": StageArtifact.from_file(
                    "final_review_list",
                    list_path,
                    row_count=0,
                    metadata={"decision_content_hash": "same-decision"},
                ),
                "final_review_summary": StageArtifact.from_file(
                    "final_review_summary", summary_path
                ),
            }
        },
    )


def test_failed_calendar_does_not_publish_empty_review(tmp_path):
    datasets = {}
    PublishStage()._attach_final_review_pilot_data(
        _context(tmp_path, "FAILED"), datasets
    )
    assert datasets["final_review_pilot_state"]["status"] == "not_published"
    assert "final_review_pilot_ready" not in datasets


def test_verified_calendar_allows_genuine_empty_review(tmp_path):
    datasets = {}
    PublishStage()._attach_final_review_pilot_data(
        _context(tmp_path, "PASS"), datasets
    )
    assert datasets["final_review_pilot_state"]["status"] == "ready"
    assert datasets["final_review_pilot_ready"] is True
