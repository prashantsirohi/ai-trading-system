from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.pipeline.contracts import StageContext
import ai_trading_system.pipeline.stages.perf_tracker as perf_tracker_stage


def test_perf_tracker_stage_writes_research_quality_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        perf_tracker_stage,
        "run_backfill",
        lambda *, project_root: {"dates_processed": 1, "rows_upserted": 2},
    )
    monkeypatch.setattr(
        perf_tracker_stage,
        "build_tracker_health",
        lambda *, project_root: {"status": "warning", "warning_reasons": ["sample"]},
    )
    monkeypatch.setattr(
        perf_tracker_stage,
        "build_research_quality_reports",
        lambda *, project_root: {
            "summary": {
                "status": "warning",
                "warnings": ["sample"],
                "artifact_rows": {
                    "rank_bucket_performance": 1,
                    "sector_performance": 1,
                    "repeated_symbol_performance": 1,
                    "excluded_rows": 1,
                },
            },
            "frames": {
                "rank_bucket_performance": pd.DataFrame([{"rank_bucket": "top-10", "avg_return": 1.0}]),
                "sector_performance": pd.DataFrame([{"sector_name": "IT", "avg_return": 1.0}]),
                "repeated_symbol_performance": pd.DataFrame([{"symbol_id": "INFY", "avg_20d_return": 1.0}]),
                "excluded_rows": pd.DataFrame([{"symbol_id": "BAD", "data_quality_reason": "extreme_fwd_20d_return"}]),
            },
        },
    )
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "operational.duckdb",
        run_id="pipeline-2026-05-08-test",
        run_date="2026-05-08",
        stage_name="perf_tracker",
        attempt_number=1,
    )

    result = perf_tracker_stage.PerfTrackerStage().run(context)

    artifact_types = {artifact.artifact_type for artifact in result.artifacts}
    assert "perf_tracker_summary" in artifact_types
    assert "perf_tracker_research_quality_summary" in artifact_types
    assert "perf_tracker_rank_bucket_performance" in artifact_types
    assert "perf_tracker_sector_performance" in artifact_types
    assert "perf_tracker_repeated_symbol_performance" in artifact_types
    assert "perf_tracker_excluded_rows" in artifact_types
    assert result.metadata["research_quality_status"] == "warning"
