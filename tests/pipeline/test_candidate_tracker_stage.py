from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.orchestrator import DEFAULT_CLI_STAGES, PipelineOrchestrator, build_parser
from ai_trading_system.pipeline.stages.candidate_tracker import CandidateTrackerStage


def _context(project_root: Path) -> StageContext:
    candidate_dir = project_root / "data" / "pipeline_runs" / "run-ct" / "candidates" / "attempt_1"
    rank_dir = project_root / "data" / "pipeline_runs" / "run-ct" / "rank" / "attempt_1"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    rank_dir.mkdir(parents=True, exist_ok=True)
    final_path = candidate_dir / "final_candidates.csv"
    ranked_path = rank_dir / "ranked_signals.csv"
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "candidate_group": "FUND_VALUE_TECH_READY",
                "composite_score": 88,
                "rel_strength_score": 82,
                "close": 120,
                "sma_50": 110,
                "sma_200": 100,
                "near_52w_high_pct": 5,
                "stage2_label": "stage2",
            }
        ]
    ).to_csv(final_path, index=False)
    pd.DataFrame([{"symbol_id": "AAA", "composite_score": 88}]).to_csv(ranked_path, index=False)
    return StageContext(
        project_root=project_root,
        db_path=project_root / "data" / "ohlcv.duckdb",
        run_id="run-ct",
        run_date="2026-06-01",
        stage_name="candidate_tracker",
        attempt_number=1,
        params={"preflight": False},
        artifacts={
            "candidates": {"final_candidates": StageArtifact.from_file("final_candidates", final_path, row_count=1)},
            "rank": {"ranked_signals": StageArtifact.from_file("ranked_signals", ranked_path, row_count=1)},
        },
    )


def test_candidate_tracker_stage_writes_expected_artifacts(tmp_path: Path) -> None:
    result = CandidateTrackerStage().run(_context(tmp_path))
    output_dir = tmp_path / "data" / "pipeline_runs" / "run-ct" / "candidate_tracker" / "attempt_1"

    assert (output_dir / "candidate_tracker_current.csv").exists()
    assert (output_dir / "candidate_tracker_alerts.csv").exists()
    assert (output_dir / "candidate_tracker_summary.json").exists()
    assert (output_dir / "candidate_fundamental_reviews.csv").exists()
    assert (output_dir / "candidate_tracking_snapshots.csv").exists()
    assert {artifact.artifact_type for artifact in result.artifacts} == {
        "candidate_tracker_current",
        "candidate_tracker_alerts",
        "candidate_tracker_summary",
        "candidate_fundamental_reviews",
        "candidate_tracking_snapshots",
    }


def test_default_stage_lists_include_candidate_tracker_and_keep_perf_tracker_final(tmp_path: Path) -> None:
    args = build_parser().parse_args([])
    stages = PipelineOrchestrator(tmp_path)._normalize_stage_names(None)

    assert "candidate_tracker" in args.stages.split(",")
    assert args.stages.split(",")[-1] == "perf_tracker"
    assert "candidate_tracker" in DEFAULT_CLI_STAGES.split(",")
    assert stages.index("candidate_tracker") < stages.index("events")
    assert stages[-1] == "perf_tracker"
