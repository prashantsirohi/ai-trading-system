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
    assert (output_dir / "candidate_fundamental_bucket_reviews.csv").exists()
    assert (output_dir / "candidate_tracking_snapshots.csv").exists()
    assert {artifact.artifact_type for artifact in result.artifacts} == {
        "candidate_tracker_current",
        "candidate_tracker_alerts",
        "candidate_tracker_summary",
        "candidate_fundamental_reviews",
        "candidate_fundamental_bucket_reviews",
        "candidate_tracking_snapshots",
    }


def test_default_stage_lists_include_candidate_tracker_and_keep_perf_tracker_final(tmp_path: Path) -> None:
    args = build_parser().parse_args([])
    stages = PipelineOrchestrator(tmp_path, allow_control_plane_migrations=True)._normalize_stage_names(None)

    assert "candidate_tracker" in args.stages.split(",")
    assert args.stages.split(",")[-1] == "perf_tracker"
    assert "candidate_tracker" in DEFAULT_CLI_STAGES.split(",")
    assert stages.index("candidate_tracker") < stages.index("events")
    assert stages[-1] == "perf_tracker"


def test_research_tracker_preserves_operational_ledger(tmp_path: Path) -> None:
    from ai_trading_system.platform.db.paths import get_domain_paths
    context = _context(tmp_path)
    context.params["data_domain"] = "research"
    paths = get_domain_paths(project_root=tmp_path, data_domain="research")
    context.db_path = paths.ohlcv_db_path
    operational = tmp_path / "data" / "candidate_tracker.duckdb"
    operational.write_bytes(b"operational ledger sentinel")
    result = CandidateTrackerStage().run(context)
    research_db = paths.root_dir / "candidate_tracker.duckdb"
    assert research_db.exists()
    assert operational.read_bytes() == b"operational ledger sentinel"
    current = next(a for a in result.artifacts if a.artifact_type == "candidate_tracker_current")
    assert Path(current.metadata["db_path"]) == research_db


def test_research_tracker_rejects_operational_override(tmp_path: Path) -> None:
    import pytest
    context = _context(tmp_path)
    context.params.update(data_domain="research", candidate_tracker_db_path=str(tmp_path / "data" / "candidate_tracker.duckdb"))
    with pytest.raises(ValueError, match="operational ledger"):
        CandidateTrackerStage().run(context)
    assert not (tmp_path / "data" / "candidate_tracker.duckdb").exists()


def test_research_tracker_rejects_symlink_to_operational_ledger(tmp_path: Path) -> None:
    import pytest
    context = _context(tmp_path)
    operational = tmp_path / "data" / "candidate_tracker.duckdb"
    operational.write_bytes(b"operational sentinel")
    alias = tmp_path / "tracker-alias.duckdb"
    alias.symlink_to(operational)
    context.params.update(data_domain="RESEARCH", candidate_tracker_db_path=str(alias))
    with pytest.raises(ValueError, match="operational ledger"):
        CandidateTrackerStage().run(context)
    assert operational.read_bytes() == b"operational sentinel"
