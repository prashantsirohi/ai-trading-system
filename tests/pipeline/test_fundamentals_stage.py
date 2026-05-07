from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.orchestrator import DEFAULT_PIPELINE_ORDER, PipelineOrchestrator
from ai_trading_system.pipeline.stages.fundamentals import FundamentalsStage


def _rank_artifacts(project_root: Path, run_id: str = "run-fund") -> dict[str, dict[str, StageArtifact]]:
    rank_dir = project_root / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    ranked = pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 82, "rel_strength": 40},
            {"symbol_id": "MISS", "composite_score": 76, "rel_strength": 35},
        ]
    )
    breakout = pd.DataFrame([{"symbol_id": "AAA", "breakout_score": 90, "candidate_tier": "A", "qualified": True}])
    pattern = pd.DataFrame([{"symbol_id": "AAA", "pattern_score": 80, "pattern_family": "vcp", "pattern_state": "confirmed"}])
    ranked.to_csv(rank_dir / "ranked_signals.csv", index=False)
    breakout.to_csv(rank_dir / "breakout_scan.csv", index=False)
    pattern.to_csv(rank_dir / "pattern_scan.csv", index=False)
    return {
        "rank": {
            "ranked_signals": StageArtifact.from_file("ranked_signals", rank_dir / "ranked_signals.csv", row_count=2),
            "breakout_scan": StageArtifact.from_file("breakout_scan", rank_dir / "breakout_scan.csv", row_count=1),
            "pattern_scan": StageArtifact.from_file("pattern_scan", rank_dir / "pattern_scan.csv", row_count=1),
        }
    }


def _scores(path: Path, *, snapshot_date: str = "2026-05-07") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "snapshot_date": snapshot_date,
                "screener_snapshot_date": snapshot_date,
                "symbol": "AAA",
                "name": "Alpha",
                "industry_group": "Capital Goods",
                "industry": "Industrial Products",
                "quality_score": 80,
                "growth_score": 75,
                "balance_sheet_score": 90,
                "valuation_score": 65,
                "ownership_score": 85,
                "fundamental_score": 79,
                "fundamental_tier": "A",
                "red_flags": "",
                "hard_red_flag": False,
            }
        ]
    ).to_csv(path, index=False)


def _context(project_root: Path, *, artifacts: dict, params: dict | None = None, run_date: str = "2026-05-07") -> StageContext:
    return StageContext(
        project_root=project_root,
        db_path=project_root / "data" / "ohlcv.duckdb",
        run_id="run-fund",
        run_date=run_date,
        stage_name="fundamentals",
        attempt_number=1,
        params=params or {},
        artifacts=artifacts,
    )


def test_fundamentals_stage_writes_attempt_artifacts(tmp_path: Path) -> None:
    scores_path = tmp_path / "data" / "fundamentals" / "fundamental_scores_latest.csv"
    _scores(scores_path)
    context = _context(
        tmp_path,
        artifacts=_rank_artifacts(tmp_path),
        params={"fundamental_scores_path": str(scores_path)},
    )

    result = FundamentalsStage().run(context)

    output_dir = tmp_path / "data" / "pipeline_runs" / "run-fund" / "fundamentals" / "attempt_1"
    assert (output_dir / "watchlist_candidates.csv").exists()
    assert (output_dir / "fundamental_scores.csv").exists()
    assert (output_dir / "fundamental_summary.json").exists()
    assert {artifact.artifact_type for artifact in result.artifacts} == {
        "watchlist_candidates",
        "fundamental_scores",
        "fundamental_summary",
    }
    summary = json.loads((output_dir / "fundamental_summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "completed"
    assert summary["matched_rank_rows"] == 1
    assert summary["missing_fundamental_rows"] == 1


def test_fundamentals_stage_skips_when_scores_missing(tmp_path: Path) -> None:
    context = _context(
        tmp_path,
        artifacts=_rank_artifacts(tmp_path),
        params={"fundamental_scores_path": str(tmp_path / "missing.csv")},
    )

    result = FundamentalsStage().run(context)

    assert [artifact.artifact_type for artifact in result.artifacts] == ["fundamental_summary"]
    assert result.metadata["status"] == "skipped_missing_snapshot"
    assert result.metadata["warnings"]


def test_fundamentals_stage_warns_on_stale_snapshot(tmp_path: Path) -> None:
    scores_path = tmp_path / "data" / "fundamentals" / "fundamental_scores_latest.csv"
    _scores(scores_path, snapshot_date="2026-01-01")
    context = _context(
        tmp_path,
        artifacts=_rank_artifacts(tmp_path),
        params={"fundamental_scores_path": str(scores_path), "fundamental_max_stale_days": 10},
        run_date="2026-05-07",
    )

    result = FundamentalsStage().run(context)

    assert result.metadata["status"] == "completed"
    assert result.metadata["stale_days"] > 10
    assert any("stale" in warning for warning in result.metadata["warnings"])


def test_fundamentals_stage_default_stale_threshold_is_quarterly(tmp_path: Path) -> None:
    scores_path = tmp_path / "data" / "fundamentals" / "fundamental_scores_latest.csv"
    _scores(scores_path, snapshot_date="2026-01-01")
    fresh_context = _context(
        tmp_path,
        artifacts=_rank_artifacts(tmp_path),
        params={"fundamental_scores_path": str(scores_path)},
        run_date="2026-05-07",
    )

    fresh_result = FundamentalsStage().run(fresh_context)

    assert fresh_result.metadata["stale_days"] == 126
    assert not any("stale" in warning for warning in fresh_result.metadata["warnings"])

    stale_context = _context(
        tmp_path,
        artifacts=_rank_artifacts(tmp_path, run_id="run-fund-stale"),
        params={"fundamental_scores_path": str(scores_path)},
        run_date="2026-05-20",
    )

    stale_result = FundamentalsStage().run(stale_context)

    assert stale_result.metadata["stale_days"] == 139
    assert any("stale" in warning for warning in stale_result.metadata["warnings"])


def test_fundamentals_stage_fails_when_rank_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="ranked_signals"):
        FundamentalsStage().run(_context(tmp_path, artifacts={}))


def test_orchestrator_auto_injects_fundamentals_when_snapshot_exists(tmp_path: Path) -> None:
    orchestrator = PipelineOrchestrator(tmp_path)
    scores_path = tmp_path / "data" / "fundamentals" / "fundamental_scores_latest.csv"

    assert orchestrator._normalize_stage_names(None) == DEFAULT_PIPELINE_ORDER
    _scores(scores_path)
    assert orchestrator._normalize_stage_names(None) == [
        "ingest",
        "features",
        "rank",
        "fundamentals",
        "events",
        "execute",
        "insight",
        "publish",
    ]
    assert orchestrator._normalize_stage_names(None, enable_fundamentals=True) == [
        "ingest",
        "features",
        "rank",
        "fundamentals",
        "events",
        "execute",
        "insight",
        "publish",
    ]
    assert orchestrator._normalize_stage_names(["rank", "fundamentals"]) == ["rank", "fundamentals"]
    assert orchestrator._normalize_stage_names(["rank", "publish"]) == ["rank", "publish"]
