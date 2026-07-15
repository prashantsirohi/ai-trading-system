from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.orchestrator import FEATURE_SUBSTAGES, PipelineOrchestrator
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
        "quarterly_result_scores",
        "stock_valuation_bands_latest",
        "fundamental_summary",
    }
    summary = json.loads((output_dir / "fundamental_summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "completed"
    assert summary["matched_rank_rows"] == 1
    assert summary["missing_fundamental_rows"] == 1
    assert summary["fundamental_statement_basis"] == "standalone"
    assert summary["quarterly_result_statement_basis"] == "standalone"


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


def _industry_scores(path: Path, *, snapshot_date: str = "2026-05-07") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "snapshot_date": snapshot_date,
                "industry": "Industrial Products",
                "industry_key": "INDUSTRIAL PRODUCTS",
                "no_of_companies": 25,
                "total_market_cap": 1000,
                "median_market_cap": 100,
                "median_pe": 18,
                "sales_growth_wavg": 15,
                "opm_wavg": 25,
                "roce_wavg": 20,
                "median_1y_return": 25,
                "industry_growth_score": 80,
                "industry_quality_score": 80,
                "industry_valuation_score": 70,
                "industry_momentum_score": 75,
                "industry_fundamental_score": 76,
                "industry_fundamental_label": "QUALITY_GROWTH_LEADER",
                "industry_warning": "",
                "screener_industry_snapshot_date": snapshot_date,
            }
        ]
    ).to_csv(path, index=False)


def test_fundamentals_stage_records_industry_artifact_when_present(tmp_path: Path) -> None:
    scores_path = tmp_path / "data" / "fundamentals" / "fundamental_scores_latest.csv"
    industry_path = tmp_path / "data" / "fundamentals" / "industry_fundamental_scores_latest.csv"
    _scores(scores_path)
    _industry_scores(industry_path)
    artifacts = _rank_artifacts(tmp_path)
    rank_dir = tmp_path / "data" / "pipeline_runs" / "run-fund" / "rank" / "attempt_1"
    pd.DataFrame([{"industry": "Industrial Products", "rs_score": 70}]).to_csv(
        rank_dir / "sector_dashboard.csv", index=False
    )
    context = _context(
        tmp_path,
        artifacts=artifacts,
        params={
            "fundamental_scores_path": str(scores_path),
            "industry_fundamental_scores_path": str(industry_path),
        },
    )

    result = FundamentalsStage().run(context)

    artifact_types = {artifact.artifact_type for artifact in result.artifacts}
    assert "industry_fundamental_scores" in artifact_types
    assert "sector_dashboard_enriched" in artifact_types
    assert result.metadata["industry_status"] == "available"
    assert result.metadata["industry_rows_scored"] == 1
    output_dir = tmp_path / "data" / "pipeline_runs" / "run-fund" / "fundamentals" / "attempt_1"
    assert (output_dir / "industry_fundamental_scores.csv").exists()
    assert (rank_dir / "sector_dashboard_enriched.csv").exists()


def _screener_financials_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE screener_financials (
                symbol TEXT,
                period_type TEXT,
                report_date DATE,
                metric_id TEXT,
                value REAL,
                available_at DATE,
                source TEXT,
                sync_batch_id TEXT,
                synced_at TIMESTAMP
            )
            """
        )
        rows = []
        reports = ["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"]
        for idx, report in enumerate(reports):
            sales = [100, 105, 110, 115, 140][idx]
            profit = [-5, 4, 5, 6, 15][idx]
            op = [10, 12, 13, 15, 25][idx]
            for metric, value in {"sales": sales, "net_profit": profit, "operating_profit": op, "expenses": sales - op}.items():
                rows.append(("AAA", "quarterly", report, metric, value, report, "fixture", "b1", "2026-01-01"))
        conn.executemany("INSERT INTO screener_financials VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    finally:
        conn.close()


def test_fundamentals_stage_exports_analytical_insight_artifacts(tmp_path: Path) -> None:
    scores_path = tmp_path / "data" / "fundamentals" / "fundamental_scores_latest.csv"
    screener_db = tmp_path / "data" / "fundamentals" / "screener_financials.db"
    _scores(scores_path)
    _screener_financials_db(screener_db)
    context = _context(
        tmp_path,
        artifacts=_rank_artifacts(tmp_path),
        params={
            "fundamental_scores_path": str(scores_path),
            "screener_financials_db_path": str(screener_db),
        },
    )

    result = FundamentalsStage().run(context)

    output_dir = tmp_path / "data" / "pipeline_runs" / "run-fund" / "fundamentals" / "attempt_1"
    artifact_types = {artifact.artifact_type for artifact in result.artifacts}
    assert "company_growth_features" in artifact_types
    assert "company_insight_tags" in artifact_types
    assert "great_results" in artifact_types
    assert "great_results_latest" in artifact_types
    assert "turnaround_candidates" in artifact_types
    assert "turnaround_candidates_latest" in artifact_types
    assert "compounder_candidates" in artifact_types
    assert "compounder_candidates_latest" in artifact_types
    assert "sector_earnings_leadership" in artifact_types
    assert "sector_earnings_latest" in artifact_types
    assert "sector_valuation_daily" in artifact_types
    assert "sector_valuation_latest" in artifact_types
    assert "universe_valuation_daily" in artifact_types
    assert "universe_valuation_latest" in artifact_types
    assert "valuation_cycle_features" in artifact_types
    assert "valuation_cycle_latest" in artifact_types
    assert "fundamental_dashboard_payload" in artifact_types
    assert (output_dir / "company_growth_features.csv").exists()
    assert (output_dir / "company_insight_tags.csv").exists()
    assert (output_dir / "great_results.csv").exists()
    assert (output_dir / "great_results_latest.csv").exists()
    assert (output_dir / "turnaround_candidates.csv").exists()
    assert (output_dir / "turnaround_candidates_latest.csv").exists()
    assert (output_dir / "compounder_candidates.csv").exists()
    assert (output_dir / "compounder_candidates_latest.csv").exists()
    assert (output_dir / "sector_valuation_daily.csv").exists()
    assert (output_dir / "valuation_cycle_latest.csv").exists()
    assert (output_dir / "fundamental_dashboard_payload.json").exists()
    assert result.metadata["fundamental_insights"]["status"] == "completed"
    assert result.metadata["fundamental_insights"]["company_growth_features"]["rows"] == 5
    artifacts = result.metadata["fundamental_insights"]["artifacts"]
    assert artifacts["fundamental_dashboard_payload"].endswith("fundamental_dashboard_payload.json")


def test_fundamentals_stage_warns_when_industry_scores_missing(tmp_path: Path) -> None:
    scores_path = tmp_path / "data" / "fundamentals" / "fundamental_scores_latest.csv"
    _scores(scores_path)
    context = _context(
        tmp_path,
        artifacts=_rank_artifacts(tmp_path),
        params={
            "fundamental_scores_path": str(scores_path),
            "industry_fundamental_scores_path": str(tmp_path / "missing_industry.csv"),
        },
    )

    result = FundamentalsStage().run(context)

    assert result.metadata["status"] == "completed"
    assert result.metadata["industry_status"] == "missing"
    assert any("Industry fundamental scores missing" in w for w in result.metadata["warnings"])


def test_fundamentals_stage_fails_when_rank_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="ranked_signals"):
        FundamentalsStage().run(_context(tmp_path, artifacts={}))


def test_orchestrator_auto_injects_fundamentals_when_snapshot_exists(tmp_path: Path) -> None:
    orchestrator = PipelineOrchestrator(tmp_path, allow_control_plane_migrations=True)
    scores_path = tmp_path / "data" / "fundamentals" / "fundamental_scores_latest.csv"

    assert orchestrator._normalize_stage_names(None) == [
        "ingest",
        *FEATURE_SUBSTAGES,
        "rank",
        "investigator",
        "candidates",
        "candidate_tracker",
        "events",
        "execute",
        "insight",
        "narrative",
        "publish",
        "perf_tracker",
    ]
    _scores(scores_path)
    assert orchestrator._normalize_stage_names(None) == [
        "ingest",
        *FEATURE_SUBSTAGES,
        "rank",
        "investigator",
        "fundamentals",
        "candidates",
        "candidate_tracker",
        "events",
        "execute",
        "insight",
        "narrative",
        "publish",
        "perf_tracker",
    ]
    assert orchestrator._normalize_stage_names(None, enable_fundamentals=True) == [
        "ingest",
        *FEATURE_SUBSTAGES,
        "rank",
        "investigator",
        "fundamentals",
        "candidates",
        "candidate_tracker",
        "events",
        "execute",
        "insight",
        "narrative",
        "publish",
        "perf_tracker",
    ]
    assert orchestrator._normalize_stage_names(["rank", "fundamentals"]) == ["rank", "fundamentals"]
    assert orchestrator._normalize_stage_names(["rank", "publish"]) == ["rank", "publish"]
