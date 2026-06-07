"""Candidate lifecycle tracker pipeline stage."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.candidate_tracker import CandidateTrackerConfig, run_candidate_tracker
from ai_trading_system.domains.candidate_tracker.service import read_csv_optional
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class CandidateTrackerStage:
    """Track live candidate episodes independently of research perf tracking."""

    name = "candidate_tracker"

    def run(self, context: StageContext) -> StageResult:
        candidate_artifact = context.require_artifact("candidates", "final_candidates")
        paths = get_domain_paths(project_root=context.project_root, data_domain="operational")
        db_path = Path(context.params.get("candidate_tracker_db_path") or (paths.root_dir / "candidate_tracker.duckdb"))

        result = run_candidate_tracker(
            config=CandidateTrackerConfig(
                db_path=db_path,
                ohlcv_db_path=context.db_path,
                run_date=context.run_date,
                run_id=context.run_id,
                max_age_days=int(context.params.get("candidate_tracker_max_age_days", 365) or 365),
                review_window_days=int(context.params.get("candidate_tracker_review_window_days", 120) or 120),
                archive_failures=bool(context.params.get("candidate_tracker_archive_failures", False)),
            ),
            final_candidates=read_csv_optional(candidate_artifact.uri),
            watchlist_candidates=_read_optional(context.artifact_for("fundamentals", "watchlist_candidates")),
            quarterly_result_scores=_read_optional(context.artifact_for("fundamentals", "quarterly_result_scores")),
            stock_valuation_bands_latest=_read_optional(context.artifact_for("fundamentals", "stock_valuation_bands_latest")),
            ranked_signals=_read_optional(context.artifact_for("rank", "ranked_signals")),
            sector_dashboard=_read_optional(
                context.artifact_for("rank", "sector_dashboard")
                or context.artifact_for("fundamentals", "sector_dashboard_enriched")
            ),
        )

        output_dir = context.output_dir()
        current_path = output_dir / "candidate_tracker_current.csv"
        alerts_path = output_dir / "candidate_tracker_alerts.csv"
        summary_path = output_dir / "candidate_tracker_summary.json"
        reviews_path = output_dir / "candidate_fundamental_reviews.csv"
        snapshots_path = output_dir / "candidate_tracking_snapshots.csv"

        result.current.to_csv(current_path, index=False)
        result.alerts.to_csv(alerts_path, index=False)
        result.fundamental_reviews.to_csv(reviews_path, index=False)
        result.snapshots.to_csv(snapshots_path, index=False)
        context.write_json("candidate_tracker_summary.json", result.summary)

        artifacts = [
            StageArtifact.from_file(
                "candidate_tracker_current",
                current_path,
                row_count=len(result.current),
                metadata={"columns": list(result.current.columns), "db_path": str(db_path)},
                attempt_number=context.attempt_number,
            ),
            StageArtifact.from_file(
                "candidate_tracker_alerts",
                alerts_path,
                row_count=len(result.alerts),
                metadata={"columns": list(result.alerts.columns)},
                attempt_number=context.attempt_number,
            ),
            StageArtifact.from_file(
                "candidate_tracker_summary",
                summary_path,
                row_count=1,
                metadata=result.summary,
                attempt_number=context.attempt_number,
            ),
            StageArtifact.from_file(
                "candidate_fundamental_reviews",
                reviews_path,
                row_count=len(result.fundamental_reviews),
                metadata={"columns": list(result.fundamental_reviews.columns)},
                attempt_number=context.attempt_number,
            ),
            StageArtifact.from_file(
                "candidate_tracking_snapshots",
                snapshots_path,
                row_count=len(result.snapshots),
                metadata={"columns": list(result.snapshots.columns)},
                attempt_number=context.attempt_number,
            ),
        ]
        return StageResult(artifacts=artifacts, metadata=result.summary)


def _read_optional(artifact: StageArtifact | None) -> pd.DataFrame:
    if artifact is None:
        return pd.DataFrame()
    return read_csv_optional(artifact.uri)

