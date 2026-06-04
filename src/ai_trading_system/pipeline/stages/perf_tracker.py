"""Performance-tracker pipeline stage (Phase 0 of feedback loop).

Runs after ``publish``. Calls the perf_tracker backfill function with no date
filter — this is intentionally simple:

  * Idempotent (DELETE+INSERT keyed on run_date).
  * Picks up today's ranked_signals.csv automatically (publish stage just wrote it).
  * Re-matures any historical rows whose forward-return horizons just hit
    today (the backfill recomputes fwd returns for every date it processes).
  * Fast at current scale: 35k rows in < 1s. Will stay fast for 252-day
    history at ~150k rows. Promote to incremental processing only if needed.

Failures here must NOT block the pipeline — measurement is observability,
not a hard dependency. The stage logs and returns an empty result on error.
"""

from __future__ import annotations

import logging

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult
from ai_trading_system.research.perf_tracker.backfill import run_backfill
from ai_trading_system.research.perf_tracker.health import build_tracker_health

logger = logging.getLogger(__name__)


class PerfTrackerStage:
    """Append today's rank cohort + re-mature pending forward returns."""

    name = "perf_tracker"

    def run(self, context: StageContext) -> StageResult:
        try:
            result = run_backfill(project_root=context.project_root)
        except Exception as exc:  # pragma: no cover - intentional broad catch
            # Tracker is observability; never block the pipeline on its failure.
            logger.warning("perf_tracker stage failed: %s", exc, exc_info=True)
            metadata = {"status": "failed", "error": str(exc)}
            artifact_path = context.write_json("perf_tracker_summary.json", metadata)
            return StageResult(
                artifacts=[StageArtifact.from_file(
                    "perf_tracker_summary",
                    artifact_path,
                    metadata=metadata,
                    attempt_number=context.attempt_number,
                )],
                metadata=metadata,
            )

        metadata = {
            "status": "ok",
            "dates_processed": int(result.get("dates_processed", 0)),
            "rows_upserted": int(result.get("rows_upserted", 0)),
        }
        health = build_tracker_health(project_root=context.project_root)
        metadata["tracker_health_status"] = health["status"]
        artifact_path = context.write_json("perf_tracker_summary.json", metadata)
        health_path = context.write_json("tracker_health.json", health)
        return StageResult(
            artifacts=[
                StageArtifact.from_file(
                    "perf_tracker_summary",
                    artifact_path,
                    row_count=metadata["rows_upserted"],
                    metadata=metadata,
                    attempt_number=context.attempt_number,
                ),
                StageArtifact.from_file(
                    "tracker_health",
                    health_path,
                    metadata=health,
                    attempt_number=context.attempt_number,
                ),
            ],
            metadata=metadata,
        )
