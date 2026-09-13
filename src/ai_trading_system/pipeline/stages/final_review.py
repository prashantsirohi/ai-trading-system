"""Optional artifact-only sidecar of opportunities; no operational decisions."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from ai_trading_system.domains.opportunities.policy_snapshot import (
    register_or_verify_policy_snapshots,
)
from ai_trading_system.domains.opportunities.review_projection import (
    build_review_projection,
    REVIEW_FIELDS,
    ReviewProjection,
    policy_snapshot,
)
from ai_trading_system.domains.opportunities.review_sources import load_review_sources
from ai_trading_system.pipeline.contracts import (
    StageArtifact,
    StageContext,
    StageResult,
)
from ai_trading_system.platform.db.paths import get_domain_paths


def write_review_artifacts(context: StageContext, projection) -> StageResult:
    artifacts = []
    for kind, rows in (
        ("final_review_universe", projection.universe),
        ("final_review_list", projection.ordered),
    ):
        path = context.output_dir() / f"{kind}.csv"
        fields = REVIEW_FIELDS
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        artifacts.append(
            StageArtifact.from_file(
                kind,
                path,
                row_count=len(rows),
                attempt_number=context.attempt_number,
                metadata={
                    "shadow_only": True,
                    "decision_content_hash": projection.summary[
                        "decision_content_hash"
                    ],
                },
            )
        )
    path = context.write_json("final_review_summary.json", projection.summary)
    artifacts.append(
        StageArtifact.from_file(
            "final_review_summary",
            path,
            attempt_number=context.attempt_number,
            metadata={"shadow_only": True, "status": projection.summary["status"]},
        )
    )
    return StageResult(
        artifacts=artifacts,
        metadata={"final_review_status": projection.summary["status"]},
    )


def materialize_final_review(context: StageContext) -> StageResult:
    if context.params.get("final_review_mode", "off") != "shadow":
        return StageResult()
    # This boundary contains failure to the new projection. Existing opportunity
    # history/artifacts and execution handling keep their own status and authority.
    try:
        if context.registry is None:
            raise ValueError("Final review requires registered source lineage")
        register_or_verify_policy_snapshots(
            context.registry, policy_snapshot(), run_id=context.run_id
        )
        paths = get_domain_paths(
            context.project_root,
            data_domain=context.params.get("data_domain", "operational"),
        )
        from datetime import date

        bundle = load_review_sources(
            data_root=paths.root_dir,
            run_id=context.run_id,
            session=date.fromisoformat(context.run_date),
            decision_at=datetime.now(timezone.utc),
            selected_artifacts=context.artifacts,
            ohlcv_db_path=context.db_path,
            control_plane_db_path=Path(context.registry.db_path),
            master_db_path=paths.master_db_path,
        )
        projection = build_review_projection(bundle)
        return write_review_artifacts(context, projection)
    except Exception as exc:
        # Never publish stale success or reuse a prior attempt's list on failure.
        payload = {
            "schema_version": "final-review-artifacts-v1",
            "status": "failed",
            "shadow_only": True,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "ordered_rows": 0,
            "decision_content_hash": None,
            "execution_or_lifecycle_authority_changed": False,
        }
        return write_review_artifacts(context, ReviewProjection([], [], payload, ()))
