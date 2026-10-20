"""Artifact-only daily review, after tracking/execution and before delivery."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import io
import json
from pathlib import Path

import pandas as pd

from ai_trading_system.domains.opportunities.daily_review import (
    CHANGE_FIELDS,
    FIELDS,
    POLICY,
    POLICY_HASH,
    POLICY_VERSION,
    Source,
    build_daily_review,
    digest,
    SOURCE_TYPES,
)
from ai_trading_system.domains.opportunities.policy_snapshot import (
    PolicySnapshot,
    register_or_verify_policy_snapshots,
)
from ai_trading_system.domains.opportunities.review_sources import (
    _path,
    digest_bytes,
    utc,
)
from ai_trading_system.pipeline.contracts import (
    PipelineStageError,
    StageArtifact,
    StageContext,
    StageResult,
)
from ai_trading_system.platform.db.paths import get_domain_paths

SOURCES = SOURCE_TYPES
PERFORMANCE_TYPES = {
    "opportunity_convergence_primary_cohorts",
    "opportunity_convergence_performance_readiness",
    "investigator_discovery_scorecard",
    "investigator_entry_scorecard",
    "technical_evidence_cohorts",
}


class DailyReviewStageError(PipelineStageError):
    """Optional review failure must not suppress existing publishing."""


def read_record(record: dict, root: Path, cutoff: datetime) -> tuple[object, dict]:
    available = max(utc(record[n]) for n in ("created_at", "promoted_at", "ended_at"))
    if available > cutoff:
        raise ValueError("SOURCE_AFTER_CUTOFF")
    path = _path(
        root,
        record["uri"],
        record["run_id"],
        record["stage_name"],
        record["attempt_number"],
    )
    raw = path.read_bytes()
    if not record["content_hash"] or digest_bytes(raw) != record["content_hash"]:
        raise ValueError("ARTIFACT_HASH_MISMATCH")
    lineage = {
        n: str(record[n])
        for n in (
            "run_id",
            "stage_name",
            "attempt_number",
            "artifact_type",
            "content_hash",
        )
    }
    lineage.update(
        session=str(record["run_date"])[:10],
        available_at=available.isoformat(),
        uri=str(path),
    )
    if path.suffix == ".json":
        return json.loads(raw), lineage
    frame = pd.read_csv(io.BytesIO(raw), keep_default_na=False)
    if record["row_count"] is None or len(frame) != int(record["row_count"]):
        raise ValueError("ARTIFACT_ROW_COUNT_MISMATCH")
    return frame.to_dict("records"), lineage


def load_inputs(context: StageContext, root: Path, cutoff: datetime):
    records = context.registry.get_completed_artifact_records(run_id=context.run_id)
    selected = {}
    for record in records:
        selected.setdefault((record["stage_name"], record["artifact_type"]), record)
    sources, payloads = {}, {}
    for name, key in SOURCES.items():
        record = selected.get(key)
        if record is None:
            sources[name] = Source(issue="SOURCE_MISSING")
            continue
        try:
            if str(record["run_date"])[:10] != context.run_date:
                raise ValueError("SOURCE_SESSION_MISMATCH")
            data, lineage = read_record(record, root, cutoff)
            if name in {"final_summary", "execution_summary"}:
                if not isinstance(data, dict):
                    raise ValueError("INVALID_SUMMARY")
                payloads[name] = data
                sources[name] = Source(lineage=lineage)
            else:
                if not isinstance(data, list):
                    raise ValueError("INVALID_TABLE")
                sources[name] = Source(data, lineage)
        except Exception as exc:
            sources[name] = Source(issue=f"{type(exc).__name__}:{exc}")
    # List and universe are inseparable decisions from one final-review attempt.
    final_records = [
        selected.get(SOURCES[n])
        for n in ("final_list", "final_universe", "final_summary")
    ]
    if all(final_records):
        attempts = {r["attempt_number"] for r in final_records}
        expected = payloads.get("final_summary", {}).get("decision_content_hash")
        valid_hashes = all(
            json.loads(r.get("metadata_json") or "{}").get("decision_content_hash")
            == expected
            for r in final_records[:2]
        )
        if len(attempts) != 1 or not expected or not valid_hashes:
            for name in ("final_list", "final_universe", "final_summary"):
                sources[name] = Source(issue="FINAL_REVIEW_PACK_MISMATCH")
            payloads.pop("final_summary", None)
    performance = []
    for key, record in selected.items():
        if key[1] in PERFORMANCE_TYPES:
            try:
                data, lineage = read_record(record, root, cutoff)
                performance.append(
                    {
                        **lineage,
                        "rows": len(data),
                        "interpretation": "Separate population/anchor/policy; no pooled score or return",
                    }
                )
            except Exception as exc:
                performance.append(
                    {
                        "artifact_type": key[1],
                        "status": "UNAVAILABLE",
                        "reason": str(exc),
                    }
                )
    return sources, payloads, performance


def previous_pack(context: StageContext, root: Path, cutoff: datetime):
    records = context.registry.get_completed_artifact_records(
        stage_name="daily_review", before_date=context.run_date
    )
    packs = {}
    for record in records:
        packs.setdefault((record["run_id"], record["attempt_number"]), {})[
            record["artifact_type"]
        ] = record
    for pack in packs.values():
        if not all(
            n in pack
            for n in (
                "daily_review_list",
                "daily_review_changes",
                "daily_review_summary",
            )
        ):
            continue
        try:
            summary, _ = read_record(pack["daily_review_summary"], root, cutoff)
            rows, _ = read_record(pack["daily_review_list"], root, cutoff)
            changes, _ = read_record(pack["daily_review_changes"], root, cutoff)
            if (
                summary.get("policy_hash") == POLICY_HASH
                and summary.get("status") in {"complete", "degraded"}
                and summary.get("run_id") == pack["daily_review_summary"]["run_id"]
                and summary.get("session")
                == str(pack["daily_review_summary"]["run_date"])[:10]
                and summary.get("shadow_only") is True
                and verify_pack(rows, changes, summary)
            ):
                return rows, summary
        except (OSError, ValueError, TypeError, KeyError):
            continue
    return None, None


def verify_pack(rows: list[dict], changes: list[dict], summary: dict) -> bool:
    """Verify the exact CSV-serialized decisions that the publisher will consume."""
    return (
        summary.get("rows") == len(rows)
        and summary.get("changes") == len(changes)
        and summary.get("serialized_content_hash")
        == digest({"rows": rows, "changes": changes})
    )


class DailyReviewStage:
    name = "daily_review"

    def run(self, context: StageContext) -> StageResult:
        if context.params.get("daily_review_mode", "off") != "shadow":
            return StageResult()
        try:
            if context.registry is None:
                raise ValueError("Daily review requires registered producer lineage")
            register_or_verify_policy_snapshots(
                context.registry,
                PolicySnapshot(
                    POLICY_HASH, {POLICY_VERSION: POLICY_HASH}, {POLICY_VERSION: POLICY}
                ),
                run_id=context.run_id,
            )
            root = get_domain_paths(
                context.project_root,
                data_domain=context.params.get("data_domain", "operational"),
            ).root_dir
            cutoff = datetime.now(timezone.utc)
            sources, payloads, performance = load_inputs(context, root, cutoff)
            previous, prior_summary = previous_pack(context, root, cutoff)
            rows, changes, summary = build_daily_review(
                session=context.run_date,
                sources=sources,
                final_summary=payloads.get("final_summary", {}),
                execution_summary=payloads.get("execution_summary", {}).get(
                    "summary", {}
                ),
                previous=previous,
                previous_summary=prior_summary,
            )
            summary.update(run_id=context.run_id, performance_diagnostics=performance)
            artifacts = []
            serialized = {}
            for kind, data, fields in (
                ("daily_review_list", rows, FIELDS),
                ("daily_review_changes", changes, CHANGE_FIELDS),
            ):
                path = context.output_dir() / f"{kind}.csv"
                with path.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields)
                    writer.writeheader()
                    writer.writerows(data)
                # Freeze CSV types for exact consumer-side pack verification.
                serialized[kind] = pd.read_csv(path, keep_default_na=False).to_dict(
                    "records"
                )
                artifacts.append(
                    StageArtifact.from_file(
                        kind,
                        path,
                        row_count=len(data),
                        attempt_number=context.attempt_number,
                        metadata={"policy_hash": POLICY_HASH, "shadow_only": True},
                    )
                )
            summary["serialized_content_hash"] = digest(
                {
                    "rows": serialized["daily_review_list"],
                    "changes": serialized["daily_review_changes"],
                }
            )
            path = context.write_json("daily_review_summary.json", summary)
            artifacts.append(
                StageArtifact.from_file(
                    "daily_review_summary",
                    path,
                    attempt_number=context.attempt_number,
                    metadata={"policy_hash": POLICY_HASH, "shadow_only": True},
                )
            )
            return StageResult(
                artifacts,
                {"status": summary["status"], "rows": len(rows), "shadow_only": True},
            )
        except Exception as exc:
            raise DailyReviewStageError(str(exc)) from exc
