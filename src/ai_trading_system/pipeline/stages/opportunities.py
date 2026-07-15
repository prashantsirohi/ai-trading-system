"""Optional Phase 3 shadow opportunity-registry stage."""

from __future__ import annotations

import csv
from datetime import date, datetime, time, timezone
from pathlib import Path

from ai_trading_system.domains.opportunities.orchestration.contracts import OpportunityRegistryMode, OpportunityShadowConfig
from ai_trading_system.domains.opportunities.orchestration.service import OpportunityArtifactSet, OpportunityShadowOrchestrator, OpportunityShadowSourceError
from ai_trading_system.pipeline.contracts import PipelineStageError, StageArtifact, StageContext, StageResult
from ai_trading_system.pipeline.alerts import AlertManager


class OpportunityStageError(PipelineStageError):
    """A shadow-stage failure that must not block execution or publishing."""


class OpportunityStage:
    name = "opportunities"

    def run(self, context: StageContext) -> StageResult:
        config = OpportunityShadowConfig.from_mapping(context.params)
        if config.mode is OpportunityRegistryMode.OFF:
            return StageResult(metadata={"status": "skipped", "mode": "off"})
        ranked = context.artifact_for("rank", "ranked_signals")
        if ranked is None:
            raise OpportunityStageError("shadow opportunities requires the registered rank/ranked_signals artifact")
        phase3b_shadow = str(context.params.get("opportunity_scan_routing_mode", "off")).lower() == "shadow"
        artifact_set = OpportunityArtifactSet(
            ranked_signals=ranked,
            investigator_scores=(context.artifact_for("investigator", "routed_investigator_scores") if phase3b_shadow else None) or context.artifact_for("investigator", "investigator_scores"),
            breakout_scan=context.artifact_for("rank", "breakout_scan"),
            pattern_scan=(context.artifact_for("investigator", "routed_pattern_scan") if phase3b_shadow else None) or context.artifact_for("rank", "pattern_scan"),
            stock_scan=(context.artifact_for("weekly_stage", "weekly_stock_stage_universe") if phase3b_shadow else None) or context.artifact_for("rank", "stock_scan"),
            sector_dashboard=(context.artifact_for("weekly_stage", "weekly_sector_stage_universe") if phase3b_shadow else None) or context.artifact_for("rank", "sector_dashboard"),
            lifecycle_state=(context.artifact_for("investigator", "stage1_current_state") or context.artifact_for("investigator", "stage1_state")),
            scan_routing=context.artifact_for("scan_router", "scan_routing") if phase3b_shadow else None,
        )
        as_of = datetime.combine(date.fromisoformat(context.run_date), time.min, tzinfo=timezone.utc)
        try:
            result = OpportunityShadowOrchestrator(context.registry).run(
                run_id=context.run_id,
                stage_attempt=context.attempt_number,
                artifact_set=artifact_set,
                as_of=as_of,
                mode=config.mode,
                config=config,
                ohlcv_db_path=context.db_path,
            )
        except OpportunityShadowSourceError as exc:
            raise OpportunityStageError(str(exc)) from exc
        except Exception as exc:
            raise OpportunityStageError(f"opportunity shadow orchestration failed: {exc}") from exc
        output_dir = context.output_dir()
        artifacts: list[StageArtifact] = []
        summary_path = context.write_json("opportunity_shadow_summary.json", dict(result.summary))
        artifacts.append(StageArtifact.from_file("opportunity_shadow_summary", summary_path, metadata={"status": result.status, "dry_run": result.dry_run}, attempt_number=context.attempt_number))
        filenames = {
            "candidate_admissions": "candidate_admissions.csv",
            "candidate_updates": "candidate_updates.csv",
            "candidate_transitions": "candidate_transitions.csv",
            "candidate_closures": "candidate_closures.csv",
            "candidate_reconciliation": "candidate_reconciliation.csv",
            "adapter_warnings": "adapter_warnings.csv",
            "adapter_rejections": "adapter_rejections.csv",
            "registry_conflicts": "registry_conflicts.csv",
            "current_candidate_state": "current_candidate_state.csv",
            "position_episode_compatibility": "position_episode_compatibility.csv",
            "position_recovery_proposals": "position_recovery_proposals.csv",
            "position_recovery_actions": "position_recovery_actions.csv",
            "position_monitor_reconciliation": "position_monitor_reconciliation.csv",
        }
        for artifact_type, filename in filenames.items():
            rows = [dict(row) for row in result.artifact_rows.get(artifact_type, ())]
            path = output_dir / filename
            _write_csv(path, rows)
            artifacts.append(StageArtifact.from_file(artifact_type, path, row_count=len(rows), attempt_number=context.attempt_number))
        if context.registry is not None:
            manager = AlertManager(context.registry)
            for row in result.artifact_rows.get("position_episode_compatibility", ()):
                status = str(row.get("compatibility_status") or "")
                if status in {"compatible", "no_open_episode"}:
                    continue
                cycle_id = str(row.get("position_cycle_id") or "unknown")
                manager.emit_incident(
                    run_id=context.run_id,
                    alert_type="position_episode_reconciliation_conflict",
                    severity="critical",
                    stage_name="opportunities",
                    message=f"active position episode compatibility conflict: {status}",
                    dedupe_key=f"position_episode_reconciliation_conflict|{cycle_id}|{status}",
                    payload=dict(row),
                )
        context.report_task(task_name="opportunity_shadow", status="degraded" if result.status == "degraded" else "completed", metadata=dict(result.summary))
        return StageResult(artifacts=artifacts, metadata=dict(result.summary))


def _write_csv(path: Path, rows: list[dict]) -> None:
    fields = sorted({key for row in rows for key in row}) or ["status"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
