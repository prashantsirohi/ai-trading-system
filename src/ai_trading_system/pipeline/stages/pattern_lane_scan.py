"""Optional R1a shadow-only lane-aware pattern scan stage.

Runs the lane-aware scanner in production on a strictly observational basis
(ADR-0007 R1a). It writes only new evidence artifacts that no decision
consumer reads: ranking, candidates, opportunities, execution and lifecycle
remain authoritative and untouched. The stage is non-blocking — the
orchestrator downgrades a failure to a degraded alert and the run continues.

Mirrors the shadow trio (weekly_stage / scan_router / opportunities): self-gate
on mode, register/verify the policy snapshot before any stage-owned write, time
every phase, and register downloadable artifacts. It never uses
``write_calibration_result`` (which forbids the pipeline_runs tree).
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ai_trading_system.domains.opportunities.coverage import load_daily_universe
from ai_trading_system.domains.opportunities.policy_snapshot import (
    PolicyVersionContentMismatchError,
    append_policy_snapshot_event,
    compute_policy_snapshot,
    register_or_verify_policy_snapshots,
)
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.platform.telemetry.performance import DatabasePerformanceMetric
from ai_trading_system.pipeline.contracts import (
    PipelineStageError,
    StageArtifact,
    StageContext,
    StageResult,
)
from ai_trading_system.research.pattern_lane_calibration.policy import default_r0_policy
from ai_trading_system.research.pattern_lane_calibration.shadow import (
    attach_evidence,
    build_parity_report,
    build_runtime_report,
    build_shadow_summary,
    build_source_diagnostics,
    render_shadow_report_html,
    run_lane_shadow_scan,
    source_diagnostics_frame,
)
from ai_trading_system.research.pattern_lane_calibration.stage_source import (
    MODE_GOVERNED_CURRENT,
    load_weekly_stage_observations,
)

# Governed weekly-stage classifier version R1a admits (governed_current).
WEEKLY_STAGE_POLICY_VERSION = "weekly-stage-v2"


class PatternLaneScanStageError(PipelineStageError):
    """Non-blocking R1a shadow lane-scan failure."""


class PatternLaneScanStage:
    name = "pattern_lane_scan"

    def run(self, context: StageContext) -> StageResult:
        mode = str(context.params.get("pattern_lane_scan_mode", "off")).lower()
        if mode == "off":
            return StageResult(metadata={"status": "skipped", "mode": "off"})

        # Verify policy content before any stage-owned registry write; a
        # content mismatch fails only this optional shadow stage.
        policy_snapshot = compute_policy_snapshot(context.params)
        if context.registry is not None:
            try:
                register_or_verify_policy_snapshots(context.registry, policy_snapshot, run_id=context.run_id)
            except PolicyVersionContentMismatchError as exc:
                raise PatternLaneScanStageError(str(exc)) from exc
            append_policy_snapshot_event(context.registry, policy_snapshot, run_id=context.run_id, stage_name=self.name)

        paths = get_domain_paths(context.project_root, context.params.get("data_domain", "operational"))
        control_plane_db = paths.root_dir / "control_plane.duckdb"
        exchange = str(context.params.get("exchange", "NSE"))

        load_started = time.perf_counter_ns()
        market = load_daily_universe(context.db_path, exchange=exchange, as_of=context.run_date)
        universe_ms = _record_duration(context, "pattern_lane_scan.load_daily_universe", load_started, rows_out=len(market))

        weekly_started = time.perf_counter_ns()
        try:
            weekly_stage_frame = load_weekly_stage_observations(
                mode=MODE_GOVERNED_CURRENT,
                require_stage_policy_version=WEEKLY_STAGE_POLICY_VERSION,
                control_plane_db=control_plane_db,
                ohlcv_db=context.db_path,
                through_date=context.run_date,
            )
        except RuntimeError as exc:
            raise PatternLaneScanStageError(f"weekly-stage source load failed: {exc}") from exc
        weekly_ms = _record_duration(context, "pattern_lane_scan.load_weekly_stage", weekly_started, rows_out=len(weekly_stage_frame))
        if context.performance is not None:
            context.performance.record_database_metric(DatabasePerformanceMetric(
                stage_name=self.name, operation_name="load_scan_inputs",
                query_count=2, read_query_count=2,
                db_read_ms=universe_ms + weekly_ms,
                rows_read=len(market) + len(weekly_stage_frame),
            ))

        policy = default_r0_policy()
        workers = int(context.params.get("pattern_lane_scan_workers", 1) or 1)
        scan_started = time.perf_counter_ns()
        classified, signals, invocations, timings = run_lane_shadow_scan(
            market,
            as_of_date=context.run_date,
            weekly_stage_frame=weekly_stage_frame,
            policy=policy,
            exclusion_frame=None,
            workers=workers,
        )
        symbols_scanned = int((classified["scan_lane_as_of"].astype(str) != "no_lane").sum()) if not classified.empty else 0
        _record_duration(
            context, "pattern_lane_scan.run_scan", scan_started,
            rows_in=len(market), rows_out=len(signals),
            symbols_out=symbols_scanned,
        )

        try:
            diagnostics = build_source_diagnostics(
                classified, policy=policy,
                require_stage_policy_version=WEEKLY_STAGE_POLICY_VERSION,
            )
        except ValueError as exc:
            raise PatternLaneScanStageError(str(exc)) from exc
        if int(diagnostics.get("stale_admitted_as_fresh_count", 0)):
            raise PatternLaneScanStageError(
                f"weekly-stage observations admitted as fresh while stale: "
                f"{diagnostics['stale_admitted_as_fresh_count']} rows"
            )

        legacy_artifact = context.artifact_for("rank", "pattern_scan")
        parity = build_parity_report(signals, legacy_artifact)
        scan_frame = attach_evidence(signals, classified)
        summary = build_shadow_summary(
            classified, scan_frame, diagnostics=diagnostics, parity=parity, status="completed",
        )
        runtime = build_runtime_report(
            timings, symbols_scanned=symbols_scanned,
            invocations=invocations, classified=classified,
        )

        output = context.output_dir()
        artifacts: list[StageArtifact] = []
        artifact_started = time.perf_counter_ns()
        artifact_names = [
            "pattern_lane_scan.csv", "pattern_lane_summary.json",
            "pattern_lane_runtime.json", "pattern_lane_source_diagnostics.csv",
            "pattern_lane_parity_report.json", "pattern_lane_manifest.json",
            "pattern_lane_shadow_report.html",
        ]

        csv_frames = {
            "pattern_lane_scan": ("pattern_lane_scan.csv", scan_frame),
            "pattern_lane_source_diagnostics": (
                "pattern_lane_source_diagnostics.csv", source_diagnostics_frame(diagnostics),
            ),
        }
        for artifact_type, (filename, frame) in csv_frames.items():
            artifacts.append(self._write_csv(context, output, artifact_type, filename, frame))

        artifacts.append(self._write_json(context, output, "pattern_lane_summary", "pattern_lane_summary.json", summary))
        artifacts.append(self._write_json(context, output, "pattern_lane_runtime", "pattern_lane_runtime.json", runtime))
        artifacts.append(self._write_json(context, output, "pattern_lane_parity_report", "pattern_lane_parity_report.json", parity))

        report_html = render_shadow_report_html(
            summary, diagnostics, parity, runtime,
            run_date=context.run_date, errors=[], artifact_names=artifact_names,
        )
        report_path = output / "pattern_lane_shadow_report.html"
        report_path.write_text(report_html, encoding="utf-8")
        artifacts.append(self._register(context, "pattern_lane_shadow_report", report_path, None))

        manifest = self._build_manifest(
            market=market, weekly_stage_frame=weekly_stage_frame, policy=policy,
            written=[art for art in artifacts], summary=summary,
        )
        artifacts.append(self._write_json(context, output, "pattern_lane_manifest", "pattern_lane_manifest.json", manifest))
        _record_duration(context, "pattern_lane_scan.write_artifacts", artifact_started, rows_out=len(scan_frame))
        return StageResult(artifacts=artifacts, metadata=summary)

    def _write_csv(self, context: StageContext, output: Path, artifact_type: str, filename: str, frame: pd.DataFrame) -> StageArtifact:
        write_started = time.perf_counter_ns()
        path = output / filename
        frame.to_csv(path, index=False)
        artifact = StageArtifact.from_file(artifact_type, path, row_count=len(frame), attempt_number=context.attempt_number)
        if context.performance is not None:
            context.performance.record_artifact(artifact, column_count=len(frame.columns), write_duration_ms=(time.perf_counter_ns() - write_started) / 1_000_000.0)
        return artifact

    def _write_json(self, context: StageContext, output: Path, artifact_type: str, filename: str, payload: dict) -> StageArtifact:
        write_started = time.perf_counter_ns()
        path = output / filename
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        return self._register(context, artifact_type, path, None, write_started=write_started)

    def _register(self, context: StageContext, artifact_type: str, path: Path, row_count: int | None, *, write_started: int | None = None) -> StageArtifact:
        artifact = StageArtifact.from_file(artifact_type, path, row_count=row_count, attempt_number=context.attempt_number)
        if context.performance is not None:
            context.performance.record_artifact(
                artifact,
                write_duration_ms=(time.perf_counter_ns() - write_started) / 1_000_000.0 if write_started else None,
            )
        return artifact

    def _build_manifest(self, *, market: pd.DataFrame, weekly_stage_frame: pd.DataFrame, policy, written: list[StageArtifact], summary: dict) -> dict:
        dataset_hashes = {Path(art.uri).name: art.content_hash for art in written}
        row_counts = {Path(art.uri).name: art.row_count for art in written if art.row_count is not None}
        return {
            "schema_version": "pattern-r1a-manifest-v1",
            "builder_version": "pattern-r1a-shadow-builder-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "code_commit": _git_commit(),
            "policy_version": policy.version,
            "policy_hash": policy.content_hash,
            "policy_versions": {
                "standard_liquidity": policy.standard_liquidity.version,
                "early_ipo_liquidity": policy.early_ipo_liquidity.version,
                "stage2": policy.stage2.version,
                "weekly_freshness": policy.weekly_freshness.version,
                "stage1_structure": policy.stage1.version,
                "families": policy.families.version,
                "outcomes": policy.outcomes.version,
                "reconstruction": policy.reconstruction.version,
            },
            "weekly_stage_policy_version": WEEKLY_STAGE_POLICY_VERSION,
            "source_hashes": {
                "daily_universe": _frame_sha(market),
                "weekly_stage_frame": _frame_sha(weekly_stage_frame),
            },
            "source_row_counts": {
                "daily_universe": int(len(market)),
                "weekly_stage_frame": int(len(weekly_stage_frame)),
            },
            "dataset_hashes": dataset_hashes,
            "row_counts": row_counts,
            "symbols_scanned": summary.get("symbols_scanned"),
            "operational_side_effects": False,
        }


def _record_duration(context: StageContext, operation: str, started_ns: int, **counts: int) -> float:
    duration_ms = max((time.perf_counter_ns() - started_ns) / 1_000_000.0, 0.0)
    if context.performance is not None:
        context.performance.record_duration(stage_name="pattern_lane_scan", operation_name=operation, duration_ms=duration_ms, **counts)
    return duration_ms


def _frame_sha(frame: pd.DataFrame) -> str:
    if frame is None or frame.empty:
        return hashlib.sha256(b"").hexdigest()
    normalized = frame.reindex(sorted(frame.columns), axis=1)
    return hashlib.sha256(normalized.to_csv(index=False, lineterminator="\n").encode("utf-8")).hexdigest()


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, timeout=5,
        ).decode().strip()
    except (subprocess.SubprocessError, OSError):
        return "unknown"
