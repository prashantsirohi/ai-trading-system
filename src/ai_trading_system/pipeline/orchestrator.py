"""Single-agent pipeline orchestrator with stage isolation and governance metadata."""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from ai_trading_system.analytics.dq import DataQualityEngine
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.domains.ingest.service import IngestOrchestrationService
from ai_trading_system.pipeline.contracts import DataQualityCriticalError, PublishStageError, StageContext, StageResult
from ai_trading_system.platform.utils.env import load_project_env
from ai_trading_system.platform.logging import logger as logging_module
from ai_trading_system.platform.db.paths import canonicalize_project_root, ensure_domain_layout
from ai_trading_system.pipeline.alerts import AlertManager
from ai_trading_system.pipeline.preflight import PreflightChecker
from ai_trading_system.pipeline.stages import ExecuteStage, FeaturesStage, IngestStage, PublishStage, RankStage

load_project_env(__file__)

configure_terminal_output = logging_module.configure_terminal_output
log_context = logging_module.log_context
logger = logging_module.logger


PIPELINE_ORDER = ["ingest", "features", "rank", "execute", "publish"]
SUPPORTED_STAGES = ["ingest", "features", "rank", "execute", "publish"]


class TerminalProgressRenderer:
    """Compact terminal renderer for stage/task execution."""

    def __init__(self, mode: str = "compact"):
        self.mode = str(mode or "compact").strip().lower()
        self.stage_states: dict[str, str] = {}
        self.task_states: dict[str, str] = {}
        self.degraded: list[str] = []
        self.failed: list[str] = []

    def _stamp(self) -> str:
        return datetime.now().strftime("%H:%M:%S")

    def emit_run_header(self, *, run_id: str, run_date: str, data_domain: str, stages: list[str]) -> None:
        if self.mode == "verbose":
            return
        print(f"{self._stamp()} | Run: {run_id} | Date: {run_date} | Domain: {data_domain}", flush=True)
        print(f"{self._stamp()} | Stages: {', '.join(stages)}", flush=True)

    def emit_stage(self, *, stage_name: str, status: str, detail: str | None = None) -> None:
        if self.mode == "verbose":
            return
        self.stage_states[stage_name] = status
        label = f"{stage_name}"
        if detail:
            label = f"{label} - {detail}"
        print(f"{self._stamp()} | [{status[:4]:<4}] {label}", flush=True)

    def emit_task(self, payload: dict[str, object]) -> None:
        if self.mode == "verbose":
            return
        stage_name = str(payload.get("stage_name", ""))
        task_name = str(payload.get("task_name", ""))
        status = str(payload.get("status", "running"))
        detail = str(payload.get("detail") or "").strip()
        key = f"{stage_name}.{task_name}" if stage_name else task_name
        self.task_states[key] = status
        if status in {"failed", "timed_out"}:
            self.failed.append(key)
        elif status in {"degraded"}:
            self.degraded.append(key)
        suffix = f" - {detail}" if detail else ""
        print(f"{self._stamp()} | [{status[:4]:<4}] {key}{suffix}", flush=True)

    def emit_final(self, *, run_id: str, status: str, stages: list[dict[str, object]], error: str | None = None) -> None:
        if self.mode == "verbose":
            return
        print("", flush=True)
        print(f"{self._stamp()} | Run Complete: {run_id} -> {status}", flush=True)
        completed = [stage["stage_name"] for stage in stages if str(stage.get("status")) == "completed"]
        if completed:
            print(f"{self._stamp()} | Completed stages: {', '.join(completed)}", flush=True)
        if self.degraded:
            print(f"{self._stamp()} | Degraded tasks: {', '.join(sorted(set(self.degraded)))}", flush=True)
        if self.failed:
            print(f"{self._stamp()} | Failed tasks: {', '.join(sorted(set(self.failed)))}", flush=True)
        if error:
            print(f"{self._stamp()} | Final status detail: {error}", flush=True)


class PipelineOrchestrator:
    """Executes the resilient pipeline with retry-safe metadata."""

    def __init__(
        self,
        project_root: Path | str,
        registry: Optional[RegistryStore] = None,
        dq_engine: Optional[DataQualityEngine] = None,
        alert_manager: Optional[AlertManager] = None,
        stages: Optional[Dict[str, object]] = None,
        progress_renderer: Optional[TerminalProgressRenderer] = None,
    ):
        self.project_root = canonicalize_project_root(project_root)
        self.registry = registry or RegistryStore(self.project_root)
        self.dq_engine = dq_engine or DataQualityEngine(self.registry)
        self.alert_manager = alert_manager or AlertManager(self.registry)
        self.preflight_checker = PreflightChecker(self.project_root)
        default_stages = {
            "ingest": IngestStage(),
            "features": FeaturesStage(),
            "rank": RankStage(),
            "execute": ExecuteStage(),
            "publish": PublishStage(),
        }
        if stages:
            default_stages.update(stages)
        self.stages = default_stages
        self.progress_renderer = progress_renderer
        self._stage_hints = {
            "ingest": "fetching OHLCV and validating source data",
            "features": "computing technical features and writing feature store",
            "rank": "scoring symbols and building ranked outputs",
            "execute": "evaluating execution actions",
            "publish": "publishing reports and channel payloads",
        }

    def _read_json_artifact(self, artifact_uri: str) -> Dict[str, object]:
        try:
            return json.loads(Path(artifact_uri).read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _ingest_fingerprint_from_summary(self, summary: Dict[str, object]) -> str:
        fingerprint = str(summary.get("downstream_input_fingerprint") or "").strip()
        if fingerprint:
            return fingerprint
        return IngestOrchestrationService.build_downstream_input_fingerprint(summary)

    def _plan_downstream_stage_skips(
        self,
        *,
        run_id: str,
        stage_names: list[str],
        ingest_metadata: Dict[str, object],
    ) -> Dict[str, object] | None:
        requested_downstream = [stage for stage in stage_names if stage in PIPELINE_ORDER and PIPELINE_ORDER.index(stage) > PIPELINE_ORDER.index("ingest")]
        if not requested_downstream:
            return None

        fingerprint = self._ingest_fingerprint_from_summary(ingest_metadata)
        if bool(ingest_metadata.get("downstream_skip_eligible")):
            return {
                "stages": requested_downstream,
                "reason_code": "no_new_ingest_data",
                "detail": "ingest reported no new catalog updates",
                "fingerprint": fingerprint or None,
            }

        if not fingerprint:
            return None

        previous_artifacts = self.registry.get_latest_artifact(
            stage_name="ingest",
            artifact_type="ingest_summary",
            limit=1,
            exclude_run_id=run_id,
            run_status="completed",
        )
        if not previous_artifacts:
            return None

        previous_summary = self._read_json_artifact(previous_artifacts[0].uri)
        previous_fingerprint = self._ingest_fingerprint_from_summary(previous_summary)
        if previous_fingerprint and previous_fingerprint == fingerprint:
            return {
                "stages": requested_downstream,
                "reason_code": "unchanged_ingest_inputs",
                "detail": "ingest inputs unchanged from previous successful run",
                "fingerprint": fingerprint,
                "previous_ingest_summary_uri": previous_artifacts[0].uri,
            }
        return None

    def _record_skipped_stage(
        self,
        *,
        run_id: str,
        stage_name: str,
        detail: str,
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        attempt_number = self.registry.next_stage_attempt(run_id, stage_name)
        stage_run_id = self.registry.start_stage(run_id, stage_name, attempt_number)
        self.registry.finish_stage(stage_run_id, status="skipped", metadata=metadata or {})
        if self.progress_renderer is not None:
            self.progress_renderer.emit_stage(stage_name=stage_name, status="skip", detail=detail)

    def _start_stage_heartbeat(
        self,
        *,
        stage_name: str,
        attempt_number: int,
        interval_seconds: int = 30,
    ) -> tuple[threading.Event, threading.Thread] | None:
        if self.progress_renderer is None:
            return None
        if getattr(self.progress_renderer, "mode", "compact") == "verbose":
            return None
        interval_seconds = max(15, int(interval_seconds))
        stop_event = threading.Event()
        start_ts = time.time()
        hint = self._stage_hints.get(stage_name, "processing")

        def _heartbeat() -> None:
            while not stop_event.wait(interval_seconds):
                elapsed = int(time.time() - start_ts)
                self.progress_renderer.emit_stage(
                    stage_name=stage_name,
                    status="running",
                    detail=f"attempt {attempt_number} · {hint} · elapsed {elapsed}s",
                )

        worker = threading.Thread(target=_heartbeat, name=f"stage-heartbeat-{stage_name}", daemon=True)
        worker.start()
        return stop_event, worker

    def run_pipeline(
        self,
        run_id: Optional[str] = None,
        stage_names: Optional[Iterable[str]] = None,
        run_date: Optional[str] = None,
        trigger: str = "manual",
        params: Optional[Dict] = None,
    ) -> Dict:
        params = params or {}
        stage_names = self._normalize_stage_names(stage_names)
        run_date = run_date or date.today().isoformat()
        run_id = run_id or self._build_run_id(run_date)
        data_domain = params.get("data_domain", "operational")
        domain_paths = ensure_domain_layout(project_root=self.project_root, data_domain=data_domain)
        new_run = not self.registry.run_exists(run_id)

        if new_run:
            self.registry.create_run(
                run_id=run_id,
                pipeline_name="daily_pipeline",
                run_date=run_date,
                trigger=trigger,
                metadata={"requested_stages": list(stage_names), "params": params},
            )
        else:
            self.registry.append_run_metadata_event(
                run_id,
                {
                    "event_type": "retry_requested",
                    "requested_at": datetime.now(timezone.utc).isoformat(),
                    "requested_stages": list(stage_names),
                    "trigger": trigger,
                    "params": params,
                },
            )
        with log_context(run_id=run_id):
            if self.progress_renderer is not None:
                self.progress_renderer.emit_run_header(
                    run_id=run_id,
                    run_date=run_date,
                    data_domain=str(data_domain),
                    stages=list(stage_names),
                )
            if params.get("preflight", True):
                preflight = self.preflight_checker.run(stage_names, params)
                run_metadata = self.registry.get_run(run_id).get("metadata", {})
                run_metadata["preflight"] = preflight
                self.registry.update_run(run_id, status="running", metadata=run_metadata)
                if preflight["status"] != "passed":
                    message = f"Preflight failed: {preflight['blocking_failures']}"
                    self.alert_manager.emit(
                        run_id=run_id,
                        alert_type="preflight_failed",
                        severity="critical",
                        message=message,
                    )
                    self.registry.update_run(
                        run_id,
                        status="failed",
                        current_stage=stage_names[0] if stage_names else None,
                        error_class="PreflightFailed",
                        error_message=message,
                        finished=True,
                    )
                    raise RuntimeError(message)

            final_status = "completed"
            planned_stage_skips: dict[str, dict[str, object]] = {}

            for stage_name in stage_names:
                stage_runs = self.registry.get_stage_runs(run_id)
                latest_attempt = None
                for run in reversed(stage_runs):
                    if run["stage_name"] == stage_name:
                        latest_attempt = run
                        break
                if latest_attempt and latest_attempt.get("status") == "completed":
                    if self.progress_renderer is not None:
                        self.progress_renderer.emit_stage(
                            stage_name=stage_name,
                            status="skip",
                            detail="already completed",
                        )
                    continue

                skip_plan = planned_stage_skips.get(stage_name)
                if skip_plan is not None:
                    self._record_skipped_stage(
                        run_id=run_id,
                        stage_name=stage_name,
                        detail=str(skip_plan.get("detail", "skipped")),
                        metadata=skip_plan,
                    )
                    continue

                self.registry.update_run(run_id, status="running", current_stage=stage_name)
                attempt_number = self.registry.next_stage_attempt(run_id, stage_name)
                stage_run_id = self.registry.start_stage(run_id, stage_name, attempt_number)
                if self.progress_renderer is not None:
                    self.progress_renderer.emit_stage(
                        stage_name=stage_name,
                        status="running",
                        detail=f"attempt {attempt_number}",
                    )
                artifacts = self.registry.get_artifact_map(run_id)
                context = StageContext(
                    project_root=self.project_root,
                    db_path=domain_paths.ohlcv_db_path,
                    run_id=run_id,
                    run_date=run_date,
                    stage_name=stage_name,
                    attempt_number=attempt_number,
                    registry=self.registry,
                    params=params,
                    artifacts=artifacts,
                    task_reporter=(
                        self.progress_renderer.emit_task if self.progress_renderer is not None else None
                    ),
                )

                with log_context(run_id=run_id, stage_name=stage_name, attempt_number=attempt_number):
                    try:
                        stage = self.stages[stage_name]
                        heartbeat = self._start_stage_heartbeat(
                            stage_name=stage_name,
                            attempt_number=attempt_number,
                            interval_seconds=int(params.get("terminal_heartbeat_seconds", 30) or 30),
                        )
                        try:
                            result: StageResult = stage.run(context)
                        finally:
                            if heartbeat is not None:
                                stop_event, worker = heartbeat
                                stop_event.set()
                                worker.join(timeout=0.2)
                        context.artifacts.setdefault(stage_name, {})
                        for artifact in result.artifacts:
                            self.registry.record_artifact(run_id, stage_name, attempt_number, artifact)
                            context.artifacts[stage_name][artifact.artifact_type] = artifact

                        if stage_name in {"ingest", "features", "rank"}:
                            self.dq_engine.evaluate(context, result)

                        if stage_name == "ingest":
                            skip_plan = self._plan_downstream_stage_skips(
                                run_id=run_id,
                                stage_names=list(stage_names),
                                ingest_metadata=result.metadata,
                            )
                            if skip_plan is not None:
                                skip_metadata = {
                                    "source_stage": "ingest",
                                    "reason_code": skip_plan["reason_code"],
                                    "detail": skip_plan["detail"],
                                    "fingerprint": skip_plan.get("fingerprint"),
                                    "planned_stages": list(skip_plan.get("stages", [])),
                                }
                                if skip_plan.get("previous_ingest_summary_uri"):
                                    skip_metadata["previous_ingest_summary_uri"] = skip_plan["previous_ingest_summary_uri"]
                                run_metadata = self.registry.get_run(run_id).get("metadata", {})
                                run_metadata["downstream_stage_skip"] = skip_metadata
                                self.registry.update_run(run_id, status="running", metadata=run_metadata)
                                for downstream_stage in skip_plan["stages"]:
                                    planned_stage_skips[downstream_stage] = {
                                        "source_stage": "ingest",
                                        "reason_code": skip_plan["reason_code"],
                                        "detail": str(skip_plan["detail"]),
                                        "fingerprint": skip_plan.get("fingerprint"),
                                    }

                        self.registry.finish_stage(
                            stage_run_id,
                            status="completed",
                            metadata=result.metadata,
                        )
                        if self.progress_renderer is not None:
                            self.progress_renderer.emit_stage(stage_name=stage_name, status="done")
                    except PublishStageError as exc:
                        final_status = "completed_with_publish_errors"
                        self.alert_manager.emit(
                            run_id=run_id,
                            alert_type="publish_degraded",
                            severity="high",
                            stage_name=stage_name,
                            message=str(exc),
                        )
                        self.registry.finish_stage(
                            stage_run_id,
                            status="failed",
                            error_class=exc.__class__.__name__,
                            error_message=str(exc),
                        )
                        self.registry.update_run(
                            run_id,
                            status=final_status,
                            current_stage=stage_name,
                            error_class=exc.__class__.__name__,
                            error_message=str(exc),
                            finished=True,
                        )
                        if self.progress_renderer is not None:
                            self.progress_renderer.emit_stage(
                                stage_name=stage_name,
                                status="fail",
                                detail=str(exc),
                            )
                        break
                    except Exception as exc:
                        final_status = "failed"
                        severity = "critical" if exc.__class__.__name__ == "DataQualityCriticalError" else "high"
                        alert_type = "critical_dq_failure" if exc.__class__.__name__ == "DataQualityCriticalError" else "pipeline_failure"
                        self.alert_manager.emit(
                            run_id=run_id,
                            alert_type=alert_type,
                            severity=severity,
                            stage_name=stage_name,
                            message=str(exc),
                        )
                        self.registry.finish_stage(
                            stage_run_id,
                            status="failed",
                            error_class=exc.__class__.__name__,
                            error_message=str(exc),
                        )
                        self.registry.update_run(
                            run_id,
                            status=final_status,
                            current_stage=stage_name,
                            error_class=exc.__class__.__name__,
                            error_message=str(exc),
                            finished=True,
                        )
                        if self.progress_renderer is not None:
                            self.progress_renderer.emit_stage(
                                stage_name=stage_name,
                                status="fail",
                                detail=str(exc),
                            )
                        raise

            if final_status == "completed":
                self.registry.update_run(
                    run_id,
                    status=final_status,
                    current_stage=stage_names[-1],
                    error_class="",
                    error_message="",
                    finished=True,
                )

        return {
            "run_id": run_id,
            "status": final_status,
            "stages": self.registry.get_stage_runs(run_id),
            "run": self.registry.get_run(run_id),
        }

    def _normalize_stage_names(self, stage_names: Optional[Iterable[str]]) -> List[str]:
        if stage_names is None:
            return list(PIPELINE_ORDER)
        requested = list(stage_names)
        invalid = [stage for stage in requested if stage not in SUPPORTED_STAGES]
        if invalid:
            raise ValueError(f"Unknown stages requested: {invalid}")
        return requested

    def _build_run_id(self, run_date: str) -> str:
        return f"pipeline-{run_date}-{uuid.uuid4().hex[:8]}"


def _extract_quarantined_dates(message: str) -> list[str]:
    """Parse quarantined trade dates from an ingest DQ failure message."""

    return sorted(set(re.findall(r"\b\d{4}-\d{2}-\d{2}\b", str(message or ""))))


def _run_auto_quarantine_repair(
    *,
    project_root: Path,
    run_id: str,
    error_message: str,
    data_domain: str,
) -> dict[str, object] | None:
    """Repair quarantined trade dates and persist the repair report."""

    quarantined_dates = _extract_quarantined_dates(error_message)
    if not quarantined_dates:
        return None

    from ai_trading_system.domains.ingest import reset_reingest_validate

    from_date = quarantined_dates[0]
    to_date = quarantined_dates[-1]
    logger.warning(
        "run_id=%s auto-repairing quarantined OHLC window from %s to %s before retry",
        run_id,
        from_date,
        to_date,
    )
    repair_report = reset_reingest_validate.run_reset_reingest_validate(
        project_root=project_root,
        from_date=from_date,
        to_date=to_date,
        exchange="NSE",
        apply=True,
        data_domain=data_domain,
        validation_source="bhavcopy",
    )
    report_dir = Path(str(repair_report["report_dir"]))
    report_path = report_dir / "reset_reingest_report.json"
    report_path.write_text(json.dumps(repair_report, indent=2, default=str), encoding="utf-8")
    logger.warning(
        "run_id=%s auto-repair completed status=%s report=%s",
        run_id,
        repair_report.get("status"),
        report_path,
    )
    return repair_report


def _safe_stage_runs(orchestrator: object, run_id: str) -> list[dict[str, object]]:
    registry = getattr(orchestrator, "registry", None)
    if registry is None or not hasattr(registry, "get_stage_runs"):
        return []
    try:
        return list(registry.get_stage_runs(run_id))
    except Exception:
        return []


def _resolve_latest_publishable_run_id(project_root: Path, *, limit: int = 50) -> str | None:
    control_plane_db = project_root / "data" / "control_plane.duckdb"
    if not control_plane_db.exists():
        return None
    import duckdb

    conn = duckdb.connect(str(control_plane_db), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT r.run_id, a.uri
            FROM pipeline_run r
            JOIN pipeline_artifact a
              ON a.run_id = r.run_id
             AND a.stage_name = 'rank'
             AND a.artifact_type = 'ranked_signals'
            ORDER BY r.started_at DESC NULLS LAST
            LIMIT ?
            """,
            [int(limit)],
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        artifact_path = Path(str(row[1]))
        if artifact_path.exists():
            return str(row[0])
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resilient trading pipeline orchestrator")
    parser.add_argument("--run-id", help="Reuse an existing run_id, typically for stage retries")
    parser.add_argument(
        "--stages",
        default="ingest,features,rank,execute,publish",
        help="Comma-separated stage list. Example: publish",
    )
    parser.add_argument(
        "--run-date",
        default=date.today().isoformat(),
        help="Logical trading date, defaults to today's date.",
    )
    parser.add_argument("--force", action="store_true", help="Preserved for compatibility with wrappers")
    parser.add_argument("--batch-size", type=int, default=700)
    parser.add_argument("--bulk", action="store_true")
    parser.add_argument("--top-n", type=int, default=None)
    parser.add_argument("--min-score", type=float, default=0.0)
    parser.add_argument(
        "--data-domain",
        choices=["operational", "research"],
        default="operational",
        help="Select the data/storage domain backing this run (defaults to operational).",
    )
    parser.add_argument("--local-publish", action="store_true", help="Skip networked publish targets")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Deprecated. Smoke mode is disabled because synthetic data is not allowed.",
    )
    parser.add_argument("--symbol-limit", type=int, default=None, help="Limit live symbol universe for canary runs")
    parser.add_argument("--canary", action="store_true", help="Run a smaller live canary flow")
    parser.add_argument(
        "--skip-preflight",
        dest="skip_preflight",
        action="store_true",
        default=True,
        help="Skip local readiness checks (default).",
    )
    parser.add_argument(
        "--run-preflight",
        dest="skip_preflight",
        action="store_false",
        help="Run local readiness checks before pipeline stages.",
    )
    parser.add_argument(
        "--auto-repair-quarantine",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Automatically repair quarantined ingest dates with reset_reingest_validate and retry once.",
    )
    parser.add_argument(
        "--skip-publish-network-checks",
        action="store_true",
        help="Skip preflight DNS checks for Telegram/Google publish endpoints.",
    )
    parser.add_argument(
        "--skip-delivery-collect",
        action="store_true",
        help="Skip ingest-stage delivery collection (enabled by default).",
    )
    parser.add_argument(
        "--skip-quantstats",
        action="store_true",
        help="Disable QuantStats dashboard tear sheet generation in publish stage.",
    )
    parser.add_argument(
        "--publish-quantstats",
        action="store_true",
        help="Legacy alias (QuantStats publish is enabled by default).",
    )
    parser.add_argument(
        "--quantstats-top-n",
        type=int,
        default=20,
        help="Top-N ranked symbols used to build dashboard strategy returns for tear sheet.",
    )
    parser.add_argument(
        "--quantstats-min-overlap",
        type=int,
        default=5,
        help="Minimum symbol overlap between consecutive rank snapshots.",
    )
    parser.add_argument(
        "--quantstats-max-runs",
        type=int,
        default=240,
        help="Maximum historical rank runs to inspect for QuantStats return series.",
    )
    parser.add_argument(
        "--quantstats-write-core-html",
        action="store_true",
        help="Also emit raw QuantStats core HTML alongside the enriched dashboard tear sheet.",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Force full feature recomputation instead of incremental operational tail updates.",
    )
    parser.add_argument(
        "--feature-tail-bars",
        type=int,
        default=252,
        help="Tail window used by incremental operational feature updates.",
    )
    parser.add_argument(
        "--strategy-mode",
        choices=["technical", "ml", "hybrid_confirm", "hybrid_overlay"],
        default="technical",
        help="Auto-trading policy used by the optional execute stage.",
    )
    parser.add_argument(
        "--execution-top-n",
        type=int,
        default=5,
        help="Target number of positions maintained by the execute stage.",
    )
    parser.add_argument(
        "--execution-ml-horizon",
        type=int,
        default=5,
        help="ML horizon used for confirm/overlay execution modes.",
    )
    parser.add_argument(
        "--execution-ml-confirm-threshold",
        type=float,
        default=0.55,
        help="Minimum ML probability required for hybrid/ml entry and hold decisions.",
    )
    parser.add_argument(
        "--execution-capital",
        type=float,
        default=1_000_000,
        help="Capital base used for risk sizing in the execute stage.",
    )
    parser.add_argument(
        "--execution-fixed-quantity",
        type=int,
        default=None,
        help="Optional fixed buy quantity for paper execution when risk sizing inputs are unavailable.",
    )
    parser.add_argument(
        "--execution-regime",
        default="TREND",
        help="Market regime label passed into the execute-stage risk engine.",
    )
    parser.add_argument(
        "--execution-regime-multiplier",
        type=float,
        default=1.0,
        help="Risk multiplier applied by the execute stage.",
    )
    parser.add_argument(
        "--paper-slippage-bps",
        type=float,
        default=5.0,
        help="Paper-trading slippage in basis points for simulated fills.",
    )
    parser.add_argument(
        "--breakout-engine",
        choices=["legacy", "v2"],
        default="v2",
        help="Breakout scanner engine mode.",
    )
    parser.add_argument(
        "--disable-breakout-legacy-families",
        action="store_true",
        help="When using breakout-v2, exclude mapped legacy setup families from results.",
    )
    parser.add_argument(
        "--breakout-market-bias-allowlist",
        default="BULLISH,NEUTRAL",
        help="Comma-separated market bias values allowed for qualified breakout states.",
    )
    parser.add_argument(
        "--breakout-min-breadth-score",
        type=float,
        default=45.0,
        help="Minimum breadth score required for breakout qualification.",
    )
    parser.add_argument(
        "--breakout-sector-rs-min",
        type=float,
        default=None,
        help="Optional absolute minimum sector RS value required for breakout qualification.",
    )
    parser.add_argument(
        "--breakout-sector-rs-percentile-min",
        type=float,
        default=60.0,
        help="Minimum sector RS percentile required for breakout qualification.",
    )
    parser.add_argument(
        "--breakout-qualified-min-score",
        type=int,
        default=3,
        help="Minimum breakout score needed to mark a breakout as qualified.",
    )
    parser.add_argument(
        "--breakout-symbol-near-high-max-pct",
        type=float,
        default=15.0,
        help="Maximum allowed distance from 52W high (%%) for Tier-A symbol trend qualification.",
    )
    parser.add_argument(
        "--disable-breakout-symbol-trend-gate",
        action="store_true",
        help="Disable symbol-level trend tier gate for breakout states.",
    )
    parser.add_argument(
        "--execution-breakout-linkage",
        choices=["off", "soft_gate"],
        default="off",
        help="Execution linkage mode for breakout signals.",
    )
    parser.add_argument(
        "--execution-require-stage2",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Optional execution-stage Stage 2 gate override. Defaults to auto-on for rank_mode=stage2_breakout.",
    )
    parser.add_argument(
        "--execution-stage2-min-score",
        type=float,
        default=70.0,
        help="Minimum stage2_score required when execution Stage 2 gate is active.",
    )
    parser.add_argument(
        "--terminal-mode",
        choices=["compact", "verbose", "json"],
        default="compact",
        help="Terminal rendering mode. Compact shows task progress instead of raw log spam.",
    )
    parser.add_argument(
        "--verbose-terminal",
        action="store_true",
        help="Force detailed terminal logs even when terminal-mode=compact.",
    )
    parser.add_argument(
        "--pattern-scan-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable operational pattern scanning sidecar artifact generation.",
    )
    parser.add_argument(
        "--pattern-scan-mode",
        choices=["weekly_full", "incremental", "full"],
        default="incremental",
        help="Pattern scan mode: weekly baseline full scan, daily incremental refresh, or one-off full scan.",
    )
    parser.add_argument(
        "--pattern-max-symbols",
        type=int,
        default=150,
        help="Maximum pattern rows retained in the operational pattern sidecar artifact.",
    )
    parser.add_argument(
        "--pattern-seed-max-symbols",
        type=int,
        default=400,
        help="Maximum broad-universe seed symbols considered before pattern scanning.",
    )
    parser.add_argument(
        "--pattern-min-liquidity-score",
        type=float,
        default=0.2,
        help="Minimum liquidity percentile required for broad-universe pattern seed eligibility.",
    )
    parser.add_argument(
        "--pattern-unusual-mover-min-vol20-avg",
        type=float,
        default=100000.0,
        help="Minimum 20-bar average volume required for unusual-mover seed inclusion.",
    )
    parser.add_argument(
        "--pattern-workers",
        type=int,
        default=4,
        help="Process workers for operational pattern scanning. Set to 1 to disable multiprocessing.",
    )
    parser.add_argument(
        "--pattern-lookback-days",
        type=int,
        default=260,
        help="Operational lookback window for pattern scanning. Lower values are faster.",
    )
    parser.add_argument(
        "--pattern-smoothing-method",
        choices=["rolling", "kernel", "auto"],
        default="rolling",
        help="Smoothing method for operational pattern scanning. Rolling is faster; kernel is slower and closer to research mode.",
    )
    parser.add_argument(
        "--pattern-timeout-seconds",
        type=int,
        default=None,
        help="Soft timeout budget reserved for pattern scanning. Reserved for future per-symbol cutoffs.",
    )
    parser.add_argument(
        "--pattern-watchlist-expiry-bars",
        type=int,
        default=10,
        help="Trading-bar expiry used for carried watchlist pattern lifecycle rows.",
    )
    parser.add_argument(
        "--pattern-confirmed-expiry-bars",
        type=int,
        default=20,
        help="Trading-bar expiry used for carried confirmed pattern lifecycle rows.",
    )
    parser.add_argument(
        "--pattern-invalidated-retention-bars",
        type=int,
        default=5,
        help="Trading-bar retention used for invalidated lifecycle rows before they expire.",
    )
    parser.add_argument(
        "--pattern-incremental-ranked-buffer",
        type=int,
        default=50,
        help="Bounded top-ranked continuity bucket included in incremental pattern rescans.",
    )
    parser.add_argument(
        "--stale-missing-symbol-grace-days",
        type=int,
        default=3,
        help="Grace period for repeated provider-unavailable symbol tails before they are treated as non-blocking.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.smoke:
        raise RuntimeError("Smoke mode has been removed because synthetic pipeline data is no longer allowed.")

    project_root = Path(__file__).resolve().parents[3]
    terminal_mode = "verbose" if args.verbose_terminal else args.terminal_mode
    configure_terminal_output(terminal_mode)
    progress_renderer = TerminalProgressRenderer(mode=terminal_mode)
    try:
        orchestrator = PipelineOrchestrator(project_root, progress_renderer=progress_renderer)
    except TypeError:
        orchestrator = PipelineOrchestrator(project_root)
        if hasattr(orchestrator, "progress_renderer"):
            orchestrator.progress_renderer = progress_renderer
    run_date = args.run_date or date.today().isoformat()
    if args.canary and args.stages == "ingest,features,rank,execute,publish":
        stage_names = ["ingest", "features", "rank"]
    else:
        stage_names = [stage.strip() for stage in args.stages.split(",") if stage.strip()]
    run_id = args.run_id
    if run_id is None and stage_names == ["publish"]:
        resolved_run_id = _resolve_latest_publishable_run_id(project_root, limit=50)
        if not resolved_run_id:
            logger.error(
                "Publish-only run requires an existing run with rank artifacts; no publishable run found."
            )
            raise SystemExit(1)
        run_id = resolved_run_id
        logger.info("Resolved publish-only run to latest publishable run_id=%s", run_id)
    if run_id is None:
        run_id = orchestrator._build_run_id(run_date)
    params = {
        "force": args.force,
        "batch_size": args.batch_size,
        "bulk": args.bulk,
        "top_n": args.top_n,
        "min_score": args.min_score,
        "data_domain": args.data_domain,
        "local_publish": args.local_publish,
        "smoke": args.smoke,
        "symbol_limit": args.symbol_limit if args.symbol_limit is not None else (25 if args.canary else None),
        "canary": args.canary,
        "preflight": not args.skip_preflight,
        "preflight_publish_network_checks": not args.skip_publish_network_checks,
        "include_delivery": not args.skip_delivery_collect,
        "publish_quantstats": not args.skip_quantstats,
        "quantstats_top_n": args.quantstats_top_n,
        "quantstats_min_overlap": args.quantstats_min_overlap,
        "quantstats_max_runs": args.quantstats_max_runs,
        "quantstats_write_core_html": args.quantstats_write_core_html,
        "full_rebuild": args.full_rebuild,
        "feature_tail_bars": args.feature_tail_bars,
        "strategy_mode": args.strategy_mode,
        "execution_top_n": args.execution_top_n,
        "execution_ml_horizon": args.execution_ml_horizon,
        "execution_ml_confirm_threshold": args.execution_ml_confirm_threshold,
        "execution_capital": args.execution_capital,
        "execution_fixed_quantity": args.execution_fixed_quantity,
        "execution_regime": args.execution_regime,
        "execution_regime_multiplier": args.execution_regime_multiplier,
        "paper_slippage_bps": args.paper_slippage_bps,
        "breakout_engine": args.breakout_engine,
        "breakout_include_legacy_families": not args.disable_breakout_legacy_families,
        "breakout_market_bias_allowlist": args.breakout_market_bias_allowlist,
        "breakout_min_breadth_score": args.breakout_min_breadth_score,
        "breakout_sector_rs_min": args.breakout_sector_rs_min,
        "breakout_sector_rs_percentile_min": args.breakout_sector_rs_percentile_min,
        "breakout_qualified_min_score": args.breakout_qualified_min_score,
        "breakout_symbol_near_high_max_pct": args.breakout_symbol_near_high_max_pct,
        "breakout_symbol_trend_gate_enabled": not args.disable_breakout_symbol_trend_gate,
        "execution_breakout_linkage": args.execution_breakout_linkage,
        "execution_require_stage2": args.execution_require_stage2,
        "execution_stage2_min_score": args.execution_stage2_min_score,
        "pattern_scan_enabled": args.pattern_scan_enabled,
        "pattern_scan_mode": args.pattern_scan_mode,
        "pattern_max_symbols": args.pattern_max_symbols,
        "pattern_seed_max_symbols": args.pattern_seed_max_symbols,
        "pattern_min_liquidity_score": args.pattern_min_liquidity_score,
        "pattern_unusual_mover_min_vol20_avg": args.pattern_unusual_mover_min_vol20_avg,
        "pattern_workers": args.pattern_workers,
        "pattern_lookback_days": args.pattern_lookback_days,
        "pattern_smoothing_method": args.pattern_smoothing_method,
        "pattern_timeout_seconds": args.pattern_timeout_seconds,
        "pattern_watchlist_expiry_bars": args.pattern_watchlist_expiry_bars,
        "pattern_confirmed_expiry_bars": args.pattern_confirmed_expiry_bars,
        "pattern_invalidated_retention_bars": args.pattern_invalidated_retention_bars,
        "pattern_incremental_ranked_buffer": args.pattern_incremental_ranked_buffer,
        "terminal_mode": terminal_mode,
        "verbose_terminal": bool(args.verbose_terminal),
        "stale_missing_symbol_grace_days": args.stale_missing_symbol_grace_days,
    }
    try:
        result = orchestrator.run_pipeline(
            run_id=run_id,
            stage_names=stage_names,
            run_date=run_date,
            params=params,
        )
    except DataQualityCriticalError as exc:
        can_auto_repair = (
            bool(args.auto_repair_quarantine)
            and args.data_domain == "operational"
            and "ingest" in stage_names
            and "ingest_unresolved_dates_present" in str(exc)
        )
        if not can_auto_repair:
            logger.error("Pipeline blocked by data-quality gate.")
            logger.error("run_id=%s status=blocked_by_dq run_date=%s", run_id, run_date)
            logger.error("dq_message=%s", str(exc))
            progress_renderer.emit_final(
                run_id=run_id,
                status="blocked_by_dq",
                stages=_safe_stage_runs(orchestrator, run_id),
                error=str(exc),
            )
            raise SystemExit(1)
        repair_report = _run_auto_quarantine_repair(
            project_root=project_root,
            run_id=run_id,
            error_message=str(exc),
            data_domain=args.data_domain,
        )
        if repair_report is None:
            logger.error("Pipeline blocked by data-quality gate; auto repair did not produce a repair report.")
            logger.error("run_id=%s status=blocked_by_dq run_date=%s", run_id, run_date)
            logger.error("dq_message=%s", str(exc))
            progress_renderer.emit_final(
                run_id=run_id,
                status="blocked_by_dq",
                stages=_safe_stage_runs(orchestrator, run_id),
                error=str(exc),
            )
            raise SystemExit(1)
        logger.warning("Retrying pipeline after quarantined-date auto repair for run_id=%s", run_id)
        result = orchestrator.run_pipeline(
            run_id=run_id,
            stage_names=stage_names,
            run_date=run_date,
            params=params,
        )
    except DataQualityCriticalError as exc:
        logger.error("Pipeline blocked by data-quality gate after retry handling.")
        logger.error("run_id=%s status=blocked_by_dq run_date=%s", run_id, run_date)
        logger.error("dq_message=%s", str(exc))
        progress_renderer.emit_final(
            run_id=run_id,
            status="blocked_by_dq",
            stages=_safe_stage_runs(orchestrator, run_id),
            error=str(exc),
        )
        raise SystemExit(1)

    logger.info("Pipeline run complete")
    logger.info("run_id=%s status=%s", result["run_id"], result["status"])
    for stage in result["stages"]:
        logger.info(
            "stage=%s attempt=%s status=%s",
            stage["stage_name"],
            stage["attempt_number"],
            stage["status"],
        )
    progress_renderer.emit_final(
        run_id=result["run_id"],
        status=result["status"],
        stages=result["stages"],
    )


if __name__ == "__main__":
    main()
