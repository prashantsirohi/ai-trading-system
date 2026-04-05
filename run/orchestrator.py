"""Single-agent pipeline orchestrator with stage isolation and governance metadata."""

from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from analytics.dq import DataQualityEngine
from analytics.registry import RegistryStore
from core.contracts import PublishStageError, StageContext, StageResult
from core.env import load_project_env
from core.logging import log_context, logger
from core.paths import ensure_domain_layout
from run.alerts import AlertManager
from run.preflight import PreflightChecker
from run.stages import ExecuteStage, FeaturesStage, IngestStage, PublishStage, RankStage

load_project_env(__file__)


PIPELINE_ORDER = ["ingest", "features", "rank", "execute", "publish"]
SUPPORTED_STAGES = ["ingest", "features", "rank", "execute", "publish"]


class PipelineOrchestrator:
    """Executes the resilient pipeline with retry-safe metadata."""

    def __init__(
        self,
        project_root: Path | str,
        registry: Optional[RegistryStore] = None,
        dq_engine: Optional[DataQualityEngine] = None,
        alert_manager: Optional[AlertManager] = None,
        stages: Optional[Dict[str, object]] = None,
    ):
        self.project_root = Path(project_root)
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

            for stage_name in stage_names:
                self.registry.update_run(run_id, status="running", current_stage=stage_name)
                attempt_number = self.registry.next_stage_attempt(run_id, stage_name)
                stage_run_id = self.registry.start_stage(run_id, stage_name, attempt_number)
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
                )

                with log_context(run_id=run_id, stage_name=stage_name, attempt_number=attempt_number):
                    try:
                        stage = self.stages[stage_name]
                        result: StageResult = stage.run(context)
                        context.artifacts.setdefault(stage_name, {})
                        for artifact in result.artifacts:
                            self.registry.record_artifact(run_id, stage_name, attempt_number, artifact)
                            context.artifacts[stage_name][artifact.artifact_type] = artifact

                        if stage_name in {"ingest", "features", "rank"}:
                            self.dq_engine.evaluate(context, result)

                        self.registry.finish_stage(
                            stage_run_id,
                            status="completed",
                            metadata=result.metadata,
                        )
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resilient trading pipeline orchestrator")
    parser.add_argument("--run-id", help="Reuse an existing run_id, typically for stage retries")
    parser.add_argument(
        "--stages",
        default="ingest,features,rank,execute,publish",
        help="Comma-separated stage list. Example: publish",
    )
    parser.add_argument("--run-date", help="Logical trading date, defaults to today")
    parser.add_argument("--force", action="store_true", help="Preserved for compatibility with wrappers")
    parser.add_argument("--batch-size", type=int, default=700)
    parser.add_argument("--bulk", action="store_true")
    parser.add_argument("--top-n", type=int, default=None)
    parser.add_argument("--min-score", type=float, default=0.0)
    parser.add_argument(
        "--data-domain",
        choices=["operational", "research"],
        default="operational",
        help="Select the data/storage domain backing this run.",
    )
    parser.add_argument("--local-publish", action="store_true", help="Skip networked publish targets")
    parser.add_argument("--smoke", action="store_true", help="Run a self-contained local smoke flow")
    parser.add_argument("--symbol-limit", type=int, default=None, help="Limit live symbol universe for canary runs")
    parser.add_argument("--canary", action="store_true", help="Run a smaller live canary flow")
    parser.add_argument("--skip-preflight", action="store_true", help="Skip local readiness checks")
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
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    orchestrator = PipelineOrchestrator(project_root)
    if args.canary and args.stages == "ingest,features,rank,execute,publish":
        stage_names = ["ingest", "features", "rank"]
    else:
        stage_names = [stage.strip() for stage in args.stages.split(",") if stage.strip()]
    result = orchestrator.run_pipeline(
        run_id=args.run_id,
        stage_names=stage_names,
        run_date=args.run_date,
        params={
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
        },
    )

    logger.info("Pipeline run complete")
    logger.info("run_id=%s status=%s", result["run_id"], result["status"])
    for stage in result["stages"]:
        logger.info(
            "stage=%s attempt=%s status=%s",
            stage["stage_name"],
            stage["attempt_number"],
            stage["status"],
        )


if __name__ == "__main__":
    main()
