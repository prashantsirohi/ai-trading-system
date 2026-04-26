"""Publish stage isolated from upstream ingest/feature/rank work."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Optional

import pandas as pd
from pandas.errors import EmptyDataError
import json

from ai_trading_system.domains.publish.delivery_manager import PublisherDeliveryManager
from ai_trading_system.pipeline.contracts import PublishStageError, StageArtifact, StageContext, StageResult
from ai_trading_system.domains.publish.publish_payloads import (
    build_publish_datasets,
    build_publish_metadata,
)
from ai_trading_system.domains.publish.telegram_summary_builder import build_telegram_summary


class PublishStage:
    """Publishes already-ranked artifacts to delivery channels."""

    name = "publish"
    CHANNEL_ROLES = {
        "google_sheets_portfolio": "publish_of_record",
        "google_sheets_dashboard": "publish_of_record",
        "quantstats_dashboard_tearsheet": "publish_of_record",
        "telegram_summary": "informational",
        "local_summary": "diagnostic",
    }

    def __init__(
        self,
        operation: Optional[Callable[[StageContext], Dict]] = None,
        channel_handlers: Optional[Dict[str, Callable[[StageContext, StageArtifact, Dict[str, pd.DataFrame]], Dict[str, Any] | bool | None]]] = None,
        delivery_manager: Optional[PublisherDeliveryManager] = None,
    ):
        self.operation = operation
        self.channel_handlers = channel_handlers
        self.delivery_manager = delivery_manager or PublisherDeliveryManager()

    def run(self, context: StageContext) -> StageResult:
        if context.params.get("smoke"):
            raise RuntimeError("Smoke mode is disabled because synthetic publish artifacts have been removed.")
        metadata = self._run_default(context)
        artifact_path = context.write_json("publish_summary.json", metadata)
        artifact = StageArtifact.from_file(
            "publish_summary",
            artifact_path,
            row_count=len(metadata.get("targets", [])),
            metadata=metadata,
            attempt_number=context.attempt_number,
        )
        return StageResult(artifacts=[artifact], metadata=metadata)

    def _run_default(self, context: StageContext) -> Dict:
        if self.operation is not None:
            return self.operation(context)

        rank_artifact = context.require_artifact("rank", "ranked_signals")
        datasets = build_publish_datasets(
            context_artifact_for=lambda artifact_type: context.artifact_for("rank", artifact_type),
            read_artifact=self._read_artifact,
            read_json_artifact=self._read_json_artifact,
            ranked_signals_artifact=rank_artifact,
            run_id=context.run_id,
            stage_name=self.name,
        )
        ranked_df = datasets.get("ranked_signals", pd.DataFrame())

        failures = []
        targets = []
        for channel, handler in self._build_handlers(context, datasets).items():
            delivery = self.delivery_manager.deliver(
                context=context,
                channel=channel,
                artifact=rank_artifact,
                sender=lambda channel_handler=handler: channel_handler(context, rank_artifact, datasets),
            )
            delivery["delivery_role"] = self.CHANNEL_ROLES.get(channel, "publish_auxiliary")
            targets.append(delivery)
            if delivery["status"] == "failed":
                failures.append(f"{channel}: {delivery.get('error_message', 'delivery failed')}")

        metadata = build_publish_metadata(
            rank_artifact=rank_artifact,
            ranked_df=ranked_df if isinstance(ranked_df, pd.DataFrame) else pd.DataFrame(),
            targets=targets,
            stage2_summary=dict(datasets.get("stage2_summary") or {}),
            stage2_breakdown_symbols=list(datasets.get("stage2_breakdown_symbols") or []),
        )
        if failures:
            metadata["failures"] = failures
            raise PublishStageError("; ".join(failures))
        return metadata

    def _read_artifact(self, artifact: StageArtifact) -> pd.DataFrame:
        try:
            return pd.read_csv(Path(artifact.uri))
        except EmptyDataError:
            expected_rows = artifact.row_count if artifact.row_count is not None else artifact.metadata.get("row_count")
            if expected_rows and int(expected_rows) > 0:
                raise PublishStageError(
                    f"Artifact {artifact.artifact_type} at {artifact.uri} is empty but metadata expected {expected_rows} rows."
                )
            return pd.DataFrame()

    def _read_json_artifact(self, artifact: StageArtifact) -> Dict[str, Any]:
        with Path(artifact.uri).open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _build_handlers(
        self,
        context: StageContext,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Callable[[StageContext, StageArtifact, Dict[str, pd.DataFrame]], Dict[str, Any] | bool | None]]:
        if self.channel_handlers is not None:
            return self.channel_handlers
        if bool(context.params.get("local_publish", False)):
            return {"local_summary": self._publish_local_summary}

        handlers: Dict[str, Callable[[StageContext, StageArtifact, Dict[str, pd.DataFrame]], Dict[str, Any] | bool | None]] = {
            "google_sheets_portfolio": self._publish_portfolio,
            "telegram_summary": self._publish_telegram_summary,
        }
        if datasets.get("dashboard_payload") or not datasets.get("ranked_signals", pd.DataFrame()).empty:
            handlers["google_sheets_dashboard"] = self._publish_dashboard_payload
        if bool(context.params.get("publish_quantstats", True)):
            handlers["quantstats_dashboard_tearsheet"] = self._publish_quantstats_dashboard
        return handlers

    def _publish_local_summary(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        return {
            "report_id": f"local-{context.run_id}",
            "trust_status": datasets.get("publish_trust_status", "unknown"),
        }

    def _publish_stock_scan(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.google_sheets import publish_stock_scan

        if not publish_stock_scan(datasets["stock_scan"]):
            raise RuntimeError("stock scan publish returned False")
        return {"report_id": "stock_scan_sheet"}

    def _publish_dashboard_payload(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.dashboard import publish_dashboard_payload

        result = publish_dashboard_payload(
            datasets.get("dashboard_payload", {}),
            project_root=context.project_root,
            run_date=context.run_date,
            ranked_df=pd.DataFrame(datasets.get("publish_rows_dashboard", [])),
            breakout_df=datasets.get("breakout_scan"),
            sector_df=datasets.get("sector_dashboard"),
        )
        return {
            "report_id": "dashboard_sheet",
            "sheet_name": result.get("sheet_name") if isinstance(result, dict) else None,
        }

    def _publish_sector_dashboard(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.google_sheets import publish_sector_dashboard

        if not publish_sector_dashboard(datasets["sector_dashboard"]):
            raise RuntimeError("sector dashboard publish returned False")
        return {"report_id": "sector_dashboard_sheet"}

    def _publish_portfolio(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.pipeline import daily_pipeline

        result = daily_pipeline.run_portfolio_analysis()
        if not isinstance(result, dict) or not bool(result.get("ok")):
            raise RuntimeError(str((result or {}).get("error") or "Portfolio publish failed"))
        return {"report_id": "portfolio_sheet", "positions": result.get("positions")}

    def _publish_quantstats_dashboard(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.quantstats import publish_dashboard_quantstats_tearsheet

        result = publish_dashboard_quantstats_tearsheet(
            project_root=context.project_root,
            run_id=context.run_id,
            run_date=context.run_date,
            top_n=int(context.params.get("quantstats_top_n", 20)),
            min_overlap=int(context.params.get("quantstats_min_overlap", 5)),
            max_runs=int(context.params.get("quantstats_max_runs", 240)),
            latest_ranked_df=datasets.get("ranked_signals"),
            latest_breakout_df=datasets.get("breakout_scan"),
            latest_sector_df=datasets.get("sector_dashboard"),
            breadth_start_date=str(context.params.get("quantstats_breadth_start_date", "2018-01-01")),
            write_core_quantstats_html=bool(context.params.get("quantstats_write_core_html", False)),
        )
        if not result.get("ok"):
            error_code = str(result.get("error", "quantstats tear sheet publish failed"))
            non_critical = {
                "insufficient_rank_history_for_tearsheet",
                "pipeline_runs_dir_missing",
                "quantstats_not_available",
            }
            if error_code in non_critical and not bool(context.params.get("quantstats_required", False)):
                return {
                    "report_id": "quantstats_dashboard_tearsheet",
                    "status": "skipped",
                    "reason": error_code,
                }
            raise RuntimeError(error_code)
        return {
            "report_id": "quantstats_dashboard_tearsheet",
            "tearsheet_path": result.get("tearsheet_path"),
            "observations": result.get("observations"),
        }

    def _publish_telegram_summary(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.telegram import TelegramReporter

        reporter = TelegramReporter(report_dir=context.project_root / "reports")
        telegram_datasets = dict(datasets)
        publish_rows = pd.DataFrame(datasets.get("publish_rows_telegram", []))
        if not publish_rows.empty:
            telegram_datasets["ranked_signals"] = publish_rows
        message = self._build_telegram_tearsheet(context, telegram_datasets)
        if not reporter.send_message(message):
            detail = reporter.last_error or "unknown Telegram error"
            if reporter.last_health_check and reporter.last_health_check.get("status") == "failed":
                detail = f"{detail} | precheck={reporter.last_health_check.get('kind')}"
            raise RuntimeError(f"send_message returned False: {detail}")
        return {
            "message_id": f"telegram-{context.run_id}",
            "delivery_role": self.CHANNEL_ROLES["telegram_summary"],
        }

    def _build_telegram_tearsheet(
        self,
        context: StageContext,
        datasets: Dict[str, pd.DataFrame],
    ) -> str:
        return build_telegram_summary(run_date=context.run_date, datasets=datasets)
