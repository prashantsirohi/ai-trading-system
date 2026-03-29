"""Publish stage isolated from upstream ingest/feature/rank work."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import pandas as pd
from pandas.errors import EmptyDataError
import json

from run.publisher import PublisherDeliveryManager
from run.stages.base import PublishStageError, StageArtifact, StageContext, StageResult


class PublishStage:
    """Publishes already-ranked artifacts to delivery channels."""

    name = "publish"

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
        metadata = self._run_smoke(context) if context.params.get("smoke") else self._run_default(context)
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
        ranked_df = self._read_artifact(rank_artifact)
        scan_artifact = context.artifact_for("rank", "stock_scan")
        dashboard_artifact = context.artifact_for("rank", "sector_dashboard")
        dashboard_payload_artifact = context.artifact_for("rank", "dashboard_payload")
        stock_scan_df = self._read_artifact(scan_artifact) if scan_artifact else pd.DataFrame()
        dashboard_df = self._read_artifact(dashboard_artifact) if dashboard_artifact else pd.DataFrame()
        datasets = {
            "ranked_signals": ranked_df,
            "stock_scan": stock_scan_df,
            "sector_dashboard": dashboard_df,
            "dashboard_payload": self._read_json_artifact(dashboard_payload_artifact) if dashboard_payload_artifact else {},
        }

        failures = []
        targets = []
        for channel, handler in self._build_handlers(context, datasets).items():
            delivery = self.delivery_manager.deliver(
                context=context,
                channel=channel,
                artifact=rank_artifact,
                sender=lambda channel_handler=handler: channel_handler(context, rank_artifact, datasets),
            )
            targets.append(delivery)
            if delivery["status"] == "failed":
                failures.append(f"{channel}: {delivery.get('error_message', 'delivery failed')}")

        metadata = {
            "rank_artifact_uri": rank_artifact.uri,
            "rank_artifact_hash": rank_artifact.content_hash,
            "targets": targets,
            "top_symbol": ranked_df.iloc[0]["symbol_id"] if not ranked_df.empty else None,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        if failures:
            metadata["failures"] = failures
            raise PublishStageError("; ".join(failures))
        return metadata

    def _run_smoke(self, context: StageContext) -> Dict:
        return {
            "mode": "smoke",
            "targets": [{"target": "local_summary", "status": "completed"}],
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

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
        if datasets.get("dashboard_payload"):
            handlers["google_sheets_dashboard"] = self._publish_dashboard_payload
        if not datasets["stock_scan"].empty:
            handlers["google_sheets_stock_scan"] = self._publish_stock_scan
        if not datasets["sector_dashboard"].empty:
            handlers["google_sheets_sector_dashboard"] = self._publish_sector_dashboard
        return handlers

    def _publish_local_summary(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        return {"report_id": f"local-{context.run_id}"}

    def _publish_stock_scan(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from channel.stock_scan import update_google_sheets as publish_stock_scan

        publish_stock_scan(datasets["stock_scan"])
        return {"report_id": "stock_scan_sheet"}

    def _publish_dashboard_payload(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from channel.dashboard_publisher import publish_dashboard_payload

        ok = publish_dashboard_payload(datasets.get("dashboard_payload", {}))
        if not ok:
            raise RuntimeError("dashboard payload publish returned False")
        return {"report_id": "dashboard_sheet"}

    def _publish_sector_dashboard(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from channel.sector_dashboard import update_google_sheets as publish_dashboard

        publish_dashboard(datasets["sector_dashboard"])
        return {"report_id": "sector_dashboard_sheet"}

    def _publish_portfolio(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from run.daily_pipeline import run_portfolio_analysis

        run_portfolio_analysis()
        return {"report_id": "portfolio_sheet"}

    def _publish_telegram_summary(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from channel.telegram_reporter import TelegramReporter

        reporter = TelegramReporter(report_dir=context.project_root / "reports")
        dashboard = datasets.get("dashboard_payload") or {}
        summary = dashboard.get("summary", {})
        breakout_rows = dashboard.get("breakout_scan", [])
        ranked_rows = dashboard.get("ranked_signals", [])
        lines = [
            f"Run {summary.get('run_date', context.run_date)}",
            f"Top ranked: {summary.get('top_symbol') or 'n/a'}",
            f"Breakouts: {summary.get('breakout_count', 0)}",
            f"Leading sector: {summary.get('top_sector') or 'n/a'}",
        ]
        if ranked_rows:
            lines.append("Rank leaders:")
            for row in ranked_rows[:5]:
                lines.append(f"{row.get('symbol_id')} {row.get('composite_score')}")
        if breakout_rows:
            lines.append("Breakouts:")
            for row in breakout_rows[:3]:
                lines.append(f"{row.get('symbol_id')} {row.get('breakout_tag')}")
        message = "\n".join(lines)
        if not reporter.send_message(message):
            raise RuntimeError("send_message returned False")
        return {"message_id": f"telegram-{context.run_id}"}
