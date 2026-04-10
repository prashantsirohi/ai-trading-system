"""Publish stage isolated from upstream ingest/feature/rank work."""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape
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
        ranked_df = self._read_artifact(rank_artifact)
        scan_artifact = context.artifact_for("rank", "stock_scan")
        breakout_artifact = context.artifact_for("rank", "breakout_scan")
        dashboard_artifact = context.artifact_for("rank", "sector_dashboard")
        dashboard_payload_artifact = context.artifact_for("rank", "dashboard_payload")
        stock_scan_df = self._read_artifact(scan_artifact) if scan_artifact else pd.DataFrame()
        breakout_df = self._read_artifact(breakout_artifact) if breakout_artifact else pd.DataFrame()
        dashboard_df = self._read_artifact(dashboard_artifact) if dashboard_artifact else pd.DataFrame()
        datasets = {
            "ranked_signals": ranked_df,
            "breakout_scan": breakout_df,
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
            delivery["delivery_role"] = self.CHANNEL_ROLES.get(channel, "publish_auxiliary")
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
        return {"report_id": f"local-{context.run_id}"}

    def _publish_stock_scan(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from publishers.google_sheets import publish_stock_scan

        if not publish_stock_scan(datasets["stock_scan"]):
            raise RuntimeError("stock scan publish returned False")
        return {"report_id": "stock_scan_sheet"}

    def _publish_dashboard_payload(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from publishers.dashboard import publish_dashboard_payload

        result = publish_dashboard_payload(
            datasets.get("dashboard_payload", {}),
            project_root=context.project_root,
            run_date=context.run_date,
            ranked_df=datasets.get("ranked_signals"),
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
        from publishers.google_sheets import publish_sector_dashboard

        if not publish_sector_dashboard(datasets["sector_dashboard"]):
            raise RuntimeError("sector dashboard publish returned False")
        return {"report_id": "sector_dashboard_sheet"}

    def _publish_portfolio(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from run.daily_pipeline import run_portfolio_analysis

        result = run_portfolio_analysis()
        if not isinstance(result, dict) or not bool(result.get("ok")):
            raise RuntimeError(str((result or {}).get("error") or "Portfolio publish failed"))
        return {"report_id": "portfolio_sheet", "positions": result.get("positions")}

    def _publish_quantstats_dashboard(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from publishers.quantstats_dashboard import publish_dashboard_quantstats_tearsheet

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
        from publishers.telegram import TelegramReporter

        reporter = TelegramReporter(report_dir=context.project_root / "reports")
        message = self._build_telegram_tearsheet(context, datasets)
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
        dashboard = datasets.get("dashboard_payload") or {}
        summary = dashboard.get("summary", {})
        data_trust = dashboard.get("data_trust", {}) or {}
        ranked_df = self._sorted_ranked_signals(datasets.get("ranked_signals"))
        breakout_df = self._sorted_breakouts(datasets.get("breakout_scan"))
        sector_df = self._sorted_sector_dashboard(datasets.get("sector_dashboard"))

        top_symbol = summary.get("top_symbol")
        if not top_symbol and not ranked_df.empty and "symbol_id" in ranked_df.columns:
            top_symbol = ranked_df.iloc[0]["symbol_id"]
        top_sector = summary.get("top_sector")
        if not top_sector and not sector_df.empty and "Sector" in sector_df.columns:
            top_sector = sector_df.iloc[0]["Sector"]

        lines = [
            f"<b>Daily Market Tearsheet</b> | {escape(str(summary.get('run_date', context.run_date)))}",
            f"Top symbol: <b>{escape(str(top_symbol or 'n/a'))}</b> | Top sector: <b>{escape(str(top_sector or 'n/a'))}</b>",
            f"Universe ranked: <b>{len(ranked_df)}</b> | Breakouts: <b>{len(breakout_df)}</b> | Sectors: <b>{len(sector_df)}</b>",
        ]
        lines.append(
            "Data trust: "
            f"<b>{escape(str(summary.get('data_trust_status', data_trust.get('status', 'unknown'))))}</b>"
            f" | Latest trade: <b>{escape(str(summary.get('latest_trade_date', data_trust.get('latest_trade_date', 'n/a'))))}</b>"
            f" | Latest validated: <b>{escape(str(summary.get('latest_validated_date', data_trust.get('latest_validated_date', 'n/a'))))}</b>"
        )
        trust_notes: list[str] = []
        quarantined_dates = list(data_trust.get("active_quarantined_dates") or [])
        if quarantined_dates:
            trust_notes.append(f"Quarantined: {', '.join(escape(str(item)) for item in quarantined_dates[:3])}")
        fallback_ratio = float(data_trust.get("fallback_ratio_latest", 0.0) or 0.0)
        if fallback_ratio > 0:
            trust_notes.append(f"Fallback ratio: {fallback_ratio * 100:.1f}%")
        if trust_notes:
            lines.append("Trust notes: " + " | ".join(trust_notes))
        lines.extend(["", "<b>Top 10 Sectors</b>"])

        if sector_df.empty:
            lines.append("No sector data available.")
        else:
            for _, row in sector_df.head(10).iterrows():
                lines.append(self._format_sector_line(row))

        lines.extend(["", "<b>Top 10 Breakouts</b>"])
        if breakout_df.empty:
            lines.append("No breakouts today.")
        else:
            for idx, (_, row) in enumerate(breakout_df.head(10).iterrows(), start=1):
                lines.append(self._format_breakout_line(idx, row))

        lines.extend(["", "<b>Top 10 Ranked Stocks</b>"])
        if ranked_df.empty:
            lines.append("No ranked stocks available.")
        else:
            for idx, (_, row) in enumerate(ranked_df.head(10).iterrows(), start=1):
                lines.append(self._format_ranked_line(idx, row))

        return "\n".join(lines)

    def _sorted_sector_dashboard(self, sector_df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if sector_df is None or sector_df.empty:
            return pd.DataFrame()
        df = sector_df.copy()
        if "RS_rank" in df.columns:
            return df.sort_values(["RS_rank", "RS"], ascending=[True, False], na_position="last")
        if "RS" in df.columns:
            return df.sort_values("RS", ascending=False, na_position="last")
        return df

    def _sorted_ranked_signals(self, ranked_df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if ranked_df is None or ranked_df.empty:
            return pd.DataFrame()
        df = ranked_df.copy()
        if "composite_score" in df.columns:
            return df.sort_values("composite_score", ascending=False, na_position="last")
        return df

    def _sorted_breakouts(self, breakout_df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if breakout_df is None or breakout_df.empty:
            return pd.DataFrame()
        df = breakout_df.copy()
        sort_columns = [
            column
            for column in ["breakout_rank", "breakout_score", "setup_quality", "symbol_id"]
            if column in df.columns
        ]
        if sort_columns:
            ascending = [
                False
                if column in {"breakout_score", "setup_quality"}
                else True
                for column in sort_columns
            ]
            return df.sort_values(sort_columns, ascending=ascending, na_position="last")
        return df

    def _format_sector_line(self, row: pd.Series) -> str:
        sector = escape(str(row.get("Sector", "n/a")))
        rs_rank = self._format_int(row.get("RS_rank"))
        rs = self._format_decimal(row.get("RS"), 2)
        momentum = self._format_signed_decimal(row.get("Momentum"), 2)
        quadrant = escape(str(row.get("Quadrant", "n/a")))
        return f"{rs_rank}. {sector} | RS {rs} | Mom {momentum} | {quadrant}"

    def _format_breakout_line(self, index: int, row: pd.Series) -> str:
        symbol = escape(str(row.get("symbol_id", "n/a")))
        sector = escape(str(row.get("sector", "n/a")))
        setup = escape(str(row.get("taxonomy_family") or row.get("setup_family") or row.get("execution_label") or "setup"))
        tag = escape(str(row.get("breakout_tag", "n/a")))
        score = self._format_int(row.get("breakout_score"))
        state = escape(str(row.get("breakout_state") or "watchlist"))
        tier = escape(str(row.get("candidate_tier") or "n/a"))
        reason = str(row.get("filter_reason") or "").strip()
        if not reason:
            reason = str(row.get("symbol_trend_reasons") or "").strip()
        reason_short = " | " + escape(",".join(reason.split(",")[:2])) if reason and state != "qualified" else ""
        return f"{index}. {symbol} | {sector} | {setup} | Tier {tier} | Score {score} | {state} | {tag}{reason_short}"

    def _format_ranked_line(self, index: int, row: pd.Series) -> str:
        symbol = escape(str(row.get("symbol_id", "n/a")))
        sector = escape(str(row.get("sector_name", row.get("sector", "n/a"))))
        score = self._format_decimal(row.get("composite_score"), 1)
        close = self._format_decimal(row.get("close"), 2)
        rs = self._format_decimal(row.get("rel_strength_score"), 1)
        return f"{index}. {symbol} | {sector} | Score {score} | Close {close} | RS {rs}"

    def _format_decimal(self, value: Any, places: int = 2) -> str:
        if pd.isna(value):
            return "n/a"
        try:
            return f"{float(value):.{places}f}"
        except (TypeError, ValueError):
            return "n/a"

    def _format_signed_decimal(self, value: Any, places: int = 2) -> str:
        if pd.isna(value):
            return "n/a"
        try:
            return f"{float(value):+.{places}f}"
        except (TypeError, ValueError):
            return "n/a"

    def _format_int(self, value: Any) -> str:
        if pd.isna(value):
            return "-"
        try:
            return str(int(value))
        except (TypeError, ValueError):
            return "-"
