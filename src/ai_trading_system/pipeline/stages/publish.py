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
from ai_trading_system.domains.publish.decision_bundle import build_publish_decision_bundle
from ai_trading_system.domains.publish.telegram_summary_builder import build_telegram_summary
from ai_trading_system.domains.publish.watchlist_buckets import (
    assign_watchlist_buckets,
    summarize_buckets,
)


class PublishStage:
    """Publishes already-ranked artifacts to delivery channels."""

    name = "publish"
    # Channel role taxonomy:
    #   publish_of_record  — primary outputs; failure should block the stage
    #   publish_auxiliary  — secondary outputs; failure should block the stage
    #   publish_optional   — best-effort outputs (live external APIs, sheet
    #                        writes); failure is logged + recorded in metadata
    #                        but does NOT raise PublishStageError. Use for
    #                        channels whose flake-on-Tuesday shouldn't take
    #                        down the telegram digest or perf_tracker stage.
    #   informational      — non-blocking notification channels
    #   diagnostic         — local-only artifacts
    # Roles whose failure does NOT raise PublishStageError. Kept narrow on
    # purpose — only channels explicitly marked publish_optional bypass the
    # blocking gate. Other roles (including informational like telegram_summary)
    # retain their existing blocking semantics so behavior changes are
    # opt-in via CHANNEL_ROLES.
    NON_BLOCKING_ROLES = frozenset({"publish_optional"})
    CHANNEL_ROLES = {
        # Portfolio handler does live YF+Google Sheets IO and rewrites the
        # user's PORTFOLIO sheet each run. Transient failures (auth expiry,
        # YF rate limit, network blip) shouldn't take down the rest of the
        # publish stage — the telegram digest and perf_tracker still need
        # to run so the operator gets *some* signal that the pipeline ran.
        "google_sheets_portfolio": "publish_optional",
        "google_sheets_dashboard": "publish_of_record",
        "google_sheets_watchlist": "publish_of_record",
        "google_sheets_event_log": "publish_auxiliary",
        "google_sheets_publish_log": "publish_auxiliary",
        "quantstats_dashboard_tearsheet": "publish_of_record",
        "telegram_summary": "informational",
        "weekly_pdf": "informational",
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
        def _context_artifact_for(artifact_type: str) -> StageArtifact | None:
            artifact = context.artifact_for("rank", artifact_type)
            if artifact is not None:
                return artifact
            if artifact_type in {"watchlist_candidates", "fundamental_summary", "fundamental_scores"}:
                return context.artifact_for("fundamentals", artifact_type)
            return None

        datasets = build_publish_datasets(
            context_artifact_for=_context_artifact_for,
            read_artifact=self._read_artifact,
            read_json_artifact=self._read_json_artifact,
            ranked_signals_artifact=rank_artifact,
            run_id=context.run_id,
            stage_name=self.name,
        )
        self._attach_event_datasets(context, datasets)
        self._attach_insight_datasets(context, datasets)
        self._attach_decision_bundle(context, datasets)
        ranked_df = datasets.get("ranked_signals", pd.DataFrame())

        # Phase 5: derive 4-bucket watchlist taxonomy from ranking + breakout output.
        # Persisted as watchlist_buckets.csv in the publish stage attempt dir and
        # attached to datasets so downstream channel handlers (Telegram, Sheets)
        # can consume it without recomputing.
        buckets_df = assign_watchlist_buckets(
            ranked_signals=ranked_df if isinstance(ranked_df, pd.DataFrame) else pd.DataFrame(),
            breakout_scan=datasets.get("breakout_scan"),
        )
        bucket_counts = summarize_buckets(buckets_df)
        try:
            buckets_path = context.output_dir() / "watchlist_buckets.csv"
            buckets_df.to_csv(buckets_path, index=False)
        except Exception:  # pragma: no cover - persistence is best-effort
            pass
        datasets["watchlist_buckets"] = buckets_df
        datasets["watchlist_bucket_counts"] = bucket_counts

        delivery_artifact = StageArtifact(
            artifact_type=rank_artifact.artifact_type,
            uri=rank_artifact.uri,
            row_count=rank_artifact.row_count,
            content_hash=rank_artifact.content_hash,
            metadata={
                **(rank_artifact.metadata or {}),
                "event_hashes": list(datasets.get("event_hashes") or []),
                "insight_hash": datasets.get("insight_hash"),
            },
            attempt_number=rank_artifact.attempt_number,
        )

        failures = []
        non_blocking_failures = []
        targets = []
        for channel, handler in self._build_handlers(context, datasets).items():
            delivery = self.delivery_manager.deliver(
                context=context,
                channel=channel,
                artifact=delivery_artifact,
                sender=lambda channel_handler=handler: channel_handler(context, rank_artifact, datasets),
            )
            role = self.CHANNEL_ROLES.get(channel, "publish_auxiliary")
            delivery["delivery_role"] = role
            targets.append(delivery)
            if delivery["status"] == "failed":
                failure_msg = f"{channel}: {delivery.get('error_message', 'delivery failed')}"
                if role in self.NON_BLOCKING_ROLES:
                    # Recorded in metadata for visibility but does not raise.
                    non_blocking_failures.append(failure_msg)
                else:
                    failures.append(failure_msg)

        metadata = build_publish_metadata(
            rank_artifact=rank_artifact,
            ranked_df=ranked_df if isinstance(ranked_df, pd.DataFrame) else pd.DataFrame(),
            targets=targets,
            stage2_summary=dict(datasets.get("stage2_summary") or {}),
            stage2_breakdown_symbols=list(datasets.get("stage2_breakdown_symbols") or []),
        )
        metadata["watchlist_buckets"] = bucket_counts
        self._attach_fundamentals_publish_summary(context, datasets, metadata)
        if non_blocking_failures:
            # Visible in publish_summary.json but does not raise.
            metadata["non_blocking_failures"] = non_blocking_failures
        if failures:
            metadata["failures"] = failures
            raise PublishStageError("; ".join(failures))
        return metadata

    def _attach_fundamentals_publish_summary(
        self,
        context: StageContext,
        datasets: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> None:
        watchlist = datasets.get("watchlist_candidates")
        if not isinstance(watchlist, pd.DataFrame) or watchlist.empty:
            return
        bucket = watchlist.get("watchlist_bucket", pd.Series("", index=watchlist.index)).astype(str)
        add_rows = watchlist.loc[bucket.eq("ADD_TO_WATCHLIST")].head(10)
        metadata["fundamentals_top_add_to_watchlist"] = (
            add_rows.get("symbol", pd.Series(dtype=str)).astype(str).tolist()
            if not add_rows.empty
            else []
        )
        summary_artifact = context.artifact_for("fundamentals", "fundamental_summary")
        if summary_artifact is not None:
            metadata["fundamental_summary_uri"] = summary_artifact.uri

    def _attach_event_datasets(self, context: StageContext, datasets: Dict[str, Any]) -> None:
        snapshot = self._read_json_artifact_safe(context.artifact_for("events", "market_events_snapshot"))
        enrichment = self._read_json_artifact_safe(context.artifact_for("events", "events_enrichment"))
        summary = self._read_json_artifact_safe(context.artifact_for("events", "events_summary"))
        signals = list(enrichment.get("signals") or [])
        snapshot_events = list(snapshot.get("events") or [])
        event_hashes = sorted({
            str(h)
            for h in (
                [row.get("event_hash") for row in snapshot_events if isinstance(row, dict)]
                + [
                    h
                    for sig in signals if isinstance(sig, dict)
                    for h in list(sig.get("event_hashes") or [])
                ]
            )
            if h
        })
        datasets["market_events_snapshot"] = snapshot
        datasets["enriched_event_signals"] = signals
        datasets["events_summary"] = summary
        datasets["event_hashes"] = event_hashes
        status = str(snapshot.get("market_intel_status") or summary.get("market_intel_status") or "unknown")
        datasets["market_intel_status"] = status
        if status in {"missing", "stale", "degraded"}:
            datasets["event_freshness_warning"] = f"market_intel status: {status}"
        dashboard_payload = datasets.get("dashboard_payload")
        if isinstance(dashboard_payload, dict):
            self._overlay_events_on_dashboard(dashboard_payload, signals, snapshot_events)

    def _attach_insight_datasets(self, context: StageContext, datasets: Dict[str, Any]) -> None:
        telegram_artifact = context.artifact_for("narrative", "telegram_summary")
        confluence_artifact = context.artifact_for("insight", "event_confluence")
        daily_json = (
            context.artifact_for("narrative", "daily_insight_json")
            or context.artifact_for("narrative", "weekly_insight_json")
        )
        if telegram_artifact is not None:
            try:
                text = Path(telegram_artifact.uri).read_text(encoding="utf-8")
                datasets["insight_telegram_summary"] = text
                datasets["insight_hash"] = telegram_artifact.content_hash
            except Exception:
                pass
        if confluence_artifact is not None:
            try:
                datasets["event_confluence"] = self._read_artifact(confluence_artifact)
            except Exception:
                datasets["event_confluence"] = pd.DataFrame()
        if daily_json is not None:
            datasets["latest_insight"] = self._read_json_artifact_safe(daily_json)
        dashboard_payload = datasets.get("dashboard_payload")
        if isinstance(dashboard_payload, dict):
            confluence_df = datasets.get("event_confluence")
            if isinstance(confluence_df, pd.DataFrame) and not confluence_df.empty:
                dashboard_payload["event_confluence"] = confluence_df.head(50).to_dict(orient="records")
            if datasets.get("latest_insight"):
                dashboard_payload["latest_insight"] = datasets["latest_insight"]

    def _read_json_artifact_safe(self, artifact: StageArtifact | None) -> Dict[str, Any]:
        if artifact is None:
            return {}
        try:
            return self._read_json_artifact(artifact)
        except Exception:
            return {}

    def _overlay_events_on_dashboard(
        self,
        dashboard_payload: Dict[str, Any],
        signals: list[dict[str, Any]],
        snapshot_events: list[dict[str, Any]],
    ) -> None:
        by_symbol: dict[str, list[dict[str, Any]]] = {}
        events_index: list[dict[str, Any]] = []
        for sig in signals:
            trigger = sig.get("trigger") or {}
            symbol = str(trigger.get("symbol") or "").upper()
            if not symbol:
                continue
            by_symbol.setdefault(symbol, []).append(sig)
            events_index.append({
                "symbol": symbol,
                "trigger_type": trigger.get("trigger_type"),
                "severity": sig.get("severity"),
                "top_category": sig.get("top_category"),
                "materiality_label": sig.get("materiality_label"),
                "event_count": sig.get("event_count"),
                "suppressed": sig.get("suppressed"),
            })
        for row in snapshot_events:
            symbol = str(row.get("symbol") or "").upper()
            if symbol and symbol not in by_symbol:
                events_index.append({
                    "symbol": symbol,
                    "trigger_type": "market_snapshot",
                    "severity": _snapshot_severity(row),
                    "top_category": row.get("category"),
                    "materiality_label": row.get("materiality_label"),
                    "event_count": 1,
                    "suppressed": False,
                })
        for value in dashboard_payload.values():
            if not isinstance(value, list):
                continue
            for row in value:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol") or row.get("symbol_id") or row.get("ticker") or "").upper()
                if symbol in by_symbol:
                    row["events"] = by_symbol[symbol]
        dashboard_payload["events_index"] = events_index

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
        if not datasets.get("watchlist_candidates", pd.DataFrame()).empty:
            handlers["google_sheets_watchlist"] = self._publish_watchlist
        if datasets.get("decision_bundle") is not None:
            handlers["google_sheets_event_log"] = self._publish_event_log
            handlers["google_sheets_publish_log"] = self._publish_publish_log
        if bool(context.params.get("publish_quantstats", True)):
            handlers["quantstats_dashboard_tearsheet"] = self._publish_quantstats_dashboard
        if bool(context.params.get("publish_weekly_pdf", False)):
            handlers["weekly_pdf"] = self._publish_weekly_pdf
            # weekly_pdf writes only to the per-attempt directory and has no
            # external side effects, so each publish attempt should produce
            # a fresh report regardless of delivery dedup state.
            existing = list(context.params.get("bypass_dedupe_channels") or [])
            if "weekly_pdf" not in existing:
                existing.append("weekly_pdf")
                context.params["bypass_dedupe_channels"] = existing
        return handlers

    def _publish_local_summary(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.watchlist_digest import render_watchlist_markdown

        watchlist_path = context.output_dir() / "watchlist_digest.md"
        watchlist_path.write_text(
            render_watchlist_markdown(datasets.get("watchlist_candidates", pd.DataFrame())),
            encoding="utf-8",
        )
        return {
            "report_id": f"local-{context.run_id}",
            "trust_status": datasets.get("publish_trust_status", "unknown"),
            "watchlist_digest": str(watchlist_path),
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
        from ai_trading_system.domains.publish.channels.weekly_pdf import metrics as weekly_metrics

        weekly_data = self._load_weekly_report_data(context, datasets)
        prior_ranked_df = pd.DataFrame()
        pattern_df = datasets.get("pattern_scan") if isinstance(datasets.get("pattern_scan"), pd.DataFrame) else pd.DataFrame()
        failed_breakouts_df = pd.DataFrame()
        if weekly_data is not None:
            prior_ranked_df = weekly_data.prior_ranked_signals
            if pattern_df.empty:
                pattern_df = weekly_data.pattern_scan
            failed_breakouts_df = weekly_metrics.detect_failed_breakouts(
                weekly_data.breakout_scan,
                weekly_data.prior_breakouts_per_run,
                weekly_data.ranked_signals,
                top_n=25,
            )

        result = publish_dashboard_payload(
            datasets.get("dashboard_payload", {}),
            project_root=context.project_root,
            run_date=context.run_date,
            ranked_df=datasets.get("ranked_signals"),
            breakout_df=datasets.get("breakout_scan"),
            sector_df=datasets.get("sector_dashboard"),
            prior_ranked_df=prior_ranked_df,
            failed_breakouts_df=failed_breakouts_df,
            pattern_df=pattern_df,
            watchlist_df=datasets.get("watchlist_candidates"),
            decision_bundle=datasets.get("decision_bundle"),
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

    def _publish_watchlist(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.google_sheets import publish_watchlist_candidates

        if not publish_watchlist_candidates(datasets["watchlist_candidates"], decision_bundle=datasets.get("decision_bundle")):
            raise RuntimeError("watchlist publish returned False")
        return {"report_id": "watchlist_candidates_sheet"}

    def _publish_event_log(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.google_sheets import publish_event_log_sheet

        bundle = datasets.get("decision_bundle")
        if bundle is None:
            return {"report_id": "event_log_sheet", "status": "skipped", "reason": "decision_bundle_missing"}
        if not publish_event_log_sheet(bundle):
            raise RuntimeError("event log publish returned False")
        return {"report_id": "event_log_sheet"}

    def _publish_publish_log(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.google_sheets import publish_log_sheet

        bundle = datasets.get("decision_bundle")
        if bundle is None:
            return {"report_id": "publish_log_sheet", "status": "skipped", "reason": "decision_bundle_missing"}
        if not publish_log_sheet(bundle):
            raise RuntimeError("publish log publish returned False")
        return {"report_id": "publish_log_sheet"}

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

    def _publish_weekly_pdf(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.weekly_pdf import publish_weekly_pdf

        return publish_weekly_pdf(context, rank_artifact, datasets)

    def _publish_telegram_summary(
        self,
        context: StageContext,
        rank_artifact: StageArtifact,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        from ai_trading_system.domains.publish.channels.telegram import TelegramReporter

        reporter = TelegramReporter(report_dir=context.project_root / "reports")
        telegram_datasets = dict(datasets)
        telegram_datasets["ranked_signals_full"] = datasets.get("ranked_signals")
        weekly_data = self._load_weekly_report_data(context, datasets)
        if weekly_data is not None:
            telegram_datasets["prior_ranked_signals"] = weekly_data.prior_ranked_signals
            telegram_datasets["prior_breakouts_per_run"] = weekly_data.prior_breakouts_per_run
        publish_rows = pd.DataFrame(datasets.get("publish_rows_telegram", []))
        if not publish_rows.empty:
            telegram_datasets["ranked_signals"] = publish_rows
        bundle = datasets.get("decision_bundle")
        if bundle is not None and getattr(bundle, "telegram_digest", None):
            message = str(bundle.telegram_digest)
        else:
            message = str(datasets.get("insight_telegram_summary") or "").strip()
        if message and bundle is None:
            watchlist_df = datasets.get("watchlist_candidates", pd.DataFrame())
            if isinstance(watchlist_df, pd.DataFrame) and not watchlist_df.empty:
                from ai_trading_system.domains.publish.channels.watchlist_digest import render_watchlist_telegram

                message = message.rstrip() + "\n\n" + render_watchlist_telegram(watchlist_df, top_n=10)
        else:
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

    def _attach_decision_bundle(self, context: StageContext, datasets: Dict[str, Any]) -> None:
        from ai_trading_system.domains.publish.dashboard import _load_operational_breadth
        from ai_trading_system.domains.publish.channels.weekly_pdf import metrics as weekly_metrics

        weekly_data = self._load_weekly_report_data(context, datasets)
        failed_breakouts_df = pd.DataFrame()
        if weekly_data is not None:
            failed_breakouts_df = weekly_metrics.detect_failed_breakouts(
                weekly_data.breakout_scan,
                weekly_data.prior_breakouts_per_run,
                weekly_data.ranked_signals,
                top_n=25,
            )
        breadth_df = _load_operational_breadth(context.project_root)
        event_frame = self._decision_event_frame(datasets)
        datasets["decision_bundle"] = build_publish_decision_bundle(
            run_date=context.run_date,
            ranked_signals=datasets.get("ranked_signals"),
            breakout_scan=datasets.get("breakout_scan"),
            pattern_scan=datasets.get("pattern_scan"),
            stock_scan=datasets.get("stock_scan"),
            sector_dashboard=datasets.get("sector_dashboard"),
            event_frame=event_frame,
            breadth_frame=breadth_df,
            watchlist_frame=datasets.get("watchlist_candidates"),
            trust_status=str(datasets.get("publish_trust_status") or "unknown"),
            failed_breakouts=failed_breakouts_df,
            insight_text=str(datasets.get("insight_telegram_summary") or ""),
            market_direction=(datasets.get("dashboard_payload") or {}).get("market_direction", {}),
            market_regime_phase=(datasets.get("dashboard_payload") or {}).get("market_regime_phase", {}),
        )

    def _decision_event_frame(self, datasets: Dict[str, Any]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        snapshot = datasets.get("market_events_snapshot") or {}
        for row in list(snapshot.get("events") or []):
            if isinstance(row, dict):
                rows.append(dict(row))
        for sig in list(datasets.get("enriched_event_signals") or []):
            if not isinstance(sig, dict):
                continue
            trigger = sig.get("trigger") or {}
            rows.append(
                {
                    "symbol": trigger.get("symbol"),
                    "category": sig.get("top_category") or trigger.get("trigger_type"),
                    "severity": sig.get("severity"),
                    "materiality_label": sig.get("materiality_label"),
                    "tier": sig.get("tier"),
                    "title": sig.get("summary") or sig.get("title"),
                    "event_hash": ",".join(str(item) for item in list(sig.get("event_hashes") or [])),
                }
            )
        return pd.DataFrame(rows)

    def _load_weekly_report_data(
        self,
        context: StageContext,
        datasets: Dict[str, Any],
    ) -> Any | None:
        """Best-effort weekly intelligence load shared by publish channels."""
        try:
            from ai_trading_system.domains.publish.channels.weekly_pdf.data_loader import load_report_data

            return load_report_data(context, datasets)
        except Exception:
            return None


def _snapshot_severity(row: dict[str, Any]) -> str:
    if row.get("materiality_label") in {"critical", "high"}:
        return "high"
    if row.get("tier") == "A":
        return "high"
    if row.get("tier") == "B":
        return "medium"
    return "low-info"
