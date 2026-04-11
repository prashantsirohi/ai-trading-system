"""Ranking stage with explicit artifact outputs."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import pandas as pd
from pandas.util import hash_pandas_object

from run.stages.base import StageArtifact, StageContext, StageResult
from utils.data_domains import ensure_domain_layout


TASK_FILE_MAP = {
    "rank_core": ("ranked_signals", "csv"),
    "breakout_scan": ("breakout_scan", "csv"),
    "pattern_scan": ("pattern_scan", "csv"),
    "stock_scan": ("stock_scan", "csv"),
    "sector_dashboard": ("sector_dashboard", "csv"),
    "dashboard_payload": ("dashboard_payload", "json"),
}

OPTIONAL_RANK_TASKS = {"breakout_scan", "pattern_scan", "stock_scan", "sector_dashboard"}


class RankStage:
    """Computes downstream ranking artifacts without publishing them."""

    name = "rank"

    def __init__(
        self,
        operation: Optional[Callable[[StageContext], Dict[str, pd.DataFrame]]] = None,
        ml_overlay_builder: Optional[Callable[[StageContext, pd.DataFrame], Dict[str, Any]]] = None,
    ):
        self.operation = operation
        self.ml_overlay_builder = ml_overlay_builder

    def run(self, context: StageContext) -> StageResult:
        if context.params.get("smoke"):
            raise RuntimeError("Smoke mode is disabled because synthetic ranking artifacts have been removed.")
        outputs = self._run_default(context)
        stage_metadata = outputs.pop("__stage_metadata__", {})
        dashboard_payload = outputs.pop("__dashboard_payload__", None)
        outputs, stage_metadata, dashboard_payload, pending_prediction_logs = self._apply_ml_overlay(
            context=context,
            outputs=outputs,
            stage_metadata=stage_metadata,
            dashboard_payload=dashboard_payload,
        )

        artifacts = []
        metadata = {"completed_at": datetime.now(timezone.utc).isoformat()}
        output_dir = context.output_dir()
        artifact_uris: Dict[str, str] = {}

        for artifact_type, df in outputs.items():
            if df is None:
                continue
            path = output_dir / f"{artifact_type}.csv"
            df.to_csv(path, index=False)
            artifact_uris[artifact_type] = str(path)
            artifacts.append(
                StageArtifact.from_file(
                    artifact_type,
                    path,
                    row_count=len(df),
                    metadata={"columns": list(df.columns)},
                    attempt_number=context.attempt_number,
                )
            )
            metadata[f"{artifact_type}_rows"] = len(df)

        if pending_prediction_logs and context.registry is not None:
            metadata["ml_prediction_log_rows"] = self._write_prediction_logs(
                context=context,
                pending_prediction_logs=pending_prediction_logs,
                artifact_uri=artifact_uris.get("ml_overlay"),
            )

        if dashboard_payload is not None:
            dashboard_path = context.write_json("dashboard_payload.json", dashboard_payload)
            artifacts.append(
                StageArtifact.from_file(
                    "dashboard_payload",
                    dashboard_path,
                    row_count=dashboard_payload.get("summary", {}).get("ranked_count"),
                    metadata={"sections": list(dashboard_payload.keys())},
                    attempt_number=context.attempt_number,
                )
            )

        ranked_signals = outputs.get("ranked_signals", pd.DataFrame())
        metadata["ranked_rows"] = len(ranked_signals)
        metadata["top_symbol"] = (
            str(ranked_signals.iloc[0]["symbol_id"])
            if not ranked_signals.empty and "symbol_id" in ranked_signals.columns
            else None
        )
        metadata.update(stage_metadata)
        summary_path = context.write_json("rank_summary.json", metadata)
        artifacts.append(
            StageArtifact.from_file(
                "rank_summary",
                summary_path,
                row_count=metadata["ranked_rows"],
                metadata=metadata,
                attempt_number=context.attempt_number,
            )
        )
        return StageResult(artifacts=artifacts, metadata=metadata)

    def _task_status_path(self, context: StageContext) -> Path:
        return context.output_dir() / "task_status.json"

    def _task_output_path(self, context: StageContext, task_name: str) -> Path:
        artifact_type, suffix = TASK_FILE_MAP[task_name]
        extension = ".json" if suffix == "json" else ".csv"
        return context.output_dir() / f"{artifact_type}{extension}"

    def _previous_task_snapshot(self, context: StageContext) -> tuple[int | None, dict[str, Any]]:
        stage_dir = context.output_dir().parent
        for attempt in range(context.attempt_number - 1, 0, -1):
            snapshot_path = stage_dir / f"attempt_{attempt}" / "task_status.json"
            if not snapshot_path.exists():
                continue
            try:
                payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if isinstance(payload, dict):
                return attempt, payload
        return None, {}

    def _persist_task_status(self, context: StageContext, task_status: dict[str, Any]) -> None:
        self._task_status_path(context).write_text(
            json.dumps(task_status, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )

    def _dataframe_fingerprint(self, frame: pd.DataFrame) -> str:
        if frame is None or frame.empty:
            return "empty"
        normalized = frame.copy()
        normalized.columns = [str(column) for column in normalized.columns]
        normalized = normalized.sort_index(axis=1)
        hashed = hash_pandas_object(normalized.fillna("<NA>"), index=True).values.tobytes()
        return hashlib.sha256(hashed).hexdigest()

    def _task_fingerprint(self, *, task_name: str, payload: dict[str, Any]) -> str:
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        return f"{task_name}:{digest}"

    def _operator_task_id(self, context: StageContext, task_name: str) -> str:
        return f"{context.run_id}:{context.stage_name}:{context.attempt_number}:{task_name}"

    def _start_task_tracking(
        self,
        *,
        context: StageContext,
        task_name: str,
        label: str,
        metadata: dict[str, Any],
    ) -> None:
        context.report_task(task_name=task_name, status="running", detail=label, metadata=metadata)
        if context.registry is None:
            return
        task_id = self._operator_task_id(context, task_name)
        context.registry.create_operator_task(
            task_id=task_id,
            task_type="pipeline_rank_task",
            label=label,
            status="running",
            metadata={
                "run_id": context.run_id,
                "stage_name": context.stage_name,
                "attempt_number": context.attempt_number,
                **metadata,
            },
        )

    def _finish_task_tracking(
        self,
        *,
        context: StageContext,
        task_name: str,
        status: str,
        detail: str,
        metadata: dict[str, Any],
        error: str | None = None,
    ) -> None:
        context.report_task(task_name=task_name, status=status, detail=detail, metadata=metadata)
        if context.registry is None:
            return
        context.registry.update_operator_task(
            self._operator_task_id(context, task_name),
            status=status,
            finished_at=datetime.now(timezone.utc).isoformat(),
            result=metadata,
            error=error,
            metadata={
                "run_id": context.run_id,
                "stage_name": context.stage_name,
                "attempt_number": context.attempt_number,
                **metadata,
            },
        )

    def _load_resumed_result(
        self,
        *,
        context: StageContext,
        task_name: str,
        previous_attempt: int | None,
        previous_statuses: dict[str, Any],
        fingerprint: str,
    ) -> tuple[bool, Any]:
        if previous_attempt is None:
            return False, None
        previous_entry = previous_statuses.get(task_name, {})
        if previous_entry.get("fingerprint") != fingerprint:
            return False, None
        if previous_entry.get("status") not in {"completed", "completed_empty", "skipped"}:
            return False, None
        previous_output = previous_entry.get("output_path")
        if not previous_output:
            return False, None
        previous_path = Path(str(previous_output))
        if not previous_path.exists():
            return False, None
        _, suffix = TASK_FILE_MAP[task_name]
        try:
            if suffix == "json":
                return True, json.loads(previous_path.read_text(encoding="utf-8"))
            return True, pd.read_csv(previous_path)
        except Exception:
            return False, None

    def _persist_task_output(
        self,
        *,
        context: StageContext,
        task_name: str,
        result: Any,
    ) -> Path:
        output_path = self._task_output_path(context, task_name)
        _, suffix = TASK_FILE_MAP[task_name]
        if suffix == "json":
            output_path.write_text(json.dumps(result, indent=2, sort_keys=True, default=str), encoding="utf-8")
        else:
            frame = result if isinstance(result, pd.DataFrame) else pd.DataFrame()
            frame.to_csv(output_path, index=False)
        return output_path

    def _execute_rank_task(
        self,
        *,
        context: StageContext,
        task_name: str,
        label: str,
        fingerprint_payload: dict[str, Any],
        task_status: dict[str, Any],
        previous_attempt: int | None,
        previous_statuses: dict[str, Any],
        builder: Callable[[], Any],
        optional: bool = False,
        skip_reason: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        fingerprint = self._task_fingerprint(task_name=task_name, payload=fingerprint_payload)
        started_at = datetime.now(timezone.utc).isoformat()

        if skip_reason:
            record = {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "ended_at": started_at,
                "detail": skip_reason,
                "fingerprint": fingerprint,
            }
            task_status[task_name] = record
            self._persist_task_status(context, task_status)
            self._finish_task_tracking(
                context=context,
                task_name=task_name,
                status="skipped",
                detail=skip_reason,
                metadata=record,
            )
            _, suffix = TASK_FILE_MAP[task_name]
            return ({ } if suffix == "json" else pd.DataFrame()), record

        resumed, previous_result = self._load_resumed_result(
            context=context,
            task_name=task_name,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            fingerprint=fingerprint,
        )
        if resumed:
            output_path = self._persist_task_output(context=context, task_name=task_name, result=previous_result)
            record = {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "detail": f"Resumed from attempt {previous_attempt}",
                "resumed_from_attempt": previous_attempt,
                "fingerprint": fingerprint,
                "output_path": str(output_path),
            }
            if isinstance(previous_result, pd.DataFrame):
                record["rows"] = int(len(previous_result))
            task_status[task_name] = record
            self._persist_task_status(context, task_status)
            self._finish_task_tracking(
                context=context,
                task_name=task_name,
                status="skipped",
                detail=record["detail"],
                metadata=record,
            )
            return previous_result, record

        self._start_task_tracking(context=context, task_name=task_name, label=label, metadata={"fingerprint": fingerprint})
        task_started_perf = time.perf_counter()
        try:
            result = builder()
            output_path = self._persist_task_output(context=context, task_name=task_name, result=result)
            status = "completed"
            rows = None
            if isinstance(result, pd.DataFrame):
                rows = int(len(result))
                if rows == 0:
                    status = "completed_empty"
            record = {
                "task_name": task_name,
                "status": status,
                "started_at": started_at,
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "detail": label,
                "fingerprint": fingerprint,
                "output_path": str(output_path),
                "elapsed_seconds": round(time.perf_counter() - task_started_perf, 3),
            }
            if rows is not None:
                record["rows"] = rows
            task_status[task_name] = record
            self._persist_task_status(context, task_status)
            self._finish_task_tracking(
                context=context,
                task_name=task_name,
                status=status,
                detail=(
                    f"{label} | {record['elapsed_seconds']:.1f}s"
                    if status == "completed"
                    else f"{label} (0 rows) | {record['elapsed_seconds']:.1f}s"
                ),
                metadata=record,
            )
            return result, record
        except TimeoutError as exc:
            status = "timed_out"
            detail = str(exc) or f"{label} timed out"
            caught_exc = exc
        except Exception as exc:
            status = "failed"
            detail = str(exc)
            caught_exc = exc
        if not optional:
            failure_record = {
                "task_name": task_name,
                "status": status,
                "started_at": started_at,
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "detail": detail,
                "fingerprint": fingerprint,
                "elapsed_seconds": round(time.perf_counter() - task_started_perf, 3),
                "error_class": caught_exc.__class__.__name__ if 'caught_exc' in locals() else status,
                "error_message": detail,
            }
            task_status[task_name] = failure_record
            self._persist_task_status(context, task_status)
            self._finish_task_tracking(
                context=context,
                task_name=task_name,
                status=status,
                detail=detail,
                metadata=failure_record,
                error=detail,
            )
            raise caught_exc
        empty_result = {} if TASK_FILE_MAP[task_name][1] == "json" else pd.DataFrame()
        output_path = self._persist_task_output(context=context, task_name=task_name, result=empty_result)
        failure_record = {
            "task_name": task_name,
            "status": status,
            "started_at": started_at,
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "detail": detail,
            "fingerprint": fingerprint,
            "output_path": str(output_path),
            "elapsed_seconds": round(time.perf_counter() - task_started_perf, 3),
            "error_class": caught_exc.__class__.__name__ if 'caught_exc' in locals() else status,
            "error_message": detail,
        }
        task_status[task_name] = failure_record
        self._persist_task_status(context, task_status)
        self._finish_task_tracking(
            context=context,
            task_name=task_name,
            status=status,
            detail=detail,
            metadata=failure_record,
            error=detail,
        )
        return empty_result, failure_record

    def _run_default(self, context: StageContext) -> Dict[str, pd.DataFrame]:
        if self.operation is not None:
            return self.operation(context)

        from analytics.patterns import PatternScanConfig, build_pattern_signals
        from analytics.patterns.data import load_pattern_frame
        from analytics.data_trust import load_data_trust_summary
        from analytics.ranker import StockRanker
        from channel import sector_dashboard, stock_scan
        from channel.breakout_scan import scan_breakouts

        paths = ensure_domain_layout(
            project_root=context.project_root,
            data_domain=context.params.get("data_domain", "operational"),
        )
        trust_summary = load_data_trust_summary(context.db_path, run_date=context.run_date)
        if trust_summary.get("status") == "blocked" and not bool(context.params.get("allow_untrusted_rank", False)):
            raise RuntimeError(
                "Ranking blocked because active data quarantine remains for the current trust window."
            )
        if trust_summary.get("status") == "degraded":
            warnings = [f"data trust degraded: fallback_ratio={float(trust_summary.get('fallback_ratio_latest', 0.0) or 0.0) * 100:.1f}%"]
        else:
            warnings = []
        previous_attempt, previous_statuses = self._previous_task_snapshot(context)
        task_status: dict[str, Any] = {}

        ranker = StockRanker(
            ohlcv_db_path=str(context.db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain=context.params.get("data_domain", "operational"),
        )
        ranked, _ = self._execute_rank_task(
            context=context,
            task_name="rank_core",
            label="Build ranked_signals",
            fingerprint_payload={
                "task": "rank_core",
                "run_date": context.run_date,
                "data_domain": context.params.get("data_domain", "operational"),
                "min_score": float(context.params.get("min_score", 0.0)),
                "top_n": context.params.get("top_n"),
                "symbol_limit": context.params.get("symbol_limit"),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: ranker.rank_all(
                date=context.run_date,
                min_score=float(context.params.get("min_score", 0.0)),
                top_n=context.params.get("top_n"),
            ),
            optional=False,
        )

        outputs: Dict[str, pd.DataFrame] = {"ranked_signals": ranked}

        breakout_market_bias_allowlist = context.params.get("breakout_market_bias_allowlist", "BULLISH,NEUTRAL")
        if isinstance(breakout_market_bias_allowlist, str):
            breakout_market_bias_allowlist = [
                item.strip()
                for item in breakout_market_bias_allowlist.split(",")
                if item.strip()
            ]
        breakout_df, breakout_status = self._execute_rank_task(
            context=context,
            task_name="breakout_scan",
            label="Build breakout_scan",
            fingerprint_payload={
                "task": "breakout_scan",
                "run_date": context.run_date,
                "breakout_engine": str(context.params.get("breakout_engine", "v2")),
                "market_bias_allowlist": list(breakout_market_bias_allowlist),
                "breakout_min_breadth_score": float(context.params.get("breakout_min_breadth_score", 45.0)),
                "ranked_fingerprint": self._dataframe_fingerprint(ranked),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: scan_breakouts(
                ohlcv_db_path=str(context.db_path),
                feature_store_dir=str(paths.feature_store_dir),
                master_db_path=str(paths.master_db_path),
                date=context.run_date,
                ranked_df=ranked,
                breakout_engine=str(context.params.get("breakout_engine", "v2")),
                include_legacy_families=bool(context.params.get("breakout_include_legacy_families", True)),
                market_bias_allowlist=breakout_market_bias_allowlist,
                min_breadth_score=float(context.params.get("breakout_min_breadth_score", 45.0)),
                sector_rs_min=(
                    float(context.params.get("breakout_sector_rs_min"))
                    if context.params.get("breakout_sector_rs_min") not in (None, "")
                    else None
                ),
                sector_rs_percentile_min=(
                    float(context.params.get("breakout_sector_rs_percentile_min", 60.0))
                    if context.params.get("breakout_sector_rs_percentile_min") not in (None, "")
                    else None
                ),
                breakout_qualified_min_score=int(context.params.get("breakout_qualified_min_score", 3)),
                breakout_symbol_trend_gate_enabled=bool(
                    context.params.get("breakout_symbol_trend_gate_enabled", True)
                ),
                breakout_symbol_near_high_max_pct=float(
                    context.params.get("breakout_symbol_near_high_max_pct", 15.0)
                ),
            ),
            optional=True,
        )
        outputs["breakout_scan"] = breakout_df
        if breakout_status["status"] in {"failed", "timed_out", "degraded"}:
            warnings.append(f"breakout_scan unavailable: {breakout_status.get('error_message', breakout_status.get('detail'))}")

        raw_pattern_symbols = ranked["symbol_id"].astype(str).tolist() if not ranked.empty and "symbol_id" in ranked.columns else []
        max_pattern_symbols = context.params.get("pattern_max_symbols", 150)
        pattern_symbols = raw_pattern_symbols[: int(max_pattern_symbols)] if max_pattern_symbols else raw_pattern_symbols
        pattern_enabled = bool(context.params.get("pattern_scan_enabled", True))

        def _build_pattern_output() -> pd.DataFrame:
            if not pattern_symbols:
                return pd.DataFrame()
            lookback_days = int(context.params.get("pattern_lookback_days", 420))
            from_ts = (pd.Timestamp(context.run_date) - pd.Timedelta(days=lookback_days)).date().isoformat()
            started_at = time.perf_counter()

            def _progress(update: dict[str, Any]) -> None:
                processed = int(update.get("processed_symbols", 0) or 0)
                total = int(update.get("total_symbols", 0) or 0)
                symbol_id = str(update.get("symbol_id", "") or "")
                elapsed = time.perf_counter() - started_at
                context.report_task(
                    task_name="pattern_scan",
                    status="running",
                    detail=f"{processed}/{total} symbols | {elapsed:.1f}s | {symbol_id}",
                    metadata={"processed_symbols": processed, "total_symbols": total, "elapsed_seconds": round(elapsed, 2)},
                )

            pattern_frame = load_pattern_frame(
                context.project_root,
                from_date=from_ts,
                to_date=context.run_date,
                exchange=str(context.params.get("exchange", "NSE")),
                symbols=pattern_symbols,
                data_domain=str(context.params.get("data_domain", "operational")),
            )
            pattern_config = PatternScanConfig(
                exchange=str(context.params.get("exchange", "NSE")),
                data_domain=str(context.params.get("data_domain", "operational")),
                symbols=tuple(pattern_symbols or ()),
                bandwidth=float(context.params.get("pattern_bandwidth", 3.0)),
                extrema_prominence=float(context.params.get("pattern_extrema_prominence", 0.02)),
                breakout_volume_ratio_min=float(context.params.get("pattern_breakout_volume_ratio_min", 1.5)),
                smoothing_method=str(context.params.get("pattern_smoothing_method", "rolling")),
            )
            return build_pattern_signals(
                project_root=context.project_root,
                signal_date=context.run_date,
                exchange=str(context.params.get("exchange", "NSE")),
                data_domain=str(context.params.get("data_domain", "operational")),
                symbols=pattern_symbols,
                config=pattern_config,
                ranked_df=ranked,
                frame=pattern_frame,
                lookback_days=lookback_days,
                progress_callback=_progress,
                pattern_workers=int(context.params.get("pattern_workers", 1) or 1),
            )

        pattern_df, pattern_status = self._execute_rank_task(
            context=context,
            task_name="pattern_scan",
            label="Build pattern_scan",
            fingerprint_payload={
                "task": "pattern_scan",
                "run_date": context.run_date,
                "pattern_scan_enabled": pattern_enabled,
                "pattern_max_symbols": len(pattern_symbols),
                "pattern_bandwidth": float(context.params.get("pattern_bandwidth", 3.0)),
                "pattern_extrema_prominence": float(context.params.get("pattern_extrema_prominence", 0.02)),
                "breakout_volume_ratio_min": float(context.params.get("pattern_breakout_volume_ratio_min", 1.5)),
                "ranked_fingerprint": self._dataframe_fingerprint(ranked),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=_build_pattern_output,
            optional=True,
            skip_reason=None if pattern_enabled else "pattern_scan disabled by config",
        )
        outputs["pattern_scan"] = pattern_df
        if pattern_status["status"] in {"failed", "timed_out", "degraded"}:
            warnings.append(f"pattern_scan unavailable: {pattern_status.get('error_message', pattern_status.get('detail'))}")

        stock_scan_df, stock_status = self._execute_rank_task(
            context=context,
            task_name="stock_scan",
            label="Build stock_scan",
            fingerprint_payload={
                "task": "stock_scan",
                "run_date": context.run_date,
                "data_domain": context.params.get("data_domain", "operational"),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: stock_scan.scan_stocks(
                stock_scan.load_sector_rs(),
                stock_scan.load_stock_vs_sector(),
                stock_scan.load_sector_mapping(),
            ).reset_index(),
            optional=True,
        )
        outputs["stock_scan"] = stock_scan_df
        if stock_status["status"] in {"failed", "timed_out", "degraded"}:
            warnings.append(f"stock_scan unavailable: {stock_status.get('error_message', stock_status.get('detail'))}")

        sector_dashboard_df, sector_status = self._execute_rank_task(
            context=context,
            task_name="sector_dashboard",
            label="Build sector_dashboard",
            fingerprint_payload={
                "task": "sector_dashboard",
                "run_date": context.run_date,
                "data_domain": context.params.get("data_domain", "operational"),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: sector_dashboard.build_dashboard(
                sector_dashboard.load_sector_rs(),
                sector_dashboard.compute_sector_momentum(
                    sector_dashboard.load_sector_rs(),
                    days=20,
                ),
            ).reset_index(),
            optional=True,
        )
        outputs["sector_dashboard"] = sector_dashboard_df
        if sector_status["status"] in {"failed", "timed_out", "degraded"}:
            warnings.append(
                f"sector_dashboard unavailable: {sector_status.get('error_message', sector_status.get('detail'))}"
            )

        dashboard_payload, dashboard_status = self._execute_rank_task(
            context=context,
            task_name="dashboard_payload",
            label="Build dashboard_payload",
            fingerprint_payload={
                "task": "dashboard_payload",
                "run_date": context.run_date,
                "ranked_fingerprint": self._dataframe_fingerprint(ranked),
                "breakout_fingerprint": self._dataframe_fingerprint(breakout_df),
                "pattern_fingerprint": self._dataframe_fingerprint(pattern_df),
                "stock_scan_fingerprint": self._dataframe_fingerprint(stock_scan_df),
                "sector_dashboard_fingerprint": self._dataframe_fingerprint(sector_dashboard_df),
                "warnings": list(warnings),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: self._build_dashboard_payload(
                context=context,
                ranked_df=ranked,
                breakout_df=breakout_df,
                pattern_df=pattern_df,
                stock_scan_df=stock_scan_df,
                sector_dashboard_df=sector_dashboard_df,
                warnings=warnings,
                trust_summary=trust_summary,
                task_status=task_status,
            ),
            optional=False,
        )

        outputs["__stage_metadata__"] = {
            "degraded_outputs": warnings,
            "degraded_output_count": len(warnings),
            "data_trust_status": trust_summary.get("status"),
            "task_status": task_status,
            "task_status_counts": self._summarize_task_statuses(task_status),
            "resumed_from_attempt": previous_attempt,
        }
        outputs["__dashboard_payload__"] = dashboard_payload
        return outputs

    def _apply_ml_overlay(
        self,
        *,
        context: StageContext,
        outputs: Dict[str, pd.DataFrame],
        stage_metadata: Dict[str, Any],
        dashboard_payload: Optional[Dict[str, object]],
    ) -> tuple[Dict[str, pd.DataFrame], Dict[str, Any], Optional[Dict[str, object]], Dict[int, Dict[str, Any]]]:
        ml_mode = str(context.params.get("ml_mode", "baseline_only"))
        stage_metadata = dict(stage_metadata or {})
        degraded_outputs = list(stage_metadata.get("degraded_outputs", []))
        stage_metadata["degraded_outputs"] = degraded_outputs
        stage_metadata["degraded_output_count"] = len(degraded_outputs)
        stage_metadata["ml_mode"] = ml_mode

        if context.params.get("smoke") or ml_mode == "baseline_only":
            stage_metadata["ml_status"] = "disabled"
            dashboard_payload = self._augment_dashboard_payload_with_ml(
                dashboard_payload,
                ml_status=stage_metadata["ml_status"],
                ml_mode=ml_mode,
                ml_overlay_df=outputs.get("ml_overlay", pd.DataFrame()),
            )
            return outputs, stage_metadata, dashboard_payload, {}

        ranked_df = outputs.get("ranked_signals", pd.DataFrame())
        if ranked_df.empty:
            stage_metadata["ml_status"] = "skipped_empty_ranked"
            dashboard_payload = self._augment_dashboard_payload_with_ml(
                dashboard_payload,
                ml_status=stage_metadata["ml_status"],
                ml_mode=ml_mode,
                ml_overlay_df=pd.DataFrame(),
            )
            return outputs, stage_metadata, dashboard_payload, {}

        if ml_mode != "shadow_ml":
            degraded_outputs.append(f"ml overlay unavailable: unsupported ml_mode={ml_mode}")
            stage_metadata["degraded_output_count"] = len(degraded_outputs)
            stage_metadata["ml_status"] = "unsupported_mode"
            dashboard_payload = self._augment_dashboard_payload_with_ml(
                dashboard_payload,
                ml_status=stage_metadata["ml_status"],
                ml_mode=ml_mode,
                ml_overlay_df=pd.DataFrame(),
            )
            return outputs, stage_metadata, dashboard_payload, {}

        builder = self.ml_overlay_builder or self._default_ml_overlay_builder
        try:
            overlay_result = builder(context, ranked_df)
        except Exception as exc:
            degraded_outputs.append(f"ml overlay unavailable: {exc}")
            stage_metadata["degraded_output_count"] = len(degraded_outputs)
            stage_metadata["ml_status"] = "degraded"
            dashboard_payload = self._augment_dashboard_payload_with_ml(
                dashboard_payload,
                ml_status=stage_metadata["ml_status"],
                ml_mode=ml_mode,
                ml_overlay_df=pd.DataFrame(),
            )
            return outputs, stage_metadata, dashboard_payload, {}

        stage_metadata["ml_status"] = overlay_result.get("status", "unknown")
        overlay_df = overlay_result.get("overlay_df", pd.DataFrame())
        if overlay_df is not None and not overlay_df.empty:
            outputs["ml_overlay"] = overlay_df
            stage_metadata["ml_overlay_rows"] = int(len(overlay_df))
            stage_metadata["ml_prediction_date"] = overlay_result.get("prediction_date")
        elif overlay_result.get("reason"):
            degraded_outputs.append(f"ml overlay unavailable: {overlay_result['reason']}")
        stage_metadata["degraded_output_count"] = len(degraded_outputs)
        stage_metadata["ml_metadata"] = overlay_result.get("metadata", {})
        dashboard_payload = self._augment_dashboard_payload_with_ml(
            dashboard_payload,
            ml_status=stage_metadata["ml_status"],
            ml_mode=ml_mode,
            ml_overlay_df=overlay_df if overlay_df is not None else pd.DataFrame(),
        )
        return outputs, stage_metadata, dashboard_payload, overlay_result.get("prediction_logs", {})

    def _default_ml_overlay_builder(self, context: StageContext, ranked_df: pd.DataFrame) -> Dict[str, Any]:
        from analytics.alpha.scoring import OperationalMLOverlayService

        service = OperationalMLOverlayService(
            project_root=context.project_root,
            registry=context.registry,
            data_domain=context.params.get("data_domain", "operational"),
        )
        prediction_logs = service.build_shadow_overlay(
            prediction_date=context.run_date,
            exchange=str(context.params.get("exchange", "NSE")),
            lookback_days=int(context.params.get("ml_lookback_days", 420)),
            technical_weight=float(context.params.get("ml_technical_weight", 0.75)),
            ml_weight=float(context.params.get("ml_weight", 0.25)),
        )
        if prediction_logs.get("prediction_logs"):
            for horizon, rows in list(prediction_logs["prediction_logs"].items()):
                model_id = rows[0].get("model_id") if rows else None
                prediction_logs["prediction_logs"][horizon] = {
                    "rows": rows,
                    "prediction_date": prediction_logs.get("prediction_date", context.run_date),
                    "deployment_mode": "shadow_ml",
                    "model_id": model_id,
                }
        return prediction_logs

    def _write_prediction_logs(
        self,
        *,
        context: StageContext,
        pending_prediction_logs: Dict[int, Dict[str, Any]],
        artifact_uri: Optional[str],
    ) -> int:
        inserted = 0
        for horizon, payload in pending_prediction_logs.items():
            if isinstance(payload, dict):
                rows = payload.get("rows", [])
                prediction_date = payload.get("prediction_date", context.run_date)
                deployment_mode = payload.get("deployment_mode", "shadow_ml")
                model_id = payload.get("model_id")
            else:
                rows = list(payload)
                prediction_date = context.run_date
                deployment_mode = "shadow_ml"
                model_id = rows[0].get("model_id") if rows else None
            if not rows:
                continue
            inserted += context.registry.replace_prediction_log(
                prediction_date,
                rows,
                deployment_mode=deployment_mode,
                horizon=int(horizon),
                model_id=model_id,
                artifact_uri=artifact_uri,
            )
        return inserted

    def _build_dashboard_payload(
        self,
        context: StageContext,
        ranked_df: pd.DataFrame,
        breakout_df: pd.DataFrame,
        pattern_df: pd.DataFrame,
        stock_scan_df: pd.DataFrame,
        sector_dashboard_df: pd.DataFrame,
        warnings: list[str],
        trust_summary: Dict[str, Any] | None = None,
        task_status: Dict[str, Any] | None = None,
    ) -> Dict[str, object]:
        """Assemble a unified operator payload from the rank-stage artifacts."""

        def _records(df: pd.DataFrame, limit: int = 10) -> list[dict]:
            if df is None or df.empty:
                return []
            return df.head(limit).to_dict(orient="records")

        top_sector = None
        if not sector_dashboard_df.empty:
            sector_col = "Sector" if "Sector" in sector_dashboard_df.columns else sector_dashboard_df.columns[0]
            top_sector = sector_dashboard_df.iloc[0].get(sector_col)
        breakout_state_counts: Dict[str, int] = {}
        if breakout_df is not None and not breakout_df.empty and "breakout_state" in breakout_df.columns:
            breakout_state_counts = (
                breakout_df["breakout_state"]
                .astype(str)
                .value_counts()
                .to_dict()
            )
        candidate_tier_counts: Dict[str, int] = {}
        if breakout_df is not None and not breakout_df.empty and "candidate_tier" in breakout_df.columns:
            candidate_tier_counts = (
                breakout_df["candidate_tier"]
                .astype(str)
                .value_counts()
                .to_dict()
            )
        pattern_state_counts: Dict[str, int] = {}
        pattern_family_counts: Dict[str, int] = {}
        if pattern_df is not None and not pattern_df.empty:
            if "pattern_state" in pattern_df.columns:
                pattern_state_counts = pattern_df["pattern_state"].astype(str).value_counts().to_dict()
            if "pattern_family" in pattern_df.columns:
                pattern_family_counts = pattern_df["pattern_family"].astype(str).value_counts().to_dict()

        return {
            "summary": {
                "run_id": context.run_id,
                "run_date": context.run_date,
                "ranked_count": int(len(ranked_df)),
                "breakout_count": int(len(breakout_df)),
                "pattern_count": int(len(pattern_df)),
                "stock_scan_count": int(len(stock_scan_df)),
                "sector_count": int(len(sector_dashboard_df)),
                "top_symbol": (
                    ranked_df.iloc[0]["symbol_id"]
                    if not ranked_df.empty and "symbol_id" in ranked_df.columns
                    else None
                ),
                "top_sector": top_sector,
                "breakout_engine": str(context.params.get("breakout_engine", "v2")),
                "breakout_qualified_count": int(breakout_state_counts.get("qualified", 0)),
                "breakout_watchlist_count": int(breakout_state_counts.get("watchlist", 0)),
                "breakout_filtered_count": int(
                    breakout_state_counts.get("filtered_by_regime", 0)
                    + breakout_state_counts.get("filtered_by_symbol_trend", 0)
                ),
                "breakout_state_counts": breakout_state_counts,
                "breakout_tier_counts": candidate_tier_counts,
                "pattern_confirmed_count": int(pattern_state_counts.get("confirmed", 0)),
                "pattern_watchlist_count": int(pattern_state_counts.get("watchlist", 0)),
                "pattern_state_counts": pattern_state_counts,
                "pattern_family_counts": pattern_family_counts,
                "data_trust_status": (trust_summary or {}).get("status", "unknown"),
                "latest_trade_date": (trust_summary or {}).get("latest_trade_date"),
                "latest_validated_date": (trust_summary or {}).get("latest_validated_date"),
                "task_status_counts": self._summarize_task_statuses(task_status or {}),
            },
            "ranked_signals": _records(ranked_df, limit=10),
            "breakout_scan": _records(breakout_df, limit=10),
            "pattern_scan": _records(pattern_df, limit=10),
            "stock_scan": _records(stock_scan_df, limit=10),
            "sector_dashboard": _records(sector_dashboard_df, limit=10),
            "task_status": task_status or {},
            "data_trust": trust_summary or {},
            "warnings": warnings,
        }

    def _summarize_task_statuses(self, task_status: Dict[str, Any]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for payload in (task_status or {}).values():
            status = str((payload or {}).get("status", "unknown"))
            counts[status] = int(counts.get(status, 0)) + 1
        return counts

    def _augment_dashboard_payload_with_ml(
        self,
        dashboard_payload: Optional[Dict[str, object]],
        *,
        ml_status: str,
        ml_mode: str,
        ml_overlay_df: pd.DataFrame,
    ) -> Optional[Dict[str, object]]:
        if dashboard_payload is None:
            return None

        payload = dict(dashboard_payload)
        summary = dict(payload.get("summary", {}))
        summary["ml_mode"] = ml_mode
        summary["ml_status"] = ml_status
        summary["ml_overlay_count"] = int(len(ml_overlay_df)) if ml_overlay_df is not None else 0
        payload["summary"] = summary
        payload["ml_overlay"] = (
            ml_overlay_df.head(10).to_dict(orient="records")
            if ml_overlay_df is not None and not ml_overlay_df.empty
            else []
        )
        return payload
