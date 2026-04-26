"""Service-layer orchestration for the feature stage."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

import duckdb

from ai_trading_system.analytics.data_trust import load_data_trust_summary
from ai_trading_system.pipeline.contracts import TrustConfidenceEnvelope
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class FeaturesOrchestrationService:
    """Run feature recomputation while preserving snapshot artifacts."""

    def __init__(self, operation: Optional[Callable[[StageContext], Dict]] = None):
        self.operation = operation

    def run(
        self,
        context: StageContext,
        *,
        record_snapshot: Optional[Callable[[StageContext], tuple[int, int, int]]] = None,
    ) -> StageResult:
        metadata = self.run_default(context, record_snapshot=record_snapshot)
        artifact_path = context.write_json("feature_snapshot.json", metadata)
        artifact = StageArtifact.from_file(
            "feature_snapshot",
            artifact_path,
            row_count=metadata.get("feature_rows"),
            metadata=metadata,
            attempt_number=context.attempt_number,
        )
        return StageResult(artifacts=[artifact], metadata=metadata)

    def run_default(
        self,
        context: StageContext,
        *,
        record_snapshot: Optional[Callable[[StageContext], tuple[int, int, int]]] = None,
    ) -> Dict:
        if self.operation is not None:
            return self.operation(context)

        from ai_trading_system.domains.ingest import daily_update_runner

        ingest_artifact = context.artifact_for("ingest", "ingest_summary")
        updated_symbols = None
        if ingest_artifact is not None:
            try:
                with open(ingest_artifact.uri, "r", encoding="utf-8") as handle:
                    ingest_summary = json.load(handle)
                updated_symbols = ingest_summary.get("updated_symbols") or None
            except Exception:
                updated_symbols = None

        full_rebuild = bool(
            context.params.get("full_rebuild", False)
            or context.params.get("data_domain") == "research"
        )

        def _render_progress_bar(completed: int, total: int, width: int = 20) -> str:
            total = max(1, int(total))
            completed = max(0, min(int(completed), total))
            filled = int((completed / total) * width)
            return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"

        def _feature_progress(update: dict) -> None:
            status = str(update.get("status") or "running").strip().lower()
            if status == "started":
                context.report_task(
                    task_name="feature_progress",
                    status="running",
                    detail="starting feature computation",
                    metadata=update,
                )
                return

            total = int(update.get("total_steps") or 0)
            completed = int(update.get("completed_steps") or 0)
            pct = int((completed / max(1, total)) * 100)
            bar = _render_progress_bar(completed, total)
            feature_type = str(update.get("feature_type") or "").strip()
            symbol_id = str(update.get("symbol_id") or "").strip()
            step_status = str(update.get("step_status") or "").strip()
            eta = update.get("eta_seconds")
            eta_txt = f"{int(eta)}s" if eta is not None else "n/a"
            detail = (
                f"{bar} {completed}/{max(1, total)} ({pct}%)"
                + (f" · {feature_type}:{symbol_id}" if feature_type and symbol_id else "")
                + (f" · step={step_status}" if step_status else "")
                + f" · eta={eta_txt}"
            )
            if status == "completed":
                context.report_task(
                    task_name="feature_progress",
                    status="done",
                    detail=detail,
                    metadata=update,
                )
            else:
                context.report_task(
                    task_name="feature_progress",
                    status="running",
                    detail=detail,
                    metadata=update,
                )

        daily_update_runner.run(
            symbols_only=False,
            features_only=True,
            batch_size=int(context.params.get("batch_size", 700)),
            bulk=bool(context.params.get("bulk", False)),
            symbol_limit=context.params.get("symbol_limit"),
            data_domain=context.params.get("data_domain", "operational"),
            symbols=updated_symbols,
            full_rebuild=full_rebuild,
            feature_tail_bars=int(context.params.get("feature_tail_bars", 252)),
            feature_progress_callback=_feature_progress,
        )

        snapshot_id, feature_rows, feature_registry_entries = (
            record_snapshot or self.record_snapshot
        )(context)
        benchmark_symbol = str(context.params.get("benchmark_symbol", "NIFTY_500"))
        trust_summary = load_data_trust_summary(context.db_path, run_date=context.run_date)
        feature_confidence = 1.0 if int(feature_rows or 0) > 0 else 0.0
        trust_confidence = TrustConfidenceEnvelope.from_trust_summary(
            trust_summary,
            feature_confidence=feature_confidence,
        )

        return {
            "snapshot_id": int(snapshot_id),
            "feature_rows": feature_rows,
            "feature_registry_entries": int(feature_registry_entries),
            "feature_mode": "full_rebuild" if full_rebuild else "incremental",
            "target_symbol_count": len(updated_symbols or []),
            "feature_enhancements": {
                "readiness": True,
                "feature_confidence": True,
                "multi_timeframe_returns": [5, 20, 60, 120, 252],
                "liquidity": True,
                "cross_sectional": True,
                "pattern_preconditions": True,
                "benchmark_relative": {"enabled": True, "benchmark_symbol": benchmark_symbol},
            },
            "trust_confidence": trust_confidence.to_dict(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    def record_snapshot(self, context: StageContext) -> tuple[int, int, int]:
        """Persist a simple feature snapshot row without relying on legacy helpers."""
        conn = duckdb.connect(str(context.db_path))
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS _snapshots (
                    snapshot_id BIGINT,
                    snapshot_ts TIMESTAMP,
                    symbols_processed BIGINT,
                    rows_written BIGINT,
                    from_date DATE,
                    to_date DATE,
                    status VARCHAR,
                    note VARCHAR
                )
                """
            )
            feature_table_exists = bool(
                conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_feature_registry'"
                ).fetchone()[0]
            )
            if feature_table_exists:
                feature_rows = int(
                    conn.execute(
                        "SELECT COALESCE(SUM(rows_computed), 0), COUNT(*) FROM _feature_registry WHERE status = 'completed'"
                    ).fetchone()[0]
                    or 0
                )
                feature_registry_entries = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM _feature_registry WHERE status = 'completed'"
                    ).fetchone()[0]
                    or 0
                )
            else:
                feature_rows = 0
                feature_registry_entries = 0

            min_date, max_date, symbol_count = conn.execute(
                """
                SELECT MIN(CAST(timestamp AS DATE)), MAX(CAST(timestamp AS DATE)), COUNT(DISTINCT symbol_id)
                FROM _catalog
                """
            ).fetchone()
            snapshot_id = int(
                conn.execute("SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM _snapshots").fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO _snapshots
                (snapshot_id, snapshot_ts, symbols_processed, rows_written, from_date, to_date, status, note)
                VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, 'completed', ?)
                """,
                [
                    snapshot_id,
                    int(symbol_count or 0),
                    feature_rows,
                    min_date,
                    max_date,
                    f"Pipeline run {context.run_id} ({context.run_date})",
                ],
            )
        finally:
            conn.close()
        return snapshot_id, feature_rows, feature_registry_entries
