"""Ingest stage for resilient pipeline orchestration."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Optional

import duckdb

from core.logging import logger
from run.stages.base import StageArtifact, StageContext, StageResult


class IngestStage:
    """Stage wrapper around the existing daily OHLCV update flow."""

    name = "ingest"

    def __init__(self, operation: Optional[Callable[[StageContext], Dict]] = None):
        self.operation = operation

    def run(self, context: StageContext) -> StageResult:
        metadata = self._run_smoke(context) if context.params.get("smoke") else self._run_default(context)
        artifact_path = context.write_json("ingest_summary.json", metadata)
        artifact = StageArtifact.from_file(
            "ingest_summary",
            artifact_path,
            row_count=metadata.get("catalog_rows"),
            metadata=metadata,
            attempt_number=context.attempt_number,
        )
        return StageResult(artifacts=[artifact], metadata=metadata)

    def _run_default(self, context: StageContext) -> Dict:
        if self.operation is not None:
            result = self.operation(context)
        else:
            from collectors.daily_update_runner import run as run_daily_update

            result = run_daily_update(
                symbols_only=True,
                features_only=False,
                batch_size=int(context.params.get("batch_size", 700)),
                bulk=bool(context.params.get("bulk", False)),
                symbol_limit=context.params.get("symbol_limit"),
                data_domain=context.params.get("data_domain", "operational"),
            )

        conn = duckdb.connect(str(context.db_path), read_only=True)
        try:
            catalog_rows, symbol_count, latest_ts = conn.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT symbol_id), MAX(timestamp)
                FROM _catalog
                """
            ).fetchone()
        finally:
            conn.close()

        payload = dict(result or {})
        payload.update(
            {
                "catalog_rows": int(catalog_rows or 0),
                "symbol_count": int(symbol_count or 0),
                "latest_timestamp": str(latest_ts) if latest_ts is not None else None,
            }
        )
        payload.update(self._run_delivery_collection(context, payload))
        return payload

    def _run_smoke(self, context: StageContext) -> Dict:
        return {
            "mode": "smoke",
            "catalog_rows": 1,
            "symbol_count": 1,
            "latest_timestamp": f"{context.run_date} 15:30:00",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _run_delivery_collection(self, context: StageContext, ingest_payload: Dict) -> Dict:
        include_delivery = bool(context.params.get("include_delivery", True))
        if not include_delivery:
            return {
                "delivery_status": "skipped",
                "delivery_reason": "disabled",
            }

        try:
            from collectors.delivery_collector import DeliveryCollector

            collector = DeliveryCollector(
                ohlcv_db_path=str(context.db_path),
                data_domain=context.params.get("data_domain", "operational"),
            )
            to_date = context.run_date
            last_delivery_date = collector.get_last_delivery_date()

            if last_delivery_date:
                from_date = (
                    datetime.fromisoformat(last_delivery_date) + timedelta(days=1)
                ).date().isoformat()
            else:
                backfill_days = int(context.params.get("delivery_backfill_days", 30))
                from_date = (
                    datetime.fromisoformat(to_date) - timedelta(days=backfill_days)
                ).date().isoformat()

            if from_date > to_date:
                return {
                    "delivery_status": "skipped",
                    "delivery_reason": "up_to_date",
                    "delivery_from_date": from_date,
                    "delivery_to_date": to_date,
                    "delivery_last_date": last_delivery_date,
                    "delivery_rows_ingested": 0,
                    "delivery_feature_rows": 0,
                }

            workers = max(1, int(context.params.get("delivery_workers", 4)))
            updated_symbols = ingest_payload.get("updated_symbols")
            symbols: list[str] | None
            if isinstance(updated_symbols, list):
                symbols = sorted({str(symbol) for symbol in updated_symbols if symbol})
            else:
                symbols = None

            rows_ingested = int(
                collector.fetch_range(
                    from_date=from_date,
                    to_date=to_date,
                    n_workers=workers,
                    symbols=symbols,
                )
                or 0
            )
            feature_rows = 0
            if bool(context.params.get("delivery_compute_features", True)) and rows_ingested > 0:
                feature_rows = int(collector.compute_delivery_features(exchange="NSE") or 0)

            return {
                "delivery_status": "completed",
                "delivery_from_date": from_date,
                "delivery_to_date": to_date,
                "delivery_last_date": collector.get_last_delivery_date(),
                "delivery_rows_ingested": rows_ingested,
                "delivery_feature_rows": feature_rows,
            }
        except Exception as exc:
            if bool(context.params.get("delivery_required", False)):
                raise
            logger.warning("Delivery collection failed during ingest stage: %s", exc)
            return {
                "delivery_status": "failed",
                "delivery_error": str(exc),
                "delivery_rows_ingested": 0,
                "delivery_feature_rows": 0,
            }
