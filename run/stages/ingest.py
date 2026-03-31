"""Ingest stage for resilient pipeline orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Optional

import duckdb

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
        return payload

    def _run_smoke(self, context: StageContext) -> Dict:
        return {
            "mode": "smoke",
            "catalog_rows": 1,
            "symbol_count": 1,
            "latest_timestamp": f"{context.run_date} 15:30:00",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
