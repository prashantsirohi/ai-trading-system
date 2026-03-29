"""Feature computation stage."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Dict, Optional

import duckdb

from run.stages.base import StageArtifact, StageContext, StageResult


class FeaturesStage:
    """Stage wrapper for feature recomputation and snapshotting."""

    name = "features"

    def __init__(self, operation: Optional[Callable[[StageContext], Dict]] = None):
        self.operation = operation

    def run(self, context: StageContext) -> StageResult:
        metadata = self._run_smoke(context) if context.params.get("smoke") else self._run_default(context)
        artifact_path = context.write_json("feature_snapshot.json", metadata)
        artifact = StageArtifact.from_file(
            "feature_snapshot",
            artifact_path,
            row_count=metadata.get("feature_rows"),
            metadata=metadata,
            attempt_number=context.attempt_number,
        )
        return StageResult(artifacts=[artifact], metadata=metadata)

    def _run_default(self, context: StageContext) -> Dict:
        if self.operation is not None:
            return self.operation(context)

        from collectors.daily_update_runner import run as run_daily_update

        run_daily_update(
            symbols_only=False,
            features_only=True,
            batch_size=int(context.params.get("batch_size", 700)),
            bulk=bool(context.params.get("bulk", False)),
            symbol_limit=context.params.get("symbol_limit"),
            data_domain=context.params.get("data_domain", "operational"),
        )

        snapshot_id, feature_rows, feature_registry_entries = self._record_snapshot(context)

        return {
            "snapshot_id": int(snapshot_id),
            "feature_rows": feature_rows,
            "feature_registry_entries": int(feature_registry_entries),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _run_smoke(self, context: StageContext) -> Dict:
        return {
            "mode": "smoke",
            "snapshot_id": 1,
            "feature_rows": 1,
            "feature_registry_entries": 1,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _record_snapshot(self, context: StageContext) -> tuple[int, int, int]:
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
