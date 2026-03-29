"""DuckDB-backed registry for pipeline runs, artifacts, DQ, and model governance."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb

from run.stages.base import StageArtifact


DEFAULT_RULES = [
    {
        "rule_id": "ingest_duplicate_ohlcv_key",
        "stage_name": "ingest",
        "dataset_name": "_catalog",
        "severity": "critical",
        "rule_sql": """
            SELECT COUNT(*)
            FROM (
                SELECT symbol_id, exchange, timestamp, COUNT(*) AS duplicate_count
                FROM _catalog
                GROUP BY 1, 2, 3
                HAVING COUNT(*) > 1
            ) duplicate_keys
        """,
        "description": "OHLCV raw key must be unique per symbol/exchange/timestamp.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_catalog_not_empty",
        "stage_name": "ingest",
        "dataset_name": "_catalog",
        "severity": "critical",
        "rule_sql": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM _catalog",
        "description": "OHLCV catalog must contain at least one row after ingest.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_required_fields_not_null",
        "stage_name": "ingest",
        "dataset_name": "_catalog",
        "severity": "critical",
        "rule_sql": """
            SELECT COUNT(*)
            FROM _catalog
            WHERE symbol_id IS NULL
               OR exchange IS NULL
               OR timestamp IS NULL
               OR open IS NULL
               OR high IS NULL
               OR low IS NULL
               OR close IS NULL
               OR volume IS NULL
        """,
        "description": "Key OHLCV columns must be populated.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_ohlc_consistency",
        "stage_name": "ingest",
        "dataset_name": "_catalog",
        "severity": "critical",
        "rule_sql": """
            SELECT COUNT(*)
            FROM _catalog
            WHERE high < GREATEST(open, close)
               OR low > LEAST(open, close)
               OR high < low
        """,
        "description": "OHLC values must obey high/low consistency rules.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_negative_volume",
        "stage_name": "ingest",
        "dataset_name": "_catalog",
        "severity": "high",
        "rule_sql": "SELECT COUNT(*) FROM _catalog WHERE volume < 0",
        "description": "Volume should not be negative.",
        "owner": "pipeline",
    },
    {
        "rule_id": "features_snapshot_created",
        "stage_name": "features",
        "dataset_name": "feature_snapshot",
        "severity": "critical",
        "rule_sql": "SELECT CASE WHEN {snapshot_id} IS NULL THEN 1 ELSE 0 END",
        "description": "Features stage must publish a snapshot reference.",
        "owner": "pipeline",
    },
    {
        "rule_id": "features_registry_not_empty",
        "stage_name": "features",
        "dataset_name": "_feature_registry",
        "severity": "critical",
        "rule_sql": "SELECT CASE WHEN {feature_rows} > 0 THEN 0 ELSE 1 END",
        "description": "Features stage must report at least one computed row.",
        "owner": "pipeline",
    },
    {
        "rule_id": "features_catalog_freshness",
        "stage_name": "features",
        "dataset_name": "_catalog",
        "severity": "high",
        "rule_sql": """
            SELECT CASE
                WHEN MAX(CAST(timestamp AS DATE)) < DATE {run_date_literal} - INTERVAL 5 DAY THEN 1
                ELSE 0
            END
            FROM _catalog
        """,
        "description": "Feature runs should operate on recent catalog data.",
        "owner": "pipeline",
    },
    {
        "rule_id": "rank_artifact_not_empty",
        "stage_name": "rank",
        "dataset_name": "ranked_signals",
        "severity": "critical",
        "rule_sql": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM read_csv_auto({rank_artifact_uri})",
        "description": "Ranking output must not be empty.",
        "owner": "pipeline",
    },
    {
        "rule_id": "rank_required_columns_present",
        "stage_name": "rank",
        "dataset_name": "ranked_signals",
        "severity": "high",
        "rule_sql": None,
        "description": "Ranking artifact must include symbol_id and composite_score.",
        "owner": "pipeline",
    },
    {
        "rule_id": "rank_duplicate_symbols",
        "stage_name": "rank",
        "dataset_name": "ranked_signals",
        "severity": "medium",
        "rule_sql": """
            SELECT COUNT(*)
            FROM (
                SELECT symbol_id, COUNT(*) AS duplicate_count
                FROM read_csv_auto({rank_artifact_uri})
                GROUP BY 1
                HAVING COUNT(*) > 1
            ) duplicate_symbols
        """,
        "description": "Ranking output should not contain duplicate symbols.",
        "owner": "pipeline",
    },
    {
        "rule_id": "rank_symbol_coverage_low",
        "stage_name": "rank",
        "dataset_name": "ranked_signals",
        "severity": "high",
        "rule_sql": """
            SELECT CASE
                WHEN COUNT(*) < {expected_rank_min_rows} THEN 1
                ELSE 0
            END
            FROM read_csv_auto({rank_artifact_uri})
        """,
        "description": "Rank output symbol coverage is lower than expected.",
        "owner": "pipeline",
    },
]


class RegistryStore:
    """Persists run metadata and governance records into DuckDB."""

    def __init__(self, project_root: Path | str, db_path: Optional[Path | str] = None):
        self.project_root = Path(project_root)
        self.db_path = Path(db_path) if db_path else self.project_root / "data" / "ohlcv.duckdb"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._apply_migrations()
        self.seed_default_rules()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    def _migration_path(self) -> Path:
        candidate_root = self.project_root / "sql" / "migrations"
        if candidate_root.exists():
            return candidate_root
        return Path(__file__).resolve().parents[2] / "sql" / "migrations"

    def _apply_migrations(self) -> None:
        conn = self._connect()
        try:
            for migration_path in sorted(self._migration_path().glob("*.sql")):
                conn.execute(migration_path.read_text(encoding="utf-8"))
        finally:
            conn.close()

    def seed_default_rules(self) -> None:
        conn = self._connect()
        try:
            for rule in DEFAULT_RULES:
                conn.execute(
                    """
                    INSERT INTO dq_rule
                    (rule_id, stage_name, dataset_name, severity, rule_sql, description, owner, enabled, active, rollout_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, TRUE, TRUE, CURRENT_DATE)
                    ON CONFLICT(rule_id) DO UPDATE SET
                        stage_name = excluded.stage_name,
                        dataset_name = excluded.dataset_name,
                        severity = excluded.severity,
                        rule_sql = excluded.rule_sql,
                        description = excluded.description,
                        owner = excluded.owner
                    """,
                    [
                        rule["rule_id"],
                        rule["stage_name"],
                        rule["dataset_name"],
                        rule["severity"],
                        rule.get("rule_sql"),
                        rule["description"],
                        rule["owner"],
                    ],
                )
        finally:
            conn.close()

    def run_exists(self, run_id: str) -> bool:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM pipeline_run WHERE run_id = ?",
                [run_id],
            ).fetchone()
            return bool(row and row[0])
        finally:
            conn.close()

    def create_run(
        self,
        run_id: str,
        pipeline_name: str,
        run_date: str,
        trigger: str = "manual",
        status: str = "running",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO pipeline_run
                (run_id, pipeline_name, run_date, trigger, status, started_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                [run_id, pipeline_name, run_date, trigger, status, self._json(metadata)],
            )
        finally:
            conn.close()

    def update_run(
        self,
        run_id: str,
        status: str,
        current_stage: Optional[str] = None,
        error_class: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        finished: bool = False,
    ) -> None:
        assignments = ["status = ?"]
        params: List[Any] = [status]
        if current_stage is not None:
            assignments.append("current_stage = ?")
            params.append(current_stage)
        if error_class is not None:
            assignments.append("error_class = ?")
            params.append(error_class)
        if error_message is not None:
            assignments.append("error_message = ?")
            params.append(error_message)
        if metadata is not None:
            assignments.append("metadata_json = ?")
            params.append(self._json(metadata))
        if finished:
            assignments.append("ended_at = CURRENT_TIMESTAMP")
        params.append(run_id)

        conn = self._connect()
        try:
            conn.execute(
                f"UPDATE pipeline_run SET {', '.join(assignments)} WHERE run_id = ?",
                params,
            )
        finally:
            conn.close()

    def append_run_metadata_event(self, run_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
        """Append an immutable audit event to pipeline_run metadata_json."""
        run_record = self.get_run(run_id)
        metadata = run_record.get("metadata", {}) if run_record else {}
        history = list(metadata.get("events", []))
        history.append(event)
        metadata["events"] = history
        self.update_run(run_id, status=run_record.get("status", "running"), metadata=metadata)
        return metadata

    def next_stage_attempt(self, run_id: str, stage_name: str) -> int:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(attempt_number), 0) + 1
                FROM pipeline_stage_run
                WHERE run_id = ? AND stage_name = ?
                """,
                [run_id, stage_name],
            ).fetchone()
            return int(row[0]) if row else 1
        finally:
            conn.close()

    def start_stage(self, run_id: str, stage_name: str, attempt_number: int) -> str:
        stage_run_id = f"{stage_name}-{attempt_number}-{uuid.uuid4().hex[:8]}"
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO pipeline_stage_run
                (stage_run_id, run_id, stage_name, attempt_number, status, started_at)
                VALUES (?, ?, ?, ?, 'running', CURRENT_TIMESTAMP)
                """,
                [stage_run_id, run_id, stage_name, attempt_number],
            )
        finally:
            conn.close()
        return stage_run_id

    def finish_stage(
        self,
        stage_run_id: str,
        status: str,
        error_class: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                UPDATE pipeline_stage_run
                SET status = ?, ended_at = CURRENT_TIMESTAMP,
                    error_class = ?, error_message = ?, metadata_json = ?
                WHERE stage_run_id = ?
                """,
                [status, error_class, error_message, self._json(metadata), stage_run_id],
            )
        finally:
            conn.close()

    def record_artifact(
        self,
        run_id: str,
        stage_name: str,
        attempt_number: int,
        artifact: StageArtifact,
    ) -> None:
        artifact_id = f"{stage_name}-{artifact.artifact_type}-{uuid.uuid4().hex[:10]}"
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO pipeline_artifact
                (artifact_id, run_id, stage_name, attempt_number, artifact_type, uri, content_hash, row_count, created_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                [
                    artifact_id,
                    run_id,
                    stage_name,
                    attempt_number,
                    artifact.artifact_type,
                    artifact.uri,
                    artifact.content_hash,
                    artifact.row_count,
                    self._json(artifact.metadata),
                ],
            )
        finally:
            conn.close()

    def get_artifact_map(self, run_id: str) -> Dict[str, Dict[str, StageArtifact]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT stage_name, artifact_type, uri, row_count, content_hash, metadata_json, attempt_number
                FROM pipeline_artifact
                WHERE run_id = ?
                ORDER BY created_at, attempt_number
                """,
                [run_id],
            ).fetchall()
        finally:
            conn.close()

        artifacts: Dict[str, Dict[str, StageArtifact]] = {}
        for row in rows:
            stage_name, artifact_type, uri, row_count, content_hash, metadata_json, attempt_number = row
            artifacts.setdefault(stage_name, {})[artifact_type] = StageArtifact(
                artifact_type=artifact_type,
                uri=uri,
                row_count=row_count,
                content_hash=content_hash,
                metadata=self._loads(metadata_json),
                attempt_number=attempt_number,
            )
        return artifacts

    def get_rules_for_stage(self, stage_name: str) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT rule_id, stage_name, dataset_name, severity, rule_sql, description, owner
                FROM dq_rule
                WHERE stage_name = ? AND COALESCE(active, enabled, TRUE) = TRUE
                ORDER BY rule_id
                """,
                [stage_name],
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "rule_id": row[0],
                "stage_name": row[1],
                "dataset_name": row[2],
                "severity": row[3],
                "rule_sql": row[4],
                "description": row[5],
                "owner": row[6],
            }
            for row in rows
        ]

    def record_dq_result(
        self,
        run_id: str,
        stage_name: str,
        rule_id: str,
        severity: str,
        status: str,
        failed_count: int,
        message: str,
        sample_uri: Optional[str] = None,
    ) -> None:
        result_id = f"dq-{uuid.uuid4().hex[:12]}"
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO dq_result
                (result_id, run_id, stage_name, rule_id, severity, status, failed_count, message, sample_uri, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [result_id, run_id, stage_name, rule_id, severity, status, failed_count, message, sample_uri],
            )
        finally:
            conn.close()

    def get_successful_delivery(self, dedupe_key: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT channel, status, external_message_id, external_report_id, attempt_number, dedupe_key
                FROM publisher_delivery_log
                WHERE dedupe_key = ? AND status = 'delivered'
                ORDER BY created_at DESC, attempt_number DESC
                LIMIT 1
                """,
                [dedupe_key],
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {
            "channel": row[0],
            "status": row[1],
            "external_message_id": row[2],
            "external_report_id": row[3],
            "attempt_number": row[4],
            "dedupe_key": row[5],
        }

    def next_delivery_attempt(self, dedupe_key: str) -> int:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(attempt_number), 0) + 1
                FROM publisher_delivery_log
                WHERE dedupe_key = ?
                """,
                [dedupe_key],
            ).fetchone()
            return int(row[0]) if row else 1
        finally:
            conn.close()

    def record_delivery_log(
        self,
        run_id: str,
        stage_name: str,
        channel: str,
        artifact_uri: str,
        artifact_hash: Optional[str],
        dedupe_key: str,
        attempt_number: int,
        status: str,
        external_message_id: Optional[str] = None,
        external_report_id: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO publisher_delivery_log
                (delivery_log_id, run_id, stage_name, channel, artifact_uri, artifact_hash, dedupe_key, attempt_number,
                 status, external_message_id, external_report_id, error_message, created_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                [
                    f"delivery-{uuid.uuid4().hex[:12]}",
                    run_id,
                    stage_name,
                    channel,
                    artifact_uri,
                    artifact_hash,
                    dedupe_key,
                    attempt_number,
                    status,
                    external_message_id,
                    external_report_id,
                    error_message,
                    self._json(metadata),
                ],
            )
        finally:
            conn.close()

    def get_delivery_logs(self, run_id: str) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT channel, dedupe_key, attempt_number, status, external_message_id, external_report_id, error_message
                FROM publisher_delivery_log
                WHERE run_id = ?
                ORDER BY created_at, attempt_number
                """,
                [run_id],
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "channel": row[0],
                "dedupe_key": row[1],
                "attempt_number": row[2],
                "status": row[3],
                "external_message_id": row[4],
                "external_report_id": row[5],
                "error_message": row[6],
            }
            for row in rows
        ]

    def record_alert(
        self,
        run_id: str,
        alert_type: str,
        severity: str,
        message: str,
        stage_name: Optional[str] = None,
    ) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO pipeline_alert
                (alert_id, run_id, alert_type, severity, stage_name, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [f"alert-{uuid.uuid4().hex[:12]}", run_id, alert_type, severity, stage_name, message],
            )
        finally:
            conn.close()

    def get_alerts(self, run_id: str) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT alert_type, severity, stage_name, message
                FROM pipeline_alert
                WHERE run_id = ?
                ORDER BY created_at
                """,
                [run_id],
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "alert_type": row[0],
                "severity": row[1],
                "stage_name": row[2],
                "message": row[3],
            }
            for row in rows
        ]

    def register_model(
        self,
        model_name: str,
        model_version: str,
        artifact_uri: str,
        feature_schema_hash: str,
        train_snapshot_ref: str,
        approval_status: str = "pending",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        model_id = f"model-{uuid.uuid4().hex[:12]}"
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO model_registry
                (model_id, model_name, model_version, artifact_uri, feature_schema_hash, training_snapshot_ref,
                 approval_status, created_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                [
                    model_id,
                    model_name,
                    model_version,
                    artifact_uri,
                    feature_schema_hash,
                    train_snapshot_ref,
                    approval_status,
                    self._json(metadata),
                ],
            )
        finally:
            conn.close()
        return model_id

    def record_model_eval(
        self,
        model_id: str,
        metrics: Dict[str, float],
        dataset_ref: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> List[str]:
        eval_ids: List[str] = []
        conn = self._connect()
        try:
            for metric_name, metric_value in metrics.items():
                eval_id = f"eval-{uuid.uuid4().hex[:12]}"
                conn.execute(
                    """
                    INSERT INTO model_eval
                    (eval_id, model_id, evaluated_at, metric_name, metric_value, dataset_ref, notes)
                    VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
                    """,
                    [eval_id, model_id, metric_name, float(metric_value), dataset_ref, notes],
                )
                eval_ids.append(eval_id)
        finally:
            conn.close()
        return eval_ids

    def approve_model(self, model_id: str) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE model_registry SET approval_status = 'approved' WHERE model_id = ?",
                [model_id],
            )
        finally:
            conn.close()

    def deploy_model(
        self,
        model_id: str,
        environment: str,
        approved_by: str,
        notes: Optional[str] = None,
        deployed_at: Optional[str] = None,
    ) -> str:
        active = self.get_active_deployment(environment)
        conn = self._connect()
        try:
            approval_status = conn.execute(
                "SELECT approval_status FROM model_registry WHERE model_id = ?",
                [model_id],
            ).fetchone()
            if approval_status is None:
                raise KeyError(f"Unknown model_id: {model_id}")
            if approval_status[0] != "approved":
                raise ValueError(f"Model {model_id} is not approved for deployment")

            conn.execute(
                "UPDATE model_deployment SET status = 'superseded' WHERE environment = ? AND status = 'active'",
                [environment],
            )
            deployment_id = f"deploy-{uuid.uuid4().hex[:12]}"
            conn.execute(
                """
                INSERT INTO model_deployment
                (deployment_id, model_id, environment, status, approved_by, approved_at, deployed_at, rollback_model_id, notes)
                VALUES (?, ?, ?, 'active', ?, CURRENT_TIMESTAMP, ?, ?, ?)
                """,
                [
                    deployment_id,
                    model_id,
                    environment,
                    approved_by,
                    deployed_at or datetime.now(timezone.utc).isoformat(),
                    active["model_id"] if active else None,
                    notes,
                ],
            )
        finally:
            conn.close()
        return deployment_id

    def rollback_model_deployment(
        self,
        environment: str,
        approved_by: str,
        notes: Optional[str] = None,
    ) -> str:
        active = self.get_active_deployment(environment)
        if active is None or not active.get("rollback_model_id"):
            raise ValueError(f"No rollback target available for environment {environment}")
        return self.deploy_model(
            model_id=active["rollback_model_id"],
            environment=environment,
            approved_by=approved_by,
            notes=notes or f"Rollback from {active['deployment_id']}",
        )

    def get_active_deployment(self, environment: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT deployment_id, model_id, environment, status, rollback_model_id
                FROM model_deployment
                WHERE environment = ? AND status = 'active'
                ORDER BY deployed_at DESC NULLS LAST, approved_at DESC NULLS LAST
                LIMIT 1
                """,
                [environment],
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {
            "deployment_id": row[0],
            "model_id": row[1],
            "environment": row[2],
            "status": row[3],
            "rollback_model_id": row[4],
        }

    def get_model_record(self, model_id: str) -> Dict[str, Any]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT model_id, model_name, model_version, artifact_uri, feature_schema_hash,
                       training_snapshot_ref, approval_status, metadata_json
                FROM model_registry
                WHERE model_id = ?
                """,
                [model_id],
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise KeyError(f"Unknown model_id: {model_id}")
        return {
            "model_id": row[0],
            "model_name": row[1],
            "model_version": row[2],
            "artifact_uri": row[3],
            "feature_schema_hash": row[4],
            "train_snapshot_ref": row[5],
            "approval_status": row[6],
            "metadata": self._loads(row[7]),
        }

    def get_model_evals(self, model_id: str) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT metric_name, metric_value, dataset_ref, notes
                FROM model_eval
                WHERE model_id = ?
                ORDER BY evaluated_at, metric_name
                """,
                [model_id],
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "metric_name": row[0],
                "metric_value": row[1],
                "dataset_ref": row[2],
                "notes": row[3],
            }
            for row in rows
        ]

    def get_deployment_history(self, environment: str) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT deployment_id, model_id, environment, status, rollback_model_id, notes
                FROM model_deployment
                WHERE environment = ?
                ORDER BY approved_at, deployed_at
                """,
                [environment],
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "deployment_id": row[0],
                "model_id": row[1],
                "environment": row[2],
                "status": row[3],
                "rollback_model_id": row[4],
                "notes": row[5],
            }
            for row in rows
        ]

    def get_stage_runs(self, run_id: str) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT stage_name, attempt_number, status, error_class, error_message
                FROM pipeline_stage_run
                WHERE run_id = ?
                ORDER BY started_at, attempt_number
                """,
                [run_id],
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "stage_name": row[0],
                "attempt_number": row[1],
                "status": row[2],
                "error_class": row[3],
                "error_message": row[4],
            }
            for row in rows
        ]

    def get_run(self, run_id: str) -> Dict[str, Any]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT run_id, pipeline_name, run_date, status, current_stage, error_class, error_message, metadata_json
                FROM pipeline_run
                WHERE run_id = ?
                """,
                [run_id],
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise KeyError(f"Unknown run_id: {run_id}")
        return {
            "run_id": row[0],
            "pipeline_name": row[1],
            "run_date": row[2],
            "status": row[3],
            "current_stage": row[4],
            "error_class": row[5],
            "error_message": row[6],
            "metadata": self._loads(row[7]),
        }

    def count_rows(self, table_name: str) -> int:
        conn = self._connect()
        try:
            return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
        finally:
            conn.close()

    def _json(self, payload: Optional[Dict[str, Any]]) -> Optional[str]:
        if payload is None:
            return None
        return json.dumps(payload, sort_keys=True, default=str)

    def _loads(self, payload: Optional[str]) -> Dict[str, Any]:
        if not payload:
            return {}
        return json.loads(payload)
