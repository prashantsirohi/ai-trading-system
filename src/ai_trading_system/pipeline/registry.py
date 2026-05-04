"""DuckDB-backed registry for pipeline runs, artifacts, DQ, and model governance."""

from __future__ import annotations

import json
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb

from ai_trading_system.platform.db.paths import canonicalize_project_root
from ai_trading_system.pipeline.contracts import StageArtifact


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
                WHERE {catalog_run_filter}
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
        "rule_sql": """
            SELECT CASE
                WHEN (
                    SELECT COUNT(*)
                    FROM _catalog
                    WHERE {catalog_run_filter}
                ) = 0
                AND (
                    SELECT COUNT(*)
                    FROM _catalog
                ) = 0
                THEN 1
                ELSE 0
            END
        """,
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
            WHERE {catalog_run_filter}
              AND (
                    symbol_id IS NULL
                 OR exchange IS NULL
                 OR timestamp IS NULL
                 OR open IS NULL
                 OR high IS NULL
                 OR low IS NULL
                 OR close IS NULL
                 OR volume IS NULL
              )
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
            WHERE {catalog_run_filter}
              AND (
                    high < GREATEST(open, close)
                 OR low > LEAST(open, close)
                 OR high < low
              )
        """,
        "description": "OHLC values must obey high/low consistency rules.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_negative_volume",
        "stage_name": "ingest",
        "dataset_name": "_catalog",
        "severity": "high",
        "rule_sql": """
            SELECT COUNT(*)
            FROM _catalog
            WHERE {catalog_run_filter}
              AND volume < 0
        """,
        "description": "Volume should not be negative.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_recent_universe_price_jump_anomaly",
        "stage_name": "ingest",
        "dataset_name": "_catalog",
        "severity": "critical",
        "rule_sql": None,
        "description": "Recent universe-wide price jumps should not show broad anomalous spikes.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_provider_coverage_low",
        "stage_name": "ingest",
        "dataset_name": "_catalog",
        "severity": "critical",
        "rule_sql": None,
        "description": "Primary provider coverage for the latest trading date is lower than expected.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_unresolved_dates_present",
        "stage_name": "ingest",
        "dataset_name": "_catalog_quarantine",
        "severity": "critical",
        "rule_sql": None,
        "description": "Ingest should not leave unresolved dates quarantined for the requested run.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_segment_distribution_drift",
        "stage_name": "ingest",
        "dataset_name": "_catalog",
        "severity": "high",
        "rule_sql": None,
        "description": "Share of non-regular (T2T/BZ) trading segments should not jump abruptly week-over-week.",
        "owner": "pipeline",
    },
    {
        "rule_id": "ingest_latest_trade_date_quarantine_clear",
        "stage_name": "ingest",
        "dataset_name": "_catalog_quarantine",
        "severity": "critical",
        "rule_sql": None,
        "description": "Ingest should repair latest-trade-date quarantine before downstream feature computation.",
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
        "rule_id": "features_trust_quarantine_clear",
        "stage_name": "features",
        "dataset_name": "_catalog_quarantine",
        "severity": "critical",
        "rule_sql": None,
        "description": "Features should not run while active quarantines exist for the current trust window.",
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
    {
        "rule_id": "rank_composite_score_range",
        "stage_name": "rank",
        "dataset_name": "ranked_signals",
        "severity": "critical",
        "rule_sql": """
            SELECT COUNT(*)
            FROM read_csv_auto({rank_artifact_uri})
            WHERE composite_score < 0 OR composite_score > 100
        """,
        "description": "Composite score must be within valid range [0, 100].",
        "owner": "pipeline",
    },
    {
        "rule_id": "rank_delivery_pct_range",
        "stage_name": "rank",
        "dataset_name": "ranked_signals",
        "severity": "high",
        "rule_sql": None,
        "description": "Delivery percentage must be within valid range [0, 100].",
        "owner": "pipeline",
    },
    {
        "rule_id": "rank_sector_coverage_threshold",
        "stage_name": "rank",
        "dataset_name": "ranked_signals",
        "severity": "medium",
        "rule_sql": None,
        "description": "Sector assignment coverage must be at least 90% of universe.",
        "owner": "pipeline",
    },
]

_REGISTRY_INIT_LOCK = threading.Lock()
_INITIALIZED_DB_PATHS: set[str] = set()


class RegistryStore:
    """Persists run metadata and governance records into DuckDB."""

    def __init__(self, project_root: Path | str, db_path: Optional[Path | str] = None):
        self.project_root = canonicalize_project_root(project_root)
        # Keep governance/control-plane metadata in a dedicated database so
        # live OHLCV writers and long-running readers do not block alerting,
        # model governance, or pipeline run tracking.
        self.db_path = Path(db_path) if db_path else self.project_root / "data" / "control_plane.duckdb"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()
        self._ensure_initialized()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    @contextmanager
    def _writer(self) -> Iterable[duckdb.DuckDBPyConnection]:
        with self._write_lock:
            conn = self._connect()
            try:
                yield conn
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except duckdb.TransactionException:
                    pass
                raise
            finally:
                conn.close()

    @contextmanager
    def _reader(self) -> Iterable[duckdb.DuckDBPyConnection]:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def _migration_files(self) -> list[Any]:
        candidate_root = self.project_root / "sql" / "migrations"
        migration_paths = sorted(candidate_root.glob("*.sql")) if candidate_root.exists() else []
        if migration_paths:
            return list(migration_paths)
        package_root = resources.files("ai_trading_system.pipeline.migrations")
        return sorted(
            (migration for migration in package_root.iterdir() if migration.name.endswith(".sql")),
            key=lambda migration: migration.name,
        )

    def _ensure_initialized(self) -> None:
        db_key = str(self.db_path.resolve())
        with _REGISTRY_INIT_LOCK:
            if db_key in _INITIALIZED_DB_PATHS:
                return
            for attempt in range(3):
                try:
                    self._apply_migrations()
                    self.seed_default_rules()
                    _INITIALIZED_DB_PATHS.add(db_key)
                    return
                except duckdb.TransactionException:
                    if attempt == 2:
                        raise
                    time.sleep(0.05 * (attempt + 1))

    def _apply_migrations(self) -> None:
        with self._writer() as conn:
            for migration_path in self._migration_files():
                conn.execute(migration_path.read_text(encoding="utf-8"))

    def seed_default_rules(self) -> None:
        with self._writer() as conn:
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

    def run_exists(self, run_id: str) -> bool:
        with self._reader() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM pipeline_run WHERE run_id = ?",
                [run_id],
            ).fetchone()
            return bool(row and row[0])

    def create_run(
        self,
        run_id: str,
        pipeline_name: str,
        run_date: str,
        trigger: str = "manual",
        status: str = "running",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._writer() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO pipeline_run
                (run_id, pipeline_name, run_date, trigger, status, started_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                [run_id, pipeline_name, run_date, trigger, status, self._json(metadata)],
            )

    def register_dataset(
        self,
        *,
        dataset_ref: str,
        dataset_uri: str,
        data_domain: str,
        engine_name: str | None = None,
        feature_schema_version: str | None = None,
        feature_schema_hash: str | None = None,
        label_version: str | None = None,
        target_column: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        horizon: int | None = None,
        row_count: int | None = None,
        symbol_count: int | None = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        with self._writer() as conn:
            existing = conn.execute(
                "SELECT dataset_id FROM dataset_registry WHERE dataset_ref = ?",
                [dataset_ref],
            ).fetchone()
            dataset_id = existing[0] if existing else f"dataset-{uuid.uuid4().hex[:12]}"
            if existing:
                conn.execute(
                    """
                    UPDATE dataset_registry
                    SET dataset_uri = ?,
                        data_domain = ?,
                        engine_name = ?,
                        feature_schema_version = ?,
                        feature_schema_hash = ?,
                        label_version = ?,
                        target_column = ?,
                        from_date = ?,
                        to_date = ?,
                        horizon = ?,
                        row_count = ?,
                        symbol_count = ?,
                        metadata_json = ?
                    WHERE dataset_ref = ?
                    """,
                    [
                        dataset_uri,
                        data_domain,
                        engine_name,
                        feature_schema_version,
                        feature_schema_hash,
                        label_version,
                        target_column,
                        from_date,
                        to_date,
                        horizon,
                        row_count,
                        symbol_count,
                        json.dumps(metadata or {}),
                        dataset_ref,
                    ],
                )
            else:
                conn.execute(
                    """
                    INSERT INTO dataset_registry
                    (dataset_id, dataset_ref, dataset_uri, data_domain, engine_name,
                     feature_schema_version, feature_schema_hash, label_version, target_column,
                     from_date, to_date, horizon, row_count, symbol_count, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        dataset_id,
                        dataset_ref,
                        dataset_uri,
                        data_domain,
                        engine_name,
                        feature_schema_version,
                        feature_schema_hash,
                        label_version,
                        target_column,
                        from_date,
                        to_date,
                        horizon,
                        row_count,
                        symbol_count,
                        json.dumps(metadata or {}),
                    ],
                )
            return dataset_id

    def get_dataset(self, dataset_ref: str) -> Optional[Dict[str, Any]]:
        with self._reader() as conn:
            row = conn.execute(
                """
                SELECT dataset_id, dataset_ref, dataset_uri, data_domain, engine_name,
                       feature_schema_version, feature_schema_hash, label_version, target_column,
                       from_date, to_date, horizon, row_count, symbol_count, created_at, metadata_json
                FROM dataset_registry
                WHERE dataset_ref = ?
                """,
                [dataset_ref],
            ).fetchone()

        if row is None:
            return None

        return {
            "dataset_id": row[0],
            "dataset_ref": row[1],
            "dataset_uri": row[2],
            "data_domain": row[3],
            "engine_name": row[4],
            "feature_schema_version": row[5],
            "feature_schema_hash": row[6],
            "label_version": row[7],
            "target_column": row[8],
            "from_date": str(row[9]) if row[9] is not None else None,
            "to_date": str(row[10]) if row[10] is not None else None,
            "horizon": row[11],
            "row_count": row[12],
            "symbol_count": row[13],
            "created_at": row[14],
            "metadata": json.loads(row[15]) if row[15] else {},
        }

    def list_datasets(
        self,
        *,
        limit: int = 50,
        data_domain: Optional[str] = None,
        engine_name: Optional[str] = None,
        horizon: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if data_domain is not None:
            clauses.append("data_domain = ?")
            params.append(data_domain)
        if engine_name is not None:
            clauses.append("engine_name = ?")
            params.append(engine_name)
        if horizon is not None:
            clauses.append("horizon = ?")
            params.append(int(horizon))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with self._reader() as conn:
            rows = conn.execute(
                f"""
                SELECT dataset_id, dataset_ref, dataset_uri, data_domain, engine_name,
                       feature_schema_version, label_version, target_column,
                       from_date, to_date, horizon, row_count, symbol_count, created_at, metadata_json
                FROM dataset_registry
                {where_sql}
                ORDER BY created_at DESC, dataset_ref DESC
                LIMIT ?
                """,
                [*params, int(limit)],
            ).fetchall()

        return [
            {
                "dataset_id": row[0],
                "dataset_ref": row[1],
                "dataset_uri": row[2],
                "data_domain": row[3],
                "engine_name": row[4],
                "feature_schema_version": row[5],
                "label_version": row[6],
                "target_column": row[7],
                "from_date": str(row[8]) if row[8] is not None else None,
                "to_date": str(row[9]) if row[9] is not None else None,
                "horizon": row[10],
                "row_count": row[11],
                "symbol_count": row[12],
                "created_at": str(row[13]) if row[13] is not None else None,
                "metadata": self._loads(row[14]),
            }
            for row in rows
        ]

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

        with self._writer() as conn:
            conn.execute(
                f"UPDATE pipeline_run SET {', '.join(assignments)} WHERE run_id = ?",
                params,
            )

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
        with self._reader() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(attempt_number), 0) + 1
                FROM pipeline_stage_run
                WHERE run_id = ? AND stage_name = ?
                """,
                [run_id, stage_name],
            ).fetchone()
            return int(row[0]) if row else 1

    def start_stage(self, run_id: str, stage_name: str, attempt_number: int) -> str:
        stage_run_id = f"{stage_name}-{attempt_number}-{uuid.uuid4().hex[:8]}"
        with self._writer() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_stage_run
                (stage_run_id, run_id, stage_name, attempt_number, status, started_at)
                VALUES (?, ?, ?, ?, 'running', CURRENT_TIMESTAMP)
                """,
                [stage_run_id, run_id, stage_name, attempt_number],
            )
        return stage_run_id

    def finish_stage(
        self,
        stage_run_id: str,
        status: str,
        error_class: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._writer() as conn:
            conn.execute(
                """
                UPDATE pipeline_stage_run
                SET status = ?, ended_at = CURRENT_TIMESTAMP,
                    error_class = ?, error_message = ?, metadata_json = ?
                WHERE stage_run_id = ?
                """,
                [status, error_class, error_message, self._json(metadata), stage_run_id],
            )

    def record_artifact(
        self,
        run_id: str,
        stage_name: str,
        attempt_number: int,
        artifact: StageArtifact,
    ) -> None:
        artifact_id = f"{stage_name}-{artifact.artifact_type}-{uuid.uuid4().hex[:10]}"
        with self._writer() as conn:
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

    def get_artifact_map(self, run_id: str) -> Dict[str, Dict[str, StageArtifact]]:
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT stage_name, artifact_type, uri, row_count, content_hash, metadata_json, attempt_number
                FROM pipeline_artifact
                WHERE run_id = ?
                ORDER BY created_at, attempt_number
                """,
                [run_id],
            ).fetchall()

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
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT rule_id, stage_name, dataset_name, severity, rule_sql, description, owner
                FROM dq_rule
                WHERE stage_name = ? AND COALESCE(active, enabled, TRUE) = TRUE
                ORDER BY rule_id
                """,
                [stage_name],
            ).fetchall()
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

    def get_latest_artifact(
        self,
        *,
        stage_name: str,
        artifact_type: str,
        limit: int = 1,
        exclude_run_id: str | None = None,
        run_status: str | None = "completed",
    ) -> List[StageArtifact]:
        clauses = [
            "a.stage_name = ?",
            "a.artifact_type = ?",
        ]
        params: List[Any] = [stage_name, artifact_type]
        if exclude_run_id is not None:
            clauses.append("a.run_id <> ?")
            params.append(exclude_run_id)
        if run_status is not None:
            clauses.append("r.status = ?")
            params.append(run_status)
        where_sql = " AND ".join(clauses)

        with self._reader() as conn:
            rows = conn.execute(
                f"""
                SELECT a.uri, a.row_count, a.content_hash, a.metadata_json, a.attempt_number
                FROM pipeline_artifact a
                JOIN pipeline_run r ON r.run_id = a.run_id
                WHERE {where_sql}
                ORDER BY r.started_at DESC, a.created_at DESC
                LIMIT ?
                """,
                [*params, int(limit)],
            ).fetchall()

        return [
            StageArtifact(
                artifact_type=artifact_type,
                uri=row[0],
                row_count=row[1],
                content_hash=row[2],
                metadata=self._loads(row[3]),
                attempt_number=row[4],
            )
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
        with self._writer() as conn:
            conn.execute(
                """
                INSERT INTO dq_result
                (result_id, run_id, stage_name, rule_id, severity, status, failed_count, message, sample_uri, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [result_id, run_id, stage_name, rule_id, severity, status, failed_count, message, sample_uri],
            )

    def get_successful_delivery(self, dedupe_key: str) -> Optional[Dict[str, Any]]:
        with self._reader() as conn:
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
        with self._reader() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(attempt_number), 0) + 1
                FROM publisher_delivery_log
                WHERE dedupe_key = ?
                """,
                [dedupe_key],
            ).fetchone()
            return int(row[0]) if row else 1

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
        with self._writer() as conn:
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

    def get_delivery_logs(self, run_id: str) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT channel, dedupe_key, attempt_number, status, external_message_id, external_report_id, error_message
                FROM publisher_delivery_log
                WHERE run_id = ?
                ORDER BY created_at, attempt_number
                """,
                [run_id],
            ).fetchall()
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
        with self._writer() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_alert
                (alert_id, run_id, alert_type, severity, stage_name, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [f"alert-{uuid.uuid4().hex[:12]}", run_id, alert_type, severity, stage_name, message],
            )

    def get_alerts(self, run_id: str) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT alert_type, severity, stage_name, message
                FROM pipeline_alert
                WHERE run_id = ?
                ORDER BY created_at
                """,
                [run_id],
            ).fetchall()
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
        with self._writer() as conn:
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
        return model_id

    def record_model_eval(
        self,
        model_id: str,
        metrics: Dict[str, float],
        dataset_ref: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> List[str]:
        eval_ids: List[str] = []
        with self._writer() as conn:
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
        return eval_ids

    def approve_model(self, model_id: str) -> None:
        with self._writer() as conn:
            conn.execute(
                "UPDATE model_registry SET approval_status = 'approved' WHERE model_id = ?",
                [model_id],
            )

    def deploy_model(
        self,
        model_id: str,
        environment: str,
        approved_by: str,
        notes: Optional[str] = None,
        deployed_at: Optional[str] = None,
    ) -> str:
        with self._writer() as conn:
            active = conn.execute(
                """
                SELECT deployment_id, model_id, environment, status, rollback_model_id
                FROM model_deployment
                WHERE environment = ? AND status = 'active'
                ORDER BY deployed_at DESC NULLS LAST, approved_at DESC NULLS LAST
                LIMIT 1
                """,
                [environment],
            ).fetchone()
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
                    active[1] if active else None,
                    notes,
                ],
            )
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
        with self._reader() as conn:
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
        with self._reader() as conn:
            row = conn.execute(
                """
                SELECT model_id, model_name, model_version, artifact_uri, feature_schema_hash,
                       training_snapshot_ref, approval_status, metadata_json
                FROM model_registry
                WHERE model_id = ?
                """,
                [model_id],
            ).fetchone()
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

    def list_models(
        self,
        *,
        limit: int = 50,
        approval_status: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if approval_status is not None:
            clauses.append("approval_status = ?")
            params.append(approval_status)
        if model_name is not None:
            clauses.append("model_name = ?")
            params.append(model_name)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with self._reader() as conn:
            rows = conn.execute(
                f"""
                SELECT model_id, model_name, model_version, artifact_uri, training_snapshot_ref,
                       approval_status, created_at, metadata_json
                FROM model_registry
                {where_sql}
                ORDER BY created_at DESC, model_name, model_version
                LIMIT ?
                """,
                [*params, int(limit)],
            ).fetchall()

        return [
            {
                "model_id": row[0],
                "model_name": row[1],
                "model_version": row[2],
                "artifact_uri": row[3],
                "train_snapshot_ref": row[4],
                "approval_status": row[5],
                "created_at": str(row[6]) if row[6] is not None else None,
                "metadata": self._loads(row[7]),
            }
            for row in rows
        ]

    def get_model_evals(self, model_id: str) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT metric_name, metric_value, dataset_ref, notes
                FROM model_eval
                WHERE model_id = ?
                ORDER BY evaluated_at, metric_name
                """,
                [model_id],
            ).fetchall()
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
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT deployment_id, model_id, environment, status, rollback_model_id, notes
                FROM model_deployment
                WHERE environment = ?
                ORDER BY approved_at, deployed_at
                """,
                [environment],
            ).fetchall()
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

    def list_deployments(
        self,
        *,
        limit: int = 50,
        environment: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if environment is not None:
            clauses.append("environment = ?")
            params.append(environment)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with self._reader() as conn:
            rows = conn.execute(
                f"""
                SELECT deployment_id, model_id, environment, status, approved_by,
                       approved_at, deployed_at, rollback_model_id, notes
                FROM model_deployment
                {where_sql}
                ORDER BY COALESCE(deployed_at, approved_at) DESC
                LIMIT ?
                """,
                [*params, int(limit)],
            ).fetchall()
        return [
            {
                "deployment_id": row[0],
                "model_id": row[1],
                "environment": row[2],
                "status": row[3],
                "approved_by": row[4],
                "approved_at": str(row[5]) if row[5] is not None else None,
                "deployed_at": str(row[6]) if row[6] is not None else None,
                "rollback_model_id": row[7],
                "notes": row[8],
            }
            for row in rows
        ]

    def get_stage_runs(self, run_id: str, *, started_after: str | None = None) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            where_sql = "WHERE run_id = ?"
            params: list[Any] = [run_id]
            if started_after is not None:
                where_sql += " AND started_at >= CAST(? AS TIMESTAMP)"
                params.append(started_after)
            rows = conn.execute(
                f"""
                SELECT stage_name, attempt_number, status, error_class, error_message, started_at, ended_at
                FROM pipeline_stage_run
                {where_sql}
                ORDER BY started_at, attempt_number
                """,
                params,
            ).fetchall()
        return [
            {
                "stage_name": row[0],
                "attempt_number": row[1],
                "status": row[2],
                "error_class": row[3],
                "error_message": row[4],
                "started_at": str(row[5]) if row[5] is not None else None,
                "ended_at": str(row[6]) if row[6] is not None else None,
            }
            for row in rows
        ]

    def get_run(self, run_id: str) -> Dict[str, Any]:
        with self._reader() as conn:
            row = conn.execute(
                """
                SELECT run_id, pipeline_name, run_date, status, current_stage, error_class, error_message, metadata_json
                FROM pipeline_run
                WHERE run_id = ?
                """,
                [run_id],
            ).fetchone()
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

    def create_operator_task(
        self,
        *,
        task_id: str,
        task_type: str,
        label: str,
        status: str = "running",
        started_at: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        with self._writer() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO operator_task
                (task_id, task_type, label, status, started_at, finished_at, result_json, error, metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, COALESCE(CAST(? AS TIMESTAMP), CURRENT_TIMESTAMP), NULL, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [
                    task_id,
                    task_type,
                    label,
                    status,
                    started_at,
                    self._json(result),
                    error,
                    self._json(metadata),
                ],
            )

    def update_operator_task(
        self,
        task_id: str,
        *,
        status: Optional[str] = None,
        finished_at: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._writer() as conn:
            existing = conn.execute(
                """
                SELECT task_id, task_type, label, status, started_at, finished_at, result_json, error, metadata_json, created_at
                FROM operator_task
                WHERE task_id = ?
                """,
                [task_id],
            ).fetchone()
            if existing is None:
                return

            next_status = status if status is not None else existing[3]
            next_finished_at = finished_at if finished_at is not None else existing[5]
            next_result_json = self._json(result) if result is not None else existing[6]
            next_error = error if error is not None else existing[7]
            next_metadata_json = self._json(metadata) if metadata is not None else existing[8]
            conn.execute("DELETE FROM operator_task WHERE task_id = ?", [task_id])
            conn.commit()
            conn.execute(
                """
                INSERT INTO operator_task
                (task_id, task_type, label, status, started_at, finished_at, result_json, error, metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    existing[0],
                    existing[1],
                    existing[2],
                    next_status,
                    existing[4],
                    next_finished_at,
                    next_result_json,
                    next_error,
                    next_metadata_json,
                    existing[9],
                ],
            )

    def append_operator_task_log(self, task_id: str, message: str) -> int:
        with self._writer() as conn:
            next_order = conn.execute(
                """
                SELECT COALESCE(MAX(log_order), 0) + 1
                FROM operator_task_log
                WHERE task_id = ?
                """,
                [task_id],
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO operator_task_log (task_id, log_order, message, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [task_id, int(next_order), message],
            )
            conn.execute(
                "UPDATE operator_task SET updated_at = CURRENT_TIMESTAMP WHERE task_id = ?",
                [task_id],
            )
            return int(next_order)

    def get_operator_task(self, task_id: str) -> Dict[str, Any]:
        with self._reader() as conn:
            row = conn.execute(
                """
                SELECT task_id, task_type, label, status, started_at, finished_at, result_json, error, metadata_json
                FROM operator_task
                WHERE task_id = ?
                """,
                [task_id],
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown task_id: {task_id}")
        return {
            "task_id": row[0],
            "task_type": row[1],
            "label": row[2],
            "status": row[3],
            "started_at": str(row[4]) if row[4] is not None else None,
            "finished_at": str(row[5]) if row[5] is not None else None,
            "result": self._loads(row[6]),
            "error": row[7],
            "metadata": self._loads(row[8]),
        }

    def list_operator_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT task_id, task_type, label, status, started_at, finished_at, result_json, error, metadata_json
                FROM operator_task
                ORDER BY started_at DESC NULLS LAST, task_id DESC
                LIMIT ?
                """,
                [int(limit)],
            ).fetchall()
        return [
            {
                "task_id": row[0],
                "task_type": row[1],
                "label": row[2],
                "status": row[3],
                "started_at": str(row[4]) if row[4] is not None else None,
                "finished_at": str(row[5]) if row[5] is not None else None,
                "result": self._loads(row[6]),
                "error": row[7],
                "metadata": self._loads(row[8]),
            }
            for row in rows
        ]

    def get_operator_task_logs(
        self,
        task_id: str,
        *,
        after: int = 0,
        limit: int = 300,
    ) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT log_order, message, created_at
                FROM operator_task_log
                WHERE task_id = ?
                  AND log_order > ?
                ORDER BY log_order
                LIMIT ?
                """,
                [task_id, int(after), int(limit)],
            ).fetchall()
        return [
            {
                "log_cursor": int(row[0]),
                "message": row[1],
                "created_at": str(row[2]) if row[2] is not None else None,
            }
            for row in rows
        ]

    def count_rows(self, table_name: str) -> int:
        with self._reader() as conn:
            return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])

    def replace_shadow_predictions(
        self,
        prediction_date: str,
        rows: List[Dict[str, Any]],
        artifact_uri: Optional[str] = None,
    ) -> int:
        with self._writer() as conn:
            conn.execute(
                "DELETE FROM model_shadow_prediction WHERE prediction_date = ?",
                [prediction_date],
            )
            inserted = 0
            for row in rows:
                prediction_id = row.get("prediction_id") or f"pred-{uuid.uuid4().hex[:12]}"
                conn.execute(
                    """
                    INSERT INTO model_shadow_prediction (
                        prediction_id, prediction_date, symbol_id, exchange, close,
                        technical_score, technical_rank, technical_top_decile,
                        ml_5d_prob, ml_5d_rank, ml_5d_top_decile,
                        ml_20d_prob, ml_20d_rank, ml_20d_top_decile,
                        blend_5d_score, blend_5d_rank, blend_5d_top_decile,
                        blend_20d_score, blend_20d_rank, blend_20d_top_decile,
                        artifact_uri, created_at, metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    """,
                    [
                        prediction_id,
                        prediction_date,
                        row["symbol_id"],
                        row.get("exchange", "NSE"),
                        row.get("close"),
                        row.get("technical_score"),
                        row.get("technical_rank"),
                        bool(row.get("technical_top_decile", False)),
                        row.get("ml_5d_prob"),
                        row.get("ml_5d_rank"),
                        bool(row.get("ml_5d_top_decile", False)),
                        row.get("ml_20d_prob"),
                        row.get("ml_20d_rank"),
                        bool(row.get("ml_20d_top_decile", False)),
                        row.get("blend_5d_score"),
                        row.get("blend_5d_rank"),
                        bool(row.get("blend_5d_top_decile", False)),
                        row.get("blend_20d_score"),
                        row.get("blend_20d_rank"),
                        bool(row.get("blend_20d_top_decile", False)),
                        artifact_uri or row.get("artifact_uri"),
                        self._json(row.get("metadata")),
                    ],
                )
                inserted += 1
        return inserted

    def replace_prediction_log(
        self,
        prediction_date: str,
        rows: List[Dict[str, Any]],
        *,
        deployment_mode: str,
        horizon: int,
        model_id: Optional[str] = None,
        artifact_uri: Optional[str] = None,
    ) -> int:
        with self._writer() as conn:
            if model_id is None:
                conn.execute(
                    """
                    DELETE FROM prediction_log
                    WHERE prediction_date = ?
                      AND deployment_mode = ?
                      AND horizon = ?
                      AND model_id IS NULL
                    """,
                    [prediction_date, deployment_mode, int(horizon)],
                )
            else:
                conn.execute(
                    """
                    DELETE FROM prediction_log
                    WHERE prediction_date = ?
                      AND deployment_mode = ?
                      AND horizon = ?
                      AND model_id = ?
                    """,
                    [prediction_date, deployment_mode, int(horizon), model_id],
                )

            inserted = 0
            for row in rows:
                prediction_log_id = row.get("prediction_log_id") or f"plog-{uuid.uuid4().hex[:12]}"
                conn.execute(
                    """
                    INSERT INTO prediction_log (
                        prediction_log_id, prediction_date, model_id, model_name, model_version,
                        deployment_mode, horizon, symbol_id, exchange,
                        score, probability, prediction, rank, artifact_uri, created_at, metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    """,
                    [
                        prediction_log_id,
                        prediction_date,
                        model_id or row.get("model_id"),
                        row.get("model_name"),
                        row.get("model_version"),
                        deployment_mode,
                        int(horizon),
                        row["symbol_id"],
                        row.get("exchange", "NSE"),
                        row.get("score"),
                        row.get("probability"),
                        row.get("prediction"),
                        row.get("rank"),
                        artifact_uri or row.get("artifact_uri"),
                        self._json(row.get("metadata")),
                    ],
                )
                inserted += 1
        return inserted

    def get_unscored_prediction_logs(
        self,
        horizon: int,
        *,
        deployment_mode: str,
        model_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            if model_id is None:
                rows = conn.execute(
                    """
                    SELECT p.prediction_log_id, p.prediction_date, p.symbol_id, p.exchange,
                           p.model_id, p.deployment_mode, p.horizon
                    FROM prediction_log p
                    LEFT JOIN shadow_eval s
                      ON s.prediction_log_id = p.prediction_log_id
                     AND s.horizon = p.horizon
                    WHERE s.prediction_log_id IS NULL
                      AND p.horizon = ?
                      AND p.deployment_mode = ?
                    ORDER BY p.prediction_date, p.symbol_id
                    """,
                    [int(horizon), deployment_mode],
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT p.prediction_log_id, p.prediction_date, p.symbol_id, p.exchange,
                           p.model_id, p.deployment_mode, p.horizon
                    FROM prediction_log p
                    LEFT JOIN shadow_eval s
                      ON s.prediction_log_id = p.prediction_log_id
                     AND s.horizon = p.horizon
                    WHERE s.prediction_log_id IS NULL
                      AND p.horizon = ?
                      AND p.deployment_mode = ?
                      AND p.model_id = ?
                    ORDER BY p.prediction_date, p.symbol_id
                    """,
                    [int(horizon), deployment_mode, model_id],
                ).fetchall()
        return [
            {
                "prediction_log_id": row[0],
                "prediction_date": str(row[1]),
                "symbol_id": row[2],
                "exchange": row[3],
                "model_id": row[4],
                "deployment_mode": row[5],
                "horizon": int(row[6]),
            }
            for row in rows
        ]

    def replace_shadow_eval(self, rows: List[Dict[str, Any]]) -> int:
        with self._writer() as conn:
            inserted = 0
            for row in rows:
                conn.execute(
                    "DELETE FROM shadow_eval WHERE prediction_log_id = ? AND horizon = ?",
                    [row["prediction_log_id"], int(row["horizon"])],
                )
                conn.execute(
                    """
                    INSERT INTO shadow_eval (
                        shadow_eval_id, prediction_log_id, prediction_date, model_id, deployment_mode,
                        horizon, symbol_id, exchange, future_date, realized_return, hit,
                        created_at, metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    """,
                    [
                        row.get("shadow_eval_id") or f"seval-{uuid.uuid4().hex[:12]}",
                        row["prediction_log_id"],
                        row["prediction_date"],
                        row.get("model_id"),
                        row["deployment_mode"],
                        int(row["horizon"]),
                        row["symbol_id"],
                        row.get("exchange", "NSE"),
                        row.get("future_date"),
                        float(row["realized_return"]),
                        bool(row["hit"]),
                        self._json(row.get("metadata")),
                    ],
                )
                inserted += 1
        return inserted

    def record_drift_metrics(self, rows: List[Dict[str, Any]]) -> int:
        with self._writer() as conn:
            inserted = 0
            for row in rows:
                drift_metric_id = row.get("drift_metric_id") or f"drift-{uuid.uuid4().hex[:12]}"
                conn.execute(
                    """
                    INSERT INTO drift_metric (
                        drift_metric_id, measured_at, prediction_date, model_id, deployment_mode,
                        horizon, metric_name, metric_value, threshold_value, status, metadata_json
                    )
                    VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        drift_metric_id,
                        row.get("prediction_date"),
                        row.get("model_id"),
                        row.get("deployment_mode"),
                        row.get("horizon"),
                        row["metric_name"],
                        float(row["metric_value"]),
                        float(row["threshold_value"]) if row.get("threshold_value") is not None else None,
                        row["status"],
                        self._json(row.get("metadata")),
                    ],
                )
                inserted += 1
        return inserted

    def get_latest_drift_metrics(
        self,
        *,
        model_id: Optional[str] = None,
        deployment_mode: Optional[str] = None,
        horizon: Optional[int] = None,
        prediction_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses = []
        params: List[Any] = []
        if model_id is not None:
            clauses.append("model_id = ?")
            params.append(model_id)
        if deployment_mode is not None:
            clauses.append("deployment_mode = ?")
            params.append(deployment_mode)
        if horizon is not None:
            clauses.append("horizon = ?")
            params.append(int(horizon))
        if prediction_date is not None:
            clauses.append("prediction_date <= CAST(? AS DATE)")
            params.append(prediction_date)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._reader() as conn:
            rows = conn.execute(
                f"""
                SELECT drift_metric_id, prediction_date, model_id, deployment_mode, horizon,
                       metric_name, metric_value, threshold_value, status, metadata_json
                FROM drift_metric
                {where_sql}
                ORDER BY measured_at DESC
                """,
                params,
            ).fetchall()
        return [
            {
                "drift_metric_id": row[0],
                "prediction_date": str(row[1]) if row[1] is not None else None,
                "model_id": row[2],
                "deployment_mode": row[3],
                "horizon": row[4],
                "metric_name": row[5],
                "metric_value": row[6],
                "threshold_value": row[7],
                "status": row[8],
                "metadata": self._loads(row[9]),
            }
            for row in rows
        ]

    def record_promotion_gate_results(self, model_id: str, rows: List[Dict[str, Any]]) -> int:
        with self._writer() as conn:
            inserted = 0
            for row in rows:
                gate_result_id = row.get("gate_result_id") or f"gate-{uuid.uuid4().hex[:12]}"
                conn.execute(
                    """
                    INSERT INTO promotion_gate_result (
                        gate_result_id, model_id, evaluated_at, gate_name, status,
                        metric_value, threshold_value, metadata_json
                    )
                    VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
                    """,
                    [
                        gate_result_id,
                        model_id,
                        row["gate_name"],
                        row["status"],
                        float(row["metric_value"]) if row.get("metric_value") is not None else None,
                        float(row["threshold_value"]) if row.get("threshold_value") is not None else None,
                        self._json(row.get("metadata")),
                    ],
                )
                inserted += 1
        return inserted

    def get_promotion_gate_results(self, model_id: str) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT gate_result_id, gate_name, status, metric_value, threshold_value, metadata_json
                FROM promotion_gate_result
                WHERE model_id = ?
                ORDER BY evaluated_at, gate_name
                """,
                [model_id],
            ).fetchall()
        return [
            {
                "gate_result_id": row[0],
                "gate_name": row[1],
                "status": row[2],
                "metric_value": row[3],
                "threshold_value": row[4],
                "metadata": self._loads(row[5]),
            }
            for row in rows
        ]

    def get_prediction_monitor_summary(
        self,
        *,
        model_id: str,
        horizon: int,
        deployment_mode: str = "shadow_ml",
        lookback_days: int = 60,
        as_of_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        as_of_sql = "COALESCE(CAST(? AS DATE), CURRENT_DATE)"
        with self._reader() as conn:
            row = conn.execute(
                f"""
                WITH scoped_predictions AS (
                    SELECT *
                    FROM prediction_log
                    WHERE model_id = ?
                      AND horizon = ?
                      AND deployment_mode = ?
                      AND prediction_date BETWEEN ({as_of_sql} - INTERVAL {int(lookback_days)} DAY) AND {as_of_sql}
                ),
                ranked AS (
                    SELECT
                        p.prediction_log_id,
                        p.prediction_date,
                        p.rank,
                        COUNT(*) OVER (PARTITION BY p.prediction_date) AS universe_count,
                        s.realized_return,
                        s.hit
                    FROM scoped_predictions p
                    LEFT JOIN shadow_eval s
                      ON s.prediction_log_id = p.prediction_log_id
                     AND s.horizon = p.horizon
                ),
                matured AS (
                    SELECT *
                    FROM ranked
                    WHERE realized_return IS NOT NULL
                ),
                top_bucket AS (
                    SELECT *
                    FROM matured
                    WHERE rank <= GREATEST(1, CAST(CEIL(universe_count * 0.1) AS INTEGER))
                )
                SELECT
                    (SELECT COUNT(*) FROM scoped_predictions) AS prediction_rows,
                    (SELECT COUNT(*) FROM matured) AS matured_rows,
                    (SELECT AVG(hit::DOUBLE) FROM matured) AS overall_hit_rate,
                    (SELECT AVG(realized_return) FROM matured) AS overall_avg_return,
                    (SELECT COUNT(*) FROM top_bucket) AS top_decile_rows,
                    (SELECT AVG(hit::DOUBLE) FROM top_bucket) AS top_decile_hit_rate,
                    (SELECT AVG(realized_return) FROM top_bucket) AS top_decile_avg_return
                """,
                [model_id, int(horizon), deployment_mode, as_of_date, as_of_date],
            ).fetchone()
        if row is None:
            return {}
        return {
            "model_id": model_id,
            "deployment_mode": deployment_mode,
            "horizon": int(horizon),
            "lookback_days": int(lookback_days),
            "prediction_rows": int(row[0] or 0),
            "matured_rows": int(row[1] or 0),
            "overall_hit_rate": float(row[2]) if row[2] is not None else None,
            "overall_avg_return": float(row[3]) if row[3] is not None else None,
            "top_decile_rows": int(row[4] or 0),
            "top_decile_hit_rate": float(row[5]) if row[5] is not None else None,
            "top_decile_avg_return": float(row[6]) if row[6] is not None else None,
        }

    def get_prediction_score_values(
        self,
        *,
        model_id: str,
        horizon: int,
        deployment_mode: str = "shadow_ml",
        prediction_date: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[float]:
        clauses = [
            "model_id = ?",
            "horizon = ?",
            "deployment_mode = ?",
        ]
        params: List[Any] = [model_id, int(horizon), deployment_mode]
        if prediction_date is not None:
            clauses.append("prediction_date = ?")
            params.append(prediction_date)
        if from_date is not None:
            clauses.append("prediction_date >= ?")
            params.append(from_date)
        if to_date is not None:
            clauses.append("prediction_date <= ?")
            params.append(to_date)
        with self._reader() as conn:
            rows = conn.execute(
                f"""
                SELECT COALESCE(probability, score)
                FROM prediction_log
                WHERE {' AND '.join(clauses)}
                  AND COALESCE(probability, score) IS NOT NULL
                ORDER BY prediction_date, rank
                """,
                params,
            ).fetchall()
        return [float(row[0]) for row in rows]

    def get_latest_shadow_prediction_date(self) -> Optional[str]:
        with self._reader() as conn:
            row = conn.execute(
                "SELECT MAX(prediction_date) FROM model_shadow_prediction"
            ).fetchone()
        return str(row[0]) if row and row[0] is not None else None

    def get_shadow_overlay(
        self,
        prediction_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            if prediction_date is None:
                prediction_date = self.get_latest_shadow_prediction_date()
            if prediction_date is None:
                return []
            limit_clause = f"LIMIT {int(limit)}" if limit else ""
            rows = conn.execute(
                f"""
                SELECT prediction_date, symbol_id, exchange, close, technical_score, technical_rank,
                       ml_5d_prob, ml_5d_rank, ml_20d_prob, ml_20d_rank,
                       blend_5d_score, blend_5d_rank, blend_20d_score, blend_20d_rank,
                       technical_top_decile, ml_5d_top_decile, ml_20d_top_decile,
                       blend_5d_top_decile, blend_20d_top_decile, artifact_uri
                FROM model_shadow_prediction
                WHERE prediction_date = ?
                ORDER BY technical_rank
                {limit_clause}
                """,
                [prediction_date],
            ).fetchall()
        return [
            {
                "prediction_date": row[0],
                "symbol_id": row[1],
                "exchange": row[2],
                "close": row[3],
                "technical_score": row[4],
                "technical_rank": row[5],
                "ml_5d_prob": row[6],
                "ml_5d_rank": row[7],
                "ml_20d_prob": row[8],
                "ml_20d_rank": row[9],
                "blend_5d_score": row[10],
                "blend_5d_rank": row[11],
                "blend_20d_score": row[12],
                "blend_20d_rank": row[13],
                "technical_top_decile": row[14],
                "ml_5d_top_decile": row[15],
                "ml_20d_top_decile": row[16],
                "blend_5d_top_decile": row[17],
                "blend_20d_top_decile": row[18],
                "artifact_uri": row[19],
            }
            for row in rows
        ]

    def replace_shadow_outcomes(self, rows: List[Dict[str, Any]]) -> int:
        with self._writer() as conn:
            inserted = 0
            for row in rows:
                conn.execute(
                    "DELETE FROM model_shadow_outcome WHERE prediction_id = ? AND horizon = ?",
                    [row["prediction_id"], int(row["horizon"])],
                )
                conn.execute(
                    """
                    INSERT INTO model_shadow_outcome (
                        outcome_id, prediction_id, prediction_date, symbol_id, exchange,
                        horizon, future_date, realized_return, hit, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    [
                        row.get("outcome_id") or f"out-{uuid.uuid4().hex[:12]}",
                        row["prediction_id"],
                        row["prediction_date"],
                        row["symbol_id"],
                        row.get("exchange", "NSE"),
                        int(row["horizon"]),
                        row.get("future_date"),
                        float(row["realized_return"]),
                        bool(row["hit"]),
                    ],
                )
                inserted += 1
        return inserted

    def get_unscored_shadow_predictions(self, horizon: int) -> List[Dict[str, Any]]:
        with self._reader() as conn:
            rows = conn.execute(
                """
                SELECT p.prediction_id, p.prediction_date, p.symbol_id, p.exchange
                FROM model_shadow_prediction p
                LEFT JOIN model_shadow_outcome o
                  ON o.prediction_id = p.prediction_id
                 AND o.horizon = ?
                WHERE o.prediction_id IS NULL
                ORDER BY p.prediction_date, p.symbol_id
                """,
                [int(horizon)],
            ).fetchall()
        return [
            {
                "prediction_id": row[0],
                "prediction_date": str(row[1]),
                "symbol_id": row[2],
                "exchange": row[3],
            }
            for row in rows
        ]

    def get_shadow_period_summary(
        self,
        *,
        grain: str,
        horizon: int,
        periods: int = 12,
    ) -> List[Dict[str, Any]]:
        if grain not in {"week", "month"}:
            raise ValueError(f"Unsupported grain: {grain}")
        if horizon not in {5, 20}:
            raise ValueError(f"Unsupported horizon: {horizon}")

        ml_flag = f"ml_{horizon}d_top_decile"
        blend_flag = f"blend_{horizon}d_top_decile"
        with self._reader() as conn:
            rows = conn.execute(
                f"""
                WITH base AS (
                    SELECT
                        DATE_TRUNC('{grain}', p.prediction_date) AS period_start,
                        o.realized_return,
                        o.hit,
                        p.technical_top_decile,
                        p.{ml_flag} AS ml_top_decile,
                        p.{blend_flag} AS blend_top_decile
                    FROM model_shadow_prediction p
                    JOIN model_shadow_outcome o
                      ON o.prediction_id = p.prediction_id
                     AND o.horizon = ?
                ),
                unioned AS (
                    SELECT period_start, 'technical' AS variant, realized_return, hit
                    FROM base WHERE technical_top_decile
                    UNION ALL
                    SELECT period_start, 'ml' AS variant, realized_return, hit
                    FROM base WHERE ml_top_decile
                    UNION ALL
                    SELECT period_start, 'blend' AS variant, realized_return, hit
                    FROM base WHERE blend_top_decile
                )
                SELECT
                    period_start,
                    variant,
                    COUNT(*) AS picks,
                    AVG(hit::DOUBLE) AS hit_rate,
                    AVG(realized_return) AS avg_return
                FROM unioned
                GROUP BY 1, 2
                ORDER BY period_start DESC, variant
                LIMIT ?
                """,
                [int(horizon), int(periods * 3)],
            ).fetchall()
        return [
            {
                "period_start": str(row[0]),
                "variant": row[1],
                "picks": int(row[2]),
                "hit_rate": float(row[3]) if row[3] is not None else None,
                "avg_return": float(row[4]) if row[4] is not None else None,
            }
            for row in rows
        ]

    def record_data_repair_run(
        self,
        *,
        repair_run_id: str,
        from_date: str,
        to_date: str,
        exchange: str,
        status: str,
        repaired_row_count: int = 0,
        unresolved_symbol_count: int = 0,
        unresolved_date_count: int = 0,
        report_uri: str | None = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._writer() as conn:
            conn.execute(
                """
                INSERT INTO data_repair_run
                (repair_run_id, from_date, to_date, exchange, status, repaired_row_count,
                 unresolved_symbol_count, unresolved_date_count, report_uri, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(repair_run_id) DO UPDATE SET
                    status = excluded.status,
                    repaired_row_count = excluded.repaired_row_count,
                    unresolved_symbol_count = excluded.unresolved_symbol_count,
                    unresolved_date_count = excluded.unresolved_date_count,
                    report_uri = excluded.report_uri,
                    metadata_json = excluded.metadata_json
                """,
                [
                    repair_run_id,
                    from_date,
                    to_date,
                    exchange,
                    status,
                    int(repaired_row_count),
                    int(unresolved_symbol_count),
                    int(unresolved_date_count),
                    report_uri,
                    self._json(metadata),
                ],
            )

    def get_latest_data_repair_run(self, exchange: str = "NSE") -> Optional[Dict[str, Any]]:
        with self._reader() as conn:
            row = conn.execute(
                """
                SELECT
                    repair_run_id,
                    created_at,
                    from_date,
                    to_date,
                    exchange,
                    status,
                    repaired_row_count,
                    unresolved_symbol_count,
                    unresolved_date_count,
                    report_uri,
                    metadata_json
                FROM data_repair_run
                WHERE exchange = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [exchange],
            ).fetchone()
        if row is None:
            return None
        return {
            "repair_run_id": row[0],
            "created_at": str(row[1]) if row[1] is not None else None,
            "from_date": str(row[2]) if row[2] is not None else None,
            "to_date": str(row[3]) if row[3] is not None else None,
            "exchange": row[4],
            "status": row[5],
            "repaired_row_count": int(row[6] or 0),
            "unresolved_symbol_count": int(row[7] or 0),
            "unresolved_date_count": int(row[8] or 0),
            "report_uri": row[9],
            "metadata": self._loads(row[10]),
        }

    def _json(self, payload: Optional[Dict[str, Any]]) -> Optional[str]:
        if payload is None:
            return None
        return json.dumps(payload, sort_keys=True, default=str)

    def _loads(self, payload: Optional[str]) -> Dict[str, Any]:
        if not payload:
            return {}
        return json.loads(payload)
