"""Dry-run and repair helpers for historical control-plane timestamp drift."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.platform.db.timestamps import utc_naive_now_string

REPAIR_ID = "control_plane_timestamps_utc_v1"
IST_OFFSET_SQL = "INTERVAL '5 hours 30 minutes'"
NEGATIVE_IST_MIN_SECONDS = -20_100
NEGATIVE_IST_MAX_SECONDS = -17_000


PAIRED_LOCAL_TABLES: tuple[tuple[str, str, str], ...] = (
    ("pipeline_run", "started_at", "ended_at"),
    ("pipeline_stage_run", "started_at", "ended_at"),
)

MIXED_ENDPOINT_TABLES: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    (
        "strategy_optimization_run",
        "started_at",
        "completed_at",
        ("started_at",),
    ),
    (
        "operator_task",
        "started_at",
        "finished_at",
        ("started_at", "created_at", "updated_at"),
    ),
)

LOCAL_AUDIT_COLUMNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("pipeline_artifact", ("created_at",)),
    ("dq_result", ("created_at",)),
    ("model_registry", ("created_at",)),
    ("model_eval", ("evaluated_at",)),
    ("model_deployment", ("approved_at",)),
    ("publisher_delivery_log", ("created_at",)),
    ("pipeline_alert", ("created_at",)),
    ("dataset_registry", ("created_at",)),
    ("prediction_log", ("created_at",)),
    ("shadow_eval", ("created_at",)),
    ("drift_metric", ("measured_at",)),
    ("promotion_gate_result", ("evaluated_at",)),
    ("data_repair_run", ("created_at",)),
    ("operator_task_log", ("created_at",)),
    ("model_shadow_prediction", ("created_at",)),
    ("model_shadow_outcome", ("created_at",)),
    ("pattern_cache", ("scanned_at",)),
    ("events_enrichment_log", ("created_at",)),
    ("watchlist_candidate_history", ("created_at",)),
    ("strategy_rule_pack", ("created_at",)),
    ("strategy_iteration_result", ("created_at",)),
    ("_universe_membership", ("created_at",)),
    ("_universe_index_diagnostics", ("created_at",)),
)

INDEXES_TO_REBUILD: dict[str, tuple[tuple[str, str], ...]] = {
    "operator_task": (
        (
            "idx_operator_task_started_at",
            "CREATE INDEX IF NOT EXISTS idx_operator_task_started_at ON operator_task (started_at)",
        ),
    ),
    "data_repair_run": (
        (
            "idx_data_repair_run_created",
            "CREATE INDEX IF NOT EXISTS idx_data_repair_run_created ON data_repair_run (exchange, created_at)",
        ),
    ),
    "promotion_gate_result": (
        (
            "idx_promotion_gate_model",
            "CREATE INDEX IF NOT EXISTS idx_promotion_gate_model ON promotion_gate_result (model_id, evaluated_at, gate_name)",
        ),
    ),
}

ALL_REPAIR_INDEXES: tuple[tuple[str, str], ...] = tuple(
    index_def for table_indexes in INDEXES_TO_REBUILD.values() for index_def in table_indexes
)


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _column_exists(conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _primary_key_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    if not _table_exists(conn, table_name):
        return []
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return [str(row[1]) for row in rows if bool(row[5])]


def _ensure_marker_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS control_plane_timestamp_repair (
            repair_id TEXT PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL,
            details_json TEXT NOT NULL
        )
        """
    )


def _repair_applied(conn: duckdb.DuckDBPyConnection) -> bool:
    _ensure_marker_table(conn)
    row = conn.execute(
        "SELECT COUNT(*) FROM control_plane_timestamp_repair WHERE repair_id = ?",
        [REPAIR_ID],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _duration_stats(conn: duckdb.DuckDBPyConnection, table: str, start_col: str, end_col: str) -> dict[str, Any]:
    if not (_table_exists(conn, table) and _column_exists(conn, table, start_col) and _column_exists(conn, table, end_col)):
        return {"available": False}
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN {start_col} IS NOT NULL AND {end_col} IS NOT NULL THEN 1 ELSE 0 END) AS paired_rows,
            SUM(
                CASE
                    WHEN {start_col} IS NOT NULL
                     AND {end_col} IS NOT NULL
                     AND date_diff('second', {start_col}, {end_col}) BETWEEN ? AND ?
                    THEN 1 ELSE 0
                END
            ) AS negative_ist_rows,
            MIN(CASE WHEN {start_col} IS NOT NULL AND {end_col} IS NOT NULL THEN date_diff('second', {start_col}, {end_col}) END) AS min_duration_seconds,
            MAX(CASE WHEN {start_col} IS NOT NULL AND {end_col} IS NOT NULL THEN date_diff('second', {start_col}, {end_col}) END) AS max_duration_seconds
        FROM {table}
        """,
        [NEGATIVE_IST_MIN_SECONDS, NEGATIVE_IST_MAX_SECONDS],
    ).fetchone()
    return {
        "available": True,
        "total_rows": int(row[0] or 0),
        "paired_rows": int(row[1] or 0),
        "negative_ist_rows": int(row[2] or 0),
        "min_duration_seconds": None if row[3] is None else int(row[3]),
        "max_duration_seconds": None if row[4] is None else int(row[4]),
    }


def dry_run_control_plane_timestamp_repair(db_path: str | Path) -> dict[str, Any]:
    """Return a non-mutating summary of timestamp drift and repair scope."""
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        duration_tables: dict[str, Any] = {}
        for table, start_col, end_col in [*PAIRED_LOCAL_TABLES, *[(t, s, e) for t, s, e, _ in MIXED_ENDPOINT_TABLES]]:
            duration_tables[table] = _duration_stats(conn, table, start_col, end_col)

        audit_tables: dict[str, Any] = {}
        for table, columns in LOCAL_AUDIT_COLUMNS:
            if not _table_exists(conn, table):
                continue
            audit_tables[table] = {
                column: int(
                    conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL").fetchone()[0]
                )
                for column in columns
                if _column_exists(conn, table, column)
            }

        marker_applied = False
        if _table_exists(conn, "control_plane_timestamp_repair"):
            row = conn.execute(
                "SELECT COUNT(*) FROM control_plane_timestamp_repair WHERE repair_id = ?",
                [REPAIR_ID],
            ).fetchone()
            marker_applied = bool(row and int(row[0]) > 0)
    finally:
        conn.close()

    return {
        "repair_id": REPAIR_ID,
        "applied": marker_applied,
        "duration_tables": duration_tables,
        "audit_tables": audit_tables,
    }


def _shift_columns(conn: duckdb.DuckDBPyConnection, table: str, columns: tuple[str, ...], where_sql: str = "TRUE") -> int:
    if not _table_exists(conn, table):
        return 0
    existing_columns = [column for column in columns if _column_exists(conn, table, column)]
    if not existing_columns:
        return 0
    count = int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_sql}").fetchone()[0])
    if count <= 0:
        return 0
    table_columns = [
        row[0]
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
            """,
            [table],
        ).fetchall()
    ]
    if table == "operator_task" and "started_at" in existing_columns and _primary_key_columns(conn, table):
        expected_columns = {
            "task_id",
            "task_type",
            "label",
            "status",
            "started_at",
            "finished_at",
            "result_json",
            "error",
            "metadata_json",
            "created_at",
            "updated_at",
        }
        if expected_columns.issubset(set(table_columns)):
            conn.execute("DROP TABLE IF EXISTS operator_task_repair_new")
            conn.execute(
                """
                CREATE TABLE operator_task_repair_new (
                    task_id VARCHAR PRIMARY KEY,
                    task_type VARCHAR NOT NULL,
                    label VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    result_json VARCHAR,
                    error VARCHAR,
                    metadata_json VARCHAR,
                    created_at TIMESTAMP DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
                    updated_at TIMESTAMP DEFAULT (current_timestamp AT TIME ZONE 'UTC')
                )
                """
            )
            shifted_expr = {
                column: f"CASE WHEN {where_sql} AND {column} IS NOT NULL THEN {column} - {IST_OFFSET_SQL} ELSE {column} END"
                for column in existing_columns
            }
            conn.execute(
                f"""
                INSERT INTO operator_task_repair_new (
                    task_id, task_type, label, status, started_at, finished_at,
                    result_json, error, metadata_json, created_at, updated_at
                )
                SELECT
                    task_id, task_type, label, status,
                    {shifted_expr.get("started_at", "started_at")},
                    finished_at,
                    result_json, error, metadata_json,
                    {shifted_expr.get("created_at", "created_at")},
                    {shifted_expr.get("updated_at", "updated_at")}
                FROM operator_task
                """
            )
            conn.execute("DROP TABLE operator_task")
            conn.execute("ALTER TABLE operator_task_repair_new RENAME TO operator_task")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_operator_task_started_at ON operator_task (started_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_operator_task_status ON operator_task (status)")
            return count

    select_columns = []
    for column in table_columns:
        if column in existing_columns:
            select_columns.append(
                f"CASE WHEN {where_sql} AND {column} IS NOT NULL THEN {column} - {IST_OFFSET_SQL} ELSE {column} END AS {column}"
            )
        else:
            select_columns.append(column)
    temp_table = f"__timestamp_repair_{table.replace('.', '_')}"
    column_csv = ", ".join(table_columns)
    primary_key_columns = _primary_key_columns(conn, table)
    conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
    conn.execute(
        f"""
        CREATE TEMP TABLE {temp_table} AS
        SELECT {", ".join(select_columns)}
        FROM {table}
        WHERE {where_sql}
        """
    )
    if primary_key_columns:
        conflict_csv = ", ".join(primary_key_columns)
        update_assignments = ", ".join(f"{column} = excluded.{column}" for column in existing_columns)
        conn.execute(
            f"""
            INSERT INTO {table} ({column_csv})
            SELECT {column_csv} FROM {temp_table}
            ON CONFLICT ({conflict_csv}) DO UPDATE SET {update_assignments}
            """
        )
    else:
        assignments = ", ".join(
            f"{column} = CASE WHEN {column} IS NULL THEN NULL ELSE {column} - {IST_OFFSET_SQL} END"
            for column in existing_columns
        )
        conn.execute(f"UPDATE {table} SET {assignments} WHERE {where_sql}")
    conn.execute(f"DROP TABLE {temp_table}")
    return count


def apply_control_plane_timestamp_repair(db_path: str | Path) -> dict[str, Any]:
    """Apply the historical IST-to-UTC repair once and return before/after stats."""
    before = dry_run_control_plane_timestamp_repair(db_path)
    conn = duckdb.connect(str(db_path))
    details: dict[str, Any] = {"before": before, "shifted": {}}
    try:
        for index_name, _create_sql in ALL_REPAIR_INDEXES:
            conn.execute(f"DROP INDEX IF EXISTS {index_name}")
        conn.execute("BEGIN TRANSACTION")
        _ensure_marker_table(conn)
        repair_already_applied = _repair_applied(conn)
        residual_mixed_drift = sum(
            int(
                before.get("duration_tables", {})
                .get(table, {})
                .get("negative_ist_rows", 0)
            )
            for table, _start_col, _end_col, _local_columns in MIXED_ENDPOINT_TABLES
        )
        if repair_already_applied and residual_mixed_drift <= 0:
            details["already_applied"] = True
        else:
            if repair_already_applied:
                details["residual_repair"] = True
            # Paired pipeline timestamps are both local-source in the original
            # bug. Gate them on optimizer drift so an interrupted repair can be
            # retried without shifting those already-repaired paired columns a
            # second time.
            optimizer_drift = (
                before.get("duration_tables", {})
                .get("strategy_optimization_run", {})
                .get("negative_ist_rows", 0)
            )
            if (not repair_already_applied) and optimizer_drift:
                for table, start_col, end_col in PAIRED_LOCAL_TABLES:
                    if _table_exists(conn, table):
                        details["shifted"][table] = _shift_columns(conn, table, (start_col, end_col))

            for table, start_col, end_col, local_columns in MIXED_ENDPOINT_TABLES:
                if not (_table_exists(conn, table) and _column_exists(conn, table, start_col) and _column_exists(conn, table, end_col)):
                    continue
                where_sql = (
                    f"{start_col} IS NOT NULL AND {end_col} IS NOT NULL "
                    f"AND date_diff('second', {start_col}, {end_col}) BETWEEN "
                    f"{NEGATIVE_IST_MIN_SECONDS} AND {NEGATIVE_IST_MAX_SECONDS}"
                )
                details["shifted"][table] = _shift_columns(conn, table, local_columns, where_sql)

            if not repair_already_applied:
                for table, columns in LOCAL_AUDIT_COLUMNS:
                    if _table_exists(conn, table):
                        details["shifted"][table] = _shift_columns(conn, table, columns)

                conn.execute(
                    """
                    INSERT INTO control_plane_timestamp_repair (repair_id, applied_at, details_json)
                    VALUES (?, CAST(? AS TIMESTAMP), ?)
                    """,
                    [REPAIR_ID, utc_naive_now_string(), json.dumps(details, sort_keys=True, default=str)],
                )
            elif details.get("residual_repair"):
                conn.execute(
                    """
                    UPDATE control_plane_timestamp_repair
                    SET details_json = ?
                    WHERE repair_id = ?
                    """,
                    [json.dumps(details, sort_keys=True, default=str), REPAIR_ID],
                )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except duckdb.TransactionException:
            pass
        raise
    finally:
        for _index_name, create_sql in ALL_REPAIR_INDEXES:
            try:
                conn.execute(create_sql)
            except duckdb.Error:
                pass
        conn.close()

    return {**details, "after": dry_run_control_plane_timestamp_repair(db_path)}
