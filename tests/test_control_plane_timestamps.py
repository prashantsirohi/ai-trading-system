from __future__ import annotations

from datetime import datetime, timedelta, timezone

import duckdb

from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.platform.db.control_plane_timestamp_repair import (
    apply_control_plane_timestamp_repair,
    dry_run_control_plane_timestamp_repair,
)
from ai_trading_system.platform.db.timestamps import utc_naive_now
from ai_trading_system.ui.execution_api.services.readmodels.pipeline_status import (
    get_execution_ops_health_snapshot,
)


def test_registry_task_timestamps_are_utc_naive_and_non_negative(tmp_path):
    store = RegistryStore(project_root=tmp_path)
    before = utc_naive_now()
    store.create_operator_task(
        task_id="task-clock",
        task_type="test",
        label="Clock test",
        started_at=None,
    )
    store.update_operator_task("task-clock", status="completed", finished_at=utc_naive_now().isoformat())
    after = utc_naive_now()

    conn = duckdb.connect(str(tmp_path / "data" / "control_plane.duckdb"), read_only=True)
    try:
        started_at, finished_at, created_at, updated_at = conn.execute(
            """
            SELECT started_at, finished_at, created_at, updated_at
            FROM operator_task
            WHERE task_id = 'task-clock'
            """
        ).fetchone()
    finally:
        conn.close()

    assert before - timedelta(seconds=5) <= started_at <= after + timedelta(seconds=5)
    assert before - timedelta(seconds=5) <= created_at <= after + timedelta(seconds=5)
    assert before - timedelta(seconds=5) <= updated_at <= after + timedelta(seconds=5)
    assert (finished_at - started_at).total_seconds() >= 0
    assert (finished_at - started_at).total_seconds() < 60


def test_pipeline_run_and_stage_timestamps_are_non_negative(tmp_path):
    store = RegistryStore(project_root=tmp_path)
    store.create_run("run-clock", "test_pipeline", "2026-05-17")
    stage_run_id = store.start_stage("run-clock", "rank", 1)
    store.finish_stage(stage_run_id, "completed")
    store.update_run("run-clock", "completed", finished=True)

    conn = duckdb.connect(str(tmp_path / "data" / "control_plane.duckdb"), read_only=True)
    try:
        run_delta = conn.execute(
            "SELECT date_diff('second', started_at, ended_at) FROM pipeline_run WHERE run_id = 'run-clock'"
        ).fetchone()[0]
        stage_delta = conn.execute(
            "SELECT date_diff('second', started_at, ended_at) FROM pipeline_stage_run WHERE stage_run_id = ?",
            [stage_run_id],
        ).fetchone()[0]
    finally:
        conn.close()

    assert run_delta >= 0
    assert run_delta < 60
    assert stage_delta >= 0
    assert stage_delta < 60


def test_ops_health_uses_utc_normalized_stage_timestamps(tmp_path):
    store = RegistryStore(project_root=tmp_path)
    db_path = tmp_path / "data" / "control_plane.duckdb"
    ended_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO pipeline_stage_run (
                stage_run_id, run_id, stage_name, attempt_number, status, started_at, ended_at
            ) VALUES ('stage-health', 'run-health', 'rank', 1, 'completed', ?, ?)
            """,
            [ended_at - timedelta(minutes=5), ended_at],
        )
    finally:
        conn.close()

    snapshot = get_execution_ops_health_snapshot(
        tmp_path,
        stale_threshold_hours={"rank": 2, "ingest": 2, "features": 2, "execute": 2, "publish": 2},
    )
    assert snapshot["stages"]["rank"]["stale"] is False
    assert 0.9 <= snapshot["stages"]["rank"]["age_hours"] <= 1.1


def test_repair_dry_run_and_apply_fix_synthetic_ist_drift(tmp_path):
    db_path = tmp_path / "control_plane.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE pipeline_run(run_id TEXT, started_at TIMESTAMP, ended_at TIMESTAMP)")
        conn.execute("CREATE TABLE pipeline_stage_run(stage_run_id TEXT, started_at TIMESTAMP, ended_at TIMESTAMP)")
        conn.execute("CREATE TABLE strategy_optimization_run(optimization_run_id TEXT, started_at TIMESTAMP, completed_at TIMESTAMP)")
        conn.execute(
            "CREATE TABLE operator_task(task_id TEXT, started_at TIMESTAMP, finished_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP)"
        )
        conn.execute("CREATE TABLE dq_result(result_id TEXT, created_at TIMESTAMP)")
        conn.execute(
            """
            INSERT INTO pipeline_run VALUES
            ('run-local', TIMESTAMP '2026-05-17 09:00:00', TIMESTAMP '2026-05-17 09:10:00')
            """
        )
        conn.execute(
            """
            INSERT INTO pipeline_stage_run VALUES
            ('stage-local', TIMESTAMP '2026-05-17 09:01:00', TIMESTAMP '2026-05-17 09:05:00')
            """
        )
        conn.execute(
            """
            INSERT INTO strategy_optimization_run VALUES
            ('opt-bad', TIMESTAMP '2026-05-17 09:00:00', TIMESTAMP '2026-05-17 03:31:00')
            """
        )
        conn.execute(
            """
            INSERT INTO operator_task VALUES
            ('task-bad', TIMESTAMP '2026-05-17 09:00:00', TIMESTAMP '2026-05-17 03:31:00',
             TIMESTAMP '2026-05-17 09:00:00', TIMESTAMP '2026-05-17 09:01:00')
            """
        )
        conn.execute("INSERT INTO dq_result VALUES ('dq-local', TIMESTAMP '2026-05-17 09:02:00')")
    finally:
        conn.close()

    dry_run = dry_run_control_plane_timestamp_repair(db_path)
    assert dry_run["duration_tables"]["strategy_optimization_run"]["negative_ist_rows"] == 1
    assert dry_run["duration_tables"]["operator_task"]["negative_ist_rows"] == 1

    applied = apply_control_plane_timestamp_repair(db_path)
    assert applied["after"]["applied"] is True
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        opt_delta = conn.execute(
            "SELECT date_diff('second', started_at, completed_at) FROM strategy_optimization_run"
        ).fetchone()[0]
        task_delta = conn.execute(
            "SELECT date_diff('second', started_at, finished_at) FROM operator_task"
        ).fetchone()[0]
        pipeline_delta = conn.execute(
            "SELECT date_diff('second', started_at, ended_at) FROM pipeline_run"
        ).fetchone()[0]
        dq_created_at = conn.execute("SELECT created_at FROM dq_result").fetchone()[0]
    finally:
        conn.close()

    assert opt_delta == 60
    assert task_delta == 60
    assert pipeline_delta == 600
    assert dq_created_at == datetime(2026, 5, 17, 3, 32)

    second_apply = apply_control_plane_timestamp_repair(db_path)
    assert second_apply["already_applied"] is True
    assert second_apply["after"]["duration_tables"]["strategy_optimization_run"]["min_duration_seconds"] == 60
