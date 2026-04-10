from __future__ import annotations

from pathlib import Path

from analytics.registry import RegistryStore


def test_operator_task_registry_roundtrip(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    registry.create_operator_task(
        task_id="task-123",
        task_type="pipeline",
        label="Smoke pipeline task",
        status="running",
        metadata={"run_id": "run-1"},
    )
    registry.append_operator_task_log("task-123", "Task created")
    registry.update_operator_task(
        "task-123",
        status="completed",
        finished_at="2026-04-10 10:00:00",
        result={"status": "completed"},
        metadata={"run_id": "run-1", "stage_count": 4},
    )

    task = registry.get_operator_task("task-123")
    logs = registry.get_operator_task_logs("task-123")
    listed = registry.list_operator_tasks()

    assert task["status"] == "completed"
    assert task["metadata"]["stage_count"] == 4
    assert logs[0]["message"] == "Task created"
    assert listed[0]["task_id"] == "task-123"
