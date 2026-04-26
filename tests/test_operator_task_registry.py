from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ai_trading_system.analytics.registry import RegistryStore


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


def test_operator_task_log_writes_are_serialized(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    registry.create_operator_task(
        task_id="task-concurrent",
        task_type="pipeline",
        label="Concurrent task log test",
        status="running",
    )

    def append_log(index: int) -> int:
        return registry.append_operator_task_log("task-concurrent", f"log-{index}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        cursors = list(executor.map(append_log, range(20)))

    logs = registry.get_operator_task_logs("task-concurrent")

    assert sorted(cursors) == list(range(1, 21))
    assert [row["log_cursor"] for row in logs] == list(range(1, 21))
    assert {row["message"] for row in logs} == {f"log-{index}" for index in range(20)}
