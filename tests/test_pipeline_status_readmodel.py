from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import duckdb

from ai_trading_system.ui.execution_api.services.readmodels.pipeline_status import (
    get_execution_data_trust_snapshot,
    get_execution_ops_health_snapshot,
    get_execution_summary_read_model,
)


def _seed_master_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE symbols (symbol_id TEXT, exchange TEXT)")
        conn.executemany(
            "INSERT INTO symbols (symbol_id, exchange) VALUES (?, ?)",
            [("AAA", "NSE"), ("BBB", "NSE")],
        )
        conn.commit()
    finally:
        conn.close()


def _seed_ohlcv_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                close DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE _delivery (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP
            )
            """
        )
        conn.executemany(
            "INSERT INTO _catalog VALUES (?, ?, ?, ?)",
            [
                ("AAA", "NSE", "2026-04-21 15:30:00", 100.0),
                ("BBB", "NSE", "2026-04-21 15:30:00", 101.0),
            ],
        )
        conn.executemany(
            "INSERT INTO _delivery VALUES (?, ?, ?)",
            [
                ("AAA", "NSE", "2026-04-20 15:30:00"),
                ("BBB", "NSE", "2026-04-20 15:30:00"),
            ],
        )
    finally:
        conn.close()


def _seed_control_plane(path: Path, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE pipeline_stage_run (
                run_id VARCHAR,
                stage_name VARCHAR,
                attempt_number INTEGER,
                status VARCHAR,
                error_class VARCHAR,
                error_message VARCHAR,
                started_at TIMESTAMP,
                ended_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE dq_result (
                run_id VARCHAR,
                severity VARCHAR,
                status VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE pipeline_run (
                run_id VARCHAR,
                pipeline_name VARCHAR,
                run_date DATE,
                status VARCHAR,
                current_stage VARCHAR,
                error_class VARCHAR,
                error_message VARCHAR,
                metadata_json VARCHAR
            )
            """
        )
        conn.executemany(
            "INSERT INTO pipeline_stage_run VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    run_id,
                    "rank",
                    1,
                    "completed",
                    None,
                    None,
                    "2026-04-21 15:00:00",
                    "2026-04-21 15:10:00",
                ),
                (
                    "old-run",
                    "ingest",
                    1,
                    "completed",
                    None,
                    None,
                    "2026-04-18 09:00:00",
                    "2026-04-18 09:05:00",
                ),
            ],
        )
        conn.executemany(
            "INSERT INTO dq_result VALUES (?, ?, ?)",
            [
                (run_id, "warn", "failed"),
                (run_id, "error", "failed"),
                (run_id, "warn", "passed"),
            ],
        )
        conn.execute(
            """
            INSERT INTO pipeline_run VALUES (?, 'daily', DATE '2026-04-21', 'completed', 'rank', NULL, NULL, ?)
            """,
            [run_id, json.dumps({"params": {}})],
        )
    finally:
        conn.close()


def _seed_payload_tree(base: Path, run_id: str) -> None:
    attempt_dir = base / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    (attempt_dir / "dashboard_payload.json").write_text(
        json.dumps({"summary": {"run_id": run_id}, "warnings": []}),
        encoding="utf-8",
    )


def test_execution_ops_health_snapshot_reads_stage_staleness_and_dq(tmp_path: Path) -> None:
    run_id = "pipeline-2026-04-21-abcd1234"
    _seed_control_plane(tmp_path / "data" / "control_plane.duckdb", run_id)

    snapshot = get_execution_ops_health_snapshot(
        tmp_path,
        stale_threshold_hours={
            "ingest": 1.0,
            "features": 1.0,
            "rank": 9999.0,
            "execute": 1.0,
            "publish": 1.0,
        },
    )

    assert snapshot["available"] is True
    assert snapshot["stages"]["rank"]["run_id"] == run_id
    assert snapshot["stages"]["rank"]["stale"] is False
    assert "ingest" in snapshot["stale_stages"]
    assert snapshot["dq_summary"]["run_id"] == run_id
    assert snapshot["dq_summary"]["failed_by_severity"] == {"warn": 1, "error": 1}
    assert snapshot["dq_summary"]["total_failed"] == 2


def test_execution_summary_read_model_combines_snapshot_and_health(tmp_path: Path, monkeypatch) -> None:
    run_id = "pipeline-2026-04-21-efgh5678"
    _seed_payload_tree(tmp_path, run_id)
    _seed_control_plane(tmp_path / "data" / "control_plane.duckdb", run_id)
    _seed_ohlcv_db(tmp_path / "data" / "ohlcv.duckdb")
    _seed_master_db(tmp_path / "data" / "masterdata.db")

    monkeypatch.setattr(
        "ai_trading_system.ui.execution_api.services.readmodels.pipeline_status.load_data_trust_summary",
        lambda *_args, **_kwargs: {"status": "ok", "freshness": "good"},
    )
    monkeypatch.setattr(
        "ai_trading_system.ui.execution_api.services.readmodels.pipeline_status.RegistryStore.get_latest_data_repair_run",
        lambda *_args, **_kwargs: {"run_id": "repair-1"},
    )
    monkeypatch.setattr(
        "ai_trading_system.ui.execution_api.services.readmodels.pipeline_status.get_recent_runs",
        lambda *_args, **_kwargs: [{"run_id": run_id, "status": "completed"}],
    )

    summary = get_execution_summary_read_model(
        tmp_path,
        tasks=[{"status": "running"}, {"status": "completed"}],
    )

    assert summary["db_stats"]["symbols"] == 2
    assert summary["health"]["status"] in {"ok", "warn"}
    assert summary["latest_run"]["run_id"] == run_id
    assert summary["active_task_count"] == 1
    assert summary["task_count"] == 2
    assert summary["payload"]["summary"]["run_id"] == run_id


def test_execution_data_trust_snapshot_attaches_latest_repair_run(tmp_path: Path, monkeypatch) -> None:
    _seed_ohlcv_db(tmp_path / "data" / "ohlcv.duckdb")
    monkeypatch.setattr(
        "ai_trading_system.ui.execution_api.services.readmodels.pipeline_status.load_data_trust_summary",
        lambda *_args, **_kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        "ai_trading_system.ui.execution_api.services.readmodels.pipeline_status.RegistryStore.get_latest_data_repair_run",
        lambda *_args, **_kwargs: {"run_id": "repair-2"},
    )

    snapshot = get_execution_data_trust_snapshot(tmp_path)

    assert snapshot == {"status": "ok", "latest_repair_run": {"run_id": "repair-2"}}
