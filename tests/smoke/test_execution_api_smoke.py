from __future__ import annotations

import json
from pathlib import Path
import shutil
import sqlite3

import duckdb
from fastapi.testclient import TestClient

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.ui.execution_api.app import create_app


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "artifacts"
API_HEADERS = {"x-api-key": "test-api-key"}


def _seed_datastores(project_root: Path) -> None:
    data_dir = project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    ohlcv_db = data_dir / "ohlcv.duckdb"
    conn = duckdb.connect(str(ohlcv_db))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE _delivery (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                delivery_pct DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog VALUES
            ('AAA', 'NSE', '2026-04-10 00:00:00', 100, 105, 99, 104, 1000),
            ('BBB', 'NSE', '2026-04-10 00:00:00', 95, 99, 94, 98, 1500)
            """
        )
        conn.execute(
            """
            INSERT INTO _delivery VALUES
            ('AAA', 'NSE', '2026-04-10 00:00:00', 42.0),
            ('BBB', 'NSE', '2026-04-10 00:00:00', 38.0)
            """
        )
    finally:
        conn.close()

    master_db = data_dir / "masterdata.db"
    sqlite_conn = sqlite3.connect(master_db)
    try:
        # ``stock_details`` is the legacy shape some readers still consult.
        sqlite_conn.execute("CREATE TABLE stock_details (Symbol TEXT, exchange TEXT)")
        sqlite_conn.execute("INSERT INTO stock_details VALUES ('AAA', 'NSE')")
        sqlite_conn.execute("INSERT INTO stock_details VALUES ('BBB', 'NSE')")
        # ``symbols`` is the canonical table created by
        # ``domains.ingest.masterdata`` and queried by the
        # ``universe_alignment`` health check (see
        # ``ai_trading_system.ui.execution_api.services.readmodels.pipeline_status``). Seed it
        # to keep the catalog and master in sync so the health check returns
        # ``ok`` instead of ``error``.
        sqlite_conn.execute(
            "CREATE TABLE symbols (symbol_id TEXT PRIMARY KEY, exchange TEXT)"
        )
        sqlite_conn.execute("INSERT INTO symbols VALUES ('AAA', 'NSE')")
        sqlite_conn.execute("INSERT INTO symbols VALUES ('BBB', 'NSE')")
        sqlite_conn.commit()
    finally:
        sqlite_conn.close()


def _seed_run_artifacts(project_root: Path, run_id: str) -> None:
    rank_dir = project_root / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    execute_dir = project_root / "data" / "pipeline_runs" / run_id / "execute" / "attempt_1"
    publish_dir = project_root / "data" / "pipeline_runs" / run_id / "publish" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    execute_dir.mkdir(parents=True, exist_ok=True)
    publish_dir.mkdir(parents=True, exist_ok=True)

    for filename in (
        "ranked_signals.csv",
        "breakout_scan.csv",
        "pattern_scan.csv",
        "stock_scan.csv",
        "sector_dashboard.csv",
        "dashboard_payload.json",
    ):
        shutil.copy2(FIXTURE_ROOT / "rank" / filename, rank_dir / filename)

    for filename in (
        "trade_actions.csv",
        "executed_orders.csv",
        "executed_fills.csv",
        "positions.csv",
        "execute_summary.json",
    ):
        shutil.copy2(FIXTURE_ROOT / "execute" / filename, execute_dir / filename)

    shutil.copy2(FIXTURE_ROOT / "publish" / "publish_summary.json", publish_dir / "publish_summary.json")


def _record_rank_artifacts(registry: RegistryStore, run_id: str) -> None:
    rank_dir = registry.project_root / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    for artifact_type, filename, row_count in (
        ("ranked_signals", "ranked_signals.csv", 2),
        ("breakout_scan", "breakout_scan.csv", 1),
        ("pattern_scan", "pattern_scan.csv", 1),
        ("stock_scan", "stock_scan.csv", 1),
        ("sector_dashboard", "sector_dashboard.csv", 1),
        ("dashboard_payload", "dashboard_payload.json", 1),
    ):
        registry.record_artifact(
            run_id,
            "rank",
            1,
            StageArtifact.from_file(artifact_type, rank_dir / filename, row_count=row_count, attempt_number=1),
        )


def _seed_execution_project(project_root: Path) -> str:
    _seed_datastores(project_root)
    registry = RegistryStore(project_root)
    run_id = "pipeline-2026-04-10-smoke"
    _seed_run_artifacts(project_root, run_id)

    registry.create_run(
        run_id=run_id,
        pipeline_name="daily_pipeline",
        run_date="2026-04-10",
        status="completed",
        metadata={"requested_stages": ["ingest", "features", "rank", "execute", "publish"]},
    )
    registry.create_operator_task(
        task_id="task-smoke",
        task_type="pipeline",
        label="Smoke pipeline task",
        status="running",
        metadata={"run_id": run_id},
    )
    registry.append_operator_task_log("task-smoke", "[2026-04-10 10:00:00] Task created")
    registry.append_operator_task_log("task-smoke", "[2026-04-10 10:00:01] Task running")
    _record_rank_artifacts(registry, run_id)
    return run_id


def test_execution_api_smoke_endpoints(monkeypatch, tmp_path: Path) -> None:
    run_id = _seed_execution_project(tmp_path)
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    client = TestClient(create_app())

    health = client.get("/api/execution/health", headers=API_HEADERS)
    assert health.status_code == 200
    health_payload = health.json()
    assert health_payload["status"] in {"ok", "warn"}
    assert health_payload["summary"]["latest_ohlcv_date"] == "2026-04-10"

    ranking = client.get("/api/execution/ranking?limit=10", headers=API_HEADERS)
    assert ranking.status_code == 200
    ranking_payload = ranking.json()
    assert ranking_payload["top_ranked"][0]["symbol_id"] == "AAA"
    assert ranking_payload["artifact_count"] == 2

    workspace = client.get("/api/execution/workspace/pipeline?limit=10", headers=API_HEADERS)
    assert workspace.status_code == 200
    workspace_payload = workspace.json()
    assert workspace_payload["top_ranked"][0]["symbol_id"] == "AAA"
    assert workspace_payload["patterns"][0]["pattern_family"] == "cup_handle"
    assert workspace_payload["counts"]["breakouts"] == 1
    assert workspace_payload["ops_health"]["available"] is True
    assert "latest_validated_date" in workspace_payload["data_trust"]

    summary = client.get("/api/execution/summary", headers=API_HEADERS)
    assert summary.status_code == 200
    assert summary.json()["db_stats"]["symbols"] == 2

    runs = client.get("/api/execution/runs", headers=API_HEADERS)
    assert runs.status_code == 200
    assert runs.json()["runs"][0]["run_id"] == run_id

    tasks = client.get("/api/execution/tasks", headers=API_HEADERS)
    assert tasks.status_code == 200
    assert tasks.json()["tasks"][0]["task_id"] == "task-smoke"
    assert tasks.json()["tasks"][0]["status"] == "completed"
