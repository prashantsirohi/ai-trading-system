from __future__ import annotations

import json
import importlib
import sqlite3
from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

from analytics.registry import RegistryStore
from core.contracts import StageArtifact
from ui.execution_api.app import create_app
from ui.services.execution_operator import retry_publish_action

API_HEADERS = {"x-api-key": "test-api-key"}


def _seed_execution_project(tmp_path: Path) -> str:
    data_dir = tmp_path / "data"
    pipeline_runs_dir = data_dir / "pipeline_runs"
    pipeline_runs_dir.mkdir(parents=True, exist_ok=True)

    ohlcv_db = data_dir / "ohlcv.duckdb"
    conn = duckdb.connect(str(ohlcv_db))
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
        ('AAA', 'NSE', '2026-04-10 00:00:00', 100, 105, 99, 104, 1000)
        """
    )
    conn.execute(
        """
        INSERT INTO _delivery VALUES
        ('AAA', 'NSE', '2026-04-10 00:00:00', 42.0)
        """
    )
    conn.close()

    master_db = data_dir / "masterdata.db"
    sqlite_conn = sqlite3.connect(master_db)
    sqlite_conn.execute("CREATE TABLE stock_details (Symbol TEXT, exchange TEXT)")
    sqlite_conn.execute("INSERT INTO stock_details VALUES ('AAA', 'NSE')")
    sqlite_conn.commit()
    sqlite_conn.close()

    registry = RegistryStore(tmp_path)
    run_id = "pipeline-2026-04-10-demo"
    registry.create_run(run_id=run_id, pipeline_name="daily_pipeline", run_date="2026-04-10", status="completed")
    registry.create_operator_task(
        task_id="task-demo",
        task_type="pipeline",
        label="Demo pipeline task",
        status="running",
        metadata={"run_id": run_id},
    )
    registry.append_operator_task_log("task-demo", "[2026-04-10 10:00:00] Task created")
    registry.append_operator_task_log("task-demo", "[2026-04-10 10:00:01] Task running")

    rank_dir = pipeline_runs_dir / run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    (rank_dir / "dashboard_payload.json").write_text(
        json.dumps({"summary": {"top_sector": "Finance", "breakout_count": 2}, "metadata": {"source": "test"}}),
        encoding="utf-8",
    )
    (rank_dir / "ranked_signals.csv").write_text(
        (
            "symbol_id,composite_score,close,stage2_score,is_stage2_uptrend,stage2_label\n"
            "AAA,88.5,104,84.0,true,stage2_uptrend\n"
            "BBB,82.0,98,62.0,false,stage1_baseline\n"
        ),
        encoding="utf-8",
    )
    (rank_dir / "breakout_scan.csv").write_text(
        "symbol_id,sector,setup_family,breakout_state\nAAA,Finance,high_52w_breakout,qualified\n",
        encoding="utf-8",
    )
    (rank_dir / "pattern_scan.csv").write_text(
        "symbol_id,pattern_family,pattern_state,pattern_score\nAAA,flag,confirmed,91\n",
        encoding="utf-8",
    )
    (rank_dir / "stock_scan.csv").write_text("symbol_id,close\nAAA,104\n", encoding="utf-8")
    (rank_dir / "sector_dashboard.csv").write_text(
        "Sector,RS_rank_pct,Quadrant\nFinance,95,Leading\n",
        encoding="utf-8",
    )
    registry.record_artifact(run_id, "rank", 1, StageArtifact.from_file("ranked_signals", rank_dir / "ranked_signals.csv", row_count=2, attempt_number=1))
    registry.record_artifact(run_id, "rank", 1, StageArtifact.from_file("breakout_scan", rank_dir / "breakout_scan.csv", row_count=1, attempt_number=1))
    registry.record_artifact(run_id, "rank", 1, StageArtifact.from_file("pattern_scan", rank_dir / "pattern_scan.csv", row_count=1, attempt_number=1))
    registry.record_artifact(run_id, "rank", 1, StageArtifact.from_file("stock_scan", rank_dir / "stock_scan.csv", row_count=1, attempt_number=1))
    registry.record_artifact(run_id, "rank", 1, StageArtifact.from_file("sector_dashboard", rank_dir / "sector_dashboard.csv", row_count=1, attempt_number=1))
    registry.record_artifact(run_id, "rank", 1, StageArtifact.from_file("dashboard_payload", rank_dir / "dashboard_payload.json", row_count=1, attempt_number=1))
    return run_id


def test_execution_api_read_endpoints(monkeypatch, tmp_path: Path) -> None:
    run_id = _seed_execution_project(tmp_path)
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    client = TestClient(create_app())

    summary = client.get("/api/execution/summary", headers=API_HEADERS)
    assert summary.status_code == 200
    assert summary.json()["db_stats"]["symbols"] == 1

    ranking = client.get("/api/execution/ranking?limit=10", headers=API_HEADERS)
    assert ranking.status_code == 200
    assert ranking.json()["top_ranked"][0]["symbol_id"] == "AAA"
    assert ranking.json()["artifact_count"] == 2
    assert ranking.json()["visible_count"] == 2
    assert ranking.json()["stage2_summary"]["uptrend_count"] == 1

    ranking_stage2 = client.get(
        "/api/execution/ranking?limit=10&stage2_only=true&stage2_min_score=70",
        headers=API_HEADERS,
    )
    assert ranking_stage2.status_code == 200
    assert ranking_stage2.json()["visible_count"] == 1
    assert ranking_stage2.json()["top_ranked"][0]["symbol_id"] == "AAA"
    assert ranking_stage2.json()["stage2_filter"]["requested"] is True

    market = client.get("/api/execution/market?limit=10", headers=API_HEADERS)
    assert market.status_code == 200
    assert market.json()["breakouts"][0]["symbol_id"] == "AAA"

    pipeline = client.get(
        "/api/execution/workspace/pipeline?limit=10&stage2_only=true&stage2_min_score=70",
        headers=API_HEADERS,
    )
    assert pipeline.status_code == 200
    pipeline_payload = pipeline.json()
    assert pipeline_payload["top_ranked"][0]["symbol_id"] == "AAA"
    assert pipeline_payload["patterns"][0]["pattern_family"] == "flag"
    assert pipeline_payload["counts"]["breakouts"] == 1
    assert pipeline_payload["counts"]["ranked"] == 2
    assert pipeline_payload["visible_counts"]["ranked"] == 1
    assert pipeline_payload["stage2_filter"]["requested"] is True
    assert pipeline_payload["stage2_summary"]["counts_by_label"]["stage2_uptrend"] == 1
    assert pipeline_payload["ops_health"]["available"] is True
    assert "data_trust" in pipeline_payload
    assert "latest_validated_date" in pipeline_payload["data_trust"]

    runs = client.get("/api/execution/runs", headers=API_HEADERS)
    assert runs.status_code == 200
    assert runs.json()["runs"][0]["run_id"] == run_id

    run_detail = client.get(f"/api/execution/runs/{run_id}", headers=API_HEADERS)
    assert run_detail.status_code == 200
    assert run_detail.json()["run"]["run_id"] == run_id

    tasks = client.get("/api/execution/tasks", headers=API_HEADERS)
    assert tasks.status_code == 200
    assert tasks.json()["tasks"][0]["task_id"] == "task-demo"
    assert tasks.json()["tasks"][0]["operator_action_type"] == "pipeline_task"
    assert tasks.json()["tasks"][0]["status"] == "completed"

    logs = client.get("/api/execution/tasks/task-demo/logs", headers=API_HEADERS)
    assert logs.status_code == 200
    assert len(logs.json()["logs"]) == 2
    assert logs.json()["task"]["run_id"] == run_id
    assert logs.json()["task"]["current_stage_label"] == "completed"


def test_execution_api_action_endpoints(monkeypatch, tmp_path: Path) -> None:
    _seed_execution_project(tmp_path)
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    client = TestClient(create_app())

    app_module = importlib.import_module("ui.execution_api.app")

    monkeypatch.setattr(
        app_module,
        "run_pipeline_action",
        lambda *args, **kwargs: {"task_id": "task-new", "status": "running", "label": kwargs["label"]},
    )
    monkeypatch.setattr(
        app_module,
        "retry_publish_action",
        lambda *args, **kwargs: {"task_id": "task-publish", "status": "running"},
    )
    monkeypatch.setattr(
        app_module,
        "terminate_process_action",
        lambda *args, **kwargs: {"ok": True, "message": "terminated"},
    )
    monkeypatch.setattr(
        app_module,
        "terminate_task_action",
        lambda *args, **kwargs: {"ok": True, "message": "task terminated"},
    )

    pipeline_resp = client.post(
        "/api/execution/pipeline/run",
        json={"label": "API full run", "stages": ["rank"]},
        headers=API_HEADERS,
    )
    assert pipeline_resp.status_code == 200
    assert pipeline_resp.json()["task"]["task_id"] == "task-new"

    publish_resp = client.post(
        "/api/execution/pipeline/publish-retry",
        json={"local_publish": True},
        headers=API_HEADERS,
    )
    assert publish_resp.status_code == 200
    assert publish_resp.json()["task"]["task_id"] == "task-publish"

    publish_resp_with_run = client.post(
        "/api/execution/pipeline/publish-retry",
        json={"local_publish": True, "run_id": "pipeline-2026-04-10-demo"},
        headers=API_HEADERS,
    )
    assert publish_resp_with_run.status_code == 200
    assert publish_resp_with_run.json()["task"]["task_id"] == "task-publish"

    terminate_resp = client.post("/api/execution/processes/123/terminate", headers=API_HEADERS)
    assert terminate_resp.status_code == 200
    assert terminate_resp.json()["ok"] is True

    terminate_task_resp = client.post("/api/execution/tasks/task-new/terminate", headers=API_HEADERS)
    assert terminate_task_resp.status_code == 200
    assert terminate_task_resp.json()["ok"] is True


def test_execution_api_requires_api_key(monkeypatch, tmp_path: Path) -> None:
    _seed_execution_project(tmp_path)
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    client = TestClient(create_app())

    unauthorized = client.get("/api/execution/summary")
    assert unauthorized.status_code == 401
    assert unauthorized.json()["detail"] == "Unauthorized"

    authorized = client.get("/api/execution/summary", headers=API_HEADERS)
    assert authorized.status_code == 200


def test_execution_api_returns_configuration_error_when_api_key_missing(monkeypatch, tmp_path: Path) -> None:
    _seed_execution_project(tmp_path)
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.delenv("EXECUTION_API_KEY", raising=False)
    client = TestClient(create_app())

    response = client.get("/api/execution/summary")

    assert response.status_code == 500
    assert response.json()["detail"] == "Execution API key is not configured"


def test_retry_publish_action_uses_latest_publishable_run(monkeypatch, tmp_path: Path) -> None:
    publishable_run_id = _seed_execution_project(tmp_path)
    registry = RegistryStore(tmp_path)
    registry.create_run(
        run_id="ui-2026-04-10-broken-retry",
        pipeline_name="daily_pipeline",
        run_date="2026-04-10",
        status="failed",
        metadata={"requested_stages": ["publish"]},
    )

    captured: dict[str, object] = {}

    def _fake_run_pipeline_action(project_root: Path, **kwargs):
        captured["project_root"] = project_root
        captured.update(kwargs)
        return {"task_id": "task-publish", **kwargs}

    monkeypatch.setattr("ui.services.execution_operator.run_pipeline_action", _fake_run_pipeline_action)

    result = retry_publish_action(tmp_path, local_publish=True)

    assert result["task_id"] == "task-publish"
    assert captured["run_id"] == publishable_run_id
    assert captured["stages"] == ["publish"]
    assert captured["params"] == {"data_domain": "operational", "preflight": False, "local_publish": True}


def test_retry_publish_action_uses_explicit_run_id(monkeypatch, tmp_path: Path) -> None:
    _seed_execution_project(tmp_path)
    captured: dict[str, object] = {}

    def _fake_run_pipeline_action(project_root: Path, **kwargs):
        captured["project_root"] = project_root
        captured.update(kwargs)
        return {"task_id": "task-publish", **kwargs}

    monkeypatch.setattr("ui.services.execution_operator.run_pipeline_action", _fake_run_pipeline_action)

    result = retry_publish_action(tmp_path, local_publish=True, run_id="pipeline-explicit-123")

    assert result["task_id"] == "task-publish"
    assert captured["run_id"] == "pipeline-explicit-123"
    assert captured["stages"] == ["publish"]
