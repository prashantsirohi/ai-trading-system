from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.ui.execution_api.app import create_app


API_HEADERS = {"x-api-key": "test-key"}


def test_investigator_endpoint_returns_latest_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    registry = RegistryStore(tmp_path)
    run_id = "pipeline-2026-05-07-demo"
    registry.create_run(run_id=run_id, pipeline_name="daily_pipeline", run_date="2026-05-07", status="completed")
    attempt_dir = tmp_path / "data" / "pipeline_runs" / run_id / "investigator" / "attempt_1"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "daily_gainer_log": pd.DataFrame([{"symbol_id": "AAA", "daily_return_pct": 8.0}]),
        "investigator_scores": pd.DataFrame([{"symbol_id": "AAA", "verdict": "HIGH_CONVICTION", "final_score": 88}]),
        "repeat_tracker": pd.DataFrame(
            [
                {"symbol_id": "LOWPRI", "repeat_score": 99, "appearance_count_20d": 6, "high_priority_repeat": False},
                {"symbol_id": "AAA", "repeat_score": 60, "appearance_count_20d": 4, "high_priority_repeat": True},
            ]
        ),
        "active_watchlist": pd.DataFrame(
            [
                {"symbol_id": "WATCH", "status": "Watchlist", "verdict": "WATCH_ONLY", "score_current": 95},
                {"symbol_id": "AAA", "status": "Active Research", "verdict": "MEDIUM_CONVICTION", "score_current": 70},
            ]
        ),
        "trap_log": pd.DataFrame([{"symbol_id": "TRAP", "verdict": "NOISE_TRAP"}]),
        "archived_investigator": pd.DataFrame([{"symbol_id": "XYZ", "drop_reason": "ONE_CANDLE_DRAMA"}]),
    }
    for artifact_type, frame in files.items():
        path = attempt_dir / f"{artifact_type}.csv"
        frame.to_csv(path, index=False)
        registry.record_artifact(run_id, "investigator", 1, StageArtifact.from_file(artifact_type, path, row_count=len(frame), attempt_number=1))
    summary_path = attempt_dir / "investigator_summary.json"
    summary_path.write_text(json.dumps({"active_count": 1}), encoding="utf-8")
    registry.record_artifact(run_id, "investigator", 1, StageArtifact.from_file("investigator_summary", summary_path, row_count=1, attempt_number=1))

    response = TestClient(create_app()).get("/api/execution/investigator", headers=API_HEADERS)

    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["active_count"] == 1
    assert body["today_gainers"][0]["symbol_id"] == "AAA"
    assert body["high_conviction"][0]["symbol_id"] == "AAA"
    assert body["repeat_tracker"][0]["symbol_id"] == "AAA"
    assert body["active_watchlist"][0]["symbol_id"] == "AAA"
    assert body["archive_summary"]["by_reason"]["ONE_CANDLE_DRAMA"] == 1
