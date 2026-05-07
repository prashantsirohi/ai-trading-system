from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from ai_trading_system.ui.execution_api.app import create_app


API_HEADERS = {"x-api-key": "test-key"}


def test_latest_fundamentals_endpoint_returns_summary_and_rows(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    attempt_dir = tmp_path / "data" / "pipeline_runs" / "run-fund" / "fundamentals" / "attempt_1"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "status": "completed",
        "run_id": "run-fund",
        "snapshot_date": "2026-05-07",
        "stale_days": 12,
        "tier_counts": {"A": 1, "B": 2, "C": 0, "Reject": 0},
        "generated_at": "2026-05-07T12:00:00+00:00",
    }
    (attempt_dir / "fundamental_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    pd.DataFrame(
        [
            {"symbol": "BBB", "watchlist_bucket": "STUDY_ONLY", "final_watchlist_score": 70},
            {"symbol": "AAA", "watchlist_bucket": "ADD_TO_WATCHLIST", "final_watchlist_score": 80},
        ]
    ).to_csv(attempt_dir / "watchlist_candidates.csv", index=False)

    response = TestClient(create_app()).get("/api/execution/fundamentals/latest", headers=API_HEADERS)

    assert response.status_code == 200
    body = response.json()
    assert body["snapshot_date"] == "2026-05-07"
    assert body["stale_days"] == 12
    assert body["tier_counts"] == {"A": 1, "B": 2, "C": 0, "Reject": 0}
    assert body["summary"]["run_id"] == "run-fund"
    assert body["top_watchlist"][0]["symbol"] == "AAA"
    assert body["source_path"].endswith("fundamental_summary.json")
    assert body["generated_at"] == "2026-05-07T12:00:00+00:00"
