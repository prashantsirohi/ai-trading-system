from __future__ import annotations

import json

from ai_trading_system.ui.execution_api.services.readmodels.latest_insight import get_latest_insight


def test_latest_insight_readmodel_exposes_artifact(tmp_path):
    insight_dir = tmp_path / "data" / "pipeline_runs" / "r1" / "insight" / "attempt_1"
    insight_dir.mkdir(parents=True)
    path = insight_dir / "daily_insight.json"
    path.write_text(json.dumps({"run_id": "r1", "report_type": "daily"}), encoding="utf-8")
    payload = get_latest_insight(tmp_path)
    assert payload["run_id"] == "r1"
    assert payload["artifact_path"] == str(path)
