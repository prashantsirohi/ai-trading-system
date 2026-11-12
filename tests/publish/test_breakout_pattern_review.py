from pathlib import Path
import json

import pandas as pd
import pytest

from ai_trading_system.domains.publish.breakout_pattern_review import build_breakout_pattern_review
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.publish import PublishStage


def test_review_includes_all_breakouts_and_orders_patterns():
    breakouts = pd.DataFrame([{"symbol_id": f"S{i}", "exchange": "NSE", "taxonomy_family": "consolidation_breakout", "review_status": "REVIEW_ONLY", "market_stage": "S4"} for i in range(30)])
    patterns = pd.DataFrame([
        {"symbol_id": "A", "exchange": "NSE", "pattern_family": "vcp", "pattern_score": 20, "evidence_origin": "carry_forward"},
        {"symbol_id": "B", "exchange": "NSE", "pattern_family": "flat_base", "pattern_score": 80, "evidence_origin": "fresh"},
    ])
    result = build_breakout_pattern_review(breakouts, patterns, session="2026-11-12")
    assert len(result) == 32
    assert result[result.Evidence == "Pattern"]["Symbol"].tolist() == ["B", "A"]
    assert result[result.Evidence == "Breakout"]["Context"].eq("S4").all()


def test_suppression_pattern_is_unscored_not_zero():
    patterns = pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "pattern_family": "head_shoulders", "pattern_score": 0, "r1a_evidence_class": "suppression_only"}])
    result = build_breakout_pattern_review(pd.DataFrame(), patterns, session="2026-11-12")
    assert pd.isna(result.iloc[0]["Score"])
    assert result.iloc[0]["Context"] == "suppression_only"


def test_publish_source_gate_requires_matching_hashes_and_success(tmp_path):
    root = tmp_path / "data" / "pipeline_runs" / "run1"
    rank = root / "rank" / "attempt_1"
    pattern = root / "pattern_lane_scan" / "attempt_1"
    rank.mkdir(parents=True)
    pattern.mkdir(parents=True)
    pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "taxonomy_family": "consolidation_breakout", "review_status": "REVIEW_ONLY", "market_stage": "S4"}]).to_csv(rank / "breakout_review_scan.csv", index=False)
    (rank / "rank_summary.json").write_text(json.dumps({"task_status": {"breakout_review_scan": {"status": "completed"}}}))
    pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "pattern_family": "flat_base", "pattern_score": 70, "as_of_date": "2026-11-12"}]).to_csv(pattern / "pattern_lane_scan.csv", index=False)
    (pattern / "pattern_lane_summary.json").write_text(json.dumps({"status": "completed"}))
    def artifact(name, path, rows=None):
        return StageArtifact.from_file(name, path, row_count=rows, attempt_number=1)
    ctx = StageContext(project_root=tmp_path, db_path=tmp_path / "data" / "ohlcv.duckdb", run_id="run1", run_date="2026-11-12", stage_name="publish", attempt_number=1,
        artifacts={"rank": {"breakout_review_scan": artifact("breakout_review_scan", rank / "breakout_review_scan.csv", 1), "rank_summary": artifact("rank_summary", rank / "rank_summary.json")},
                   "pattern_lane_scan": {"pattern_lane_scan": artifact("pattern_lane_scan", pattern / "pattern_lane_scan.csv", 1), "pattern_lane_summary": artifact("pattern_lane_summary", pattern / "pattern_lane_summary.json")}})
    out = {}
    PublishStage()._attach_breakout_pattern_review(ctx, out)
    assert out["breakout_pattern_review_ready"] is True
    assert len(out["breakout_pattern_review_rows"]) == 2
    (pattern / "pattern_lane_scan.csv").write_text("tampered")
    out = {}
    PublishStage()._attach_breakout_pattern_review(ctx, out)
    assert "breakout_pattern_review_ready" not in out
    assert "hash mismatch" in out["breakout_pattern_review_state"]["reason"]


def test_sheet_preserves_other_columns_and_rejects_manual_values(monkeypatch):
    from ai_trading_system.domains.publish.channels import google_sheets
    class Sheet:
        row_count = 1000
        def get_all_values(self): return [["old"], ["header"]]
    class Manager:
        last_error = None
        requests_attempted = 0
        rows_written = 0
        def open_spreadsheet(self): return True
        def get_or_create_sheet(self, *args, **kwargs): return sheet
        def _execute_with_backoff(self, function, **kwargs): return function()
        def update_worksheet_values(self, worksheet, values, **kwargs): writes.append((values, kwargs))
    sheet = Sheet()
    writes = []
    monkeypatch.setattr(google_sheets, "_require_spreadsheet_id", lambda: "present")
    monkeypatch.setattr(google_sheets, "GoogleSheetsManager", Manager)
    google_sheets.publish_breakout_pattern_review(pd.DataFrame(), run_id="r", run_date="2026-11-12")
    assert writes[0][1] == {"range_name": "A1:K2", "raw": True}
    sheet.get_all_values = lambda: [[""] * 11 + ["my notes"]]
    with pytest.raises(ValueError, match="operator data"):
        google_sheets.publish_breakout_pattern_review(pd.DataFrame(), run_id="r", run_date="2026-11-12")
    assert len(writes) == 1
